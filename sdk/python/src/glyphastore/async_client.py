"""Asynchronous native Python client for GlyphaStore."""

from __future__ import annotations

import asyncio
import socket
import time
from collections.abc import Sequence
from typing import Final

from .client import (
    ClientConfig,
    GlyphaError,
    InternalError,
    InvalidArgument,
    MutationOutcome,
    MutationResult,
    NotFound,
    Overloaded,
    PipelineOpcode,
    PipelineOutcome,
    PipelineRequest,
    PipelineResponse,
    ProtocolError,
    TransportError,
    Unavailable,
    _enrich,
)
from .protocol import (
    MAX_FRAME_BYTES,
    RESPONSE_HEADER_BYTES,
    Opcode,
    Response,
    Status,
    decode_response,
    encode_request,
    worker_for,
)

_IDENTITY: Final = b"GlyphaStore/2"


class _SendFailure(Exception):
    def __init__(self, error: TransportError, bytes_sent: int) -> None:
        super().__init__(str(error))
        self.error = error
        self.bytes_sent = bytes_sent


def _deadline_after(seconds: float) -> float:
    return time.monotonic() + seconds


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TransportError("request deadline expired")
    return remaining


class _Connection:
    def __init__(self, worker: int) -> None:
        self.worker = worker
        self.lock = asyncio.Lock()
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.input = bytearray()
        self.input_offset = 0

    async def reset(self) -> None:
        writer, self.writer = self.writer, None
        self.reader = None
        self.input.clear()
        self.input_offset = 0
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass


class AsyncClient:
    """Asyncio client with one bound connection per Worker.

    Health is a plain boolean (no nested locks with connection locks). Incomplete
    exchanges, including task cancellation, poison the Worker connection.
    """

    def __init__(self, config: ClientConfig) -> None:
        self._validate_config(config)
        self._config = config
        self._connections: list[_Connection] = []
        self._worker_count = 0
        self._routing_epoch = 0
        self._request_id = 1
        self._request_id_lock = asyncio.Lock()
        self._healthy = True

    @classmethod
    async def connect(cls, config: ClientConfig | None = None) -> AsyncClient:
        client = cls(config or ClientConfig())
        first = _Connection(0)
        try:
            worker_count, routing_epoch = await client._bootstrap(first, None)
            client._worker_count = worker_count
            client._routing_epoch = routing_epoch
            client._connections.append(first)
            expected = (worker_count, routing_epoch)
            for worker in range(1, worker_count):
                connection = _Connection(worker)
                await client._bootstrap(connection, expected)
                client._connections.append(connection)
        except Exception:
            await client.close()
            raise
        return client

    @staticmethod
    def _validate_config(config: ClientConfig) -> None:
        if (
            not config.host
            or not 0 < config.port <= 65_535
            or config.connect_timeout <= 0
            or config.request_timeout <= 0
            or not RESPONSE_HEADER_BYTES <= config.maximum_frame_bytes <= MAX_FRAME_BYTES
            or config.maximum_pipeline_requests <= 0
            or config.maximum_pipeline_bytes < 40
        ):
            raise InvalidArgument("client configuration is outside protocol limits")

    @property
    def worker_count(self) -> int:
        return self._worker_count

    @property
    def routing_epoch(self) -> int:
        return self._routing_epoch

    @property
    def healthy(self) -> bool:
        return self._healthy

    def worker_for(self, key: bytes | bytearray | memoryview) -> int:
        if self._worker_count <= 0:
            raise Unavailable("client is not connected")
        return worker_for(bytes(key), self._worker_count)

    async def get(
        self,
        key: bytes | bytearray | memoryview,
        *,
        timeout: float | None = None,
    ) -> bytes:
        return await self._read(Opcode.GET, bytes(key), b"", timeout=timeout)

    async def ping(
        self,
        payload: bytes | bytearray | memoryview = b"",
        *,
        timeout: float | None = None,
    ) -> bytes:
        return await self._read(Opcode.PING, b"", bytes(payload), timeout=timeout)

    async def put(
        self,
        key: bytes | bytearray | memoryview,
        value: bytes | bytearray | memoryview,
        *,
        expire_at_ns: int = 0,
        timeout: float | None = None,
    ) -> MutationResult:
        return await self._mutate(
            Opcode.PUT, bytes(key), bytes(value), expire_at_ns, timeout=timeout
        )

    async def erase(
        self,
        key: bytes | bytearray | memoryview,
        *,
        timeout: float | None = None,
    ) -> MutationResult:
        return await self._mutate(Opcode.ERASE, bytes(key), b"", 0, timeout=timeout)

    async def execute_pipeline(
        self,
        requests: Sequence[PipelineRequest],
        *,
        timeout: float | None = None,
        _deadline: float | None = None,
    ) -> list[PipelineResponse]:
        if not requests:
            return []
        if not self._healthy:
            raise Unavailable("client is closed or routing metadata changed")
        if len(requests) > self._config.maximum_pipeline_requests:
            raise InvalidArgument("pipeline exceeds the configured request limit")
        deadline = _deadline if _deadline is not None else self._request_deadline(timeout)

        normalized: list[tuple[PipelineOpcode, bytes, bytes, int]] = []
        frames: list[bytes] = []
        metadata: list[tuple[int, int]] = []
        output_size = 0
        worker: int | None = None
        for request in requests:
            if not isinstance(request.opcode, PipelineOpcode):
                raise InvalidArgument("pipeline request contains an invalid opcode")
            key, value = bytes(request.key), bytes(request.value)
            request_worker = self.worker_for(key)
            if worker is None:
                worker = request_worker
            elif request_worker != worker:
                raise InvalidArgument("every pipeline key must route to the same Worker")
            if request.opcode in (PipelineOpcode.GET, PipelineOpcode.ERASE) and (
                value or request.expire_at_ns != 0
            ):
                raise InvalidArgument("GET and ERASE pipeline requests cannot carry PUT fields")
            request_id = await self._next_request_id()
            try:
                frame = encode_request(
                    request.opcode.value,
                    request_id,
                    key=key,
                    value=value,
                    expire_at_ns=request.expire_at_ns,
                )
            except ValueError as error:
                raise InvalidArgument(str(error)) from error
            if len(frame) > self._config.maximum_frame_bytes:
                raise InvalidArgument("pipeline request exceeds the configured frame limit")
            if len(frame) > self._config.maximum_pipeline_bytes - output_size:
                raise InvalidArgument("pipeline exceeds the configured aggregate byte limit")
            normalized.append((request.opcode, key, value, request.expire_at_ns))
            metadata.append((request_id, output_size))
            frames.append(frame)
            output_size += len(frame)

        assert worker is not None
        output = b"".join(frames)
        responses = [PipelineResponse(PipelineOutcome.FAILED) for _ in requests]
        connection = self._connections[worker]
        async with connection.lock:
            if not self._healthy:
                raise Unavailable("client closed before pipeline admission")
            await self._ensure_connected(connection)
            completed = False

            def mark_unresolved(first: int, error: GlyphaError, bytes_sent: int) -> None:
                for index in range(first, len(normalized)):
                    opcode = normalized[index][0]
                    mutation_may_have_arrived = (
                        opcode in (PipelineOpcode.PUT, PipelineOpcode.ERASE)
                        and bytes_sent > metadata[index][1]
                    )
                    responses[index] = PipelineResponse(
                        PipelineOutcome.INDETERMINATE
                        if mutation_may_have_arrived
                        else PipelineOutcome.FAILED,
                        error=error,
                    )

            try:
                try:
                    await self._send(connection, output, deadline)
                except _SendFailure as error:
                    await connection.reset()
                    mark_unresolved(0, error.error, error.bytes_sent)
                    completed = True
                    return responses

                for index, (opcode, _, _, _) in enumerate(normalized):
                    try:
                        response = await self._receive_response(connection, deadline)
                        self._validate_response(response, metadata[index][0], worker)
                    except (TransportError, ProtocolError, Unavailable) as error:
                        await connection.reset()
                        mark_unresolved(index, error, len(output))
                        completed = True
                        return responses
                    if response.status is Status.OK:
                        if opcode in (PipelineOpcode.PUT, PipelineOpcode.ERASE) and response.value:
                            await connection.reset()
                            mark_unresolved(
                                index,
                                ProtocolError("mutation response value must be empty"),
                                len(output),
                            )
                            completed = True
                            return responses
                        responses[index] = PipelineResponse(
                            PipelineOutcome.SUCCEEDED, value=response.value
                        )
                        continue
                    error = self._status_error(response.status)
                    responses[index] = PipelineResponse(
                        PipelineOutcome.INDETERMINATE
                        if opcode in (PipelineOpcode.PUT, PipelineOpcode.ERASE)
                        and response.status is Status.INTERNAL_ERROR
                        else PipelineOutcome.FAILED,
                        error=error,
                    )
                    if response.status in (Status.WRONG_OWNER, Status.NOT_BOUND):
                        self._healthy = False
                completed = True
                return responses
            finally:
                if not completed:
                    await connection.reset()

    async def execute_batch(
        self,
        requests: Sequence[PipelineRequest],
        *,
        timeout: float | None = None,
    ) -> list[PipelineResponse]:
        """Group by Worker, run per-Worker pipelines concurrently, restore caller order."""
        if not requests:
            return []
        if not self._healthy:
            raise Unavailable("client is closed or routing metadata changed")
        shared_deadline = self._request_deadline(timeout)

        groups: dict[int, list[tuple[int, PipelineRequest]]] = {}
        for index, request in enumerate(requests):
            if not isinstance(request.opcode, PipelineOpcode):
                raise InvalidArgument("batch request contains an invalid opcode")
            key = bytes(request.key)
            value = bytes(request.value)
            if request.opcode in (PipelineOpcode.GET, PipelineOpcode.ERASE) and (
                value or request.expire_at_ns != 0
            ):
                raise InvalidArgument("GET and ERASE batch requests cannot carry PUT fields")
            worker = self.worker_for(key)
            bucket = groups.setdefault(worker, [])
            if len(bucket) >= self._config.maximum_pipeline_requests:
                raise InvalidArgument("batch exceeds the configured per-Worker request limit")
            bucket.append((index, request))

        responses: list[PipelineResponse | None] = [None] * len(requests)

        async def run_group(
            items: list[tuple[int, PipelineRequest]],
        ) -> tuple[list[int], list[PipelineResponse]]:
            indices = [index for index, _ in items]
            group_requests = [request for _, request in items]
            return indices, await self.execute_pipeline(group_requests, _deadline=shared_deadline)

        results = await asyncio.gather(*(run_group(items) for items in groups.values()))
        for indices, group_responses in results:
            for index, response in zip(indices, group_responses, strict=True):
                responses[index] = response

        return [
            response if response is not None else PipelineResponse(PipelineOutcome.FAILED)
            for response in responses
        ]

    async def close(self) -> None:
        self._healthy = False
        for connection in self._connections:
            async with connection.lock:
                await connection.reset()

    async def __aenter__(self) -> AsyncClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def _next_request_id(self) -> int:
        async with self._request_id_lock:
            current = self._request_id
            self._request_id = 1 if current == 0xFFFF_FFFF_FFFF_FFFF else current + 1
            return current

    async def _open_stream(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        try:
            return await asyncio.wait_for(
                asyncio.open_connection(self._config.host, self._config.port),
                timeout=self._config.connect_timeout,
            )
        except (OSError, asyncio.TimeoutError) as error:
            raise Unavailable(f"could not connect to GlyphaStore: {error}") from error

    async def _bootstrap(
        self, connection: _Connection, expected: tuple[int, int] | None
    ) -> tuple[int, int]:
        await connection.reset()
        connection.reader, connection.writer = await self._open_stream()
        deadline = _deadline_after(self._config.request_timeout)
        try:
            sock = connection.writer.get_extra_info("socket")
            if sock is not None:
                try:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                except OSError:
                    pass
            init_id = await self._next_request_id()
            response = await self._exchange(
                connection, encode_request(Opcode.INIT, init_id), deadline
            )
            if (
                response.status is not Status.OK
                or response.request_id != init_id
                or response.value != _IDENTITY
                or not 0 < response.worker_count <= 256
                or response.routing_epoch == 0
            ):
                raise ProtocolError("server INIT response is inconsistent")
            metadata = (response.worker_count, response.routing_epoch)
            if expected is not None and metadata != expected:
                raise Unavailable("server routing metadata changed during bootstrap")
            bind_id = await self._next_request_id()
            bound = await self._exchange(
                connection,
                encode_request(
                    Opcode.BIND_WORKER,
                    bind_id,
                    target_worker=connection.worker,
                ),
                deadline,
            )
            if (
                bound.status is not Status.OK
                or bound.request_id != bind_id
                or bound.owner_worker != connection.worker
                or (bound.worker_count, bound.routing_epoch) != metadata
            ):
                raise ProtocolError("server BIND_WORKER response is inconsistent")
            return metadata
        except _SendFailure as error:
            await connection.reset()
            raise Unavailable(str(error.error)) from error
        except TransportError as error:
            await connection.reset()
            raise Unavailable(str(error)) from error
        except Exception:
            await connection.reset()
            raise

    async def _ensure_connected(self, connection: _Connection) -> None:
        if connection.writer is None or connection.reader is None:
            await self._bootstrap(connection, (self._worker_count, self._routing_epoch))

    async def _send(self, connection: _Connection, frame: bytes, deadline: float) -> None:
        assert connection.writer is not None
        try:
            connection.writer.write(frame)
        except OSError as error:
            raise _SendFailure(TransportError(f"request send failed: {error}"), 0) from error
        try:
            await asyncio.wait_for(
                connection.writer.drain(), timeout=_remaining(deadline)
            )
        except (OSError, asyncio.TimeoutError, TransportError) as error:
            wrapped = (
                error
                if isinstance(error, TransportError)
                else TransportError(f"request send failed: {error}")
            )
            # write() handed the frame to the transport buffer; treat as may-have-sent.
            raise _SendFailure(wrapped, len(frame)) from error

    async def _receive_response(self, connection: _Connection, deadline: float) -> Response:
        assert connection.reader is not None
        while True:
            available = len(connection.input) - connection.input_offset
            if available >= 4:
                frame_size = int.from_bytes(
                    connection.input[connection.input_offset : connection.input_offset + 4],
                    "little",
                )
                if not RESPONSE_HEADER_BYTES <= frame_size <= self._config.maximum_frame_bytes:
                    raise ProtocolError("server response size is outside client limits")
                if available >= frame_size:
                    start = connection.input_offset
                    encoded = memoryview(connection.input)[start : start + frame_size]
                    try:
                        response = decode_response(encoded, self._config.maximum_frame_bytes)
                    except ValueError as error:
                        raise ProtocolError(str(error)) from error
                    finally:
                        encoded.release()
                    connection.input_offset += frame_size
                    if connection.input_offset == len(connection.input):
                        connection.input.clear()
                        connection.input_offset = 0
                    return response
            if connection.input_offset:
                del connection.input[: connection.input_offset]
                connection.input_offset = 0
            try:
                chunk = await asyncio.wait_for(
                    connection.reader.read(64 * 1024), timeout=_remaining(deadline)
                )
                if not chunk:
                    raise OSError("server closed the connection")
                connection.input.extend(chunk)
            except (OSError, asyncio.TimeoutError, TransportError) as error:
                if isinstance(error, TransportError):
                    raise
                raise TransportError(f"response receive failed: {error}") from error

    async def _exchange(
        self, connection: _Connection, frame: bytes, deadline: float
    ) -> Response:
        await self._send(connection, frame, deadline)
        return await self._receive_response(connection, deadline)

    def _validate_response(self, response: Response, request_id: int, worker: int) -> None:
        if response.request_id != request_id:
            raise ProtocolError("server response request ID does not match")
        if (
            response.worker_count != self._worker_count
            or response.routing_epoch != self._routing_epoch
        ):
            self._healthy = False
            raise Unavailable("server routing metadata changed")
        if response.owner_worker != worker and response.status is not Status.WRONG_OWNER:
            self._healthy = False
            raise ProtocolError("server response came from the wrong Worker")

    @staticmethod
    def _status_error(status: Status) -> GlyphaError:
        if status is Status.NOT_FOUND:
            error: GlyphaError = NotFound("key was not found")
        elif status is Status.OVERLOADED:
            error = Overloaded("server is overloaded")
        elif status is Status.NOT_BOUND:
            error = Unavailable("server connection is not bound")
        elif status is Status.WRONG_OWNER:
            error = ProtocolError("server rejected Worker routing")
        elif status in (Status.INVALID_REQUEST, Status.UNSUPPORTED):
            error = InvalidArgument("server rejected the request")
        else:
            error = InternalError("server reported an internal error")
        error.wire_status = int(status)
        return error

    def _request_deadline(self, timeout: float | None) -> float:
        if timeout is not None and timeout <= 0:
            raise InvalidArgument("request timeout must be positive")
        seconds = self._config.request_timeout if timeout is None else timeout
        return _deadline_after(seconds)

    async def _read(
        self,
        opcode: Opcode,
        key: bytes,
        value: bytes,
        *,
        timeout: float | None = None,
    ) -> bytes:
        if not self._healthy:
            raise Unavailable("client is closed or routing metadata changed")
        deadline = self._request_deadline(timeout)
        worker = 0 if opcode is Opcode.PING else self.worker_for(key)
        connection = self._connections[worker]
        async with connection.lock:
            if not self._healthy:
                raise Unavailable("client closed before read admission")
            last_error: GlyphaError = Unavailable("request was not attempted")
            for _ in range(2):
                exchange_started = False
                try:
                    await self._ensure_connected(connection)
                    request_id = await self._next_request_id()
                    try:
                        frame = encode_request(opcode, request_id, key=key, value=value)
                    except ValueError as error:
                        raise InvalidArgument(str(error)) from error
                    if len(frame) > self._config.maximum_frame_bytes:
                        raise InvalidArgument("request exceeds the configured frame limit")
                    exchange_started = True
                    response = await self._exchange(connection, frame, deadline)
                    self._validate_response(response, request_id, worker)
                    if response.status is not Status.OK:
                        if response.status in (Status.WRONG_OWNER, Status.NOT_BOUND):
                            self._healthy = False
                        raise self._status_error(response.status)
                    return response.value
                except (TransportError, _SendFailure) as error:
                    last_error = error.error if isinstance(error, _SendFailure) else error
                    await connection.reset()
                except Unavailable as error:
                    await connection.reset()
                    if not self._healthy:
                        raise
                    last_error = error
                except ProtocolError:
                    await connection.reset()
                    raise
                except BaseException:
                    if exchange_started:
                        await connection.reset()
                    raise
            raise last_error

    async def _mutate(
        self,
        opcode: Opcode,
        key: bytes,
        value: bytes,
        expire_at_ns: int,
        *,
        timeout: float | None = None,
    ) -> MutationResult:
        op = "put" if opcode is Opcode.PUT else "erase"
        if not self._healthy:
            return MutationResult(
                MutationOutcome.REJECTED,
                _enrich(
                    Unavailable("client is closed or routing metadata changed"),
                    operation=op,
                    mutation_outcome=MutationOutcome.REJECTED,
                ),
            )
        try:
            deadline = self._request_deadline(timeout)
        except InvalidArgument as error:
            return MutationResult(
                MutationOutcome.REJECTED,
                _enrich(error, operation=op, mutation_outcome=MutationOutcome.REJECTED),
            )
        worker = self.worker_for(key)
        connection = self._connections[worker]
        async with connection.lock:
            if not self._healthy:
                return MutationResult(
                    MutationOutcome.REJECTED,
                    _enrich(
                        Unavailable("client closed before mutation admission"),
                        operation=op,
                        worker=worker,
                        routing_epoch=self._routing_epoch,
                        mutation_outcome=MutationOutcome.REJECTED,
                    ),
                )
            for attempt in range(2):
                exchange_started = False
                request_id = 0
                try:
                    await self._ensure_connected(connection)
                except GlyphaError as error:
                    return MutationResult(
                        MutationOutcome.REJECTED,
                        _enrich(
                            error,
                            operation=op,
                            worker=worker,
                            routing_epoch=self._routing_epoch,
                            mutation_outcome=MutationOutcome.REJECTED,
                        ),
                    )
                try:
                    request_id = await self._next_request_id()
                    try:
                        frame = encode_request(
                            opcode,
                            request_id,
                            key=key,
                            value=value,
                            expire_at_ns=expire_at_ns,
                        )
                    except ValueError as error:
                        return MutationResult(
                            MutationOutcome.REJECTED,
                            _enrich(
                                InvalidArgument(str(error)),
                                operation=op,
                                request_id=request_id,
                                worker=worker,
                                routing_epoch=self._routing_epoch,
                                mutation_outcome=MutationOutcome.REJECTED,
                            ),
                        )
                    if len(frame) > self._config.maximum_frame_bytes:
                        return MutationResult(
                            MutationOutcome.REJECTED,
                            _enrich(
                                InvalidArgument("request exceeds the configured frame limit"),
                                operation=op,
                                request_id=request_id,
                                worker=worker,
                                routing_epoch=self._routing_epoch,
                                mutation_outcome=MutationOutcome.REJECTED,
                            ),
                        )
                    exchange_started = True
                    response = await self._exchange(connection, frame, deadline)
                    self._validate_response(response, request_id, worker)
                except _SendFailure as error:
                    await connection.reset()
                    outcome = (
                        MutationOutcome.REJECTED
                        if error.bytes_sent == 0
                        else MutationOutcome.INDETERMINATE
                    )
                    promoted = _enrich(
                        error.error,
                        operation=op,
                        request_id=request_id,
                        worker=worker,
                        routing_epoch=self._routing_epoch,
                        bytes_sent=error.bytes_sent,
                        mutation_outcome=outcome,
                    )
                    if error.bytes_sent == 0:
                        if attempt == 0:
                            continue
                        return MutationResult(MutationOutcome.REJECTED, promoted)
                    return MutationResult(MutationOutcome.INDETERMINATE, promoted)
                except (TransportError, ProtocolError, Unavailable) as error:
                    await connection.reset()
                    return MutationResult(
                        MutationOutcome.INDETERMINATE,
                        _enrich(
                            error,
                            operation=op,
                            request_id=request_id,
                            worker=worker,
                            routing_epoch=self._routing_epoch,
                            mutation_outcome=MutationOutcome.INDETERMINATE,
                        ),
                    )
                except BaseException:
                    if exchange_started:
                        await connection.reset()
                    raise
                if response.status is Status.OK:
                    if response.value:
                        await connection.reset()
                        return MutationResult(
                            MutationOutcome.INDETERMINATE,
                            _enrich(
                                ProtocolError("mutation response value must be empty"),
                                operation=op,
                                request_id=request_id,
                                worker=worker,
                                routing_epoch=self._routing_epoch,
                                mutation_outcome=MutationOutcome.INDETERMINATE,
                            ),
                        )
                    return MutationResult(MutationOutcome.COMMITTED)
                error = _enrich(
                    self._status_error(response.status),
                    operation=op,
                    request_id=request_id,
                    worker=worker,
                    routing_epoch=self._routing_epoch,
                    wire_status=int(response.status),
                )
                if response.status is Status.INTERNAL_ERROR:
                    return MutationResult(
                        MutationOutcome.INDETERMINATE,
                        _enrich(error, mutation_outcome=MutationOutcome.INDETERMINATE),
                    )
                if response.status in (Status.WRONG_OWNER, Status.NOT_BOUND):
                    self._healthy = False
                return MutationResult(
                    MutationOutcome.REJECTED,
                    _enrich(error, mutation_outcome=MutationOutcome.REJECTED),
                )
            return MutationResult(
                MutationOutcome.REJECTED,
                _enrich(
                    Unavailable("could not send mutation"),
                    operation=op,
                    worker=worker,
                    routing_epoch=self._routing_epoch,
                    mutation_outcome=MutationOutcome.REJECTED,
                ),
            )

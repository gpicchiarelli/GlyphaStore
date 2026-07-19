"""Synchronous native Python client for GlyphaStore."""

from __future__ import annotations

import socket
import threading
from dataclasses import dataclass
from enum import Enum
from typing import Final

from .protocol import (
    MAX_FRAME_BYTES,
    NO_WORKER,
    RESPONSE_HEADER_BYTES,
    Opcode,
    Response,
    Status,
    decode_response,
    encode_request,
    worker_for,
)

_IDENTITY: Final = b"GlyphaStore/2"


class GlyphaError(Exception):
    """Base class for Python client failures."""


class InvalidArgument(GlyphaError):
    pass


class ProtocolError(GlyphaError):
    pass


class TransportError(GlyphaError):
    pass


class NotFound(GlyphaError):
    pass


class Overloaded(GlyphaError):
    pass


class Unavailable(GlyphaError):
    pass


class MutationOutcome(Enum):
    COMMITTED = "committed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"


@dataclass(frozen=True, slots=True)
class MutationResult:
    outcome: MutationOutcome
    error: GlyphaError | None = None

    @property
    def committed(self) -> bool:
        return self.outcome is MutationOutcome.COMMITTED


@dataclass(frozen=True, slots=True)
class ClientConfig:
    host: str = "127.0.0.1"
    port: int = 7379
    connect_timeout: float = 3.0
    request_timeout: float = 5.0
    maximum_frame_bytes: int = MAX_FRAME_BYTES


class _SendFailure(Exception):
    def __init__(self, error: TransportError, bytes_sent: int) -> None:
        super().__init__(str(error))
        self.error = error
        self.bytes_sent = bytes_sent


class _Connection:
    def __init__(self, worker: int) -> None:
        self.worker = worker
        self.lock = threading.Lock()
        self.socket: socket.socket | None = None

    def reset(self) -> None:
        current, self.socket = self.socket, None
        if current is not None:
            try:
                current.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            current.close()


class Client:
    """Thread-safe synchronous client with one bound connection per Worker."""

    def __init__(self, config: ClientConfig) -> None:
        self._validate_config(config)
        self._config = config
        self._connections: list[_Connection] = []
        self._worker_count = 0
        self._routing_epoch = 0
        self._request_id = 1
        self._request_id_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._healthy = True

    @classmethod
    def connect(cls, config: ClientConfig | None = None) -> Client:
        client = cls(config or ClientConfig())
        first = _Connection(0)
        try:
            worker_count, routing_epoch = client._bootstrap(first, None)
            client._worker_count = worker_count
            client._routing_epoch = routing_epoch
            client._connections.append(first)
            expected = (worker_count, routing_epoch)
            for worker in range(1, worker_count):
                connection = _Connection(worker)
                client._bootstrap(connection, expected)
                client._connections.append(connection)
        except Exception:
            client.close()
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
        with self._state_lock:
            return self._healthy

    def worker_for(self, key: bytes | bytearray | memoryview) -> int:
        return worker_for(bytes(key), self._worker_count)

    def get(self, key: bytes | bytearray | memoryview) -> bytes:
        return self._read(Opcode.GET, bytes(key), b"")

    def ping(self, payload: bytes | bytearray | memoryview = b"") -> bytes:
        return self._read(Opcode.PING, b"", bytes(payload))

    def put(
        self,
        key: bytes | bytearray | memoryview,
        value: bytes | bytearray | memoryview,
        *,
        expire_at_ns: int = 0,
    ) -> MutationResult:
        return self._mutate(Opcode.PUT, bytes(key), bytes(value), expire_at_ns)

    def erase(self, key: bytes | bytearray | memoryview) -> MutationResult:
        return self._mutate(Opcode.ERASE, bytes(key), b"", 0)

    def close(self) -> None:
        with self._state_lock:
            self._healthy = False
        for connection in self._connections:
            with connection.lock:
                connection.reset()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _next_request_id(self) -> int:
        with self._request_id_lock:
            current = self._request_id
            self._request_id = 1 if current == 0xFFFF_FFFF_FFFF_FFFF else current + 1
            return current

    def _open_socket(self) -> socket.socket:
        try:
            opened = socket.create_connection(
                (self._config.host, self._config.port), self._config.connect_timeout
            )
            opened.settimeout(self._config.request_timeout)
            opened.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            return opened
        except OSError as error:
            raise Unavailable(f"could not connect to GlyphaStore: {error}") from error

    def _bootstrap(
        self, connection: _Connection, expected: tuple[int, int] | None
    ) -> tuple[int, int]:
        connection.reset()
        connection.socket = self._open_socket()
        try:
            init_id = self._next_request_id()
            response = self._exchange(
                connection, encode_request(Opcode.INIT, init_id)
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
            bind_id = self._next_request_id()
            bound = self._exchange(
                connection,
                encode_request(
                    Opcode.BIND_WORKER,
                    bind_id,
                    target_worker=connection.worker,
                ),
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
            connection.reset()
            raise Unavailable(str(error.error)) from error
        except TransportError as error:
            connection.reset()
            raise Unavailable(str(error)) from error
        except Exception:
            connection.reset()
            raise

    def _ensure_connected(self, connection: _Connection) -> None:
        if connection.socket is None:
            self._bootstrap(connection, (self._worker_count, self._routing_epoch))

    def _send(self, connection: _Connection, frame: bytes) -> None:
        assert connection.socket is not None
        sent = 0
        try:
            while sent < len(frame):
                count = connection.socket.send(frame[sent:])
                if count == 0:
                    raise OSError("socket closed during send")
                sent += count
        except (OSError, TimeoutError) as error:
            raise _SendFailure(TransportError(f"request send failed: {error}"), sent) from error

    def _receive_exact(self, connection: _Connection, size: int) -> bytes:
        assert connection.socket is not None
        output = bytearray()
        try:
            while len(output) < size:
                chunk = connection.socket.recv(size - len(output))
                if not chunk:
                    raise OSError("server closed the connection")
                output.extend(chunk)
        except (OSError, TimeoutError) as error:
            raise TransportError(f"response receive failed: {error}") from error
        return bytes(output)

    def _exchange(self, connection: _Connection, frame: bytes) -> Response:
        self._send(connection, frame)
        size_bytes = self._receive_exact(connection, 4)
        frame_size = int.from_bytes(size_bytes, "little")
        if not RESPONSE_HEADER_BYTES <= frame_size <= self._config.maximum_frame_bytes:
            raise ProtocolError("server response size is outside client limits")
        encoded = size_bytes + self._receive_exact(connection, frame_size - 4)
        try:
            return decode_response(encoded, self._config.maximum_frame_bytes)
        except ValueError as error:
            raise ProtocolError(str(error)) from error

    def _validate_response(self, response: Response, request_id: int, worker: int) -> None:
        if response.request_id != request_id:
            raise ProtocolError("server response request ID does not match")
        if (
            response.worker_count != self._worker_count
            or response.routing_epoch != self._routing_epoch
        ):
            with self._state_lock:
                self._healthy = False
            raise Unavailable("server routing metadata changed")
        if response.owner_worker != worker and response.status is not Status.WRONG_OWNER:
            with self._state_lock:
                self._healthy = False
            raise ProtocolError("server response came from the wrong Worker")

    @staticmethod
    def _status_error(status: Status) -> GlyphaError:
        if status is Status.NOT_FOUND:
            return NotFound("key was not found")
        if status is Status.OVERLOADED:
            return Overloaded("server is overloaded")
        if status is Status.NOT_BOUND:
            return Unavailable("server connection is not bound")
        if status is Status.WRONG_OWNER:
            return ProtocolError("server rejected Worker routing")
        if status in (Status.INVALID_REQUEST, Status.UNSUPPORTED):
            return InvalidArgument("server rejected the request")
        return GlyphaError("server reported an internal error")

    def _read(self, opcode: Opcode, key: bytes, value: bytes) -> bytes:
        if not self.healthy:
            raise Unavailable("client is closed or routing metadata changed")
        worker = 0 if opcode is Opcode.PING else self.worker_for(key)
        connection = self._connections[worker]
        with connection.lock:
            if not self.healthy:
                raise Unavailable("client closed before read admission")
            last_error: GlyphaError = Unavailable("request was not attempted")
            for _ in range(2):
                try:
                    self._ensure_connected(connection)
                    request_id = self._next_request_id()
                    try:
                        frame = encode_request(opcode, request_id, key=key, value=value)
                    except ValueError as error:
                        raise InvalidArgument(str(error)) from error
                    if len(frame) > self._config.maximum_frame_bytes:
                        raise InvalidArgument("request exceeds the configured frame limit")
                    response = self._exchange(
                        connection,
                        frame,
                    )
                    self._validate_response(response, request_id, worker)
                    if response.status is not Status.OK:
                        if response.status in (Status.WRONG_OWNER, Status.NOT_BOUND):
                            with self._state_lock:
                                self._healthy = False
                        raise self._status_error(response.status)
                    return response.value
                except (TransportError, _SendFailure) as error:
                    last_error = error.error if isinstance(error, _SendFailure) else error
                    connection.reset()
                except Unavailable as error:
                    connection.reset()
                    if not self.healthy:
                        raise
                    last_error = error
                except ProtocolError:
                    connection.reset()
                    raise
            raise last_error

    def _mutate(
        self, opcode: Opcode, key: bytes, value: bytes, expire_at_ns: int
    ) -> MutationResult:
        if not self.healthy:
            return MutationResult(
                MutationOutcome.REJECTED,
                Unavailable("client is closed or routing metadata changed"),
            )
        worker = self.worker_for(key)
        connection = self._connections[worker]
        with connection.lock:
            if not self.healthy:
                return MutationResult(
                    MutationOutcome.REJECTED,
                    Unavailable("client closed before mutation admission"),
                )
            for attempt in range(2):
                try:
                    self._ensure_connected(connection)
                except GlyphaError as error:
                    return MutationResult(MutationOutcome.REJECTED, error)
                try:
                    request_id = self._next_request_id()
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
                            MutationOutcome.REJECTED, InvalidArgument(str(error))
                        )
                    if len(frame) > self._config.maximum_frame_bytes:
                        return MutationResult(
                            MutationOutcome.REJECTED,
                            InvalidArgument("request exceeds the configured frame limit"),
                        )
                    response = self._exchange(
                        connection,
                        frame,
                    )
                    self._validate_response(response, request_id, worker)
                except _SendFailure as error:
                    connection.reset()
                    if error.bytes_sent == 0 and attempt == 0:
                        continue
                    return MutationResult(MutationOutcome.INDETERMINATE, error.error)
                except (TransportError, ProtocolError, Unavailable) as error:
                    connection.reset()
                    return MutationResult(MutationOutcome.INDETERMINATE, error)
                if response.status is Status.OK:
                    return MutationResult(MutationOutcome.COMMITTED)
                error = self._status_error(response.status)
                if response.status is Status.INTERNAL_ERROR:
                    return MutationResult(MutationOutcome.INDETERMINATE, error)
                if response.status in (Status.WRONG_OWNER, Status.NOT_BOUND):
                    with self._state_lock:
                        self._healthy = False
                return MutationResult(MutationOutcome.REJECTED, error)
            return MutationResult(
                MutationOutcome.REJECTED, Unavailable("could not send mutation")
            )

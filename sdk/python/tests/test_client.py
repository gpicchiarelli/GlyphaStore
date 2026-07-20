from __future__ import annotations

import asyncio
import socket
import sys
import threading
import unittest
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
try:
    import glyphastore  # noqa: F401
except ImportError:
    sys.path.insert(0, str(PACKAGE_ROOT / "src"))

from glyphastore import (  # noqa: E402
    AsyncClient,
    Client,
    ClientConfig,
    InvalidArgument,
    MutationOutcome,
    NotFound,
    PipelineOpcode,
    PipelineOutcome,
    PipelineRequest,
    TransportError,
)
from glyphastore.protocol import (  # noqa: E402
    Opcode,
    Status,
    decode_request,
    encode_response,
    worker_for,
)


class FakeServer:
    """Minimal protocol-v2 server with one bound connection per Worker."""

    def __init__(
        self,
        *,
        worker_count: int = 1,
        disconnect_on_put: bool = False,
        internal_error_on_put: bool = False,
        stall_on_get: bool = False,
    ) -> None:
        self._worker_count = worker_count
        self._disconnect_on_put = disconnect_on_put
        self._internal_error_on_put = internal_error_on_put
        self._stall_on_get = stall_on_get
        self._values: dict[bytes, bytes] = {}
        self._values_lock = threading.Lock()
        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(max(worker_count * 4, 8))
        self.port = self._listener.getsockname()[1]
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()
        self._acceptor = threading.Thread(target=self._accept, daemon=True)
        self._acceptor.start()

    @staticmethod
    def _receive_exact(connection: socket.socket, size: int) -> bytes:
        output = bytearray()
        while len(output) < size:
            chunk = connection.recv(size - len(output))
            if not chunk:
                raise EOFError
            output.extend(chunk)
        return bytes(output)

    def _receive(self, connection: socket.socket):
        prefix = self._receive_exact(connection, 4)
        size = int.from_bytes(prefix, "little")
        return decode_request(prefix + self._receive_exact(connection, size - 4))

    def _send(
        self,
        connection: socket.socket,
        status: Status,
        request_id: int,
        *,
        owner_worker: int,
        value: bytes = b"",
    ) -> None:
        connection.sendall(
            encode_response(
                status,
                request_id,
                value=value,
                owner_worker=owner_worker,
                worker_count=self._worker_count,
                routing_epoch=9,
            )
        )

    def _accept(self) -> None:
        self._listener.settimeout(0.2)
        try:
            while not self._stop.is_set():
                try:
                    connection, _ = self._listener.accept()
                except TimeoutError:
                    continue
                except OSError:
                    break
                thread = threading.Thread(
                    target=self._serve, args=(connection,), daemon=True
                )
                self._threads.append(thread)
                thread.start()
        finally:
            self._listener.close()

    def stop(self) -> None:
        self._stop.set()
        try:
            socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
        except OSError:
            pass

    def _serve(self, connection: socket.socket) -> None:
        bound_worker: int | None = None
        try:
            with connection:
                while True:
                    request = self._receive(connection)
                    if request.opcode is Opcode.INIT:
                        self._send(
                            connection,
                            Status.OK,
                            request.request_id,
                            owner_worker=0,
                            value=b"GlyphaStore/2",
                        )
                    elif request.opcode is Opcode.BIND_WORKER:
                        if request.target_worker >= self._worker_count:
                            self._send(
                                connection,
                                Status.INVALID_REQUEST,
                                request.request_id,
                                owner_worker=0,
                            )
                            continue
                        bound_worker = request.target_worker
                        self._send(
                            connection,
                            Status.OK,
                            request.request_id,
                            owner_worker=bound_worker,
                        )
                    elif bound_worker is None:
                        self._send(
                            connection,
                            Status.NOT_BOUND,
                            request.request_id,
                            owner_worker=0,
                        )
                    elif request.opcode is Opcode.PING:
                        self._send(
                            connection,
                            Status.OK,
                            request.request_id,
                            owner_worker=bound_worker,
                            value=request.value,
                        )
                    elif request.opcode is Opcode.PUT:
                        owner = worker_for(request.key, self._worker_count)
                        if bound_worker != owner:
                            self._send(
                                connection,
                                Status.WRONG_OWNER,
                                request.request_id,
                                owner_worker=owner,
                            )
                            continue
                        with self._values_lock:
                            self._values[request.key] = request.value
                        if self._disconnect_on_put:
                            return
                        if self._internal_error_on_put:
                            self._send(
                                connection,
                                Status.INTERNAL_ERROR,
                                request.request_id,
                                owner_worker=bound_worker,
                            )
                            continue
                        self._send(
                            connection,
                            Status.OK,
                            request.request_id,
                            owner_worker=bound_worker,
                        )
                    elif request.opcode is Opcode.GET:
                        if self._stall_on_get:
                            self._stop.wait(timeout=3600)
                            return
                        owner = worker_for(request.key, self._worker_count)
                        if bound_worker != owner:
                            self._send(
                                connection,
                                Status.WRONG_OWNER,
                                request.request_id,
                                owner_worker=owner,
                            )
                            continue
                        with self._values_lock:
                            value = self._values.get(request.key)
                        if value is None:
                            self._send(
                                connection,
                                Status.NOT_FOUND,
                                request.request_id,
                                owner_worker=bound_worker,
                            )
                        else:
                            self._send(
                                connection,
                                Status.OK,
                                request.request_id,
                                owner_worker=bound_worker,
                                value=value,
                            )
                    elif request.opcode is Opcode.ERASE:
                        owner = worker_for(request.key, self._worker_count)
                        if bound_worker != owner:
                            self._send(
                                connection,
                                Status.WRONG_OWNER,
                                request.request_id,
                                owner_worker=owner,
                            )
                            continue
                        with self._values_lock:
                            found = self._values.pop(request.key, None) is not None
                        self._send(
                            connection,
                            Status.OK if found else Status.NOT_FOUND,
                            request.request_id,
                            owner_worker=bound_worker,
                        )
        except (EOFError, OSError, ValueError):
            pass

    def join(self) -> None:
        self.stop()
        self._acceptor.join(timeout=2)
        for thread in self._threads:
            thread.join(timeout=2)


class ClientTests(unittest.TestCase):
    def test_binary_put_get_ping_and_erase(self) -> None:
        server = FakeServer()
        with Client.connect(ClientConfig(port=server.port)) as client:
            self.assertEqual(client.worker_count, 1)
            self.assertEqual(client.routing_epoch, 9)
            self.assertEqual(client.worker_for(b"binary\x00key"), 0)
            self.assertTrue(client.put(b"binary\x00key", b"value\x00\xff").committed)
            self.assertEqual(client.get(b"binary\x00key"), b"value\x00\xff")
            self.assertEqual(client.ping(b"hello"), b"hello")
            rejected = client.put(b"bad-expiry", b"value", expire_at_ns=-1)
            self.assertEqual(rejected.outcome, MutationOutcome.REJECTED)
            self.assertTrue(client.erase(b"binary\x00key").committed)
            with self.assertRaises(NotFound):
                client.get(b"binary\x00key")
        server.join()

    def test_internal_error_mutation_is_indeterminate(self) -> None:
        server = FakeServer(internal_error_on_put=True)
        with Client.connect(ClientConfig(port=server.port)) as client:
            result = client.put(b"key", b"value")
            self.assertEqual(result.outcome, MutationOutcome.INDETERMINATE)
        server.join()

    def test_close_does_not_deadlock_against_inflight_read(self) -> None:
        server = FakeServer()
        client = Client.connect(ClientConfig(port=server.port, request_timeout=1.0))
        started = threading.Event()
        finished = threading.Event()

        def reader() -> None:
            started.set()
            try:
                client.get(b"missing")
            except Exception:
                pass
            finished.set()

        thread = threading.Thread(target=reader)
        thread.start()
        self.assertTrue(started.wait(1.0))
        client.close()
        self.assertTrue(finished.wait(2.0))
        thread.join(timeout=2.0)
        self.assertFalse(client.healthy)
        server.join()

    def test_multi_worker_bootstrap_and_routing(self) -> None:
        server = FakeServer(worker_count=2)
        with Client.connect(ClientConfig(port=server.port)) as client:
            self.assertEqual(client.worker_count, 2)
            keys = [f"mw-{index}".encode() for index in range(32)]
            owners = {key: client.worker_for(key) for key in keys}
            self.assertEqual(set(owners.values()), {0, 1})
            for key in keys:
                self.assertTrue(client.put(key, key[::-1]).committed)
            for key in keys:
                self.assertEqual(client.get(key), key[::-1])
                self.assertEqual(worker_for(key, 2), owners[key])
        server.join()

    def test_disconnect_after_mutation_is_indeterminate(self) -> None:
        server = FakeServer(disconnect_on_put=True)
        with Client.connect(ClientConfig(port=server.port)) as client:
            result = client.put(b"key", b"value")
            self.assertEqual(result.outcome, MutationOutcome.INDETERMINATE)
            self.assertIsNotNone(result.error)
        server.join()

    def test_pipeline_preserves_order_and_owned_values(self) -> None:
        server = FakeServer()
        with Client.connect(ClientConfig(port=server.port)) as client:
            requests: list[PipelineRequest] = []
            expected: list[bytes] = []
            for index in range(64):
                value = f"pipeline-{index}".encode()
                expected.append(value)
                requests.append(PipelineRequest(PipelineOpcode.PUT, b"key", value))
                requests.append(PipelineRequest(PipelineOpcode.GET, b"key"))
            responses = client.execute_pipeline(requests)
            self.assertEqual(len(responses), len(requests))
            for index, value in enumerate(expected):
                self.assertTrue(responses[index * 2].succeeded)
                self.assertTrue(responses[index * 2 + 1].succeeded)
                self.assertEqual(responses[index * 2 + 1].value, value)
        server.join()

    def test_pipeline_disconnect_classifies_each_request(self) -> None:
        server = FakeServer(disconnect_on_put=True)
        with Client.connect(ClientConfig(port=server.port)) as client:
            responses = client.execute_pipeline(
                [
                    PipelineRequest(PipelineOpcode.PUT, b"key", b"value"),
                    PipelineRequest(PipelineOpcode.GET, b"key"),
                    PipelineRequest(PipelineOpcode.ERASE, b"key"),
                ]
            )
            self.assertEqual(responses[0].outcome, PipelineOutcome.INDETERMINATE)
            self.assertEqual(responses[1].outcome, PipelineOutcome.FAILED)
            self.assertEqual(responses[2].outcome, PipelineOutcome.INDETERMINATE)
        server.join()

    def test_pipeline_limits_fail_before_network_transmission(self) -> None:
        server = FakeServer()
        with Client.connect(
            ClientConfig(port=server.port, maximum_pipeline_requests=1)
        ) as client:
            with self.assertRaises(InvalidArgument):
                client.execute_pipeline(
                    [
                        PipelineRequest(PipelineOpcode.GET, b"key"),
                        PipelineRequest(PipelineOpcode.GET, b"key"),
                    ]
                )
        server.join()

    def test_per_call_timeout_overrides_config(self) -> None:
        server = FakeServer(stall_on_get=True)
        with Client.connect(
            ClientConfig(port=server.port, request_timeout=5.0)
        ) as client:
            with self.assertRaises(TransportError):
                client.get(b"key", timeout=0.05)
            with self.assertRaises(InvalidArgument):
                client.get(b"key", timeout=0)
        server.join()

    def test_batch_groups_workers_and_restores_order(self) -> None:
        server = FakeServer(worker_count=2)
        with Client.connect(ClientConfig(port=server.port)) as client:
            keys = [f"batch-{index}".encode() for index in range(64)]
            owners = {key: client.worker_for(key) for key in keys}
            self.assertEqual(set(owners.values()), {0, 1})
            requests = [
                PipelineRequest(PipelineOpcode.PUT, key, key[::-1]) for key in keys
            ] + [PipelineRequest(PipelineOpcode.GET, key) for key in keys]
            responses = client.execute_batch(requests)
            self.assertEqual(len(responses), len(requests))
            for response in responses:
                self.assertTrue(response.succeeded)
            for index, key in enumerate(keys):
                self.assertEqual(responses[len(keys) + index].value, key[::-1])
        server.join()

        server = FakeServer(worker_count=2)
        with Client.connect(
            ClientConfig(port=server.port, maximum_pipeline_requests=1)
        ) as limited:
            key0 = next(
                key
                for key in (f"batch-limit-{index}".encode() for index in range(64))
                if limited.worker_for(key) == 0
            )
            with self.assertRaises(InvalidArgument):
                limited.execute_batch(
                    [
                        PipelineRequest(PipelineOpcode.GET, key0),
                        PipelineRequest(PipelineOpcode.GET, key0),
                    ]
                )
        server.join()


class AsyncClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_put_get_pipeline_and_close(self) -> None:
        server = FakeServer()
        async with await AsyncClient.connect(ClientConfig(port=server.port)) as client:
            self.assertEqual(client.worker_count, 1)
            self.assertTrue((await client.put(b"async\x00key", b"value\xff")).committed)
            self.assertEqual(await client.get(b"async\x00key"), b"value\xff")
            self.assertEqual(await client.ping(b"ping"), b"ping")
            responses = await client.execute_pipeline(
                [
                    PipelineRequest(PipelineOpcode.PUT, b"async\x00key", b"next"),
                    PipelineRequest(PipelineOpcode.GET, b"async\x00key"),
                ]
            )
            self.assertTrue(responses[0].succeeded)
            self.assertEqual(responses[1].value, b"next")
        server.join()

    async def test_async_multi_worker_concurrent_pipelines(self) -> None:
        server = FakeServer(worker_count=2)
        async with await AsyncClient.connect(ClientConfig(port=server.port)) as client:
            self.assertEqual(client.worker_count, 2)

            async def round_trip(key: bytes) -> None:
                value = key[::-1]
                result = await client.put(key, value)
                self.assertTrue(result.committed)
                self.assertEqual(await client.get(key), value)

            await asyncio.gather(*(round_trip(f"a{i}".encode()) for i in range(20)))
        server.join()

    async def test_async_batch_groups_workers_and_restores_order(self) -> None:
        server = FakeServer(worker_count=2)
        async with await AsyncClient.connect(ClientConfig(port=server.port)) as client:
            keys = [f"abatch-{index}".encode() for index in range(32)]
            self.assertEqual({client.worker_for(key) for key in keys}, {0, 1})
            requests = [
                PipelineRequest(PipelineOpcode.PUT, key, key[::-1]) for key in keys
            ] + [PipelineRequest(PipelineOpcode.GET, key) for key in keys]
            responses = await client.execute_batch(requests)
            self.assertEqual(len(responses), len(requests))
            for response in responses:
                self.assertTrue(response.succeeded)
            for index, key in enumerate(keys):
                self.assertEqual(responses[len(keys) + index].value, key[::-1])
        server.join()

    async def test_async_disconnect_after_mutation_is_indeterminate(self) -> None:
        server = FakeServer(disconnect_on_put=True)
        async with await AsyncClient.connect(ClientConfig(port=server.port)) as client:
            result = await client.put(b"key", b"value")
            self.assertEqual(result.outcome, MutationOutcome.INDETERMINATE)
        server.join()


if __name__ == "__main__":
    unittest.main()

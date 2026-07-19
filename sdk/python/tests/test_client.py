from __future__ import annotations

import socket
import struct
import sys
import threading
import unittest
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT / "src"))

from glyphastore import (  # noqa: E402
    Client,
    ClientConfig,
    InvalidArgument,
    MutationOutcome,
    NotFound,
    PipelineOpcode,
    PipelineOutcome,
    PipelineRequest,
)
from glyphastore.protocol import Opcode, Status  # noqa: E402

REQUEST = struct.Struct("<IHBBQIIQII")
RESPONSE = struct.Struct("<IHHQIIIIQ")


class FakeServer:
    def __init__(self, *, disconnect_on_put: bool = False) -> None:
        self._disconnect_on_put = disconnect_on_put
        self._values: dict[bytes, bytes] = {}
        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(1)
        self.port = self._listener.getsockname()[1]
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    @staticmethod
    def _receive_exact(connection: socket.socket, size: int) -> bytes:
        output = bytearray()
        while len(output) < size:
            chunk = connection.recv(size - len(output))
            if not chunk:
                raise EOFError
            output.extend(chunk)
        return bytes(output)

    def _receive(self, connection: socket.socket) -> tuple[Opcode, int, int, bytes, bytes]:
        prefix = self._receive_exact(connection, 4)
        size = int.from_bytes(prefix, "little")
        frame = prefix + self._receive_exact(connection, size - 4)
        (_, version, opcode, flags, request_id, key_size, value_size, expiry, target, reserved) = (
            REQUEST.unpack_from(frame)
        )
        if version != 2 or flags != 0 or reserved != 0:
            raise ValueError
        key = frame[40 : 40 + key_size]
        value = frame[40 + key_size : 40 + key_size + value_size]
        return Opcode(opcode), request_id, target, key, value

    @staticmethod
    def _send(
        connection: socket.socket,
        status: Status,
        request_id: int,
        value: bytes = b"",
    ) -> None:
        connection.sendall(
            RESPONSE.pack(40 + len(value), 2, status, request_id, len(value), 0, 1, 0, 9)
            + value
        )

    def _run(self) -> None:
        try:
            connection, _ = self._listener.accept()
            with connection:
                bound = False
                while True:
                    opcode, request_id, target, key, value = self._receive(connection)
                    if opcode is Opcode.INIT:
                        self._send(connection, Status.OK, request_id, b"GlyphaStore/2")
                    elif opcode is Opcode.BIND_WORKER and target == 0:
                        bound = True
                        self._send(connection, Status.OK, request_id)
                    elif not bound:
                        self._send(connection, Status.NOT_BOUND, request_id)
                    elif opcode is Opcode.PING:
                        self._send(connection, Status.OK, request_id, value)
                    elif opcode is Opcode.PUT:
                        self._values[key] = value
                        if self._disconnect_on_put:
                            return
                        self._send(connection, Status.OK, request_id)
                    elif opcode is Opcode.GET:
                        if key in self._values:
                            self._send(connection, Status.OK, request_id, self._values[key])
                        else:
                            self._send(connection, Status.NOT_FOUND, request_id)
                    elif opcode is Opcode.ERASE:
                        status = Status.OK if self._values.pop(key, None) is not None else Status.NOT_FOUND
                        self._send(connection, status, request_id)
        except (EOFError, OSError):
            pass
        finally:
            self._listener.close()

    def join(self) -> None:
        self._thread.join(timeout=2)


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


if __name__ == "__main__":
    unittest.main()

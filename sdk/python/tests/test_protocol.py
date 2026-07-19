from __future__ import annotations

import sys
import unittest
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
try:
    import glyphastore  # noqa: F401
except ImportError:
    sys.path.insert(0, str(PACKAGE_ROOT / "src"))

from glyphastore.protocol import (  # noqa: E402
    REQUEST_HEADER_BYTES,
    RESPONSE_HEADER_BYTES,
    Opcode,
    Status,
    decode_request,
    decode_response,
    encode_request,
    encode_response,
    worker_for,
)


def fixture(name: str) -> bytes:
    path = PACKAGE_ROOT / "tests" / "fixtures" / name
    return bytes(int(token, 16) for token in path.read_text(encoding="utf-8").split())


def frames(corpus: bytes) -> list[bytes]:
    output: list[bytes] = []
    offset = 0
    while offset < len(corpus):
        size = int.from_bytes(corpus[offset : offset + 4], "little")
        output.append(corpus[offset : offset + size])
        offset += size
    return output


class ProtocolTests(unittest.TestCase):
    def test_request_encoder_matches_every_canonical_fixture(self) -> None:
        expected = frames(fixture("wire_requests_v2.hex"))
        encoded = [
            encode_request(Opcode.INIT, 1),
            encode_request(Opcode.PING, 2, value=b"\x00ping\xff"),
            encode_request(Opcode.GET, 3, key=b"get\x00key"),
            encode_request(
                Opcode.PUT,
                4,
                key=b"put\x00key",
                value=b"\x10\x20\xff",
                expire_at_ns=123_456_789,
            ),
            encode_request(Opcode.ERASE, 5, key=b"erase-key"),
            encode_request(Opcode.BIND_WORKER, 6, target_worker=2),
        ]
        self.assertEqual(encoded, expected)

    def test_request_decoder_round_trips_canonical_fixture(self) -> None:
        decoded = [decode_request(frame) for frame in frames(fixture("wire_requests_v2.hex"))]
        self.assertEqual([request.opcode for request in decoded], list(Opcode))
        self.assertEqual(decoded[1].value, b"\x00ping\xff")
        self.assertEqual(decoded[3].expire_at_ns, 123_456_789)
        self.assertEqual(decoded[5].target_worker, 2)
        reencoded = [
            encode_request(
                request.opcode,
                request.request_id,
                key=request.key,
                value=request.value,
                expire_at_ns=request.expire_at_ns,
                target_worker=request.target_worker,
            )
            for request in decoded
        ]
        self.assertEqual(reencoded, frames(fixture("wire_requests_v2.hex")))

    def test_response_decoder_matches_every_canonical_fixture(self) -> None:
        decoded = [decode_response(frame) for frame in frames(fixture("wire_responses_v2.hex"))]
        self.assertEqual([response.status for response in decoded], list(Status))
        self.assertEqual(decoded[0].value, b"GlyphaStore/2")
        self.assertEqual(decoded[6].owner_worker, 2)
        self.assertTrue(all(response.worker_count == 4 for response in decoded))

    def test_response_encoder_round_trips_canonical_fixture(self) -> None:
        decoded = [decode_response(frame) for frame in frames(fixture("wire_responses_v2.hex"))]
        reencoded = [
            encode_response(
                response.status,
                response.request_id,
                value=response.value,
                owner_worker=response.owner_worker,
                worker_count=response.worker_count,
                routing_epoch=response.routing_epoch,
            )
            for response in decoded
        ]
        self.assertEqual(reencoded, frames(fixture("wire_responses_v2.hex")))

    def test_decoder_rejects_noncanonical_and_truncated_frames(self) -> None:
        request = bytearray(frames(fixture("wire_requests_v2.hex"))[0])
        request[36] = 1
        with self.assertRaises(ValueError):
            decode_request(bytes(request))
        with self.assertRaises(ValueError):
            decode_request(bytes(request[: REQUEST_HEADER_BYTES - 1]))

        response = bytearray(frames(fixture("wire_responses_v2.hex"))[0])
        response[28] = 1
        with self.assertRaises(ValueError):
            decode_response(bytes(response))
        with self.assertRaises(ValueError):
            decode_response(bytes(response[: RESPONSE_HEADER_BYTES - 1]))

    def test_routing_is_binary_and_deterministic(self) -> None:
        self.assertEqual(worker_for(b"", 4), 1)
        self.assertEqual(worker_for(b"key\x00\xff", 4), worker_for(b"key\x00\xff", 4))

    def test_u64_fields_reject_out_of_range_values(self) -> None:
        with self.assertRaises(ValueError):
            encode_request(Opcode.PUT, -1, key=b"k", value=b"v")
        with self.assertRaises(ValueError):
            encode_request(Opcode.PUT, 1, key=b"k", value=b"v", expire_at_ns=-1)
        with self.assertRaises(ValueError):
            encode_response(Status.OK, 1, routing_epoch=-1)


if __name__ == "__main__":
    unittest.main()

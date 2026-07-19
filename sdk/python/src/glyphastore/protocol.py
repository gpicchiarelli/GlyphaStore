"""Canonical wire-protocol v2 codec and Worker routing."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from struct import Struct

VERSION = 2
REQUEST_HEADER_BYTES = 40
RESPONSE_HEADER_BYTES = 40
MAX_FRAME_BYTES = 2 * 1024 * 1024
NO_WORKER = 0xFFFF_FFFF

_REQUEST_HEADER = Struct("<IHBBQIIQII")
_RESPONSE_HEADER = Struct("<IHHQIIIIQ")


class Opcode(IntEnum):
    INIT = 1
    PING = 2
    GET = 3
    PUT = 4
    ERASE = 5
    BIND_WORKER = 6


class Status(IntEnum):
    OK = 0
    INVALID_REQUEST = 1
    UNSUPPORTED = 2
    INTERNAL_ERROR = 3
    NOT_FOUND = 4
    OVERLOADED = 5
    WRONG_OWNER = 6
    NOT_BOUND = 7


@dataclass(frozen=True, slots=True)
class Response:
    status: Status
    request_id: int
    owner_worker: int
    worker_count: int
    routing_epoch: int
    value: bytes


def _u64(value: int, field: str) -> int:
    if not 0 <= value <= 0xFFFF_FFFF_FFFF_FFFF:
        raise ValueError(f"{field} is outside unsigned 64-bit range")
    return value


def encode_request(
    opcode: Opcode,
    request_id: int,
    *,
    key: bytes = b"",
    value: bytes = b"",
    expire_at_ns: int = 0,
    target_worker: int = NO_WORKER,
) -> bytes:
    """Encode one canonical protocol-v2 request frame."""
    if not isinstance(opcode, Opcode):
        raise ValueError("opcode is not defined by wire protocol v2")
    _u64(request_id, "request_id")
    _u64(expire_at_ns, "expire_at_ns")
    if not 0 <= target_worker <= 0xFFFF_FFFF:
        raise ValueError("target_worker is outside unsigned 32-bit range")
    frame_size = REQUEST_HEADER_BYTES + len(key) + len(value)
    if frame_size > MAX_FRAME_BYTES:
        raise ValueError("request exceeds the protocol frame limit")
    return _REQUEST_HEADER.pack(
        frame_size,
        VERSION,
        opcode,
        0,
        request_id,
        len(key),
        len(value),
        expire_at_ns,
        target_worker,
        0,
    ) + key + value


def decode_response(frame: bytes, maximum_frame_bytes: int = MAX_FRAME_BYTES) -> Response:
    """Decode one complete response and reject noncanonical fields."""
    if len(frame) < RESPONSE_HEADER_BYTES:
        raise ValueError("response is shorter than its header")
    (
        frame_size,
        version,
        status,
        request_id,
        value_size,
        owner_worker,
        worker_count,
        reserved,
        routing_epoch,
    ) = _RESPONSE_HEADER.unpack_from(frame)
    if frame_size != len(frame) or frame_size > maximum_frame_bytes:
        raise ValueError("response frame extent is invalid")
    if version != VERSION:
        raise ValueError("response protocol version is unsupported")
    if reserved != 0:
        raise ValueError("response reserved field is noncanonical")
    if RESPONSE_HEADER_BYTES + value_size != frame_size:
        raise ValueError("response value extent is invalid")
    try:
        decoded_status = Status(status)
    except ValueError as error:
        raise ValueError("response status is unknown") from error
    return Response(
        status=decoded_status,
        request_id=request_id,
        owner_worker=owner_worker,
        worker_count=worker_count,
        routing_epoch=routing_epoch,
        value=frame[RESPONSE_HEADER_BYTES:],
    )


def fnv1a64(key: bytes) -> int:
    value = 14_695_981_039_346_656_037
    for byte in key:
        value ^= byte
        value = (value * 1_099_511_628_211) & 0xFFFF_FFFF_FFFF_FFFF
    return value


def worker_for(key: bytes, worker_count: int) -> int:
    if worker_count <= 0:
        raise ValueError("worker_count must be positive")
    return fnv1a64(key) % worker_count

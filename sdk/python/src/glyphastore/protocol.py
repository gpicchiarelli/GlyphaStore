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
    HEALTH = 7
    READY = 8
    STATS = 9


class Status(IntEnum):
    OK = 0
    INVALID_REQUEST = 1
    UNSUPPORTED = 2
    INTERNAL_ERROR = 3
    NOT_FOUND = 4
    OVERLOADED = 5
    WRONG_OWNER = 6
    NOT_BOUND = 7
    PERMISSION_DENIED = 8


@dataclass(frozen=True, slots=True)
class Request:
    opcode: Opcode
    request_id: int
    expire_at_ns: int = 0
    target_worker: int = NO_WORKER
    key: bytes = b""
    value: bytes = b""


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


def _u32(value: int, field: str) -> int:
    if not 0 <= value <= 0xFFFF_FFFF:
        raise ValueError(f"{field} is outside unsigned 32-bit range")
    return value


def _validate_request_fields(
    opcode: Opcode,
    key: bytes,
    value: bytes,
    expire_at_ns: int,
    target_worker: int,
) -> None:
    if opcode is Opcode.INIT and (key or value or expire_at_ns != 0 or target_worker != NO_WORKER):
        raise ValueError("INIT request cannot carry key, value, expiry, or target_worker")
    if opcode is Opcode.PING and (key or expire_at_ns != 0 or target_worker != NO_WORKER):
        raise ValueError("PING request cannot carry key, expiry, or target_worker")
    if opcode is Opcode.GET and (
        not key or value or expire_at_ns != 0 or target_worker != NO_WORKER
    ):
        raise ValueError("GET request requires a key and cannot carry value, expiry, or target_worker")
    if opcode is Opcode.PUT and (not key or target_worker != NO_WORKER):
        raise ValueError("PUT request requires a key and cannot carry target_worker")
    if opcode is Opcode.ERASE and (
        not key or value or expire_at_ns != 0 or target_worker != NO_WORKER
    ):
        raise ValueError("ERASE request requires a key and cannot carry value, expiry, or target_worker")
    if opcode is Opcode.BIND_WORKER and (key or value or expire_at_ns != 0):
        raise ValueError("BIND_WORKER request cannot carry key, value, or expiry")
    if opcode is Opcode.BIND_WORKER and target_worker == NO_WORKER:
        raise ValueError("BIND_WORKER request requires an explicit target_worker")
    if opcode in (Opcode.HEALTH, Opcode.READY, Opcode.STATS) and (
        key or value or expire_at_ns != 0 or target_worker != NO_WORKER
    ):
        raise ValueError("lifecycle probe cannot carry key, value, expiry, or target_worker")


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
    if not isinstance(key, (bytes, bytearray, memoryview)) or not isinstance(
        value, (bytes, bytearray, memoryview)
    ):
        raise TypeError("key and value must be bytes-like")
    key = bytes(key)
    value = bytes(value)
    _validate_request_fields(opcode, key, value, expire_at_ns, target_worker)
    _u64(request_id, "request_id")
    _u64(expire_at_ns, "expire_at_ns")
    _u32(target_worker, "target_worker")
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


def decode_request(
    frame: bytes | bytearray | memoryview,
    maximum_frame_bytes: int = MAX_FRAME_BYTES,
) -> Request:
    """Decode one complete request and reject noncanonical fields."""
    if len(frame) < REQUEST_HEADER_BYTES:
        raise ValueError("request is shorter than its header")
    (
        frame_size,
        version,
        opcode,
        flags,
        request_id,
        key_size,
        value_size,
        expire_at_ns,
        target_worker,
        reserved,
    ) = _REQUEST_HEADER.unpack_from(frame)
    if frame_size != len(frame) or frame_size > maximum_frame_bytes:
        raise ValueError("request frame extent is invalid")
    if version != VERSION:
        raise ValueError("request protocol version is unsupported")
    if flags != 0 or reserved != 0:
        raise ValueError("request canonical fields are invalid")
    if REQUEST_HEADER_BYTES + key_size + value_size != frame_size:
        raise ValueError("request payload extent is invalid")
    try:
        decoded_opcode = Opcode(opcode)
    except ValueError as error:
        raise ValueError("request opcode is unknown") from error
    key_start = REQUEST_HEADER_BYTES
    value_start = key_start + key_size
    key = bytes(frame[key_start:value_start])
    value = bytes(frame[value_start : value_start + value_size])
    _validate_request_fields(decoded_opcode, key, value, expire_at_ns, target_worker)
    return Request(
        opcode=decoded_opcode,
        request_id=request_id,
        expire_at_ns=expire_at_ns,
        target_worker=target_worker,
        key=key,
        value=value,
    )


def encode_response(
    status: Status,
    request_id: int,
    *,
    value: bytes = b"",
    owner_worker: int = NO_WORKER,
    worker_count: int = 0,
    routing_epoch: int = 0,
) -> bytes:
    """Encode one canonical protocol-v2 response frame."""
    if not isinstance(status, Status):
        raise ValueError("status is not defined by wire protocol v2")
    _u64(request_id, "request_id")
    _u32(owner_worker, "owner_worker")
    _u32(worker_count, "worker_count")
    _u64(routing_epoch, "routing_epoch")
    frame_size = RESPONSE_HEADER_BYTES + len(value)
    if frame_size > MAX_FRAME_BYTES:
        raise ValueError("response exceeds the protocol frame limit")
    return _RESPONSE_HEADER.pack(
        frame_size,
        VERSION,
        status,
        request_id,
        len(value),
        owner_worker,
        worker_count,
        0,
        routing_epoch,
    ) + value


def decode_response(
    frame: bytes | bytearray | memoryview,
    maximum_frame_bytes: int = MAX_FRAME_BYTES,
) -> Response:
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
        value=bytes(frame[RESPONSE_HEADER_BYTES:]),
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

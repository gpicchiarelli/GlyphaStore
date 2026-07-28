#!/usr/bin/env python3
"""Cross-SDK put/get helpers for the interoperability matrix."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "sdk" / "python" / "src"))

from glyphastore import (  # noqa: E402
    Client,
    ClientConfig,
    GlyphaError,
    MutationOutcome,
    PipelineOpcode,
    PipelineRequest,
)


def parse_hex(text: str) -> bytes:
    cleaned = "".join(text.split())
    if not cleaned:
        return b""
    if len(cleaned) % 2:
        raise ValueError("odd hex length")
    return bytes.fromhex(cleaned)


def to_hex(payload: bytes) -> str:
    return payload.hex()


def main() -> int:
    parser = argparse.ArgumentParser(description="GlyphaStore Python interop helper")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument(
        "command",
        choices=(
            "put",
            "get",
            "erase",
            "pipeline-put-get",
            "expect-not-found",
            "expect-frame-limit",
        ),
    )
    parser.add_argument("--key-hex", default="")
    parser.add_argument("--value-hex", default="")
    parser.add_argument("--expire-at-ns", type=int, default=0)
    parser.add_argument("--tls", action="store_true", help="opt-in TLS 1.3")
    parser.add_argument("--tls-ca", default=None, help="PEM CA / trust anchor")
    parser.add_argument("--tls-cert", default=None, help="client cert for mTLS")
    parser.add_argument("--tls-key", default=None, help="client key for mTLS")
    parser.add_argument("--server-name", default=None, help="SNI / hostname verify")
    parser.add_argument(
        "--insecure-skip-verify",
        action="store_true",
        help="lab escape only; disable cert/hostname verify",
    )
    args = parser.parse_args()

    key = parse_hex(args.key_hex)
    value = parse_hex(args.value_hex)
    config = ClientConfig(
        host=args.host,
        port=args.port,
        tls=args.tls,
        tls_ca=args.tls_ca,
        cert_file=args.tls_cert,
        key_file=args.tls_key,
        server_name=args.server_name,
        insecure_skip_verify=args.insecure_skip_verify,
    )
    with Client.connect(config) as client:
        if args.command == "put":
            result = client.put(key, value, expire_at_ns=args.expire_at_ns)
            if not result.committed:
                print(f"put not committed: {result}", file=sys.stderr)
                return 1
            return 0
        if args.command == "get":
            got = client.get(key)
            print(to_hex(got))
            return 0
        if args.command == "erase":
            result = client.erase(key)
            if not result.committed:
                print(f"erase not committed: {result}", file=sys.stderr)
                return 1
            return 0
        if args.command == "expect-not-found":
            try:
                client.get(key)
            except GlyphaError as error:
                if error.category == "not_found" and error.retryability == "new_attempt":
                    return 0
                print(f"unexpected GET error: {error.category}", file=sys.stderr)
                return 1
            print("GET unexpectedly found the key", file=sys.stderr)
            return 1
        if args.command == "expect-frame-limit":
            result = client.put(b"limit", bytes([0xA5]) * config.maximum_frame_bytes)
            if (
                result.outcome is MutationOutcome.REJECTED
                and result.error is not None
                and result.error.category == "invalid_argument"
                and result.error.bytes_sent == 0
                and result.error.retryability == "never"
            ):
                return 0
            print(f"unexpected frame-limit result: {result}", file=sys.stderr)
            return 1
        responses = client.execute_pipeline(
            [
                PipelineRequest(PipelineOpcode.PUT, key, value),
                PipelineRequest(PipelineOpcode.GET, key),
            ]
        )
        if len(responses) != 2 or not responses[0].succeeded or not responses[1].succeeded:
            print("pipeline outcomes failed", file=sys.stderr)
            return 1
        if responses[1].value != value:
            print("pipeline value mismatch", file=sys.stderr)
            return 1
        print(to_hex(responses[1].value))
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001 — CLI boundary
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error

# ADR 0019: Client error taxonomy, retry, and timeout contract

- Status: accepted
- Date: 2026-07-19
- Deciders: networking maintainers
- Applies to: official TCP clients for wire protocol v2
- Amends: none
- Supersedes: none

## Context

C++, Python, and Perl clients already implement compatible mutation outcomes and basic retries, but
the rules lived mainly in the C++ reference prose and source. Production readiness still required
normative error, time, and concurrency semantics for clients. Without a frozen taxonomy, a fourth
SDK (Go) and structured-error enrichment would diverge.

## Decision drivers

- Correctness: never silently duplicate mutations; never call a timed-out mutation `committed`.
- Compatibility: portable category names across languages without forcing a C ABI.
- Operability: applications can classify `retryability` without parsing message strings.
- Honesty: protocol v2 has no cancel frame; late responses after a deadline remain a client hazard
  solved by connection reset, not by pretending the server aborted.

## Alternatives considered

1. **Map every language onto C++ `ErrorCode`.** Rejected: mixes Store/disk codes with wire client
   failures and complicates Python/Perl packaging.
2. **Message-string conventions only.** Rejected: fragile and untestable as a contract.
3. **Protocol-level timeout/cancel opcodes in v2.** Deferred: requires a new wire version; clients
   still need local deadlines today.
4. **No automatic retries.** Rejected: read-only and zero-byte mutation retries are already shipped
   and useful; freeze them rather than remove them.

## Decision

Adopt [Client semantics v1](../spec/client-semantics-v1.md) as normative for official clients:

- closed portable error categories;
- wire status → category and mutation outcome tables;
- retryability classes and exact automatic retry limits;
- monotonic request deadlines, connection reset on expiry, and late-response rules;
- unhealthy-client fail-closed behavior for routing metadata faults.

## Consequences

- Positive: cross-SDK conformance has a single checklist; Go can implement against the spec;
  structured error fields can be added without changing outcomes.
- Negative: C++ `ErrorCode` names remain a superset and are not the portable taxonomy.
- Deferred: per-request deadline API options, connection pools, and wire idempotency keys
  ([SDK roadmap](../architecture/sdk-roadmap.md)).

## Compatibility and migration

No wire byte change. Existing official clients must not change outcome classification except to
match gaps called out by the spec (already aligned for the critical mutation cases). Documentation
and CI interop remain the verification path for release compatibility of client behavior.

## Verification

- Shared wire fixtures and `scripts/test-sdk-interop.sh`.
- Per-SDK unit tests for disconnect → indeterminate, zero-byte send → rejected after retry, and
  deadline/reset behavior as covered by each language suite.
- Future conformance guide vectors may cite this ADR’s tables directly.

## References

- [Client semantics v1](../spec/client-semantics-v1.md)
- [Wire protocol v2](../spec/wire-protocol-v2.md)
- [C++ TCP client API](../reference/cpp-client-api.md)
- [ADR 0013](0013-native-wire-protocol-v2.md)

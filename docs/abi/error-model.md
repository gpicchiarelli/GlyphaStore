# C ABI error model

`gs_status` carries stable numeric `code`, broad `category`, retryability guidance, and a zero
reserved word. Diagnostic C++ strings are intentionally not exposed. `gs_status_message` maps a
status code to deterministic text and returns required bytes including the terminating NUL.

Mutation outcome is separate from error status. `GS_MUTATION_REJECTED` is the only outcome that
asserts known-not-committed. I/O, internal, unavailable, integrity, and other ambiguous failures are
reported as `GS_MUTATION_INDETERMINATE` unless the facade has stronger evidence. This may be more
conservative than the internal failure but never authorizes an unsafe replay.

Retryability is advice, not permission to replay an indeterminate mutation. Callers must combine
both fields and their own idempotency protocol.

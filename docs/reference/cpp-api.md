# Public C++ API Reference

Status: normative for the current public headers
Applies to: `glyphastore::Store` version `0.1.x`
Owner: API maintainers
Last reviewed: 2026-07-19

## 1. General contract

The public API is declared under `include/glyphastore`. Keys and values are arbitrary byte spans. Unless stated otherwise, member operations are safe to call concurrently on the same open `Store`; this provides per-key operation safety, not transactions across calls or keys.

Fallible operations return `Result<T>` or `Status`, based on `std::expected`. No failure category requires parsing an error message. Error messages are diagnostic, non-stable text.

The Store owns all data returned by an owning result. Input spans are borrowed only for the duration of the call.

## 2. `Store::open`

```cpp
static auto open(const StoreConfig& config = {}) -> Result<std::unique_ptr<Store>>;
```

Validates configuration, selects volatile or durable runtime, and completes recovery before returning success. On failure no usable Store is published. Durable open may inspect and safely complete documented recovery intent, but must not mutate an incompatible or ambiguous namespace.

Worker count becomes fixed for the returned Store. Durable reopen uses persisted routing metadata and rejects incompatible explicit configuration.

## 3. Metadata

```cpp
auto worker_count() const noexcept -> std::size_t;
```

Returns the immutable Worker count. It is valid after close and performs no operation admission. It says nothing about current availability.

## 4. Reads

```cpp
auto get(std::string_view key) -> Result<OwnedValue>;
auto get(std::span<const std::byte> key) -> Result<OwnedValue>;
auto get_copy(std::string_view key) -> Result<OwnedValue>;
auto get_copy(std::span<const std::byte> key) -> Result<OwnedValue>;
```

Returns an owning copy of the latest visible byte value and its sequence/expiration metadata. An
absent key, tombstone, or expired latest Record returns `ErrorCode::not_found`; absence is not an
optional successful value.

`get_copy` is the current compatibility spelling for the same owning-read behavior; it is not a zero-copy API. A future pinned read must use a distinct handle that owns segment lifetime and must not change these methods' ownership.

The returned bytes remain valid independently of later Store mutation or destruction.

## 5. Put

```cpp
auto put(std::string_view key, std::span<const std::byte> value,
         std::uint64_t expire_at_ns = 0) -> Status;
auto put(std::span<const std::byte> key, std::span<const std::byte> value,
         std::uint64_t expire_at_ns = 0) -> Status;
```

Publishes the byte value for the complete binary key. `expire_at_ns == 0` means no expiration; otherwise it is an absolute Unix timestamp in nanoseconds. Input is copied or durably encoded before return and need not outlive the call.

Success does not expose the internal `RecordRef` or replacement status. The durability guarantee
depends on storage mode. The Store never retains input spans.

## 6. Erase

```cpp
auto erase(std::string_view key) -> Status;
auto erase(std::span<const std::byte> key) -> Status;
```

Removes current visibility for the binary key. Durable mode records a tombstone when required for
recovery ordering. An absent or expired key returns `ErrorCode::not_found`.

## 7. Flush

```cpp
auto flush() -> Status;
```

For durable modes, waits until all mutations covered by the call have crossed the mode's required persistence boundary, or returns an error. Concurrent later mutations need not be covered. A flush failure may poison the durable runtime and make later operations fail closed.

For volatile mode, `flush()` succeeds as a no-op.

## 8. Compaction

```cpp
auto compact() -> Result<CompactionResult>;
```

Runs explicit compaction according to the storage mode's selection policy. Durable mode uses its crash-consistent whole-Worker transaction. Volatile mode copy-builds replacements for selected sparse sealed Segments and publishes them only when the Worker uses fewer physical Segments afterward. Only one public compaction attempt may run at a time; a concurrent attempt returns `sequence_conflict`. Success with no eligible or physically beneficial work returns a result with `compacted == false`.

Compaction preserves logical key/value visibility and may change Record references and physical Segment identities. Volatile source bytes remain alive while an already returned internal snapshot retains shared ownership; durable compaction additionally preserves crash recovery authority.

```cpp
auto maintenance_snapshot() const -> MaintenanceSnapshot;
```

Returns a point-in-time copy of controller state and counters. For durable background maintenance,
`last_observation` includes the selected round-robin Worker and its sealed, Index-referenced live,
dead, and dead-ratio Record-byte counters. The normal controller compares that ratio to
`dead_byte_ratio_bp_normal` and preflights live bytes against the inclusive
`max_copy_bytes_per_cycle` limit (128 MiB by default; zero means unlimited); pressure and emergency
bypass both controls. Unread expired Records remain conservatively live until GET, recovery, or
compaction validates their expiration. For durable Stores, `rotation` reports runtime-local
attempt/commit/wait counters, post-rotation final-Record attempt/commit counters, and
last/total/maximum nanoseconds for publication wait, Segment seal, replacement Segment creation,
Manifest publication, aggregate execution, the complete rotation, and the final Record commit.
Publication wait includes acquiring the Manifest serializer and any wait for an active compaction
lease. Aggregate execution also includes planning, reader setup, and the in-memory catalog
transition; subtracting the three named execution phases yields that residual.

## 9. Verification

```cpp
auto verify_index() const -> Status;
```

Checks internal Index and reference invariants. It may stop all Worker progress briefly by taking Worker locks in a global order. It is intended for tests, diagnostics, and controlled verification, not a high-frequency request path.

Success is evidence that checked in-memory invariants hold at one synchronized point; it is not a full disk scrub or hardware-integrity guarantee.

## 10. Close and destruction

```cpp
auto close() -> Status;
~Store();
```

`close()` closes operation admission, drains already admitted calls, performs required final durable flush, stops background work, and releases resources. It is idempotent and returns the sticky result of the first close sequence. Calls admitted after closing starts fail with `unavailable`.

Destruction invokes the same safety path but cannot report failure. Applications that require durable-close error handling must call `close()` explicitly before destruction.

Concurrent destruction with calls that do not otherwise own the `Store` is invalid C++ lifetime usage and is not made safe by internal admission.

## 11. Error categories

The current `ErrorCode` set is:

| Category | Typical meaning |
|---|---|
| `invalid_argument` | invalid options or operation parameters |
| `arithmetic_overflow` | checked size, sequence, or generation overflow |
| `record_too_large` | encoded Record exceeds the v1 bound |
| `segment_full`, `segment_sealed` | append cannot proceed on that Segment state |
| `invalid_record`, `checksum_mismatch`, `corrupted_data` | malformed or corrupted encoded state |
| `invalid_reference` | Record reference does not identify a valid current extent |
| `sequence_conflict` | conflicting sequence or mutually exclusive operation |
| `not_found` | requested internal object is absent; normal `get` absence uses `nullopt` |
| `resource_exhausted`, `storage_exhausted` | memory/policy or storage capacity exhausted |
| `file_too_large`, `descriptor_exhausted`, `read_only_filesystem` | stable filesystem/resource category |
| `unavailable` | Store closed, poisoned, or temporarily unavailable |
| `io_error` | operating-system I/O failure not covered more specifically |
| `internal_error` | invariant or unexpected internal failure |

There is not yet a dedicated `incompatible_format` enumerator. Unsupported required persistent versions therefore surface through an existing validation/corruption category. Adding a stable compatibility-specific category is release work and must be additive.

## 12. API stability

Version `0.1.x` is pre-1.0. Source compatibility may change, but changes must be intentional and documented. Binary ABI stability is not promised. Persistent and wire compatibility are governed by their own versions and do not follow the C++ package version automatically.

Public headers must document ownership, units, limits, and thread safety for every new type or method. Implementation-private classes under `src/` are not public API merely because tests can include them.

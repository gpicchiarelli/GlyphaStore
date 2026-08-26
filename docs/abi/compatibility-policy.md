# C ABI compatibility policy

ABI major 1 preserves already-published symbol signatures, numeric constants, fixed layout
prefixes, ownership rules, mutation polarity, and acknowledgement semantics. Compatible additions
are new symbols or optional tail fields that old consumers can ignore. Source aliases and new
header helpers may be source-compatible without changing binary layout.

The following require a new ABI major: removing/renaming a symbol, changing parameter or return
types, changing struct size/offset/alignment in an existing prefix, reusing a numeric value, exposing
an internal pointer lifetime, weakening close/thread guarantees, or changing a mutation from
rejected to indeterminate/committed (or the reverse) without a new contract.

The initial 1.0 library has same-build C and symbol evidence. Each candidate packages the installed
C consumer as a deterministic, sealed, platform-specific fixture. A later release must verify the
prior release seal, release-wide policy, annotated tag, and Sigstore attestation before extracting
that already-compiled consumer. It then runs both old-binary/new-library and
new-binary/old-library directions, proving dynamic-loader resolution in each run. Until a complete
prior official release exists, the gate remains below release acceptance even though the normative
contract and producer are in force.

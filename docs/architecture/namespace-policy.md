# Data-directory namespace audit

The recovery path now performs a read-only, manifest-relative namespace audit after decoding the
authoritative manifest and before opening any Segment. The audit never adopts, renames, quarantines,
deletes, or rewrites an entry. Ordinary recovery therefore cannot turn an attacker-controlled or
crash-left file into Store authority and cannot destroy evidence needed by an operator.

## Descriptor-relative enumeration

Enumeration remains anchored to the already validated and exclusively locked `DataDirectory`.
Each audit obtains a new open file description with `openat(directory_fd, ".", ...)`, transfers that
descriptor to `fdopendir`, and uses its `dirfd` for metadata checks. A plain `dup` is deliberately not
used: duplicated descriptors share the directory offset, so a second audit could otherwise begin at
the first audit's end.

The `readdir` result supplies only the name. The implementation ignores `.` and `..` by name, does
not depend on non-portable `d_type`, resets `errno` before each read so EOF is distinguishable from an
error, and validates recognized objects with `fstatat(..., AT_SYMLINK_NOFOLLOW)`. A recognized object
is safe only when it is a regular file, owned by the effective user, singly linked, and inaccessible
to group and other users. Recovery later reopens every catalog Segment with `O_NOFOLLOW` and validates
its encoded identity, so the filename never replaces on-disk metadata authority.

These choices follow POSIX descriptor-relative directory interfaces and the platform manuals for
[Linux `opendir`/`fdopendir`](https://man7.org/linux/man-pages/man3/opendir.3.html),
[Linux `readdir`](https://man7.org/linux/man-pages/man3/readdir.3.html),
[Apple directory operations](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man3/dirfd.3.html),
[FreeBSD `fdopendir`](https://man.freebsd.org/cgi/man.cgi?query=fdopendir&sektion=3), and
[OpenBSD directory operations](https://man.openbsd.org/readdir.3). The no-follow metadata behavior is
also explicit in [OpenBSD `fstatat(2)`](https://man.openbsd.org/fstatat.2).

## Canonical names and classifications

Segment filenames accept exactly lowercase hexadecimal, fixed widths, non-zero IDs, and these two
forms:

```text
segment-<16 hex Segment ID>-<8 hex generation>.glypha
.segment-<16 hex Segment ID>-<8 hex generation>.glypha.tmp
```

Uppercase digits, width changes, trailing bytes, zero identities, and near-matches are malformed;
there is no permissive normalization. Final Segment membership requires both parsed ID and generation
to match one exact manifest entry.

| Classification | Meaning | Normal recovery policy |
|---|---|---|
| expected | lock, manifest, or exact manifest-listed final Segment with safe metadata | continue |
| stale manifest temporary | exact private `.manifest.glypha.tmp` | report and continue; do not delete |
| stale Segment temporary | exact private canonical Segment temporary | report and continue; do not delete |
| unlisted Segment | canonical private final Segment absent from the manifest | reject without opening or adopting |
| malformed engine name | near-match to an engine-owned namespace | reject |
| unknown entry | any other name | reject |
| unsafe entry | recognized name that is a symlink, hard-linked, non-regular, foreign-owned, or non-private | reject |
| missing catalog Segment | manifest entry with no exact final name | reject as corruption |
| missing required entry | lock or authoritative manifest vanished during audit | reject as corruption |

Recognized temporaries are the only non-clean state accepted by normal recovery. They cannot be
authority, and leaving them untouched preserves crash evidence. An unlisted Segment may be a valid
crash orphan, but normal recovery does not infer that from its name and does not advance IDs around
it. A future explicit quarantine/repair operation must first validate the encoded header, reserve the
identity safely, synchronize every namespace mutation, and retain an operator-visible audit trail.

## Determinism and bounds

The manifest decoder caps the catalog at 1,000,000 entries. Namespace enumeration additionally caps
non-dot entries at `manifest Segment count + 2 required files + 4096 anomalies`; exceeding that bound
fails before an unbounded report is built. The issue vector has a separate absolute bound of
`1,000,000 + 4096`, and manifest presence uses one bit per catalog Segment.

Directory order never affects diagnostics: issues are sorted by complete binary filename and then by
classification. Catalog lookup uses binary search over the already canonical, strictly ordered
manifest, avoiding a second million-entry hash table. For `N` directory entries, `M` manifest
Segments, and `A` anomalies, audit cost is `O(N log M + A log A)` time and `O(M bits + A)` auxiliary
space. This favors bounded deterministic memory during a failure path; the fixed 40/45-byte name
grammar keeps parsing linear in a tiny constant.

The exclusive lock serializes cooperating GlyphaStore processes. As with the rest of the filesystem
layer, it cannot prevent a same-user process that deliberately ignores the advisory protocol from
mutating the directory concurrently; later no-follow opens and identity validation remain mandatory.

## Evidence and remaining work

Unit tests cover exact parsing, repeated enumeration, deterministic classification, canonical crash
temporaries, unlisted files, malformed and unknown names, symlinks, hard links, missing catalog files,
and the enumeration bound. Recovery integration proves that temporaries are reported without deletion
and that an unlisted Segment blocks recovery without adoption or deletion.

Still required before durable mode can be enabled: explicit operator quarantine/repair tooling,
collision-safe orphan ID reservation, native Linux/FreeBSD/OpenBSD CI, process-kill and disk-full
matrices, and runtime durable Store materialization.

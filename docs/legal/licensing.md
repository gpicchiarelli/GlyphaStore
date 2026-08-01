# Licensing and copyright policy

Status: normative for contributors and redistributors
Applies to: GlyphaStore repository, official SDKs, installs, CI
Owner: maintainers
Last reviewed: 2026-08-01

## Project license

GlyphaStore (core, tools, official SDKs, tests, docs, and artwork in this repository) is
**BSD-3-Clause**. Authoritative text: [`LICENSE`](../../LICENSE).

Copyright holder: **Giacomo Picchiarelli** (2026).

SPDX: `BSD-3-Clause`

## What must travel with redistributions

| Artifact | Required files |
| --- | --- |
| Source tree / tarball | `LICENSE`, `NOTICE`, `THIRD_PARTY_NOTICES.md` |
| CMake install (Runtime component) | `LICENSE`, `NOTICE`, `VERSION`, `THIRD_PARTY_NOTICES.md` under `${CMAKE_INSTALL_DATADIR}/GlyphaStore` |
| Official SDK packages | Each SDK’s `LICENSE` (byte-identical to root), plus `NOTICE`; Python also ships `NOTICE` via `license-files` |
| TLS-linked binaries | OpenSSL/LibreSSL notices per [tls-redistribution.md](tls-redistribution.md) |

BSD-3-Clause requires retaining the copyright notice, conditions, and disclaimer in source
redistributions, and reproducing them with binary redistributions.

## Third parties

Do not add a third-party dependency without:

1. Confirming a license compatible with BSD-3-Clause redistribution (prefer Apache-2.0, BSD,
   MIT, ISC, MPL-2.0 with compliance, BlueOak, Unlicense/0BSD/CC0).
2. **Rejecting** exclusive GPL/AGPL/SSPL/“Commons Clause” runtime dependencies.
3. Updating [`THIRD_PARTY_NOTICES.md`](../../THIRD_PARTY_NOTICES.md) and [`NOTICE`](../../NOTICE).
4. Keeping CI license scans green (`scripts/ci-license-check.sh`,
   `.github/workflows/license-check.yml`).

Optional TLS linkage to OpenSSL/LibreSSL is allowed; redistributors must follow **those**
libraries’ licenses for linked binaries ([THIRD_PARTY_NOTICES §1](../../THIRD_PARTY_NOTICES.md)).

## Algorithms vs copied code

Independent implementations of published algorithms (SipHash-2-4, FNV-1a, CRC32C, SwissTable-style
layouts) are fine when:

- no substantial third-party source is pasted without its license;
- papers and design inspirations are cited in `THIRD_PARTY_NOTICES.md` / ADRs;
- test vectors from papers are attributed.

## Contributions

By contributing, you agree that your contribution is licensed under the same BSD-3-Clause terms
and that you have the right to submit it. Prefer adding an SPDX line on new substantial files:

```text
// SPDX-License-Identifier: BSD-3-Clause
// SPDX-FileCopyrightText: 2026 Giacomo Picchiarelli
```

Do not contribute code you are not authorized to relicense under BSD-3-Clause.

## CI enforcement

- `scripts/ci-license-check.sh` — notice files, exact SDK LICENSE sync, `reuse lint`, Go/Python
  dep scan (fails closed on exclusive GPL/AGPL/SSPL).
- `engineering/tools/validate_actions_pins.py` — third-party Actions stay SHA-pinned (supply chain,
  not copyright substitution).
- Tag supply-chain jobs emit SPDX SBOMs for packaged SDK artifacts.

## Related

- [NOTICE](../../NOTICE)
- [THIRD_PARTY_NOTICES.md](../../THIRD_PARTY_NOTICES.md)
- [tls-redistribution.md](tls-redistribution.md)
- [actions-pinning](../security/actions-pinning.md)
- [Contributing](../../CONTRIBUTING.md)

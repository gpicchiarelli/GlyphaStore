Status: normative checklist
Applies to: official SDK packaging (Python, Perl, Go, Ruby, Erlang, C++ client)
Owner: release maintainers
Last reviewed: 2026-07-24

# SDK packaging standard

Every official client must be **buildable, installable, version-locked, and verifiable** without
asking a maintainer. Publishing to registries (PyPI, PAUSE, RubyGems, Hex, proxy.golang.org) is a
separate credentialed step; everything below is required **before** publish.

Related: [sdk-roadmap](sdk-roadmap.md), per-SDK `PACKAGING.md` under `sdk/*/`, root `VERSION`.

## Gates (must be green)

| Gate | Command |
| --- | --- |
| Version lockstep | `./scripts/check-sdk-versions.sh` |
| Fixture sync | `./scripts/sync-sdk-fixtures.sh` |
| Python package | `./scripts/package-python-client.sh` |
| Perl package | `./scripts/package-perl-client.sh` |
| Go package | `./scripts/package-go-client.sh` |
| Ruby package | `./scripts/package-ruby-client.sh` |
| Erlang package | `./scripts/package-erlang-client.sh` (requires OTP + rebar3) |
| C++ CMake client | `./scripts/verify-cpp-client-package.sh` |
| All of the above + checksums | `./scripts/package-all-sdk-clients.sh` |

CI job `sdk-clients` runs version lock, language tests, and package scripts for
Python/Perl/Go/Ruby/Erlang.
The `install-consumer` job covers CMake install + external consumer smokes (requires OpenSSL when
the tree was built with TLS; `FindGlyphaStoreTls.cmake` is installed next to the package config).

## Version policy

1. Root [`VERSION`](../../VERSION) is the canonical release number for **all** official SDKs while
   they remain in lockstep (current: `0.1.0`).
2. Language sources:
   - Python: `glyphastore.__version__`
   - Perl: `our $VERSION` in every `lib/**/*.pm` (must be identical)
   - Go: `client.Version`
   - Ruby: `GlyphaStore::VERSION`
   - Erlang: `glyphastore_version:version/0`
   - C++: CMake `PROJECT_VERSION` from root `VERSION`
3. Diverging an SDK version requires an ADR; until then CI fails on drift.

## Artifact perfection (release day)

1. Run `./scripts/package-all-sdk-clients.sh`
2. Attach `dist/sdk-artifacts/SHA256SUMS` and `*.spdx.json` from the supply-chain workflow
   (or `SYFT_REQUIRED=1 ./scripts/checksum-sdk-artifacts.sh`) to the GitHub Release
   (Python/Perl sdist names are prefixed `python-` / `perl-` in that directory so they remain
   distinct on case-insensitive filesystems)
3. On tagged releases the supply-chain workflow keyless-signs blobs with Cosign/Sigstore
   (`.cosign.bundle` next to each artifact + `SHA256SUMS`). Optional project GPG remains
   operator-owned.
4. Publish:
   - Python: `twine upload` (Trusted Publisher preferred)
   - Perl: PAUSE upload of `GlyphaStore-VERSION.tar.gz`
   - Ruby: `gem push` (MFA required; `allowed_push_host=rubygems.org`)
   - Erlang: `rebar3 hex publish` when Hex credentials are configured
   - Go: `git tag sdk/go/vVERSION && git push origin sdk/go/vVERSION`
   - C++: ship CMake installable prefix / source tag; optional future vcpkg/Conan

## Still operator-owned (credentials)

Automated publish workflows, production signing keys, and registry account recovery are **not**
stored in this repository. Packaging gates above are complete without them.

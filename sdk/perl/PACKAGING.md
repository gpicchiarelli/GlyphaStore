# Packaging and publishing (MetaCPAN / PAUSE)

This directory is a self-contained Perl distribution. Runtime dependencies are
core modules only (Perl ≥ 5.32).

## Preconditions

- Perl ≥ 5.32 with ExtUtils::MakeMaker ≥ 7.12
- Test::More ≥ 0.98 (usually bundled with Test-Simple)
- Canonical wire fixtures under `t/fixtures/` (vendored; must match repository
  `tests/fixtures/wire_*_v2.hex`). Refresh with `./scripts/sync-sdk-fixtures.sh`
  after changing the repository corpus.
- License: BSD-3-Clause (`LICENSE`), matching the GlyphaStore project
- PAUSE account authorized to upload the `GlyphaStore` namespace
- For local quality gates: `Perl::Critic` and `Perl::Tidy` (see `cpanfile` develop
  requirements). `./scripts/test-perl-client.sh` runs critic at severity 1 before tests.

## Local verification

From the repository root:

```bash
./scripts/package-perl-client.sh
```

The script:

1. Confirms vendored fixtures match the repository corpus
2. Regenerates `MANIFEST`
3. Runs `make disttest` (configure, build, and test from the tarball)
4. Builds `GlyphaStore-VERSION.tar.gz`
5. Checks that `META.json` declares the expected provides and BSD license
6. Normalizes and extracts the final tarball, installs it under an isolated prefix, and reruns all
   tests from a separate directory that cannot resolve the distribution's source `lib/`

With the packaged Python peer and TLS daemon/client helpers already built, the installed Perl
tarball participates in the fail-closed secure-profile matrix through:

```bash
PERL=/path/to/perl-with-IO-Socket-SSL \
GLYPHASTORED=/path/to/glyphastored \
GLYPHASTORE_INTEROP_CLIENT=/path/to/glyphastore_interop_client \
GLYPHASTORE_GO_INTEROP=/path/to/glyphastore-interop \
./scripts/test-secure-profile-installed-artifacts.sh
```

## Manual build

```bash
cd sdk/perl
perl Makefile.PL
make manifest
make disttest
make dist
```

## Publish to PAUSE

1. Ensure `$VERSION` is identical in:
   - `lib/GlyphaStore.pm`
   - `lib/GlyphaStore/Client.pm`
   - `lib/GlyphaStore/Protocol.pm`
   - `Changes`
2. Re-run `./scripts/package-perl-client.sh`
3. Upload `GlyphaStore-0.1.0.tar.gz` through
   [PAUSE](https://pause.perl.org/) → Upload a file to CPAN
4. Confirm indexing on [MetaCPAN](https://metacpan.org/pod/GlyphaStore)

Optional local install check:

```bash
cpanm ./GlyphaStore-0.1.0.tar.gz
```

## Version bump checklist

1. Bump `$VERSION` in **every** `.pm` under `lib/` (must stay identical)
2. Update root `VERSION` and every other official SDK in lockstep (or land an ADR)
3. Update `Changes`
4. Keep `Makefile.PL` `provides` versions in sync (same scalar)
5. Run `./scripts/check-sdk-versions.sh` and `./scripts/package-perl-client.sh`
6. Upload the new tarball to PAUSE

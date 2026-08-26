# Packaging and publishing (Hex)

OTP application: `glyphastore` under `sdk/erlang`.

Runtime dependencies: OTP ≥ 25 stdlib only (`kernel`, `stdlib`, `crypto`; `ssl` optional for TLS).
License: BSD-3-Clause.

## Version

`glyphastore_version:version/0` is the single source for the Erlang client. It must match the
repository root `VERSION` file (enforced by `./scripts/check-sdk-versions.sh`).

## Local verification

```bash
./scripts/package-erlang-client.sh
```

The script:

1. Confirms vendored wire fixtures match the repository corpus
2. Runs `rebar3 compile` and `rebar3 ct`
3. Asserts `glyphastore_version:version/0` matches `VERSION`
4. Smoke-checks `scripts/glyphastore-version.escript`
5. Builds a normalized `sdk/erlang/dist/glyphastore-erlang-VERSION.tar.gz` from tracked files only
6. Extracts and compiles that archive outside the checkout, then verifies its runtime version
7. Writes `sdk/erlang/dist/package-info.txt`

With the other SDK packages and TLS daemon/client peers built, the extracted archive participates in
the fail-closed secure-profile matrix through:

```bash
GLYPHASTORED=/path/to/glyphastored \
GLYPHASTORE_INTEROP_CLIENT=/path/to/glyphastore_interop_client \
GLYPHASTORE_GO_INTEROP=/path/to/glyphastore-interop \
./scripts/test-secure-profile-installed-artifacts.sh
```

## Consumers

```erlang
%% After Hex publish:
%% {deps, [{glyphastore, "0.1.0"}]}.
{ok, Client} = glyphastore_client:connect(#{host => "127.0.0.1", port => 7379}).
```

Until Hex publish is configured, consume from this repository path with `rebar3` path deps or
copy the OTP application into your release.

## Version bump checklist

1. Bump `src/glyphastore_version.erl` and `src/glyphastore.app.src` (`vsn`)
2. Update `CHANGELOG.md`
3. Ensure root `VERSION` matches (all SDKs lockstep unless an ADR says otherwise)
4. Run `./scripts/check-sdk-versions.sh` and `./scripts/package-erlang-client.sh`
5. Publish to Hex when credentials are available (`rebar3 hex publish`)

## Release artifacts

Hex packages are the primary distribution. Until registry publication exists, CI attaches the
normalized tracked-source archive, `dist/package-info.txt`, checksums and SBOM through
`scripts/checksum-sdk-artifacts.sh`. The source archive is not described as a Hex package.

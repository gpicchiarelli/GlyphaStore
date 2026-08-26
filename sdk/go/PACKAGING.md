# Packaging and publishing (Go module)

Module path: `github.com/gpicchiarelli/GlyphaStore/sdk/go`

Runtime dependencies: none (stdlib only). License: BSD-3-Clause.

## Version

`client.Version` in `client/version.go` is the single source for the Go client. It must match
the repository root `VERSION` file (enforced by `./scripts/check-sdk-versions.sh`).

## Local verification

```bash
./scripts/package-go-client.sh
```

The script:

1. Confirms vendored wire fixtures match the repository corpus
2. Asserts `client.Version` matches `VERSION`
3. Runs `go test ./...` and race tests on `client` / `protocol`
4. Builds `glyphastore-interop`, `glyphastore-bench`, and `glyphastore-version`
5. Checks `go mod tidy` cleanliness when supported
6. Reconstructs a tag-shaped module from tracked files only and reruns its tests
7. Compiles and runs an external consumer of the snapshot's public `client` and `protocol` packages
8. Produces a normalized `glyphastore-go-VERSION.tar.gz` from that tracked snapshot
9. Extracts the archive and rebuilds the interop executable outside the checkout
10. Writes `sdk/go/dist/package-info.txt`

## Consumers

```bash
go get github.com/gpicchiarelli/GlyphaStore/sdk/go@v0.1.0
```

Nested-module tags **must** be of the form:

```text
sdk/go/v0.1.0
```

Not a repository-root `v0.1.0` tag alone (proxy.golang.org resolves the subdirectory module via
the `sdk/go/` prefix).

## Version bump checklist

1. Bump `client/version.go` (`Version`)
2. Update `CHANGELOG.md`
3. Ensure root `VERSION` matches (all SDKs lockstep unless an ADR says otherwise)
4. Run `./scripts/check-sdk-versions.sh` and `./scripts/package-go-client.sh`
5. Tag `sdk/go/vX.Y.Z` and push the tag

## Release artifacts

Go modules are fetched by the module proxy from git tags. Optional CLI binaries may be attached
to a GitHub Release with SHA-256 checksums (see `scripts/checksum-sdk-artifacts.sh`).
The normalized source archive is verification and release evidence for the tag-shaped module; it
does not replace the `sdk/go/vVERSION` tag consumed by the Go module proxy.

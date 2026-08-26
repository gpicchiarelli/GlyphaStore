# GlyphaStore Go client

Native Go client for GlyphaStore wire protocol v2. It opens one TCP connection per Worker
(`TCP_NODELAY`), routes binary keys with canonical FNV-1a 64-bit, retries safe reads after a
transient disconnect, and never reports an uncertain mutation as rejected.

Implements [client semantics v1](../../docs/spec/client-semantics-v1.md): portable error
categories, monotonic request deadlines, at-most-one automatic retry, and
`committed` / `rejected` / `indeterminate` mutation outcomes. Pipelines are single-Worker;
`ExecuteBatch` groups by Worker, overlaps goroutines, and restores caller order.

Worker routing follows ADR 0030: plain `GlyphaStore/2` is FNV-1a; the extended INIT identity selects SipHash-2-4.

Module: `github.com/gpicchiarelli/GlyphaStore/sdk/go`  
Version: `client.Version` (must match repository `VERSION`)  
License: BSD-3-Clause. Requires Go ≥ 1.22.  
Packaging: [PACKAGING.md](PACKAGING.md)

The packaging gate reconstructs the nested module from tracked files only, tests that snapshot, and
builds an external consumer before a `sdk/go/vVERSION` tag is considered publishable.

```go
package main

import (
	"fmt"
	"log"

	"github.com/gpicchiarelli/GlyphaStore/sdk/go/client"
)

func main() {
	cache, err := client.Connect(client.Config{Host: "127.0.0.1", Port: 7379})
	if err != nil {
		log.Fatal(err)
	}
	defer cache.Close()

	if !cache.Put([]byte("session\x0042"), []byte("payload"), 0).Committed() {
		log.Fatal("put rejected")
	}
	value, err := cache.Get([]byte("session\x0042"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%q\n", value)

	responses, err := cache.ExecuteBatch([]client.PipelineRequest{
		{Opcode: client.PipelinePut, Key: []byte("a"), Value: []byte("1")},
		{Opcode: client.PipelinePut, Key: []byte("b"), Value: []byte("2")},
		{Opcode: client.PipelineGet, Key: []byte("a")},
		{Opcode: client.PipelineGet, Key: []byte("b")},
	})
	if err != nil {
		log.Fatal(err)
	}
	_ = responses
}
```

## Install

```bash
go get github.com/gpicchiarelli/GlyphaStore/sdk/go@latest
```

From this source tree:

```bash
cd sdk/go && go test ./...
```

## Layout

| Package | Role |
| --- | --- |
| `protocol` | Wire codec, FNV-1a routing, golden fixture tests |
| `client` | Sync TCP client (Get/Put/Erase/Ping, pipeline, batch) |
| `cmd/glyphastore-interop` | CLI for `scripts/test-sdk-interop.sh` |

## Benchmarks

Against a live `glyphastored` (same PUT/GET pipeline matrix as Python/Perl):

```bash
./scripts/benchmark_go_client.sh
```

Or build the harness and point it at an existing server:

```bash
cd sdk/go && go build -o bin/glyphastore-bench ./cmd/glyphastore-bench
./bin/glyphastore-bench --port 7379 --workers 4 --ops 100000 --pipeline 128 --execution concurrent
```

Use `--execution batch` to measure mixed-owner `ExecuteBatch` grouping and fan-out. The benchmark
generates keys through the connected client's negotiated routing identity rather than assuming
default FNV routing.

## Performance notes

- Contiguous little-endian frame encoding into pre-sized, connection-local scratch buffers
- One write deadline per send and one read deadline per exchange (or per pipeline receive phase)
- `DecodeResponseView` + `OwnBytes` avoid allocating empty values; GET payloads are copied once
- One TCP connection and `sync.Mutex` per Worker; `ExecuteBatch` fans out one goroutine per Worker
- `ExecuteBatch` uses lazily preallocated, Worker-indexed request/index vectors and writes disjoint
  caller-order result slots, avoiding request recopying and a result-collection mutex
- One-Worker `ExecuteBatch` calls the validated pipeline path directly and retains positional
  rejected results for group-level pre-admission failures
- Atomic request IDs and fail-closed health (no nested lock under connection mutex)
- Pipeline ownership validation hashes every key once; the first key is not routed twice
- Prefer pipeline depth around **8–32** pairs for peak Go throughput; depth 128 often regresses
  on client CPU while raw TCP continues to climb

Treat the server as loopback / private network / sidecar for cleartext. Opt-in TLS 1.3 is
available via `Config.TLS` (`Enable`, `CAFile`, `CertFile`/`KeyFile` for mTLS, `ServerName`,
`InsecureSkipVerify` lab escape only). Hostname/SNI verification is on by default; there is no
silent cleartext fallback when TLS is requested
([security roadmap](../../docs/security/roadmap.md); ADRs 0020–0022). OpenBSD is a first-class
target (LibreSSL on the daemon).

# GlyphaStore Go client

Native Go client for GlyphaStore wire protocol v2. It opens one TCP connection per Worker
(`TCP_NODELAY`), routes binary keys with canonical FNV-1a 64-bit, retries safe reads after a
transient disconnect, and never reports an uncertain mutation as rejected.

Implements [client semantics v1](../../docs/spec/client-semantics-v1.md): portable error
categories, monotonic request deadlines, at-most-one automatic retry, and
`committed` / `rejected` / `indeterminate` mutation outcomes. Pipelines are single-Worker;
`ExecuteBatch` groups by Worker, overlaps goroutines, and restores caller order.

Module: `github.com/gpicchiarelli/GlyphaStore/sdk/go`  
License: BSD-3-Clause. Requires Go ≥ 1.22.

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

## Performance notes

- Contiguous little-endian frame encoding into pre-sized buffers (`EncodeRequestInto` / `AppendRequest`)
- One write per exchange or pipeline; reusable receive buffer with offset compaction
- Per-Worker `sync.Mutex`; `ExecuteBatch` fans out one goroutine per Worker
- Atomic request IDs and fail-closed health (no nested lock under connection mutex)

Treat the server as loopback / private network / sidecar until TLS and authentication exist.

package client_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/gpicchiarelli/GlyphaStore/sdk/go/client"
)

// Benchmarks against a live glyphastored. Set GLYPHASTORE_BENCH_PORT to enable.
func BenchmarkPutGetPipeline(b *testing.B) {
	port := os.Getenv("GLYPHASTORE_BENCH_PORT")
	if port == "" {
		b.Skip("set GLYPHASTORE_BENCH_PORT to run against a live server")
	}
	var p int
	if _, err := fmt.Sscanf(port, "%d", &p); err != nil || p <= 0 {
		b.Fatalf("bad GLYPHASTORE_BENCH_PORT=%q", port)
	}
	c, err := client.Connect(client.Config{Port: p})
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	key := []byte("bench-key")
	value := []byte("bench-value-0123456789")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reqs := []client.PipelineRequest{
			{Opcode: client.PipelinePut, Key: key, Value: value},
			{Opcode: client.PipelineGet, Key: key},
		}
		responses, err := c.ExecutePipeline(reqs)
		if err != nil || len(responses) != 2 || !responses[1].Succeeded() {
			b.Fatalf("pipeline failed: %v %+v", err, responses)
		}
	}
}

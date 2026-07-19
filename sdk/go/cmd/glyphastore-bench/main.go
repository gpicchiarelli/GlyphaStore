// Command glyphastore-bench measures validated PUT/GET pipeline throughput
// against an external glyphastored (same workload as the Python/Perl SDK benches).
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/gpicchiarelli/GlyphaStore/sdk/go/client"
	"github.com/gpicchiarelli/GlyphaStore/sdk/go/protocol"
)

const sdkVersion = "0.1.0"

func main() {
	host := flag.String("host", "127.0.0.1", "server host")
	port := flag.Int("port", 0, "server port")
	workers := flag.Int("workers", 4, "expected Worker count")
	ops := flag.Int("ops", 100_000, "PUT/GET pair count")
	pipeline := flag.Int("pipeline", 64, "PUT/GET pairs per batch")
	warmup := flag.Int("warmup", 1, "warmup iterations")
	repeats := flag.Int("repeats", 7, "timed iterations")
	execution := flag.String("execution", "concurrent", "concurrent|sequential")
	flag.Parse()

	if *port <= 0 || *workers < 1 || *ops < 1 || *pipeline < 1 || *repeats < 1 || *warmup < 0 {
		fail("numeric arguments are outside benchmark limits")
	}
	if *execution != "concurrent" && *execution != "sequential" {
		fail("--execution must be concurrent or sequential")
	}

	batches := material(*ops, uint32(*workers), *pipeline)
	operationCount := *ops * 2
	cfg := client.Config{
		Host:                    *host,
		Port:                    *port,
		MaximumPipelineRequests: *pipeline * 2,
	}
	c, err := client.Connect(cfg)
	if err != nil {
		fail(err.Error())
	}
	defer c.Close()
	if int(c.WorkerCount()) != *workers {
		fail("server Worker count does not match --workers")
	}

	runOnce := runSequential
	if *execution == "concurrent" {
		runOnce = runConcurrent
	}
	for i := 0; i < *warmup; i++ {
		if err := runOnce(c, batches); err != nil {
			fail(err.Error())
		}
	}
	samples := make([]float64, 0, *repeats)
	for i := 0; i < *repeats; i++ {
		started := time.Now()
		if err := runOnce(c, batches); err != nil {
			fail(err.Error())
		}
		samples = append(samples, time.Since(started).Seconds())
	}

	rates := make([]float64, len(samples))
	for i, sample := range samples {
		rates[i] = float64(operationCount) / sample
	}
	fmt.Println("# glyphastore Go client benchmark")
	fmt.Printf(
		"# sdk_version=%s runtime=sync execution=%s workers=%d pipeline_pairs=%d operations=%d\n",
		sdkVersion, *execution, *workers, *pipeline, operationCount,
	)
	fmt.Printf(
		"name=go_client_pipeline_read_after_write sdk_version=%s runtime=sync execution=%s "+
			"workers=%d pipeline_pairs=%d operations=%d samples=%d "+
			"median_seconds=%.9f min_seconds=%.9f max_seconds=%.9f "+
			"median_ops_per_second=%.3f min_ops_per_second=%.3f max_ops_per_second=%.3f\n",
		sdkVersion, *execution, *workers, *pipeline, operationCount, len(samples),
		median(samples), minFloat(samples), maxFloat(samples),
		median(rates), minFloat(rates), maxFloat(rates),
	)
}

func material(operations int, workers uint32, pipeline int) [][][]client.PipelineRequest {
	quotas := make([]int, workers)
	base := operations / int(workers)
	rem := operations % int(workers)
	for w := range quotas {
		quotas[w] = base
		if w < rem {
			quotas[w]++
		}
	}
	requests := make([][]client.PipelineRequest, workers)
	for candidate := 0; anyPositive(quotas); candidate++ {
		key := []byte(fmt.Sprintf("go-bench-%012d", candidate))
		owner, err := protocol.WorkerFor(key, workers)
		if err != nil {
			fail(err.Error())
		}
		if quotas[owner] == 0 {
			continue
		}
		value := bytesFilled(byte(candidate&0xFF), 64)
		requests[owner] = append(requests[owner],
			client.PipelineRequest{Opcode: client.PipelinePut, Key: key, Value: value},
			client.PipelineRequest{Opcode: client.PipelineGet, Key: key},
		)
		quotas[owner]--
	}
	batchFrames := pipeline * 2
	out := make([][][]client.PipelineRequest, workers)
	for w, workerRequests := range requests {
		for offset := 0; offset < len(workerRequests); offset += batchFrames {
			end := offset + batchFrames
			if end > len(workerRequests) {
				end = len(workerRequests)
			}
			out[w] = append(out[w], workerRequests[offset:end])
		}
	}
	return out
}

func runSequential(c *client.Client, batches [][][]client.PipelineRequest) error {
	for _, workerBatches := range batches {
		for _, batch := range workerBatches {
			if err := validatePipeline(c, batch); err != nil {
				return err
			}
		}
	}
	return nil
}

func runConcurrent(c *client.Client, batches [][][]client.PipelineRequest) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(batches))
	start := make(chan struct{})
	for _, workerBatches := range batches {
		workerBatches := workerBatches
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for _, batch := range workerBatches {
				if err := validatePipeline(c, batch); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func validatePipeline(c *client.Client, batch []client.PipelineRequest) error {
	responses, err := c.ExecutePipeline(batch)
	if err != nil {
		return err
	}
	if len(responses) != len(batch) {
		return fmt.Errorf("pipeline response count mismatch")
	}
	for i, response := range responses {
		if !response.Succeeded() {
			return fmt.Errorf("pipeline request failed")
		}
		if batch[i].Opcode == client.PipelineGet {
			if string(response.Value) != string(batch[i-1].Value) {
				return fmt.Errorf("pipeline GET value mismatch")
			}
		}
	}
	return nil
}

func anyPositive(values []int) bool {
	for _, v := range values {
		if v > 0 {
			return true
		}
	}
	return false
}

func bytesFilled(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func median(values []float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

func minFloat(values []float64) float64 {
	m := values[0]
	for _, v := range values[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func maxFloat(values []float64) float64 {
	m := values[0]
	for _, v := range values[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}

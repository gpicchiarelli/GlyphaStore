package client_test

import (
	"encoding/binary"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gpicchiarelli/GlyphaStore/sdk/go/client"
	"github.com/gpicchiarelli/GlyphaStore/sdk/go/protocol"
)

type fakeServer struct {
	workerCount        uint32
	disconnectOnPut    bool
	internalErrorOnPut bool
	stallOnGet         bool
	ln                 net.Listener
	values             map[string][]byte
	valuesMu           sync.Mutex
	wg                 sync.WaitGroup
	stop               chan struct{}
}

func startFakeServer(t *testing.T, workerCount uint32, disconnectOnPut, internalErrorOnPut bool) *fakeServer {
	t.Helper()
	if workerCount == 0 {
		workerCount = 1
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeServer{
		workerCount:        workerCount,
		disconnectOnPut:    disconnectOnPut,
		internalErrorOnPut: internalErrorOnPut,
		ln:                 ln,
		values:             make(map[string][]byte),
		stop:               make(chan struct{}),
	}
	s.wg.Add(1)
	go s.accept()
	return s
}

func (s *fakeServer) port() int {
	return s.ln.Addr().(*net.TCPAddr).Port
}

func (s *fakeServer) close() {
	close(s.stop)
	_ = s.ln.Close()
	s.wg.Wait()
}

func (s *fakeServer) accept() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.stop:
				return
			default:
				return
			}
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.serve(c)
		}(conn)
	}
}

func (s *fakeServer) serve(conn net.Conn) {
	defer conn.Close()
	var bound *uint32
	buf := make([]byte, 0, 8192)
	for {
		for {
			if len(buf) >= 4 {
				size := int(binary.LittleEndian.Uint32(buf[:4]))
				if len(buf) >= size {
					frame := buf[:size]
					buf = buf[size:]
					req, err := protocol.DecodeRequest(frame, protocol.MaxFrameBytes)
					if err != nil {
						return
					}
					if !s.handle(conn, &bound, req) {
						return
					}
					continue
				}
			}
			break
		}
		chunk := make([]byte, 64*1024)
		n, err := conn.Read(chunk)
		if n > 0 {
			buf = append(buf, chunk[:n]...)
		}
		if err != nil {
			return
		}
	}
}

func (s *fakeServer) send(conn net.Conn, status protocol.Status, requestID uint64, owner uint32, value []byte) bool {
	frame, err := protocol.EncodeResponse(status, requestID, value, owner, s.workerCount, 9)
	if err != nil {
		return false
	}
	_, err = conn.Write(frame)
	return err == nil
}

func (s *fakeServer) handle(conn net.Conn, bound **uint32, req protocol.Request) bool {
	switch req.Opcode {
	case protocol.OpcodeInit:
		return s.send(conn, protocol.StatusOK, req.RequestID, 0, []byte(protocol.Identity))
	case protocol.OpcodeBindWorker:
		if req.TargetWorker >= s.workerCount {
			return s.send(conn, protocol.StatusInvalidRequest, req.RequestID, 0, nil)
		}
		w := req.TargetWorker
		*bound = &w
		return s.send(conn, protocol.StatusOK, req.RequestID, w, nil)
	}
	if *bound == nil {
		return s.send(conn, protocol.StatusNotBound, req.RequestID, 0, nil)
	}
	ownerBound := **bound
	switch req.Opcode {
	case protocol.OpcodePing:
		return s.send(conn, protocol.StatusOK, req.RequestID, ownerBound, req.Value)
	case protocol.OpcodePut:
		owner, _ := protocol.WorkerFor(req.Key, s.workerCount)
		if ownerBound != owner {
			return s.send(conn, protocol.StatusWrongOwner, req.RequestID, owner, nil)
		}
		s.valuesMu.Lock()
		s.values[string(req.Key)] = append([]byte(nil), req.Value...)
		s.valuesMu.Unlock()
		if s.disconnectOnPut {
			return false
		}
		if s.internalErrorOnPut {
			return s.send(conn, protocol.StatusInternalError, req.RequestID, ownerBound, nil)
		}
		return s.send(conn, protocol.StatusOK, req.RequestID, ownerBound, nil)
	case protocol.OpcodeGet:
		if s.stallOnGet {
			select {
			case <-s.stop:
			case <-time.After(time.Hour):
			}
			return false
		}
		owner, _ := protocol.WorkerFor(req.Key, s.workerCount)
		if ownerBound != owner {
			return s.send(conn, protocol.StatusWrongOwner, req.RequestID, owner, nil)
		}
		s.valuesMu.Lock()
		value, ok := s.values[string(req.Key)]
		s.valuesMu.Unlock()
		if !ok {
			return s.send(conn, protocol.StatusNotFound, req.RequestID, ownerBound, nil)
		}
		return s.send(conn, protocol.StatusOK, req.RequestID, ownerBound, value)
	case protocol.OpcodeErase:
		owner, _ := protocol.WorkerFor(req.Key, s.workerCount)
		if ownerBound != owner {
			return s.send(conn, protocol.StatusWrongOwner, req.RequestID, owner, nil)
		}
		s.valuesMu.Lock()
		_, ok := s.values[string(req.Key)]
		if ok {
			delete(s.values, string(req.Key))
		}
		s.valuesMu.Unlock()
		status := protocol.StatusOK
		if !ok {
			status = protocol.StatusNotFound
		}
		return s.send(conn, status, req.RequestID, ownerBound, nil)
	default:
		return s.send(conn, protocol.StatusUnsupported, req.RequestID, ownerBound, nil)
	}
}

func TestBinaryPutGetPingAndErase(t *testing.T) {
	server := startFakeServer(t, 1, false, false)
	defer server.close()

	c, err := client.Connect(client.Config{Port: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if c.WorkerCount() != 1 || c.RoutingEpoch() != 9 {
		t.Fatalf("bootstrap metadata unexpected")
	}
	owner, err := c.WorkerFor([]byte("binary\x00key"))
	if err != nil || owner != 0 {
		t.Fatalf("worker_for: %v %d", err, owner)
	}
	if !c.Put([]byte("binary\x00key"), []byte("value\x00\xff"), 0).Committed() {
		t.Fatal("put failed")
	}
	got, err := c.Get([]byte("binary\x00key"))
	if err != nil || string(got) != "value\x00\xff" {
		t.Fatalf("get: %v %q", err, got)
	}
	pong, err := c.Ping([]byte("hello"))
	if err != nil || string(pong) != "hello" {
		t.Fatalf("ping: %v %q", err, pong)
	}
	if !c.Erase([]byte("binary\x00key")).Committed() {
		t.Fatal("erase failed")
	}
	_, err = c.Get([]byte("binary\x00key"))
	if err == nil {
		t.Fatal("expected not found")
	}
	ge, ok := err.(*client.Error)
	if !ok || ge.Category != client.CategoryNotFound {
		t.Fatalf("expected not_found, got %v", err)
	}
}

func TestInternalErrorMutationIsIndeterminate(t *testing.T) {
	server := startFakeServer(t, 1, false, true)
	defer server.close()
	c, err := client.Connect(client.Config{Port: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	result := c.Put([]byte("key"), []byte("value"), 0)
	if result.Outcome != client.MutationIndeterminate {
		t.Fatalf("outcome=%s", result.Outcome)
	}
}

func TestDisconnectAfterMutationIsIndeterminate(t *testing.T) {
	server := startFakeServer(t, 1, true, false)
	defer server.close()
	c, err := client.Connect(client.Config{Port: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	result := c.Put([]byte("key"), []byte("value"), 0)
	if result.Outcome != client.MutationIndeterminate {
		t.Fatalf("outcome=%s err=%v", result.Outcome, result.Err)
	}
}

func TestPipelinePreservesOrder(t *testing.T) {
	server := startFakeServer(t, 1, false, false)
	defer server.close()
	c, err := client.Connect(client.Config{Port: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var requests []client.PipelineRequest
	var expected [][]byte
	for i := 0; i < 64; i++ {
		value := []byte("pipeline-" + strconv.Itoa(i))
		expected = append(expected, value)
		requests = append(requests,
			client.PipelineRequest{Opcode: client.PipelinePut, Key: []byte("key"), Value: value},
			client.PipelineRequest{Opcode: client.PipelineGet, Key: []byte("key")},
		)
	}
	responses, err := c.ExecutePipeline(requests)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != len(requests) {
		t.Fatalf("len=%d", len(responses))
	}
	for i, value := range expected {
		if !responses[i*2].Succeeded() || !responses[i*2+1].Succeeded() {
			t.Fatalf("pipeline outcomes failed at %d", i)
		}
		if string(responses[i*2+1].Value) != string(value) {
			t.Fatalf("ordered value mismatch at %d", i)
		}
	}
}

func TestPipelineDisconnectClassifiesEachRequest(t *testing.T) {
	server := startFakeServer(t, 1, true, false)
	defer server.close()
	c, err := client.Connect(client.Config{Port: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	responses, err := c.ExecutePipeline([]client.PipelineRequest{
		{Opcode: client.PipelinePut, Key: []byte("key"), Value: []byte("value")},
		{Opcode: client.PipelineGet, Key: []byte("key")},
		{Opcode: client.PipelineErase, Key: []byte("key")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if responses[0].Outcome != client.PipelineIndeterminate ||
		responses[1].Outcome != client.PipelineFailed ||
		responses[2].Outcome != client.PipelineIndeterminate {
		t.Fatalf("unexpected outcomes: %+v", responses)
	}
}

func TestPipelineLimitsFailBeforeTransmission(t *testing.T) {
	server := startFakeServer(t, 1, false, false)
	defer server.close()
	c, err := client.Connect(client.Config{Port: server.port(), MaximumPipelineRequests: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = c.ExecutePipeline([]client.PipelineRequest{
		{Opcode: client.PipelineGet, Key: []byte("key")},
		{Opcode: client.PipelineGet, Key: []byte("key")},
	})
	ge, ok := err.(*client.Error)
	if !ok || ge.Category != client.CategoryInvalidArgument {
		t.Fatalf("expected invalid_argument, got %v", err)
	}
}

func TestMultiWorkerBootstrapAndBatch(t *testing.T) {
	server := startFakeServer(t, 2, false, false)
	defer server.close()
	c, err := client.Connect(client.Config{Port: server.port()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.WorkerCount() != 2 {
		t.Fatalf("worker_count=%d", c.WorkerCount())
	}

	keys := make([][]byte, 0, 64)
	owners := map[uint32]bool{}
	for i := 0; i < 64; i++ {
		key := []byte("batch-" + strconv.Itoa(i))
		keys = append(keys, key)
		owner, err := c.WorkerFor(key)
		if err != nil {
			t.Fatal(err)
		}
		owners[owner] = true
	}
	if len(owners) != 2 {
		t.Fatalf("keys did not span both Workers: %v", owners)
	}

	var requests []client.PipelineRequest
	for _, key := range keys {
		rev := reverse(key)
		requests = append(requests, client.PipelineRequest{Opcode: client.PipelinePut, Key: key, Value: rev})
	}
	for _, key := range keys {
		requests = append(requests, client.PipelineRequest{Opcode: client.PipelineGet, Key: key})
	}
	responses, err := c.ExecuteBatch(requests)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != len(requests) {
		t.Fatalf("len=%d", len(responses))
	}
	for i, key := range keys {
		if !responses[i].Succeeded() || !responses[len(keys)+i].Succeeded() {
			t.Fatalf("batch outcomes failed at %d", i)
		}
		if string(responses[len(keys)+i].Value) != string(reverse(key)) {
			t.Fatalf("batch order mismatch at %d", i)
		}
	}

	c.Close()
	limited, err := client.Connect(client.Config{Port: server.port(), MaximumPipelineRequests: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	var key0 []byte
	for _, key := range keys {
		owner, _ := limited.WorkerFor(key)
		if owner == 0 {
			key0 = key
			break
		}
	}
	_, err = limited.ExecuteBatch([]client.PipelineRequest{
		{Opcode: client.PipelineGet, Key: key0},
		{Opcode: client.PipelineGet, Key: key0},
	})
	ge, ok := err.(*client.Error)
	if !ok || ge.Category != client.CategoryInvalidArgument {
		t.Fatalf("expected invalid_argument, got %v", err)
	}
}

func TestPerCallTimeoutOverridesConfig(t *testing.T) {
	server := startFakeServer(t, 1, false, false)
	server.stallOnGet = true
	defer server.close()

	c, err := client.Connect(client.Config{Port: server.port(), RequestTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	started := time.Now()
	_, err = c.Get([]byte("key"), client.CallOptions{Timeout: 50 * time.Millisecond})
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("expected timeout")
	}
	ge, ok := err.(*client.Error)
	if !ok || ge.Category != client.CategoryTransport {
		t.Fatalf("expected transport, got %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("timeout took too long: %v", elapsed)
	}
	_, err = c.Get([]byte("key"), client.CallOptions{Timeout: -1})
	if err == nil {
		t.Fatal("expected invalid timeout")
	}
}

func TestRejectsInvalidConfig(t *testing.T) {
	_, err := client.Connect(client.Config{
		Host:                    "127.0.0.1",
		Port:                    1,
		ConnectTimeout:          time.Second,
		RequestTimeout:          time.Second,
		MaximumFrameBytes:       1,
		MaximumPipelineRequests: 1,
		MaximumPipelineBytes:    40,
	})
	if err == nil {
		t.Fatal("expected invalid config")
	}
}

func TestStructuredErrorFieldsOnInternalPut(t *testing.T) {
	server := startFakeServer(t, 1, false, true)
	defer server.close()
	c, err := client.Connect(client.Config{
		Host:           "127.0.0.1",
		Port:           server.port(),
		ConnectTimeout: time.Second,
		RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	result := c.Put([]byte("key"), []byte("value"), 0)
	if result.Outcome != client.MutationIndeterminate {
		t.Fatalf("outcome=%v", result.Outcome)
	}
	ge, ok := result.Err.(*client.Error)
	if !ok {
		t.Fatalf("expected *client.Error, got %T", result.Err)
	}
	if ge.Category != client.CategoryInternal {
		t.Fatalf("category=%v", ge.Category)
	}
	if ge.WireStatus == nil || *ge.WireStatus != protocol.StatusInternalError {
		t.Fatalf("wire_status=%v", ge.WireStatus)
	}
	if ge.Operation != "put" {
		t.Fatalf("operation=%q", ge.Operation)
	}
	if ge.Retryability != client.RetryReconcileFirst {
		t.Fatalf("retryability=%v", ge.Retryability)
	}
	if ge.MutationOutcome != client.MutationIndeterminate {
		t.Fatalf("mutation_outcome=%v", ge.MutationOutcome)
	}
}

func reverse(in []byte) []byte {
	out := make([]byte, len(in))
	for i := range in {
		out[len(in)-1-i] = in[i]
	}
	return out
}

// Package client is the official synchronous GlyphaStore Go TCP client.
//
// It implements wire protocol v2 and client-semantics v1: one bound connection
// per Worker, monotonic request deadlines, at-most-one automatic retry for
// safe cases, and committed/rejected/indeterminate mutation outcomes.
package client

import (
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gpicchiarelli/GlyphaStore/sdk/go/protocol"
)

type sendFailure struct {
	err       *Error
	bytesSent int
}

func (e *sendFailure) Error() string { return e.err.Error() }

type connection struct {
	worker uint32
	mu     sync.Mutex
	conn   net.Conn
	input  []byte
	offset int
	encode []byte // scratch for outbound frames; valid only under mu
}

func (c *connection) reset() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.input = c.input[:0]
	c.offset = 0
	// Keep encode capacity across reconnects; only drop length.
	c.encode = c.encode[:0]
}

func (c *connection) encodeScratch(size int) []byte {
	if cap(c.encode) < size {
		c.encode = make([]byte, 0, size)
	}
	return c.encode[:0]
}

// Client is a thread-safe GlyphaStore client with one bound TCP connection per Worker.
type Client struct {
	cfg          Config
	connections  []*connection
	workerCount  uint32
	routingEpoch uint64
	requestID    atomic.Uint64
	healthy      atomic.Bool
}

// Connect dials the server, bootstraps every Worker connection, and returns a ready client.
func Connect(cfg Config) (*Client, error) {
	cfg = mergeConfig(cfg)
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	client := &Client{cfg: cfg}
	client.healthy.Store(true)
	client.requestID.Store(1)

	first := &connection{worker: 0}
	workerCount, routingEpoch, err := client.bootstrap(first, nil)
	if err != nil {
		client.Close()
		return nil, err
	}
	client.workerCount = workerCount
	client.routingEpoch = routingEpoch
	client.connections = append(client.connections, first)
	expected := [2]uint64{uint64(workerCount), routingEpoch}
	for worker := uint32(1); worker < workerCount; worker++ {
		conn := &connection{worker: worker}
		if _, _, err := client.bootstrap(conn, &expected); err != nil {
			client.Close()
			return nil, err
		}
		client.connections = append(client.connections, conn)
	}
	return client, nil
}

func mergeConfig(cfg Config) Config {
	defaults := DefaultConfig()
	if cfg.Host == "" {
		cfg.Host = defaults.Host
	}
	if cfg.Port == 0 {
		cfg.Port = defaults.Port
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = defaults.ConnectTimeout
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = defaults.RequestTimeout
	}
	if cfg.MaximumFrameBytes == 0 {
		cfg.MaximumFrameBytes = defaults.MaximumFrameBytes
	}
	if cfg.MaximumPipelineRequests == 0 {
		cfg.MaximumPipelineRequests = defaults.MaximumPipelineRequests
	}
	if cfg.MaximumPipelineBytes == 0 {
		cfg.MaximumPipelineBytes = defaults.MaximumPipelineBytes
	}
	return cfg
}

func validateConfig(cfg Config) error {
	if cfg.Host == "" ||
		cfg.Port <= 0 || cfg.Port > 65535 ||
		cfg.ConnectTimeout <= 0 ||
		cfg.RequestTimeout <= 0 ||
		cfg.MaximumFrameBytes < protocol.ResponseHeaderBytes ||
		cfg.MaximumFrameBytes > protocol.MaxFrameBytes ||
		cfg.MaximumPipelineRequests <= 0 ||
		cfg.MaximumPipelineBytes < 40 {
		return invalidArgument("client configuration is outside protocol limits")
	}
	return nil
}

// WorkerCount returns the discovered Worker mesh size.
func (c *Client) WorkerCount() uint32 { return c.workerCount }

// RoutingEpoch returns the session routing epoch.
func (c *Client) RoutingEpoch() uint64 { return c.routingEpoch }

// Healthy reports whether the client may still be used.
func (c *Client) Healthy() bool { return c.healthy.Load() }

// WorkerFor returns the owning Worker for key.
func (c *Client) WorkerFor(key []byte) (uint32, error) {
	if c.workerCount == 0 {
		return 0, unavailable("client is not connected")
	}
	return protocol.WorkerFor(key, c.workerCount)
}

// Get loads a value by key.
func (c *Client) Get(key []byte, opts ...CallOptions) ([]byte, error) {
	return c.read(protocol.OpcodeGet, key, nil, opts...)
}

// Ping echoes an opaque payload via Worker 0.
func (c *Client) Ping(payload []byte, opts ...CallOptions) ([]byte, error) {
	return c.read(protocol.OpcodePing, nil, payload, opts...)
}

// Put stores a value. Outcomes are returned; the call does not fail solely for rejection.
func (c *Client) Put(key, value []byte, expireAtNs uint64, opts ...CallOptions) MutationResult {
	return c.mutate(protocol.OpcodePut, key, value, expireAtNs, opts...)
}

// Erase deletes a key.
func (c *Client) Erase(key []byte, opts ...CallOptions) MutationResult {
	return c.mutate(protocol.OpcodeErase, key, nil, 0, opts...)
}

func (c *Client) resolveDeadline(opts ...CallOptions) (time.Time, error) {
	timeout := c.cfg.RequestTimeout
	if len(opts) > 0 {
		if opts[0].Timeout < 0 {
			return time.Time{}, invalidArgument("request timeout must be positive")
		}
		if opts[0].Timeout > 0 {
			timeout = opts[0].Timeout
		}
	}
	if timeout <= 0 {
		return time.Time{}, invalidArgument("request timeout must be positive")
	}
	return time.Now().Add(timeout), nil
}

// ExecutePipeline runs an ordered, non-atomic pipeline on one Worker.
func (c *Client) ExecutePipeline(requests []PipelineRequest, opts ...CallOptions) ([]PipelineResponse, error) {
	deadline, err := c.resolveDeadline(opts...)
	if err != nil {
		return nil, err
	}
	return c.executePipelineDeadline(requests, deadline)
}

func (c *Client) executePipelineDeadline(requests []PipelineRequest, deadline time.Time) ([]PipelineResponse, error) {
	if len(requests) == 0 {
		return nil, nil
	}
	if !c.healthy.Load() {
		return nil, unavailable("client is closed or routing metadata changed")
	}
	if len(requests) > c.cfg.MaximumPipelineRequests {
		return nil, invalidArgument("pipeline exceeds the configured request limit")
	}

	type meta struct {
		opcode    PipelineOpcode
		requestID uint64
		begin     int
	}
	normalized := make([]meta, 0, len(requests))
	var worker *uint32
	needed := 0
	for _, request := range requests {
		if request.Opcode != PipelineGet && request.Opcode != PipelinePut && request.Opcode != PipelineErase {
			return nil, invalidArgument("pipeline request contains an invalid opcode")
		}
		owner, err := c.WorkerFor(request.Key)
		if err != nil {
			return nil, err
		}
		if worker == nil {
			w := owner
			worker = &w
		} else if owner != *worker {
			return nil, invalidArgument("every pipeline key must route to the same Worker")
		}
		if (request.Opcode == PipelineGet || request.Opcode == PipelineErase) &&
			(len(request.Value) != 0 || request.ExpireAtNs != 0) {
			return nil, invalidArgument("GET and ERASE pipeline requests cannot carry PUT fields")
		}
		frameLen := protocol.RequestFrameSize(request.Key, request.Value)
		if frameLen > c.cfg.MaximumFrameBytes {
			return nil, invalidArgument("pipeline request exceeds the configured frame limit")
		}
		if frameLen > c.cfg.MaximumPipelineBytes-needed {
			return nil, invalidArgument("pipeline exceeds the configured aggregate byte limit")
		}
		needed += frameLen
	}

	responses := make([]PipelineResponse, len(requests))
	for i := range responses {
		responses[i] = PipelineResponse{Outcome: PipelineFailed}
	}
	conn := c.connections[*worker]
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !c.healthy.Load() {
		return nil, unavailable("client closed before pipeline admission")
	}
	if err := c.ensureConnected(conn); err != nil {
		return nil, err
	}

	output := conn.encodeScratch(needed)
	outputSize := 0
	for _, request := range requests {
		requestID := c.nextRequestID()
		begin := outputSize
		before := len(output)
		var err error
		output, err = protocol.AppendRequest(
			output,
			protocol.Opcode(request.Opcode),
			requestID,
			request.Key,
			request.Value,
			request.ExpireAtNs,
			protocol.NoWorker,
		)
		if err != nil {
			return nil, invalidArgument(err.Error())
		}
		frameLen := len(output) - before
		normalized = append(normalized, meta{opcode: request.Opcode, requestID: requestID, begin: begin})
		outputSize += frameLen
	}
	conn.encode = output[:0]

	markUnresolved := func(first int, err error, bytesSent int) {
		for index := first; index < len(normalized); index++ {
			opcode := normalized[index].opcode
			mutationMayHaveArrived := (opcode == PipelinePut || opcode == PipelineErase) &&
				bytesSent > normalized[index].begin
			outcome := PipelineFailed
			if mutationMayHaveArrived {
				outcome = PipelineIndeterminate
			}
			responses[index] = PipelineResponse{Outcome: outcome, Err: err}
		}
	}

	if err := c.send(conn, output, deadline); err != nil {
		var sf *sendFailure
		if asSendFailure(err, &sf) {
			conn.reset()
			markUnresolved(0, sf.err, sf.bytesSent)
			return responses, nil
		}
		conn.reset()
		markUnresolved(0, err, 0)
		return responses, nil
	}

	if err := conn.conn.SetReadDeadline(deadline); err != nil {
		conn.reset()
		markUnresolved(0, transport("response receive failed: "+err.Error()), len(output))
		return responses, nil
	}
	for index, item := range normalized {
		response, err := c.receiveResponse(conn, time.Time{})
		if err != nil {
			conn.reset()
			markUnresolved(index, err, len(output))
			return responses, nil
		}
		if err := c.validateResponse(response, item.requestID, *worker); err != nil {
			conn.reset()
			markUnresolved(index, err, len(output))
			return responses, nil
		}
		if response.Status == protocol.StatusOK {
			if (item.opcode == PipelinePut || item.opcode == PipelineErase) && len(response.Value) != 0 {
				conn.reset()
				markUnresolved(index, protocolErr("mutation response value must be empty"), len(output))
				return responses, nil
			}
			responses[index] = PipelineResponse{Outcome: PipelineSucceeded, Value: response.Value}
			continue
		}
		err = statusError(response.Status)
		outcome := PipelineFailed
		if (item.opcode == PipelinePut || item.opcode == PipelineErase) &&
			response.Status == protocol.StatusInternalError {
			outcome = PipelineIndeterminate
		}
		responses[index] = PipelineResponse{Outcome: outcome, Err: err}
		if response.Status == protocol.StatusWrongOwner || response.Status == protocol.StatusNotBound {
			c.healthy.Store(false)
		}
	}
	return responses, nil
}

// ExecuteBatch groups by Worker, runs one pipeline per Worker concurrently, and
// restores caller order. It is not an atomic transaction.
func (c *Client) ExecuteBatch(requests []PipelineRequest, opts ...CallOptions) ([]PipelineResponse, error) {
	if len(requests) == 0 {
		return nil, nil
	}
	if !c.healthy.Load() {
		return nil, unavailable("client is closed or routing metadata changed")
	}
	deadline, err := c.resolveDeadline(opts...)
	if err != nil {
		return nil, err
	}

	type item struct {
		index   int
		request PipelineRequest
	}
	groups := make(map[uint32][]item)
	for index, request := range requests {
		if request.Opcode != PipelineGet && request.Opcode != PipelinePut && request.Opcode != PipelineErase {
			return nil, invalidArgument("batch request contains an invalid opcode")
		}
		if (request.Opcode == PipelineGet || request.Opcode == PipelineErase) &&
			(len(request.Value) != 0 || request.ExpireAtNs != 0) {
			return nil, invalidArgument("GET and ERASE batch requests cannot carry PUT fields")
		}
		worker, err := c.WorkerFor(request.Key)
		if err != nil {
			return nil, err
		}
		bucket := groups[worker]
		if len(bucket) >= c.cfg.MaximumPipelineRequests {
			return nil, invalidArgument("batch exceeds the configured per-Worker request limit")
		}
		groups[worker] = append(bucket, item{index: index, request: request})
	}

	responses := make([]PipelineResponse, len(requests))
	for i := range responses {
		responses[i] = PipelineResponse{Outcome: PipelineFailed}
	}

	type result struct {
		indices []int
		resps   []PipelineResponse
		err     error
	}

	runGroup := func(items []item) result {
		indices := make([]int, len(items))
		reqs := make([]PipelineRequest, len(items))
		for i, it := range items {
			indices[i] = it.index
			reqs[i] = it.request
		}
		resps, err := c.executePipelineDeadline(reqs, deadline)
		return result{indices: indices, resps: resps, err: err}
	}

	if len(groups) == 1 {
		for _, items := range groups {
			out := runGroup(items)
			if out.err != nil {
				return nil, out.err
			}
			for i, index := range out.indices {
				responses[index] = out.resps[i]
			}
		}
		return responses, nil
	}

	type groupOut struct {
		out result
	}
	outs := make([]groupOut, 0, len(groups))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, items := range groups {
		items := items
		wg.Add(1)
		go func() {
			defer wg.Done()
			out := runGroup(items)
			mu.Lock()
			defer mu.Unlock()
			if out.err != nil {
				if firstErr == nil {
					firstErr = out.err
				}
				return
			}
			outs = append(outs, groupOut{out: out})
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	for _, item := range outs {
		for i, index := range item.out.indices {
			responses[index] = item.out.resps[i]
		}
	}
	return responses, nil
}

// Close marks the client unhealthy and closes every Worker connection.
func (c *Client) Close() {
	c.healthy.Store(false)
	for _, conn := range c.connections {
		conn.mu.Lock()
		conn.reset()
		conn.mu.Unlock()
	}
}

func (c *Client) nextRequestID() uint64 {
	for {
		current := c.requestID.Load()
		next := current + 1
		if current == 0xFFFF_FFFF_FFFF_FFFF {
			next = 1
		}
		if c.requestID.CompareAndSwap(current, next) {
			return current
		}
	}
}

func (c *Client) dial() (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(c.cfg.Host, strconv.Itoa(c.cfg.Port)), c.cfg.ConnectTimeout)
	if err != nil {
		return nil, unavailable("could not connect to GlyphaStore: " + err.Error())
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	return conn, nil
}

func (c *Client) bootstrap(conn *connection, expected *[2]uint64) (uint32, uint64, error) {
	conn.reset()
	raw, err := c.dial()
	if err != nil {
		return 0, 0, err
	}
	conn.conn = raw
	deadline := time.Now().Add(c.cfg.RequestTimeout)

	initID := c.nextRequestID()
	frame, err := protocol.EncodeRequest(protocol.OpcodeInit, initID, nil, nil, 0, protocol.NoWorker)
	if err != nil {
		conn.reset()
		return 0, 0, invalidArgument(err.Error())
	}
	response, err := c.exchange(conn, frame, deadline)
	if err != nil {
		conn.reset()
		if sf, ok := err.(*sendFailure); ok {
			return 0, 0, unavailable(sf.err.Message)
		}
		if ge, ok := err.(*Error); ok && ge.Category == CategoryTransport {
			return 0, 0, unavailable(ge.Message)
		}
		return 0, 0, err
	}
	if response.Status != protocol.StatusOK ||
		response.RequestID != initID ||
		string(response.Value) != protocol.Identity ||
		response.WorkerCount == 0 || response.WorkerCount > 256 ||
		response.RoutingEpoch == 0 {
		conn.reset()
		return 0, 0, protocolErr("server INIT response is inconsistent")
	}
	if expected != nil &&
		(uint64(response.WorkerCount) != expected[0] || response.RoutingEpoch != expected[1]) {
		conn.reset()
		return 0, 0, unavailable("server routing metadata changed during bootstrap")
	}

	bindID := c.nextRequestID()
	bindFrame, err := protocol.EncodeRequest(protocol.OpcodeBindWorker, bindID, nil, nil, 0, conn.worker)
	if err != nil {
		conn.reset()
		return 0, 0, invalidArgument(err.Error())
	}
	bound, err := c.exchange(conn, bindFrame, deadline)
	if err != nil {
		conn.reset()
		if sf, ok := err.(*sendFailure); ok {
			return 0, 0, unavailable(sf.err.Message)
		}
		if ge, ok := err.(*Error); ok && ge.Category == CategoryTransport {
			return 0, 0, unavailable(ge.Message)
		}
		return 0, 0, err
	}
	if bound.Status != protocol.StatusOK ||
		bound.RequestID != bindID ||
		bound.OwnerWorker != conn.worker ||
		bound.WorkerCount != response.WorkerCount ||
		bound.RoutingEpoch != response.RoutingEpoch {
		conn.reset()
		return 0, 0, protocolErr("server BIND_WORKER response is inconsistent")
	}
	return response.WorkerCount, response.RoutingEpoch, nil
}

func (c *Client) ensureConnected(conn *connection) error {
	if conn.conn != nil {
		return nil
	}
	expected := [2]uint64{uint64(c.workerCount), c.routingEpoch}
	_, _, err := c.bootstrap(conn, &expected)
	return err
}

func (c *Client) send(conn *connection, frame []byte, deadline time.Time) error {
	if err := conn.conn.SetWriteDeadline(deadline); err != nil {
		return &sendFailure{err: transport("request send failed: " + err.Error()), bytesSent: 0}
	}
	sent := 0
	for sent < len(frame) {
		n, err := conn.conn.Write(frame[sent:])
		if n > 0 {
			sent += n
		}
		if err != nil {
			msg := "request send failed: " + err.Error()
			if isDeadline(err) {
				msg = "request deadline expired"
			}
			return &sendFailure{err: transport(msg), bytesSent: sent}
		}
		if n == 0 {
			return &sendFailure{err: transport("socket closed during send"), bytesSent: sent}
		}
	}
	return nil
}

func (c *Client) receiveResponse(conn *connection, deadline time.Time) (protocol.Response, error) {
	if !deadline.IsZero() {
		if err := conn.conn.SetReadDeadline(deadline); err != nil {
			return protocol.Response{}, transport("response receive failed: " + err.Error())
		}
	}
	for {
		available := len(conn.input) - conn.offset
		if available >= 4 {
			frameSize := int(binary.LittleEndian.Uint32(conn.input[conn.offset : conn.offset+4]))
			if frameSize < protocol.ResponseHeaderBytes || frameSize > c.cfg.MaximumFrameBytes {
				return protocol.Response{}, protocolErr("server response size is outside client limits")
			}
			if available >= frameSize {
				start := conn.offset
				frame := conn.input[start : start+frameSize]
				response, err := protocol.DecodeResponseView(frame, c.cfg.MaximumFrameBytes)
				if err != nil {
					return protocol.Response{}, protocolErr(err.Error())
				}
				// Own before compacting the receive buffer (Value may alias frame).
				response.Value = protocol.OwnBytes(response.Value)
				conn.offset += frameSize
				if conn.offset == len(conn.input) {
					conn.input = conn.input[:0]
					conn.offset = 0
				}
				return response, nil
			}
		}
		if conn.offset > 0 {
			copy(conn.input, conn.input[conn.offset:])
			conn.input = conn.input[: len(conn.input)-conn.offset]
			conn.offset = 0
		}
		if cap(conn.input)-len(conn.input) < 64*1024 {
			grown := make([]byte, len(conn.input), len(conn.input)+64*1024)
			copy(grown, conn.input)
			conn.input = grown
		}
		readInto := conn.input[len(conn.input):cap(conn.input)]
		n, err := conn.conn.Read(readInto)
		if n > 0 {
			conn.input = conn.input[:len(conn.input)+n]
		}
		if err != nil {
			if n > 0 && err == io.EOF {
				continue
			}
			if isDeadline(err) {
				return protocol.Response{}, transport("request deadline expired")
			}
			if err == io.EOF {
				return protocol.Response{}, transport("server closed the connection")
			}
			return protocol.Response{}, transport("response receive failed: " + err.Error())
		}
		if n == 0 {
			return protocol.Response{}, transport("server closed the connection")
		}
	}
}

func (c *Client) exchange(conn *connection, frame []byte, deadline time.Time) (protocol.Response, error) {
	if err := c.send(conn, frame, deadline); err != nil {
		return protocol.Response{}, err
	}
	return c.receiveResponse(conn, deadline)
}

func (c *Client) validateResponse(response protocol.Response, requestID uint64, worker uint32) error {
	if response.RequestID != requestID {
		return protocolErr("server response request ID does not match")
	}
	if response.WorkerCount != c.workerCount || response.RoutingEpoch != c.routingEpoch {
		c.healthy.Store(false)
		return unavailable("server routing metadata changed")
	}
	if response.OwnerWorker != worker && response.Status != protocol.StatusWrongOwner {
		c.healthy.Store(false)
		return protocolErr("server response came from the wrong Worker")
	}
	return nil
}

func (c *Client) read(opcode protocol.Opcode, key, value []byte, opts ...CallOptions) ([]byte, error) {
	if !c.healthy.Load() {
		return nil, unavailable("client is closed or routing metadata changed")
	}
	deadline, err := c.resolveDeadline(opts...)
	if err != nil {
		return nil, err
	}
	worker := uint32(0)
	if opcode != protocol.OpcodePing {
		var err error
		worker, err = c.WorkerFor(key)
		if err != nil {
			return nil, err
		}
	}
	conn := c.connections[worker]
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !c.healthy.Load() {
		return nil, unavailable("client closed before read admission")
	}

	var last error = unavailable("request was not attempted")
	for attempt := 0; attempt < 2; attempt++ {
		if err := c.ensureConnected(conn); err != nil {
			last = err
			if !c.healthy.Load() {
				return nil, err
			}
			conn.reset()
			continue
		}
		requestID := c.nextRequestID()
		size := protocol.RequestFrameSize(key, value)
		if size > c.cfg.MaximumFrameBytes {
			return nil, invalidArgument("request exceeds the configured frame limit")
		}
		scratch := conn.encodeScratch(size)
		n, err := protocol.EncodeRequestInto(scratch[:size], opcode, requestID, key, value, 0, protocol.NoWorker)
		if err != nil {
			return nil, invalidArgument(err.Error())
		}
		frame := scratch[:n]
		response, err := c.exchange(conn, frame, deadline)
		conn.encode = scratch[:0]
		if err != nil {
			if sf, ok := err.(*sendFailure); ok {
				last = promoteSendFailure(sf, readOpName(opcode), requestID, worker, c.routingEpoch, false)
			} else if ge, ok := err.(*Error); ok {
				last = annotate(ge, readOpName(opcode), requestID, worker, c.routingEpoch)
			} else {
				last = err
			}
			conn.reset()
			if ge, ok := last.(*Error); ok && ge.Category == CategoryUnavailable && !c.healthy.Load() {
				return nil, last
			}
			continue
		}
		if err := c.validateResponse(response, requestID, worker); err != nil {
			conn.reset()
			if ge, ok := err.(*Error); ok {
				ge = annotate(ge, readOpName(opcode), requestID, worker, c.routingEpoch)
				if ge.Category == CategoryUnavailable {
					if !c.healthy.Load() {
						return nil, ge
					}
					last = ge
					continue
				}
				if ge.Category == CategoryProtocol {
					return nil, ge
				}
				last = ge
				continue
			}
			last = err
			continue
		}
		if response.Status != protocol.StatusOK {
			if response.Status == protocol.StatusWrongOwner || response.Status == protocol.StatusNotBound {
				c.healthy.Store(false)
			}
			return nil, annotate(statusError(response.Status), readOpName(opcode), requestID, worker, c.routingEpoch)
		}
		return response.Value, nil
	}
	return nil, last
}

func readOpName(opcode protocol.Opcode) string {
	switch opcode {
	case protocol.OpcodeGet:
		return "get"
	case protocol.OpcodePing:
		return "ping"
	default:
		return "read"
	}
}

func (c *Client) mutate(opcode protocol.Opcode, key, value []byte, expireAtNs uint64, opts ...CallOptions) MutationResult {
	op := mutateOpName(opcode)
	if !c.healthy.Load() {
		return MutationResult{Outcome: MutationRejected, Err: unavailable("client is closed or routing metadata changed").withOp(op).withMutation(MutationRejected)}
	}
	deadline, err := c.resolveDeadline(opts...)
	if err != nil {
		if ge, ok := err.(*Error); ok {
			return MutationResult{Outcome: MutationRejected, Err: ge.withOp(op).withMutation(MutationRejected)}
		}
		return MutationResult{Outcome: MutationRejected, Err: err}
	}
	worker, err := c.WorkerFor(key)
	if err != nil {
		if ge, ok := err.(*Error); ok {
			return MutationResult{Outcome: MutationRejected, Err: ge.withOp(op).withMutation(MutationRejected)}
		}
		return MutationResult{Outcome: MutationRejected, Err: err}
	}
	conn := c.connections[worker]
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if !c.healthy.Load() {
		return MutationResult{Outcome: MutationRejected, Err: unavailable("client closed before mutation admission").withOp(op).withSession(worker, c.routingEpoch).withMutation(MutationRejected)}
	}

	for attempt := 0; attempt < 2; attempt++ {
		if err := c.ensureConnected(conn); err != nil {
			if ge, ok := err.(*Error); ok {
				return MutationResult{Outcome: MutationRejected, Err: ge.withOp(op).withSession(worker, c.routingEpoch).withMutation(MutationRejected)}
			}
			return MutationResult{Outcome: MutationRejected, Err: err}
		}
		requestID := c.nextRequestID()
		size := protocol.RequestFrameSize(key, value)
		if size > c.cfg.MaximumFrameBytes {
			return MutationResult{Outcome: MutationRejected, Err: invalidArgument("request exceeds the configured frame limit").withOp(op).withRequest(requestID, worker, c.routingEpoch).withMutation(MutationRejected)}
		}
		scratch := conn.encodeScratch(size)
		n, err := protocol.EncodeRequestInto(scratch[:size], opcode, requestID, key, value, expireAtNs, protocol.NoWorker)
		if err != nil {
			return MutationResult{Outcome: MutationRejected, Err: invalidArgument(err.Error()).withOp(op).withRequest(requestID, worker, c.routingEpoch).withMutation(MutationRejected)}
		}
		frame := scratch[:n]
		response, err := c.exchange(conn, frame, deadline)
		conn.encode = scratch[:0]
		if err != nil {
			if sf, ok := err.(*sendFailure); ok {
				conn.reset()
				promoted := promoteSendFailure(sf, op, requestID, worker, c.routingEpoch, true)
				if sf.bytesSent == 0 {
					if attempt == 0 {
						continue
					}
					return MutationResult{Outcome: MutationRejected, Err: promoted}
				}
				return MutationResult{Outcome: MutationIndeterminate, Err: promoted}
			}
			conn.reset()
			if ge, ok := err.(*Error); ok {
				return MutationResult{Outcome: MutationIndeterminate, Err: annotate(ge, op, requestID, worker, c.routingEpoch).withMutation(MutationIndeterminate)}
			}
			return MutationResult{Outcome: MutationIndeterminate, Err: err}
		}
		if err := c.validateResponse(response, requestID, worker); err != nil {
			conn.reset()
			if ge, ok := err.(*Error); ok {
				return MutationResult{Outcome: MutationIndeterminate, Err: annotate(ge, op, requestID, worker, c.routingEpoch).withMutation(MutationIndeterminate)}
			}
			return MutationResult{Outcome: MutationIndeterminate, Err: err}
		}
		if response.Status == protocol.StatusOK {
			if len(response.Value) != 0 {
				conn.reset()
				return MutationResult{Outcome: MutationIndeterminate, Err: protocolErr("mutation response value must be empty").withOp(op).withRequest(requestID, worker, c.routingEpoch).withMutation(MutationIndeterminate)}
			}
			return MutationResult{Outcome: MutationCommitted}
		}
		statusErr := annotate(statusError(response.Status), op, requestID, worker, c.routingEpoch)
		if response.Status == protocol.StatusInternalError {
			return MutationResult{Outcome: MutationIndeterminate, Err: statusErr.withMutation(MutationIndeterminate)}
		}
		if response.Status == protocol.StatusWrongOwner || response.Status == protocol.StatusNotBound {
			c.healthy.Store(false)
		}
		return MutationResult{Outcome: MutationRejected, Err: statusErr.withMutation(MutationRejected)}
	}
	return MutationResult{Outcome: MutationRejected, Err: unavailable("could not send mutation").withOp(op).withSession(worker, c.routingEpoch).withMutation(MutationRejected)}
}

func mutateOpName(opcode protocol.Opcode) string {
	switch opcode {
	case protocol.OpcodePut:
		return "put"
	case protocol.OpcodeErase:
		return "erase"
	default:
		return "mutate"
	}
}

func asSendFailure(err error, target **sendFailure) bool {
	sf, ok := err.(*sendFailure)
	if ok {
		*target = sf
	}
	return ok
}

func isDeadline(err error) bool {
	if err == nil {
		return false
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return true
	}
	return false
}

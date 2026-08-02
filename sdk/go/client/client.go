// Package client is the official synchronous GlyphaStore Go TCP client.
//
// It implements wire protocol v2 and client-semantics v1: one bound connection
// per Worker, monotonic request deadlines, at-most-one automatic retry for
// safe cases, and committed/rejected/indeterminate mutation outcomes.
package client

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
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
	routing      protocol.WorkerRouting
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
	workerCount, routingEpoch, routing, err := client.bootstrap(first, nil)
	if err != nil {
		client.Close()
		return nil, err
	}
	client.workerCount = workerCount
	client.routingEpoch = routingEpoch
	client.routing = routing
	client.connections = append(client.connections, first)
	expected := &sessionMeta{workerCount: workerCount, routingEpoch: routingEpoch, routing: routing}
	for worker := uint32(1); worker < workerCount; worker++ {
		conn := &connection{worker: worker}
		if _, _, _, err := client.bootstrap(conn, expected); err != nil {
			client.Close()
			return nil, err
		}
		client.connections = append(client.connections, conn)
	}
	return client, nil
}

type sessionMeta struct {
	workerCount  uint32
	routingEpoch uint64
	routing      protocol.WorkerRouting
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
	if cfg.TLS.Enable {
		hasCert := cfg.TLS.CertFile != ""
		hasKey := cfg.TLS.KeyFile != ""
		if hasCert != hasKey {
			return invalidArgument("TLS mTLS requires both CertFile and KeyFile (fail closed)")
		}
	}
	return nil
}

func (c *Client) dial() (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(c.cfg.Host, strconv.Itoa(c.cfg.Port)), c.cfg.ConnectTimeout)
	if err != nil {
		return nil, unavailable("could not connect to GlyphaStore: " + err.Error())
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}
	if !c.cfg.TLS.Enable {
		return conn, nil
	}
	tlsConn, err := c.handshakeTLS(conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return tlsConn, nil
}

func (c *Client) handshakeTLS(conn net.Conn) (*tls.Conn, error) {
	tlsCfg, err := buildTLSConfig(c.cfg)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(c.cfg.ConnectTimeout))
	tlsConn := tls.Client(conn, tlsCfg)
	if err := tlsConn.Handshake(); err != nil {
		return nil, unavailable("TLS handshake failed: " + sanitizeTLSError(err))
	}
	_ = conn.SetDeadline(time.Time{})
	state := tlsConn.ConnectionState()
	if state.Version < tls.VersionTLS13 {
		return nil, unavailable("TLS negotiation produced a version below TLS 1.3")
	}
	return tlsConn, nil
}

func buildTLSConfig(cfg Config) (*tls.Config, error) {
	serverName := cfg.TLS.ServerName
	if serverName == "" {
		serverName = cfg.Host
	}
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		ServerName:         serverName,
		InsecureSkipVerify: cfg.TLS.InsecureSkipVerify, //nolint:gosec // explicit lab escape
	}
	if cfg.TLS.CAFile != "" {
		pem, err := os.ReadFile(cfg.TLS.CAFile)
		if err != nil {
			return nil, invalidArgument("cannot read TLS CA file")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, invalidArgument("TLS CA file does not contain a usable certificate")
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			return nil, invalidArgument("cannot load TLS client certificate/key pair")
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

func sanitizeTLSError(err error) string {
	if err == nil {
		return "unknown TLS error"
	}
	var certErr *tls.CertificateVerificationError
	if errors.As(err, &certErr) {
		return "certificate verification failed"
	}
	msg := err.Error()
	if len(msg) > 200 {
		return "TLS failure"
	}
	return msg
}

// WorkerCount returns the discovered Worker mesh size.
func (c *Client) WorkerCount() uint32 { return c.workerCount }

// RoutingEpoch returns the session routing epoch.
func (c *Client) RoutingEpoch() uint64 { return c.routingEpoch }

// Routing returns the session Worker routing state from INIT.
func (c *Client) Routing() protocol.WorkerRouting { return c.routing }

// Healthy reports whether the client may still be used.
func (c *Client) Healthy() bool { return c.healthy.Load() }

// WorkerFor returns the owning Worker for key.
func (c *Client) WorkerFor(key []byte) (uint32, error) {
	if c.workerCount == 0 {
		return 0, unavailable("client is not connected")
	}
	return protocol.WorkerFor(key, c.workerCount, c.routing)
}

// Get loads a value by key.
func (c *Client) Get(key []byte, opts ...CallOptions) ([]byte, error) {
	return c.read(protocol.OpcodeGet, key, nil, opts...)
}

// Ping echoes an opaque payload via Worker 0.
func (c *Client) Ping(payload []byte, opts ...CallOptions) ([]byte, error) {
	return c.read(protocol.OpcodePing, nil, payload, opts...)
}

// Backup requests an online fenced durable catalog copy (wire BACKUP, opcode 10) via Worker 0.
// Destination is a UTF-8 path to an empty directory. Not zero-impact hot I/O; admin under secure authz.
// On success the returned bytes are a bounded ASCII report containing status=ok.
func (c *Client) Backup(destination string, opts ...CallOptions) ([]byte, error) {
	if destination == "" {
		return nil, invalidArgument("backup destination must be non-empty")
	}
	return c.read(protocol.OpcodeBackup, []byte(destination), nil, opts...)
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
			isMut := opcode == PipelinePut || opcode == PipelineErase
			mutationMayHaveArrived := isMut && bytesSent > normalized[index].begin
			outcome := PipelineFailed
			if mutationMayHaveArrived {
				outcome = PipelineIndeterminate
			}
			posBytes := 0
			if bytesSent > normalized[index].begin {
				posBytes = bytesSent - normalized[index].begin
			}
			enriched := err
			if ge, ok := err.(*Error); ok {
				op := "get"
				switch opcode {
				case PipelinePut:
					op = "put"
				case PipelineErase:
					op = "erase"
				}
				annotated := annotate(ge, op, normalized[index].requestID, *worker, c.routingEpoch).
					withBytesSent(posBytes)
				if isMut {
					mutOutcome := MutationRejected
					if mutationMayHaveArrived {
						mutOutcome = MutationIndeterminate
					}
					annotated = annotated.withMutation(mutOutcome)
				}
				enriched = annotated
			}
			responses[index] = PipelineResponse{Outcome: outcome, Err: enriched}
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
		isMut := item.opcode == PipelinePut || item.opcode == PipelineErase
		indeterminate := isMut && response.Status == protocol.StatusInternalError
		outcome := PipelineFailed
		if indeterminate {
			outcome = PipelineIndeterminate
		}
		op := "get"
		switch item.opcode {
		case PipelinePut:
			op = "put"
		case PipelineErase:
			op = "erase"
		}
		statusErr := annotate(statusError(response.Status), op, item.requestID, *worker, c.routingEpoch).
			withBytesSent(len(output) - item.begin)
		if isMut {
			mutOutcome := MutationRejected
			if indeterminate {
				mutOutcome = MutationIndeterminate
			}
			statusErr = statusErr.withMutation(mutOutcome)
		}
		responses[index] = PipelineResponse{Outcome: outcome, Err: statusErr}
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
	}

	runGroup := func(items []item) result {
		indices := make([]int, len(items))
		reqs := make([]PipelineRequest, len(items))
		for i, it := range items {
			indices[i] = it.index
			reqs[i] = it.request
		}
		resps, err := c.executePipelineDeadline(reqs, deadline)
		if err != nil {
			// Match C++/Erlang: convert group-level pre-admission errors into
			// per-index failed slots with rejected polarity (bytes_sent=0).
			failed := make([]PipelineResponse, len(items))
			for i, it := range items {
				enriched := err
				if ge, ok := err.(*Error); ok {
					op := "get"
					switch it.request.Opcode {
					case PipelinePut:
						op = "put"
					case PipelineErase:
						op = "erase"
					}
					worker, _ := c.WorkerFor(it.request.Key)
					annotated := annotate(ge, op, 0, worker, c.routingEpoch).withBytesSent(0)
					if it.request.Opcode == PipelinePut || it.request.Opcode == PipelineErase {
						annotated = annotated.withMutation(MutationRejected)
					}
					enriched = annotated
				}
				failed[i] = PipelineResponse{Outcome: PipelineFailed, Err: enriched}
			}
			return result{indices: indices, resps: failed}
		}
		return result{indices: indices, resps: resps}
	}

	if len(groups) == 1 {
		for _, items := range groups {
			out := runGroup(items)
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
	for _, items := range groups {
		items := items
		wg.Add(1)
		go func() {
			defer wg.Done()
			out := runGroup(items)
			mu.Lock()
			defer mu.Unlock()
			outs = append(outs, groupOut{out: out})
		}()
	}
	wg.Wait()
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

func (c *Client) bootstrap(conn *connection, expected *sessionMeta) (uint32, uint64, protocol.WorkerRouting, error) {
	conn.reset()
	raw, err := c.dial()
	if err != nil {
		return 0, 0, protocol.WorkerRouting{}, err
	}
	conn.conn = raw
	deadline := time.Now().Add(c.cfg.RequestTimeout)

	initID := c.nextRequestID()
	frame, err := protocol.EncodeRequest(protocol.OpcodeInit, initID, nil, nil, 0, protocol.NoWorker)
	if err != nil {
		conn.reset()
		return 0, 0, protocol.WorkerRouting{}, invalidArgument(err.Error())
	}
	response, err := c.exchange(conn, frame, deadline)
	if err != nil {
		conn.reset()
		if sf, ok := err.(*sendFailure); ok {
			return 0, 0, protocol.WorkerRouting{}, unavailable(sf.err.Message)
		}
		if ge, ok := err.(*Error); ok && ge.Category == CategoryTransport {
			return 0, 0, protocol.WorkerRouting{}, unavailable(ge.Message)
		}
		return 0, 0, protocol.WorkerRouting{}, err
	}
	if response.Status != protocol.StatusOK ||
		response.RequestID != initID ||
		response.WorkerCount == 0 || response.WorkerCount > 256 ||
		response.RoutingEpoch == 0 {
		conn.reset()
		return 0, 0, protocol.WorkerRouting{}, protocolErr("server INIT response is inconsistent")
	}
	routing, err := protocol.DecodeInitIdentity(response.Value)
	if err != nil {
		conn.reset()
		return 0, 0, protocol.WorkerRouting{}, protocolErr("server INIT response is inconsistent")
	}
	meta := sessionMeta{
		workerCount:  response.WorkerCount,
		routingEpoch: response.RoutingEpoch,
		routing:      routing,
	}
	if expected != nil && *expected != meta {
		conn.reset()
		return 0, 0, protocol.WorkerRouting{}, unavailable("server routing metadata changed during bootstrap")
	}

	bindID := c.nextRequestID()
	bindFrame, err := protocol.EncodeRequest(protocol.OpcodeBindWorker, bindID, nil, nil, 0, conn.worker)
	if err != nil {
		conn.reset()
		return 0, 0, protocol.WorkerRouting{}, invalidArgument(err.Error())
	}
	bound, err := c.exchange(conn, bindFrame, deadline)
	if err != nil {
		conn.reset()
		if sf, ok := err.(*sendFailure); ok {
			return 0, 0, protocol.WorkerRouting{}, unavailable(sf.err.Message)
		}
		if ge, ok := err.(*Error); ok && ge.Category == CategoryTransport {
			return 0, 0, protocol.WorkerRouting{}, unavailable(ge.Message)
		}
		return 0, 0, protocol.WorkerRouting{}, err
	}
	if bound.Status != protocol.StatusOK ||
		bound.RequestID != bindID ||
		bound.OwnerWorker != conn.worker ||
		bound.WorkerCount != response.WorkerCount ||
		bound.RoutingEpoch != response.RoutingEpoch {
		conn.reset()
		return 0, 0, protocol.WorkerRouting{}, protocolErr("server BIND_WORKER response is inconsistent")
	}
	return response.WorkerCount, response.RoutingEpoch, routing, nil
}

func (c *Client) ensureConnected(conn *connection) error {
	if conn.conn != nil {
		return nil
	}
	expected := &sessionMeta{
		workerCount:  c.workerCount,
		routingEpoch: c.routingEpoch,
		routing:      c.routing,
	}
	_, _, _, err := c.bootstrap(conn, expected)
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
	if opcode != protocol.OpcodePing && opcode != protocol.OpcodeBackup {
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
			bytesSent := len(frame)
			if sf, ok := err.(*sendFailure); ok {
				bytesSent = sf.bytesSent
				last = promoteSendFailure(sf, readOpName(opcode), requestID, worker, c.routingEpoch,
					opcode == protocol.OpcodeBackup && sf.bytesSent > 0)
			} else if ge, ok := err.(*Error); ok {
				last = annotate(ge, readOpName(opcode), requestID, worker, c.routingEpoch)
				if opcode == protocol.OpcodeBackup {
					last = last.(*Error).withBytesSent(bytesSent).withMutation(MutationIndeterminate)
				}
			} else {
				last = err
			}
			conn.reset()
			if opcode == protocol.OpcodeBackup && bytesSent > 0 {
				return nil, last
			}
			if ge, ok := last.(*Error); ok && ge.Category == CategoryUnavailable && !c.healthy.Load() {
				return nil, last
			}
			continue
		}
		if err := c.validateResponse(response, requestID, worker); err != nil {
			conn.reset()
			if ge, ok := err.(*Error); ok {
				ge = annotate(ge, readOpName(opcode), requestID, worker, c.routingEpoch)
				if opcode == protocol.OpcodeBackup {
					// Response arrived after send — fenced copy may already exist.
					return nil, ge.withBytesSent(len(frame)).withMutation(MutationIndeterminate)
				}
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
			statusErr := annotate(statusError(response.Status), readOpName(opcode), requestID, worker, c.routingEpoch)
			if opcode == protocol.OpcodeBackup && response.Status == protocol.StatusInternalError {
				// Fenced copy may already be committed — same polarity as C++.
				return nil, statusErr.withBytesSent(len(frame)).withMutation(MutationIndeterminate)
			}
			return nil, statusErr
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
	case protocol.OpcodeBackup:
		return "backup"
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
				return MutationResult{Outcome: MutationIndeterminate, Err: annotate(ge, op, requestID, worker, c.routingEpoch).withBytesSent(len(frame)).withMutation(MutationIndeterminate)}
			}
			return MutationResult{Outcome: MutationIndeterminate, Err: err}
		}
		if err := c.validateResponse(response, requestID, worker); err != nil {
			conn.reset()
			if ge, ok := err.(*Error); ok {
				return MutationResult{Outcome: MutationIndeterminate, Err: annotate(ge, op, requestID, worker, c.routingEpoch).withBytesSent(len(frame)).withMutation(MutationIndeterminate)}
			}
			return MutationResult{Outcome: MutationIndeterminate, Err: err}
		}
		if response.Status == protocol.StatusOK {
			if len(response.Value) != 0 {
				conn.reset()
				return MutationResult{Outcome: MutationIndeterminate, Err: protocolErr("mutation response value must be empty").withOp(op).withRequest(requestID, worker, c.routingEpoch).withBytesSent(len(frame)).withMutation(MutationIndeterminate)}
			}
			return MutationResult{Outcome: MutationCommitted}
		}
		statusErr := annotate(statusError(response.Status), op, requestID, worker, c.routingEpoch).
			withBytesSent(len(frame))
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

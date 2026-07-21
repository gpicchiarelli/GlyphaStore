package protocol_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/gpicchiarelli/GlyphaStore/sdk/go/protocol"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	path := filepath.Join(filepath.Dir(file), "..", "testdata", name)
	text, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	tokens := strings.Fields(string(text))
	out := make([]byte, 0, len(tokens))
	for _, token := range tokens {
		value, err := strconv.ParseUint(token, 16, 8)
		if err != nil {
			t.Fatalf("bad hex token %q: %v", token, err)
		}
		out = append(out, byte(value))
	}
	return out
}

func frames(corpus []byte) [][]byte {
	var out [][]byte
	for offset := 0; offset < len(corpus); {
		size := int(uint32(corpus[offset]) | uint32(corpus[offset+1])<<8 |
			uint32(corpus[offset+2])<<16 | uint32(corpus[offset+3])<<24)
		out = append(out, corpus[offset:offset+size])
		offset += size
	}
	return out
}

func TestRequestEncoderMatchesEveryCanonicalFixture(t *testing.T) {
	expected := frames(fixture(t, "wire_requests_v2.hex"))
	encoded := make([][]byte, 0, 6)
	frame, err := protocol.EncodeRequest(protocol.OpcodeInit, 1, nil, nil, 0, protocol.NoWorker)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, frame)
	frame, err = protocol.EncodeRequest(protocol.OpcodePing, 2, nil, []byte("\x00ping\xff"), 0, protocol.NoWorker)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, frame)
	frame, err = protocol.EncodeRequest(protocol.OpcodeGet, 3, []byte("get\x00key"), nil, 0, protocol.NoWorker)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, frame)
	frame, err = protocol.EncodeRequest(protocol.OpcodePut, 4, []byte("put\x00key"), []byte{0x10, 0x20, 0xff}, 123_456_789, protocol.NoWorker)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, frame)
	frame, err = protocol.EncodeRequest(protocol.OpcodeErase, 5, []byte("erase-key"), nil, 0, protocol.NoWorker)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, frame)
	frame, err = protocol.EncodeRequest(protocol.OpcodeBindWorker, 6, nil, nil, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, frame)
	frame, err = protocol.EncodeRequest(protocol.OpcodeHealth, 7, nil, nil, 0, protocol.NoWorker)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, frame)
	frame, err = protocol.EncodeRequest(protocol.OpcodeReady, 8, nil, nil, 0, protocol.NoWorker)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, frame)
	frame, err = protocol.EncodeRequest(protocol.OpcodeStats, 9, nil, nil, 0, protocol.NoWorker)
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, frame)

	if len(encoded) != len(expected) {
		t.Fatalf("frame count: got %d want %d", len(encoded), len(expected))
	}
	for i := range expected {
		if string(encoded[i]) != string(expected[i]) {
			t.Fatalf("request fixture mismatch at index %d", i)
		}
	}
}

func TestRequestDecoderRoundTripsCanonicalFixture(t *testing.T) {
	raw := frames(fixture(t, "wire_requests_v2.hex"))
	decoded := make([]protocol.Request, 0, len(raw))
	for _, frame := range raw {
		req, err := protocol.DecodeRequest(frame, protocol.MaxFrameBytes)
		if err != nil {
			t.Fatal(err)
		}
		decoded = append(decoded, req)
	}
	wantOpcodes := []protocol.Opcode{
		protocol.OpcodeInit, protocol.OpcodePing, protocol.OpcodeGet,
		protocol.OpcodePut, protocol.OpcodeErase, protocol.OpcodeBindWorker,
		protocol.OpcodeHealth, protocol.OpcodeReady, protocol.OpcodeStats,
	}
	for i, opcode := range wantOpcodes {
		if decoded[i].Opcode != opcode {
			t.Fatalf("opcode[%d]=%d want %d", i, decoded[i].Opcode, opcode)
		}
	}
	if string(decoded[1].Value) != "\x00ping\xff" {
		t.Fatalf("ping value mismatch")
	}
	if decoded[3].ExpireAtNs != 123_456_789 {
		t.Fatalf("expire_at_ns=%d", decoded[3].ExpireAtNs)
	}
	if decoded[5].TargetWorker != 2 {
		t.Fatalf("target_worker=%d", decoded[5].TargetWorker)
	}
	for i, req := range decoded {
		re, err := protocol.EncodeRequest(req.Opcode, req.RequestID, req.Key, req.Value, req.ExpireAtNs, req.TargetWorker)
		if err != nil {
			t.Fatal(err)
		}
		if string(re) != string(raw[i]) {
			t.Fatalf("reencode mismatch at %d", i)
		}
	}
}

func TestResponseDecoderMatchesEveryCanonicalFixture(t *testing.T) {
	decoded := make([]protocol.Response, 0, 8)
	for _, frame := range frames(fixture(t, "wire_responses_v2.hex")) {
		resp, err := protocol.DecodeResponse(frame, protocol.MaxFrameBytes)
		if err != nil {
			t.Fatal(err)
		}
		decoded = append(decoded, resp)
	}
	for i := 0; i <= int(protocol.StatusNotBound); i++ {
		if decoded[i].Status != protocol.Status(i) {
			t.Fatalf("status[%d]=%d", i, decoded[i].Status)
		}
	}
	if string(decoded[0].Value) != protocol.Identity {
		t.Fatalf("identity mismatch")
	}
	if decoded[6].OwnerWorker != 2 {
		t.Fatalf("owner_worker=%d", decoded[6].OwnerWorker)
	}
	for _, resp := range decoded {
		if resp.WorkerCount != 4 {
			t.Fatalf("worker_count=%d", resp.WorkerCount)
		}
	}
}

func TestDecodeResponseViewEmptyValueIsNil(t *testing.T) {
	frame, err := protocol.EncodeResponse(protocol.StatusOK, 1, nil, 0, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	view, err := protocol.DecodeResponseView(frame, protocol.MaxFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	if view.Value != nil {
		t.Fatalf("expected nil empty value, got %#v", view.Value)
	}
	owned, err := protocol.DecodeResponse(frame, protocol.MaxFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	if owned.Value != nil {
		t.Fatalf("expected nil owned empty value, got %#v", owned.Value)
	}
}

func TestOwnBytes(t *testing.T) {
	if protocol.OwnBytes(nil) != nil || protocol.OwnBytes([]byte{}) != nil {
		t.Fatal("empty should be nil")
	}
	src := []byte("abc")
	out := protocol.OwnBytes(src)
	if string(out) != "abc" {
		t.Fatal(out)
	}
	src[0] = 'z'
	if string(out) != "abc" {
		t.Fatal("OwnBytes must copy")
	}
}

func TestResponseEncoderRoundTripsCanonicalFixture(t *testing.T) {
	raw := frames(fixture(t, "wire_responses_v2.hex"))
	for i, frame := range raw {
		resp, err := protocol.DecodeResponse(frame, protocol.MaxFrameBytes)
		if err != nil {
			t.Fatal(err)
		}
		re, err := protocol.EncodeResponse(resp.Status, resp.RequestID, resp.Value, resp.OwnerWorker, resp.WorkerCount, resp.RoutingEpoch)
		if err != nil {
			t.Fatal(err)
		}
		if string(re) != string(frame) {
			t.Fatalf("response reencode mismatch at %d", i)
		}
	}
}

func TestDecoderRejectsNoncanonicalAndTruncatedFrames(t *testing.T) {
	request := append([]byte(nil), frames(fixture(t, "wire_requests_v2.hex"))[0]...)
	request[36] = 1
	if _, err := protocol.DecodeRequest(request, protocol.MaxFrameBytes); err == nil {
		t.Fatal("expected noncanonical request rejection")
	}
	if _, err := protocol.DecodeRequest(request[:protocol.RequestHeaderBytes-1], protocol.MaxFrameBytes); err == nil {
		t.Fatal("expected truncated request rejection")
	}

	response := append([]byte(nil), frames(fixture(t, "wire_responses_v2.hex"))[0]...)
	response[28] = 1
	if _, err := protocol.DecodeResponse(response, protocol.MaxFrameBytes); err == nil {
		t.Fatal("expected noncanonical response rejection")
	}
	if _, err := protocol.DecodeResponse(response[:protocol.ResponseHeaderBytes-1], protocol.MaxFrameBytes); err == nil {
		t.Fatal("expected truncated response rejection")
	}
}

func TestRoutingIsBinaryAndDeterministic(t *testing.T) {
	owner, err := protocol.WorkerFor(nil, 4)
	if err != nil {
		t.Fatal(err)
	}
	if owner != 1 {
		t.Fatalf("worker_for(empty,4)=%d want 1", owner)
	}
	a, err := protocol.WorkerFor([]byte("key\x00\xff"), 4)
	if err != nil {
		t.Fatal(err)
	}
	b, err := protocol.WorkerFor([]byte("key\x00\xff"), 4)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("routing not deterministic")
	}
}

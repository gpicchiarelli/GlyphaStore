// Package protocol implements GlyphaStore wire protocol v2 encoding, decoding,
// and canonical FNV-1a Worker routing.
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	Version             = 2
	RequestHeaderBytes  = 40
	ResponseHeaderBytes = 40
	MaxFrameBytes       = 2 * 1024 * 1024
	NoWorker            = 0xFFFF_FFFF
	Identity            = "GlyphaStore/2"
)

// Opcode is a wire-protocol v2 request opcode.
type Opcode uint8

const (
	OpcodeInit       Opcode = 1
	OpcodePing       Opcode = 2
	OpcodeGet        Opcode = 3
	OpcodePut        Opcode = 4
	OpcodeErase      Opcode = 5
	OpcodeBindWorker Opcode = 6
)

// Status is a wire-protocol v2 response status.
type Status uint16

const (
	StatusOK             Status = 0
	StatusInvalidRequest Status = 1
	StatusUnsupported    Status = 2
	StatusInternalError  Status = 3
	StatusNotFound       Status = 4
	StatusOverloaded     Status = 5
	StatusWrongOwner     Status = 6
	StatusNotBound      Status = 7
)

// Request is a decoded protocol-v2 request.
type Request struct {
	Opcode       Opcode
	RequestID    uint64
	ExpireAtNs   uint64
	TargetWorker uint32
	Key          []byte
	Value        []byte
}

// Response is a decoded protocol-v2 response.
type Response struct {
	Status       Status
	RequestID    uint64
	OwnerWorker  uint32
	WorkerCount  uint32
	RoutingEpoch uint64
	Value        []byte
}

var (
	errShortRequest  = errors.New("request is shorter than its header")
	errShortResponse = errors.New("response is shorter than its header")
)

// RequestFrameSize returns the encoded size of a request with the given payloads.
func RequestFrameSize(key, value []byte) int {
	return RequestHeaderBytes + len(key) + len(value)
}

// EncodeRequest encodes one canonical protocol-v2 request frame.
func EncodeRequest(
	opcode Opcode,
	requestID uint64,
	key, value []byte,
	expireAtNs uint64,
	targetWorker uint32,
) ([]byte, error) {
	size := RequestFrameSize(key, value)
	buf := make([]byte, size)
	n, err := EncodeRequestInto(buf, opcode, requestID, key, value, expireAtNs, targetWorker)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

// EncodeRequestInto writes a request into dst (must be large enough) and returns
// the number of bytes written.
func EncodeRequestInto(
	dst []byte,
	opcode Opcode,
	requestID uint64,
	key, value []byte,
	expireAtNs uint64,
	targetWorker uint32,
) (int, error) {
	if err := validateRequestFields(opcode, key, value, expireAtNs, targetWorker); err != nil {
		return 0, err
	}
	frameSize := RequestFrameSize(key, value)
	if frameSize > MaxFrameBytes {
		return 0, errors.New("request exceeds the protocol frame limit")
	}
	if len(dst) < frameSize {
		return 0, errors.New("destination buffer is too small for request frame")
	}
	binary.LittleEndian.PutUint32(dst[0:4], uint32(frameSize))
	binary.LittleEndian.PutUint16(dst[4:6], Version)
	dst[6] = byte(opcode)
	dst[7] = 0
	binary.LittleEndian.PutUint64(dst[8:16], requestID)
	binary.LittleEndian.PutUint32(dst[16:20], uint32(len(key)))
	binary.LittleEndian.PutUint32(dst[20:24], uint32(len(value)))
	binary.LittleEndian.PutUint64(dst[24:32], expireAtNs)
	binary.LittleEndian.PutUint32(dst[32:36], targetWorker)
	binary.LittleEndian.PutUint32(dst[36:40], 0)
	copy(dst[RequestHeaderBytes:], key)
	copy(dst[RequestHeaderBytes+len(key):], value)
	return frameSize, nil
}

// AppendRequest appends an encoded request to dst.
func AppendRequest(
	dst []byte,
	opcode Opcode,
	requestID uint64,
	key, value []byte,
	expireAtNs uint64,
	targetWorker uint32,
) ([]byte, error) {
	size := RequestFrameSize(key, value)
	start := len(dst)
	dst = growSlice(dst, size)
	n, err := EncodeRequestInto(dst[start:start+size], opcode, requestID, key, value, expireAtNs, targetWorker)
	if err != nil {
		return nil, err
	}
	return dst[: start+n], nil
}

func validateRequestFields(
	opcode Opcode,
	key, value []byte,
	expireAtNs uint64,
	targetWorker uint32,
) error {
	switch opcode {
	case OpcodeInit:
		if len(key) != 0 || len(value) != 0 || expireAtNs != 0 || targetWorker != NoWorker {
			return errors.New("INIT request cannot carry key, value, expiry, or target_worker")
		}
	case OpcodePing:
		if len(key) != 0 || expireAtNs != 0 || targetWorker != NoWorker {
			return errors.New("PING request cannot carry key, expiry, or target_worker")
		}
	case OpcodeGet:
		if len(value) != 0 || expireAtNs != 0 || targetWorker != NoWorker {
			return errors.New("GET request cannot carry value, expiry, or target_worker")
		}
	case OpcodePut:
		if targetWorker != NoWorker {
			return errors.New("PUT request cannot carry target_worker")
		}
	case OpcodeErase:
		if len(value) != 0 || expireAtNs != 0 || targetWorker != NoWorker {
			return errors.New("ERASE request cannot carry value, expiry, or target_worker")
		}
	case OpcodeBindWorker:
		if len(key) != 0 || len(value) != 0 || expireAtNs != 0 {
			return errors.New("BIND_WORKER request cannot carry key, value, or expiry")
		}
		if targetWorker == NoWorker {
			return errors.New("BIND_WORKER request requires an explicit target_worker")
		}
	default:
		return errors.New("opcode is not defined by wire protocol v2")
	}
	return nil
}

// DecodeRequest decodes one complete request and rejects noncanonical fields.
func DecodeRequest(frame []byte, maximumFrameBytes int) (Request, error) {
	if maximumFrameBytes <= 0 {
		maximumFrameBytes = MaxFrameBytes
	}
	if len(frame) < RequestHeaderBytes {
		return Request{}, errShortRequest
	}
	frameSize := int(binary.LittleEndian.Uint32(frame[0:4]))
	if frameSize != len(frame) || frameSize > maximumFrameBytes {
		return Request{}, errors.New("request frame extent is invalid")
	}
	version := binary.LittleEndian.Uint16(frame[4:6])
	if version != Version {
		return Request{}, errors.New("request protocol version is unsupported")
	}
	flags := frame[7]
	reserved := binary.LittleEndian.Uint32(frame[36:40])
	if flags != 0 || reserved != 0 {
		return Request{}, errors.New("request canonical fields are invalid")
	}
	keySize := int(binary.LittleEndian.Uint32(frame[16:20]))
	valueSize := int(binary.LittleEndian.Uint32(frame[20:24]))
	if RequestHeaderBytes+keySize+valueSize != frameSize {
		return Request{}, errors.New("request payload extent is invalid")
	}
	opcode := Opcode(frame[6])
	if !validOpcode(opcode) {
		return Request{}, errors.New("request opcode is unknown")
	}
	keyStart := RequestHeaderBytes
	valueStart := keyStart + keySize
	key := append([]byte(nil), frame[keyStart:valueStart]...)
	value := append([]byte(nil), frame[valueStart:valueStart+valueSize]...)
	return Request{
		Opcode:       opcode,
		RequestID:    binary.LittleEndian.Uint64(frame[8:16]),
		ExpireAtNs:   binary.LittleEndian.Uint64(frame[24:32]),
		TargetWorker: binary.LittleEndian.Uint32(frame[32:36]),
		Key:          key,
		Value:        value,
	}, nil
}

// EncodeResponse encodes one canonical protocol-v2 response frame.
func EncodeResponse(
	status Status,
	requestID uint64,
	value []byte,
	ownerWorker, workerCount uint32,
	routingEpoch uint64,
) ([]byte, error) {
	if !validStatus(status) {
		return nil, errors.New("status is not defined by wire protocol v2")
	}
	frameSize := ResponseHeaderBytes + len(value)
	if frameSize > MaxFrameBytes {
		return nil, errors.New("response exceeds the protocol frame limit")
	}
	buf := make([]byte, frameSize)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(frameSize))
	binary.LittleEndian.PutUint16(buf[4:6], Version)
	binary.LittleEndian.PutUint16(buf[6:8], uint16(status))
	binary.LittleEndian.PutUint64(buf[8:16], requestID)
	binary.LittleEndian.PutUint32(buf[16:20], uint32(len(value)))
	binary.LittleEndian.PutUint32(buf[20:24], ownerWorker)
	binary.LittleEndian.PutUint32(buf[24:28], workerCount)
	binary.LittleEndian.PutUint32(buf[28:32], 0)
	binary.LittleEndian.PutUint64(buf[32:40], routingEpoch)
	copy(buf[ResponseHeaderBytes:], value)
	return buf, nil
}

// DecodeResponseView decodes one complete response. Value aliases frame and is
// only valid while frame remains unchanged; use OwnBytes before retaining it.
func DecodeResponseView(frame []byte, maximumFrameBytes int) (Response, error) {
	if maximumFrameBytes <= 0 {
		maximumFrameBytes = MaxFrameBytes
	}
	if len(frame) < ResponseHeaderBytes {
		return Response{}, errShortResponse
	}
	frameSize := int(binary.LittleEndian.Uint32(frame[0:4]))
	if frameSize != len(frame) || frameSize > maximumFrameBytes {
		return Response{}, errors.New("response frame extent is invalid")
	}
	version := binary.LittleEndian.Uint16(frame[4:6])
	if version != Version {
		return Response{}, errors.New("response protocol version is unsupported")
	}
	reserved := binary.LittleEndian.Uint32(frame[28:32])
	if reserved != 0 {
		return Response{}, errors.New("response reserved field is noncanonical")
	}
	valueSize := int(binary.LittleEndian.Uint32(frame[16:20]))
	if ResponseHeaderBytes+valueSize != frameSize {
		return Response{}, errors.New("response value extent is invalid")
	}
	status := Status(binary.LittleEndian.Uint16(frame[6:8]))
	if !validStatus(status) {
		return Response{}, errors.New("response status is unknown")
	}
	var value []byte
	if valueSize > 0 {
		value = frame[ResponseHeaderBytes:frameSize]
	}
	return Response{
		Status:       status,
		RequestID:    binary.LittleEndian.Uint64(frame[8:16]),
		OwnerWorker:  binary.LittleEndian.Uint32(frame[20:24]),
		WorkerCount:  binary.LittleEndian.Uint32(frame[24:28]),
		RoutingEpoch: binary.LittleEndian.Uint64(frame[32:40]),
		Value:        value,
	}, nil
}

// OwnBytes returns a retained copy of b, or nil when b is empty.
func OwnBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// DecodeResponse decodes one complete response and returns an owned Value copy.
func DecodeResponse(frame []byte, maximumFrameBytes int) (Response, error) {
	response, err := DecodeResponseView(frame, maximumFrameBytes)
	if err != nil {
		return Response{}, err
	}
	response.Value = OwnBytes(response.Value)
	return response, nil
}

// FNV1a64 returns the canonical 64-bit FNV-1a hash used for Worker routing.
func FNV1a64(key []byte) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	value := uint64(offset)
	for _, b := range key {
		value ^= uint64(b)
		value *= prime
	}
	return value
}

// WorkerFor returns the owning Worker index for key under workerCount.
func WorkerFor(key []byte, workerCount uint32) (uint32, error) {
	if workerCount == 0 {
		return 0, errors.New("worker_count must be positive")
	}
	return uint32(FNV1a64(key) % uint64(workerCount)), nil
}

func validOpcode(opcode Opcode) bool {
	switch opcode {
	case OpcodeInit, OpcodePing, OpcodeGet, OpcodePut, OpcodeErase, OpcodeBindWorker:
		return true
	default:
		return false
	}
}

func validStatus(status Status) bool {
	return status <= StatusNotBound
}

func growSlice(dst []byte, n int) []byte {
	if cap(dst)-len(dst) >= n {
		return dst[: len(dst)+n]
	}
	out := make([]byte, len(dst)+n)
	copy(out, dst)
	return out
}

// FormatStatus returns a stable name for diagnostics.
func FormatStatus(status Status) string {
	switch status {
	case StatusOK:
		return "OK"
	case StatusInvalidRequest:
		return "INVALID_REQUEST"
	case StatusUnsupported:
		return "UNSUPPORTED"
	case StatusInternalError:
		return "INTERNAL_ERROR"
	case StatusNotFound:
		return "NOT_FOUND"
	case StatusOverloaded:
		return "OVERLOADED"
	case StatusWrongOwner:
		return "WRONG_OWNER"
	case StatusNotBound:
		return "NOT_BOUND"
	default:
		return fmt.Sprintf("STATUS_%d", status)
	}
}

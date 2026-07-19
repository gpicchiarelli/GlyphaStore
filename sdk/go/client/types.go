package client

import (
	"time"

	"github.com/gpicchiarelli/GlyphaStore/sdk/go/protocol"
)

// MutationOutcome classifies standalone PUT/ERASE results.
type MutationOutcome string

const (
	MutationCommitted     MutationOutcome = "committed"
	MutationRejected      MutationOutcome = "rejected"
	MutationIndeterminate MutationOutcome = "indeterminate"
)

// MutationResult is returned by Put and Erase (never panics on outcome).
type MutationResult struct {
	Outcome MutationOutcome
	Err     error
}

// Committed reports whether the mutation is known applied.
func (r MutationResult) Committed() bool {
	return r.Outcome == MutationCommitted
}

// PipelineOpcode is a Store operation allowed inside a pipeline/batch.
type PipelineOpcode uint8

const (
	PipelineGet   PipelineOpcode = PipelineOpcode(protocol.OpcodeGet)
	PipelinePut   PipelineOpcode = PipelineOpcode(protocol.OpcodePut)
	PipelineErase PipelineOpcode = PipelineOpcode(protocol.OpcodeErase)
)

// PipelineRequest is one ordered pipeline/batch entry.
type PipelineRequest struct {
	Opcode     PipelineOpcode
	Key        []byte
	Value      []byte
	ExpireAtNs uint64
}

// PipelineOutcome classifies a positional pipeline/batch response.
type PipelineOutcome string

const (
	PipelineSucceeded     PipelineOutcome = "succeeded"
	PipelineFailed        PipelineOutcome = "failed"
	PipelineIndeterminate PipelineOutcome = "indeterminate"
)

// PipelineResponse is one positional pipeline/batch result.
type PipelineResponse struct {
	Outcome PipelineOutcome
	Value   []byte
	Err     error
}

// Succeeded reports whether the position completed with a valid OK.
func (r PipelineResponse) Succeeded() bool {
	return r.Outcome == PipelineSucceeded
}

// Config configures a Client.
type Config struct {
	Host                     string
	Port                     int
	ConnectTimeout           time.Duration
	RequestTimeout           time.Duration
	MaximumFrameBytes        int
	MaximumPipelineRequests  int
	MaximumPipelineBytes     int
}

// DefaultConfig returns production-oriented defaults matching the other SDKs.
func DefaultConfig() Config {
	return Config{
		Host:                    "127.0.0.1",
		Port:                    7379,
		ConnectTimeout:          3 * time.Second,
		RequestTimeout:          5 * time.Second,
		MaximumFrameBytes:       protocol.MaxFrameBytes,
		MaximumPipelineRequests: 256,
		MaximumPipelineBytes:    1024 * 1024,
	}
}

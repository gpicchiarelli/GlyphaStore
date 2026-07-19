package client

import (
	"github.com/gpicchiarelli/GlyphaStore/sdk/go/protocol"
)

// Category is the portable client-semantics v1 error category.
type Category string

const (
	CategoryInvalidArgument Category = "invalid_argument"
	CategoryNotFound        Category = "not_found"
	CategoryOverloaded      Category = "overloaded"
	CategoryUnavailable     Category = "unavailable"
	CategoryTransport       Category = "transport"
	CategoryProtocol        Category = "protocol"
	CategoryInternal        Category = "internal"
)

// Retryability classifies whether an automatic or application retry is safe.
type Retryability string

const (
	RetryNever          Retryability = "never"
	RetrySameRequest    Retryability = "same_request"
	RetryNewAttempt     Retryability = "new_attempt"
	RetryReconcileFirst Retryability = "reconcile_first"
)

// Error is a structured GlyphaStore client failure.
type Error struct {
	Category        Category
	Message         string
	WireStatus      *protocol.Status
	BytesSent       int
	RequestID       uint64
	Worker          uint32
	RoutingEpoch    uint64
	Retryability    Retryability
	Operation       string
	MutationOutcome MutationOutcome
	hasMutation     bool
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func (e *Error) Is(target error) bool {
	other, ok := target.(*Error)
	if !ok || e == nil || other == nil {
		return false
	}
	return e.Category == other.Category
}

func newError(category Category, message string) *Error {
	return &Error{
		Category:     category,
		Message:      message,
		Retryability: retryabilityFor(category, false, false),
	}
}

func retryabilityFor(category Category, mutationSent bool, indeterminate bool) Retryability {
	switch {
	case indeterminate:
		return RetryReconcileFirst
	case category == CategoryInvalidArgument && !mutationSent:
		return RetryNever
	case category == CategoryTransport && !mutationSent:
		return RetrySameRequest
	case category == CategoryOverloaded || category == CategoryNotFound:
		return RetryNewAttempt
	case category == CategoryUnavailable:
		return RetryNever
	default:
		if mutationSent {
			return RetryReconcileFirst
		}
		return RetryNewAttempt
	}
}

func invalidArgument(message string) *Error {
	return newError(CategoryInvalidArgument, message)
}

func unavailable(message string) *Error {
	err := newError(CategoryUnavailable, message)
	err.Retryability = RetryNever
	return err
}

func transport(message string) *Error {
	err := newError(CategoryTransport, message)
	err.Retryability = RetrySameRequest
	return err
}

func protocolErr(message string) *Error {
	return newError(CategoryProtocol, message)
}

func notFound(message string) *Error {
	err := newError(CategoryNotFound, message)
	err.Retryability = RetryNewAttempt
	return err
}

func overloaded(message string) *Error {
	err := newError(CategoryOverloaded, message)
	err.Retryability = RetryNewAttempt
	return err
}

func internalErr(message string) *Error {
	return newError(CategoryInternal, message)
}

func statusError(status protocol.Status) *Error {
	var err *Error
	switch status {
	case protocol.StatusNotFound:
		err = notFound("key was not found")
	case protocol.StatusOverloaded:
		err = overloaded("server is overloaded")
	case protocol.StatusNotBound:
		err = unavailable("server connection is not bound")
	case protocol.StatusWrongOwner:
		err = protocolErr("server rejected Worker routing")
	case protocol.StatusInvalidRequest, protocol.StatusUnsupported:
		err = invalidArgument("server rejected the request")
	default:
		err = internalErr("server reported an internal error")
	}
	s := status
	err.WireStatus = &s
	return err
}

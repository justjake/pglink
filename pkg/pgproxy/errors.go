package pgproxy

import (
	"errors"
	"io"
)

// ErrRuntime classifies errors from the [Runtime].
var ErrRuntime = errors.New("runtime error")

// ErrSessionClosed indicates an error from an operation on a closed session.
var ErrSessionClosed = errors.New("session closed")

// ErrBackendNotAcquired indicates an error required a backend, but it could not be acquired.
var ErrBackendNotAcquired = errors.New("backend not acquired")

// ErrTornClientConnection indicates the client connection was closed with
// incomplete data in the ring buffer - a partial message was being received.
// This typically means the client crashed or the network connection was severed.
var ErrTornClientConnection = errors.New("torn client connection: incomplete message in buffer")

// ErrTornBackendConnection indicates the backend connection was closed with
// incomplete data in the ring buffer.
var ErrTornBackendConnection = errors.New("torn backend connection: incomplete message in buffer")

// IsCleanTermination returns true if err indicates a clean session termination.
// The Session iterator now handles EOF classification internally, so this mainly
// checks for torn connection errors.
func IsCleanTermination(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, ErrTornClientConnection) || errors.Is(err, ErrTornBackendConnection) {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true // Legacy compatibility
	}
	return false
}

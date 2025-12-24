package frontend

import (
	"context"
	"fmt"

	"github.com/justjake/pglink/pkg/backend"
	"github.com/justjake/pglink/pkg/pgwire"
)

// EventLoopRecv provides message receiving for the event loop session.
// It uses BufferedReader instead of ChanReader to eliminate per-message
// synchronization overhead.
type EventLoopRecv struct {
	ctx context.Context

	frontendReader *backend.BufferedReader[pgwire.ClientMessage]
	frontendCh     <-chan backend.BufferedReadResult[pgwire.ClientMessage]

	backendReader *backend.BufferedReader[pgwire.ServerMessage]
	backendCh     <-chan backend.BufferedReadResult[pgwire.ServerMessage]
}

// NewEventLoopRecv creates a new event loop receiver.
// frontendRead is called to read messages from the client.
// The receiver starts the frontend reader immediately.
func NewEventLoopRecv(
	ctx context.Context,
	frontendRead func(ctx context.Context) (*pgwire.ClientMessage, error),
) *EventLoopRecv {
	r := &EventLoopRecv{ctx: ctx}
	r.frontendReader = backend.NewBufferedReader(frontendRead)
	r.frontendCh = r.frontendReader.Start()
	return r
}

// SetBackend configures the backend reader.
// Call this when a backend connection is acquired.
// backendRead is called to read messages from the backend.
func (r *EventLoopRecv) SetBackend(backendRead func(ctx context.Context) (*pgwire.ServerMessage, error)) {
	if r.backendReader != nil {
		panic("backend reader already set - call ClearBackend first")
	}
	r.backendReader = backend.NewBufferedReader(backendRead)
	r.backendCh = r.backendReader.Start()
}

// ClearBackend stops and removes the backend reader.
// Call this when releasing the backend connection.
func (r *EventLoopRecv) ClearBackend() {
	if r.backendReader == nil {
		return
	}
	r.backendReader.Stop()
	r.backendReader = nil
	r.backendCh = nil
}

// Close stops all readers.
func (r *EventLoopRecv) Close() {
	if r.frontendReader != nil {
		r.frontendReader.Stop()
		r.frontendReader = nil
		r.frontendCh = nil
	}
	r.ClearBackend()
}

// RecvFrontend receives a message from the frontend only.
// Use this when no backend is connected.
func (r *EventLoopRecv) RecvFrontend() (pgwire.ClientMessage, error) {
	select {
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case result, ok := <-r.frontendCh:
		if !ok {
			return nil, fmt.Errorf("frontend reader closed")
		}
		if result.Error != nil {
			return nil, result.Error
		}
		return result.Value, nil
	}
}

// RecvAny receives a message from either frontend or backend.
// Use this when a backend is connected.
// Returns the message and which source it came from.
func (r *EventLoopRecv) RecvAny() (pgwire.Message, error) {
	if r.backendCh == nil {
		return r.RecvFrontend()
	}

	select {
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case result, ok := <-r.frontendCh:
		if !ok {
			return nil, fmt.Errorf("frontend reader closed")
		}
		if result.Error != nil {
			return nil, result.Error
		}
		return result.Value, nil
	case result, ok := <-r.backendCh:
		if !ok {
			return nil, fmt.Errorf("backend reader closed")
		}
		if result.Error != nil {
			return nil, result.Error
		}
		return result.Value, nil
	}
}

// RecvBackend receives a message from the backend only.
// Use this when you only expect backend messages (e.g., draining responses).
func (r *EventLoopRecv) RecvBackend() (pgwire.ServerMessage, error) {
	if r.backendCh == nil {
		return nil, fmt.Errorf("no backend connected")
	}

	select {
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case result, ok := <-r.backendCh:
		if !ok {
			return nil, fmt.Errorf("backend reader closed")
		}
		if result.Error != nil {
			return nil, result.Error
		}
		return result.Value, nil
	}
}

// HasBackend returns true if a backend reader is configured.
func (r *EventLoopRecv) HasBackend() bool {
	return r.backendReader != nil
}

// DrainBackendAsync returns a channel that receives all remaining backend messages.
// This is useful for draining the backend buffer when releasing a connection.
// The returned channel is closed when all messages have been drained.
func (r *EventLoopRecv) DrainBackendAsync() <-chan backend.BufferedReadResult[pgwire.ServerMessage] {
	if r.backendReader == nil {
		ch := make(chan backend.BufferedReadResult[pgwire.ServerMessage])
		close(ch)
		return ch
	}

	// Stop the reader (async) so it doesn't read more
	r.backendReader.StopAsync()

	// Return the channel - caller can drain remaining buffered messages
	return r.backendCh
}

// BackendBufferLen returns the approximate number of messages buffered from the backend.
// This is useful for debugging and metrics.
func (r *EventLoopRecv) BackendBufferLen() int {
	if r.backendCh == nil {
		return 0
	}
	return len(r.backendCh)
}

// FrontendBufferLen returns the approximate number of messages buffered from the frontend.
func (r *EventLoopRecv) FrontendBufferLen() int {
	if r.frontendCh == nil {
		return 0
	}
	return len(r.frontendCh)
}

package backend

import (
	"context"
	"sync/atomic"
)

// BufferedReader reads messages in a background goroutine with minimal synchronization.
// Unlike ChanReader, it does not require Continue() calls - the channel buffer
// provides natural backpressure. This eliminates per-message synchronization overhead.
//
// IMPORTANT: This reader uses a buffer size of 1 by default. This is intentional
// because pgproto3.Frontend.Receive() and pgproto3.Backend.Receive() reuse internal
// buffers - the returned message is only valid until the next Receive() call.
// A buffer of 1 allows read-ahead (reader can fetch next message while receiver
// processes current one) without corrupting message data.
//
// The reader goroutine runs freely until:
// - The read function returns an error
// - The read function returns (nil, nil) indicating EOF
// - Stop() is called
type BufferedReader[T any] struct {
	read   func(ctx context.Context) (*T, error)
	outCh  chan BufferedReadResult[T]
	ctx    context.Context
	cancel context.CancelFunc

	// done is closed when the reader goroutine exits
	done chan struct{}

	started atomic.Bool
}

// BufferedReadResult holds a message or error from the reader.
type BufferedReadResult[T any] struct {
	Value T
	Error error
}

// DefaultBufferSize is 1 to allow read-ahead without message corruption.
// pgproto3 reuses buffers, so messages are only valid until the next Receive().
// A buffer of 1 lets the reader fetch the next message while the receiver
// processes the current one, but never more than one unprocessed message exists.
const DefaultBufferSize = 1

// NewBufferedReader creates a new buffered reader with buffer size 1.
// The read function is called repeatedly in a goroutine to produce messages.
// It receives a context that is cancelled when Stop() is called.
func NewBufferedReader[T any](read func(ctx context.Context) (*T, error)) *BufferedReader[T] {
	return NewBufferedReaderWithSize(read, DefaultBufferSize)
}

// NewBufferedReaderWithSize creates a new buffered reader with a specific buffer size.
func NewBufferedReaderWithSize[T any](read func(ctx context.Context) (*T, error), bufferSize int) *BufferedReader[T] {
	ctx, cancel := context.WithCancel(context.Background())
	return &BufferedReader[T]{
		read:   read,
		outCh:  make(chan BufferedReadResult[T], bufferSize),
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
}

// Start begins reading in the background.
// Returns the channel to receive messages from.
// Can only be called once.
func (r *BufferedReader[T]) Start() <-chan BufferedReadResult[T] {
	if !r.started.CompareAndSwap(false, true) {
		panic("BufferedReader.Start called more than once")
	}
	go r.readerLoop()
	return r.outCh
}

// C returns the output channel. Start() must have been called first.
func (r *BufferedReader[T]) C() <-chan BufferedReadResult[T] {
	return r.outCh
}

// Stop signals the reader to stop and waits for it to exit.
// After Stop returns, no more messages will be sent to the output channel.
// Safe to call multiple times.
func (r *BufferedReader[T]) Stop() {
	r.cancel()
	// Wait for reader goroutine to exit
	<-r.done
}

// StopAsync signals the reader to stop but does not wait.
// Use Done() to wait for the reader to exit.
func (r *BufferedReader[T]) StopAsync() {
	r.cancel()
}

// Done returns a channel that is closed when the reader goroutine exits.
func (r *BufferedReader[T]) Done() <-chan struct{} {
	return r.done
}

func (r *BufferedReader[T]) readerLoop() {
	defer close(r.done)
	defer close(r.outCh)

	for {
		// Check for cancellation before blocking on read
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		// Read next message
		value, err := r.read(r.ctx)

		// EOF: nil value and nil error
		if value == nil && err == nil {
			return
		}

		// Build result
		var result BufferedReadResult[T]
		if err != nil {
			result.Error = err
		} else {
			result.Value = *value
		}

		// Send to channel, respecting cancellation
		select {
		case r.outCh <- result:
		case <-r.ctx.Done():
			return
		}

		// Stop on error
		if err != nil {
			return
		}
	}
}

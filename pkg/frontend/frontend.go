// Package frontend handles communication with clients.
package frontend

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/pgwire"
)

// Low-level frontend connection reader/writer.
type Frontend struct {
	*pgproto3.Backend
	conn net.Conn

	// Ring buffer for zero-copy message reading (started after startup)
	ringBuffer *pgwire.RingBuffer
	cursor     *pgwire.Cursor
	ctx        context.Context
}

func NewFrontend(ctx context.Context, conn net.Conn) *Frontend {
	ringBuffer := pgwire.NewRingBuffer(pgwire.DefaultRingBufferConfig())
	return &Frontend{
		conn:       conn,
		Backend:    pgproto3.NewBackend(conn, conn),
		ringBuffer: ringBuffer,
		cursor:     pgwire.NewClientCursor(ringBuffer),
		ctx:        ctx,
	}
}

func (f *Frontend) Receive() (pgwire.ClientMessage, error) {
	if f.ringBuffer.Running() {
		return nil, fmt.Errorf("ring buffer already running, cannot blocking receive")
	}

	msg, err := f.Backend.Receive()
	if err != nil {
		return nil, err
	}

	if m, ok := pgwire.ToClientMessage(msg); ok {
		return m, nil
	}

	return nil, fmt.Errorf("unknown frontend message: %T", msg)
}

// StartRingBuffer starts the ring buffer reader goroutine.
// Call this after startup is complete to enable zero-copy message reading.
func (f *Frontend) StartRingBuffer() {
	f.ringBuffer.StartNetConnReader(f.ctx, f.conn)
}

// StopRingBuffer stops the ring buffer reader using deadline-based interruption.
// Returns an error if the reader couldn't be stopped cleanly.
func (f *Frontend) StopRingBuffer() error {
	return f.ringBuffer.StopNetConnReader()
}

// Flush flushes any messages queued with Send() to the client.
func (f *Frontend) Flush() error {
	return f.Backend.Flush()
}

// Cursor returns the cursor for reading client messages from the ring buffer.
// Only valid after StartRingBuffer() has been called.
func (f *Frontend) Cursor() *pgwire.Cursor {
	return f.cursor
}

// WriteRange writes a range of messages to the client connection.
func (f *Frontend) WriteRange(r *pgwire.RingRange) error {
	_, err := io.Copy(f.conn, r.NewReader())
	return err
}

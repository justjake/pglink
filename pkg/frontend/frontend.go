// Package frontend handles communication with clients.
package frontend

import (
	"context"
	"fmt"
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
	pgwire.WriteBatch[pgwire.ServerMessage]
}

func NewFrontend(ctx context.Context, conn net.Conn) *Frontend {
	ringBuffer := pgwire.NewRingBuffer()
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
func (f *Frontend) StopRingBuffer() {
	f.ringBuffer.StopNetConnReader()
}

func (f *Frontend) Flush() error {
	backendNeedsFlush := false
	maybeFlushBackend := func() error {
		if backendNeedsFlush {
			if err := f.Backend.Flush(); err != nil {
				return err
			}
			backendNeedsFlush = false
		}
		return nil
	}

	for batch, msg := range f.IterWriteBatch() {
		if batch != nil {
			if err := maybeFlushBackend(); err != nil {
				return err
			}

			if err := batch.WriteAll(f.conn); err != nil {
				return err
			}
		}

		if msg != nil {
			f.Backend.Send(msg.Server())
			backendNeedsFlush = true
		}
	}

	return maybeFlushBackend()
}

// Cursor returns the cursor for reading client messages from the ring buffer.
// Only valid after StartRingBuffer() has been called.
func (f *Frontend) Cursor() *pgwire.Cursor {
	return f.cursor
}

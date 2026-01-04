// Package frontend handles communication with clients.
package frontend

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/observability"
	"github.com/justjake/pglink/pkg/pgserver"
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

	// Metrics tracking (set via SetMetrics after session established)
	metrics      *observability.Metrics
	databaseName string
}

func NewFrontend(ctx context.Context, conn net.Conn) *Frontend {
	return &Frontend{
		conn:    conn,
		Backend: pgproto3.NewBackend(conn, conn),
		ctx:     ctx,
	}
}

// NewFrontendFromClientConn creates a Frontend from a pgserver.ClientConn.
// This reuses the pgproto3.Backend that pgserver already created.
func NewFrontendFromClientConn(ctx context.Context, conn *pgserver.ClientConn) *Frontend {
	return &Frontend{
		conn:    conn.Conn,
		Backend: conn.Frontend,
		ctx:     ctx,
	}
}

func (f *Frontend) Receive() (pgwire.ClientMessage, error) {
	if f.ringBuffer != nil && f.ringBuffer.Running() {
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

// StartRingBuffer creates and starts the ring buffer reader goroutine.
// Call this after startup is complete to enable zero-copy message reading.
// messageBufferBytes specifies the ring buffer size; use 0 for the default (16KiB).
func (f *Frontend) StartRingBuffer(messageBufferBytes int64) {
	f.ringBuffer = pgwire.NewRingBuffer(pgwire.RingBufferConfigForSize(messageBufferBytes))
	f.cursor = pgwire.NewClientCursor(f.ringBuffer)
	f.ringBuffer.StartNetConnReader(f.ctx, f.conn)
}

// StopRingBuffer stops the ring buffer reader using deadline-based interruption.
// Returns an error if the reader couldn't be stopped cleanly.
// Safe to call if the ring buffer was never started.
func (f *Frontend) StopRingBuffer() error {
	if f.ringBuffer == nil {
		return nil
	}
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

// RingBuffer returns the underlying ring buffer.
// Only valid after StartRingBuffer() has been called.
func (f *Frontend) RingBuffer() *pgwire.RingBuffer {
	return f.ringBuffer
}

// SetMetrics configures metrics tracking for this frontend connection.
// Call after the session is established and database name is known.
func (f *Frontend) SetMetrics(metrics *observability.Metrics, databaseName string) {
	f.metrics = metrics
	f.databaseName = databaseName
}

// WriteRange writes a range of messages to the client connection.
// Returns the number of bytes written and any error.
func (f *Frontend) WriteRange(r *pgwire.RingRange) (int64, error) {
	n, err := io.Copy(f.conn, r.NewReader())
	// pgbouncer-compatible: track bytes sent to client (pgbouncer: server_bytes)
	if f.metrics != nil && n > 0 {
		f.metrics.AddSentBytes(f.databaseName, n)
	}
	return n, err
}

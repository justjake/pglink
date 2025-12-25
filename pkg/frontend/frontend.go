// Package frontend handles communication with clients.
package frontend

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/pgwire"
)

// Low-level frontend connection reader/writer.
type Frontend struct {
	*pgproto3.Backend
	conn net.Conn

	// Ring buffer for zero-copy message reading (started after startup)
	ringBuffer *pgwire.RingBuffer
	readerDone chan struct{}
	cursor     *pgwire.Cursor
}

func (f *Frontend) Receive() (pgwire.ClientMessage, error) {
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
	if f.readerDone != nil {
		return // Already started
	}

	if f.ringBuffer == nil {
		f.ringBuffer = pgwire.NewRingBuffer()
	} else {
		f.ringBuffer = f.ringBuffer.NewWithSameBuffers()
	}
	f.readerDone = make(chan struct{})
	f.cursor = pgwire.NewClientCursor(f.ringBuffer)

	go func() {
		defer close(f.readerDone)
		f.ringBuffer.ReadFrom(context.Background(), f.conn)
	}()
}

// StopRingBuffer stops the ring buffer reader using deadline-based interruption.
func (f *Frontend) StopRingBuffer() {
	if f.readerDone == nil {
		return
	}

	// Set deadline to interrupt any blocking Read()
	f.conn.SetDeadline(time.Now())
	// Wait for reader goroutine to exit
	<-f.readerDone
	// Clear deadline
	f.conn.SetDeadline(time.Time{})

	f.readerDone = nil
}

// Cursor returns the cursor for reading client messages from the ring buffer.
// Only valid after StartRingBuffer() has been called.
func (f *Frontend) Cursor() *pgwire.Cursor {
	return f.cursor
}

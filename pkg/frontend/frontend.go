// Package frontend handles communication with clients.
package frontend

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/backend"
	"github.com/justjake/pglink/pkg/pgwire"
)

// Low-level frontend connection reader/writer.
type Frontend struct {
	*pgproto3.Backend
	ctx    context.Context
	conn   io.Writer // Direct access to connection for raw forwarding
	reader *backend.ChanReader[pgwire.ClientMessage]
}

func (f *Frontend) Receive() (pgwire.ClientMessage, error) {
	if err := f.ctx.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled: %w", err)
	}
	msg, err := f.Backend.Receive()
	if err != nil {
		return nil, err
	}
	if err := f.ctx.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled: %w", err)
	}

	if m, ok := pgwire.ToClientMessage(msg); ok {
		return m, nil
	}

	return nil, fmt.Errorf("unknown frontend message: %T", msg)
}

func (f *Frontend) receiveBackgroundThread() (*pgwire.ClientMessage, error) {
	msg, err := f.Receive()
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	return &msg, nil
}

func (f *Frontend) Reader() *backend.ChanReader[pgwire.ClientMessage] {
	if f.reader == nil {
		f.reader = backend.NewChanReader(f.receiveBackgroundThread)
	}
	return f.reader
}

// ForwardRaw writes raw wire protocol bytes directly to the client connection.
// This is the fast path for forwarding server messages without re-encoding.
// It flushes any buffered data first to ensure correct message ordering.
func (f *Frontend) ForwardRaw(raw pgwire.RawBody) error {
	// Flush any pending buffered data to ensure ordering
	if err := f.Flush(); err != nil {
		return fmt.Errorf("flush before forward: %w", err)
	}
	// Write raw bytes directly to connection
	_, err := raw.WriteTo(f.conn)
	return err
}

// ForwardRawMultiple writes multiple raw messages directly to the client connection.
// More efficient than multiple ForwardRaw calls as it only flushes once.
func (f *Frontend) ForwardRawMultiple(raws []pgwire.RawBody) error {
	// Flush any pending buffered data to ensure ordering
	if err := f.Flush(); err != nil {
		return fmt.Errorf("flush before forward: %w", err)
	}
	// Write all raw bytes directly to connection
	for _, raw := range raws {
		if _, err := raw.WriteTo(f.conn); err != nil {
			return err
		}
	}
	return nil
}

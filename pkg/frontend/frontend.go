// Package frontend handles communication with clients.
package frontend

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/backend"
	"github.com/justjake/pglink/pkg/pgwire"
)

// Low-level frontend connection reader/writer.
type Frontend struct {
	*pgproto3.Backend
	ctx    context.Context
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

package pgserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/pgwire"
)

var ErrNetConnInUse = errors.New("net.Conn busy")
var ErrNetConnNotAcquired = errors.New("net.Conn not acquired")

type FrontendConn struct {
	// Conn is the underlying connection.
	// It's often a [*tls.Conn] wrapping a [net.TCPConn].
	Conn         net.Conn
	Frontend     *pgproto3.Backend
	connAcquired bool
	// extraData stores arbitrary data associated with this connection.
	// Used to pass data between auth and startup handlers.
	extraData map[any]any
}

// SetExtraData stores a value associated with the given key.
func (f *FrontendConn) SetExtraData(key, value any) {
	if f.extraData == nil {
		f.extraData = make(map[any]any)
	}
	f.extraData[key] = value
}

// GetExtraData retrieves a value associated with the given key.
func (f *FrontendConn) GetExtraData(key any) any {
	if f.extraData == nil {
		return nil
	}
	return f.extraData[key]
}

// Receive receives a message with context deadline support.
// If ctx has a deadline, it's applied to the read operation.
func (f *FrontendConn) Receive(ctx context.Context) (pgwire.ClientMessage, error) {
	if f.connAcquired {
		return nil, ErrNetConnInUse
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Apply context deadline to connection
	if deadline, ok := ctx.Deadline(); ok {
		if err := f.Conn.SetReadDeadline(deadline); err != nil {
			return nil, fmt.Errorf("failed to set read deadline: %w", err)
		}
		defer f.Conn.SetReadDeadline(time.Time{})
	}

	msg, err := f.Frontend.Receive()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}

	if m, ok := pgwire.ToClientMessage(msg); ok {
		return m, nil
	}

	return nil, fmt.Errorf("unknown frontend message: %T", msg)
}

// Send sends and flushes a message with context deadline support.
// If ctx has a deadline, it's applied to the write operation.
func (f *FrontendConn) Send(ctx context.Context, msg pgproto3.BackendMessage) error {
	if f.connAcquired {
		return ErrNetConnInUse
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if deadline, ok := ctx.Deadline(); ok {
		if err := f.Conn.SetWriteDeadline(deadline); err != nil {
			return fmt.Errorf("failed to set write deadline: %w", err)
		}
		defer f.Conn.SetWriteDeadline(time.Time{})
	}

	f.Frontend.Send(msg)
	if err := f.Frontend.Flush(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	return nil
}

func (f *FrontendConn) AcquireNetConn(ctx context.Context) (net.Conn, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if f.connAcquired {
		return nil, ErrNetConnInUse
	}
	f.connAcquired = true
	return f.Conn, nil
}

func (f *FrontendConn) ReleaseNetConn(ctx context.Context) error {
	if !f.connAcquired {
		return ErrNetConnNotAcquired
	}
	f.connAcquired = false
	return nil
}

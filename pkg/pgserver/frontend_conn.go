package pgserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	// Logger is used for debug tracing of deadline operations.
	// If nil, no deadline tracing is done.
	Logger *slog.Logger
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
func (f *FrontendConn) Receive(ctx context.Context) (pgproto3.FrontendMessage, error) {
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
		defer func() {
			if err := f.Conn.SetReadDeadline(time.Time{}); err != nil && f.Logger != nil {
				f.Logger.Debug("failed to clear read deadline", "error", err)
			}
		}()
	}

	msg, err := f.Frontend.Receive()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	return msg, nil
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
		defer func() {
			if err := f.Conn.SetWriteDeadline(time.Time{}); err != nil && f.Logger != nil {
				f.Logger.Debug("failed to clear write deadline", "error", err)
			}
		}()
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

	conn := f.Conn
	// Wrap conn with deadline tracing if logger is available
	if f.Logger != nil {
		conn = pgwire.NewDeadlineTraceConn(conn, f.Logger)
	}
	return conn, nil
}

func (f *FrontendConn) ReleaseNetConn(ctx context.Context) error {
	if !f.connAcquired {
		return ErrNetConnNotAcquired
	}
	f.connAcquired = false
	return nil
}

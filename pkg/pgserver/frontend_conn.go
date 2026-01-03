package pgserver

import (
	"context"
	"errors"
	"fmt"
	"net"

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
}

func (f *FrontendConn) Receive() (pgwire.ClientMessage, error) {
	msg, err := f.Frontend.Receive()
	if err != nil {
		return nil, err
	}

	if m, ok := pgwire.ToClientMessage(msg); ok {
		return m, nil
	}

	return nil, fmt.Errorf("unknown frontend message: %T", msg)
}

func (f *FrontendConn) SendFlush(msg pgproto3.BackendMessage) error {
	f.Frontend.Send(msg)
	return f.Frontend.Flush()
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

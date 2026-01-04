package frontend

import (
	"context"
	"fmt"
	"net"

	"github.com/justjake/pglink/pkg/pgproxy"
	"github.com/justjake/pglink/pkg/pgserver"
)

// ClientFrontend wraps a pgserver.ClientConn to implement pgproxy.Frontend.
// It provides the bridge between the server-level client connection and the
// proxy session layer.
type ClientFrontend struct {
	conn *pgserver.ClientConn
}

var _ pgproxy.Frontend = (*ClientFrontend)(nil)

// NewClientFrontend creates a new ClientFrontend wrapping the given ClientConn.
func NewClientFrontend(conn *pgserver.ClientConn) *ClientFrontend {
	return &ClientFrontend{conn: conn}
}

// ClientConn returns the underlying pgserver.ClientConn.
func (f *ClientFrontend) ClientConn() *pgserver.ClientConn {
	return f.conn
}

// AcquireNetConn implements pgproxy.Conn.
func (f *ClientFrontend) AcquireNetConn(ctx context.Context) (net.Conn, error) {
	return f.conn.AcquireNetConn(ctx)
}

// ReleaseNetConn implements pgproxy.Conn.
func (f *ClientFrontend) ReleaseNetConn() error {
	// FrontendConn.ReleaseNetConn takes a context but doesn't use it.
	return f.conn.ReleaseNetConn(context.Background())
}

// Terminate implements pgproxy.Conn.
// It closes the underlying connection. The error parameter describes why
// the connection is being terminated (for logging purposes).
func (f *ClientFrontend) Terminate(ctx context.Context, err error) error {
	// Close the underlying network connection.
	// The pgserver.FrontendConn doesn't have a dedicated Terminate method,
	// so we close the underlying net.Conn directly.
	return f.conn.Conn.Close()
}

// MessageTrackers implements pgproxy.Conn.
// Returns any message trackers that should be applied to messages from/to this frontend.
func (f *ClientFrontend) MessageTrackers() []pgproxy.MessageTracker {
	// The frontend itself doesn't have any built-in trackers.
	// Session-level trackers are configured via SessionConfig.FrontendTrackers.
	return nil
}

// String implements fmt.Stringer.
func (f *ClientFrontend) String() string {
	return fmt.Sprintf("ClientFrontend{pid=%d addr=%s}", f.conn.ProcessID, f.conn.Conn.RemoteAddr())
}

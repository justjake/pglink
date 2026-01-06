package pgproxy

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/justjake/pglink/pkg/pgwire"
)

// MessageTracker is a pluggable mechanism for tracking state as messages are processed.
type MessageTracker interface {
	// TrackMessage tracks the message.
	// The tracker may return a modified context for tracing.
	TrackMessage(ctx context.Context, msg pgwire.Message) (context.Context, error)
}

// Conn represents a connection to a frontend (client) or backend (server).
// Intended to be implemented by the caller.
type Conn interface {
	// AcquireNetConn takes exclusive ownership of the Conn's underlying net.Conn.
	// While acquired, Conn should not attempt to use the net.Conn.
	// It should return an error if the net.Conn is already acquired.
	AcquireNetConn(ctx context.Context) (net.Conn, error)
	// ReleaseNetConn releases the net.Conn back to the Conn.
	// It should return an error if the net.Conn is not acquired.
	ReleaseNetConn() error
	// Terminate terminates the connection.
	// The implementation may handle `err` as it sees fit, although typically the proxy already sends a termination message.
	Terminate(ctx context.Context, err error) error
	// MessageTrackers returns the trackers for messages read from or written to the connection,
	// that must be updated for the Conn's internal state to stay valid.
	MessageTrackers() []MessageTracker
	fmt.Stringer
}

// Frontend represents a connection to a client.
type Frontend interface {
	Conn
}

// Backend represents a connection to a server.
type Backend interface {
	Conn
	// Release releases the backend connection back to some underlying pool.
	// It is expected that calling backend methods after Release may panic.
	Release()
	// OutstandingRequests returns the queue of outstanding requests sent to the backend.
	// This is used to attach response handlers to requests.
	OutstandingRequests() *OutstandingRequestQueue
}

// ProxyHandler is called by a [Session] to handle messages.
// It's called during [Session.Run].
//
// The err parameter will be non-nil if there was an error reading a message, or
// handling the previous message's action.
//
// If the handler returns an error, the call to [Session.Run] will flush
// remaining messages and end.
//
// The error returned from the handler will be returned by [Session.Run],
// possibly wrapped with additional errors encountered stopping the session.
type ProxyHandler func(ctx context.Context, session *Session, pos Pos, err error) error

// SessionConfig configures a [Session].
type SessionConfig struct {
	// The client. Required.
	Frontend Frontend

	// Handler is called for each message position and any errors.
	Handler ProxyHandler

	// FrontendTrackers are called when messages are read from or written to the frontend,
	// in addition to trackers in [Conn.Trackers].
	FrontendTrackers []MessageTracker

	// NewBackendTrackers are called when messages are read from or written to the backend,
	// in addition to trackers in [Conn.Trackers].
	// Called when a new backend is acquired.
	NewBackendTrackers func(ctx context.Context, backend Backend) ([]MessageTracker, error)

	// Function to acquire a [Backend] connection.
	// Should perform whatever setup is needed on a backend before it can be used for this session.
	AcquireBackend func(ctx context.Context) (Backend, error)

	// NewRuntime creates the [Runtime] for the session.
	// If not set, the default runtime is used.
	NewRuntime RuntimeFactory

	// HealthCheck is called periodically while idle. Use to implement idle timeouts.
	// If not set, no health check is performed, and HealthCheckPeriod is ignored.
	HealthCheck func(ctx context.Context) error
	// HealthCheckPeriod is the period between calls to HealthCheck.
	// If not set, defaults to 1 second.
	HealthCheckPeriod time.Duration

	// If not set, defaults to slog.Default().
	Logger *slog.Logger
}

// RuntimeFactory is the type for functions that create a [Runtime] for a [Session].
type RuntimeFactory func(ctx context.Context, session *Session) (Runtime, error)

// Runtime handles scheduling work for a proxy session and doing low-level I/O on connections.
// Runtime is intended to be implemented by pgproxy internally.
//
// Responsibilities:
// - Session: Correct handling of user's proxy policy actions.
// - Runtime: Calls into Session when new messages are available.
// - Runtime: Writes to connections when requested by Session.
type Runtime interface {
	// Run the runtime. This is expected to block on the caller thread until the
	// context is cancelled, or Stop is called.
	//
	// During Run, the runtime should call the Session's [HandlePos] for each message.
	Run(ctx context.Context) error
	// Stop the runtime. The runtime should stop calling Session's methods.
	Stop(ctx context.Context) error
	// Add a connection. The Session is in charge of when connections are acquired and released.
	// This may call conn.AcquireNetConn().
	// This is called during AcquireBackend for example.
	StartConn(ctx context.Context, role ProxyRole, conn Conn) error
	// Remove a connection. The Session is in charge of when connections are acquired and released.
	// This may call conn.ReleaseNetConn().
	// This is called during ReleaseBackend for example.
	//
	// If StopConn returns an error, the Conn cannot safely be re-used.
	// The Session will attempt to terminate the Conn.
	StopConn(ctx context.Context, role ProxyRole) error
	// Flush pending writes to a connection.
	WriteConn(ctx context.Context, role ProxyRole, queued *pgwire.WriteQueue) error
}

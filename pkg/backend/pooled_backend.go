package backend

import (
	"context"
	"fmt"
	"net"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/pgproxy"
	"github.com/justjake/pglink/pkg/pgwire"
)

// PooledBackend wraps a session connected to a backend database server.
//
// This type implements the Facade pattern: it provides a simplified interface
// to the underlying Session and MultiPoolConn, while preventing direct access
// to these internal components. This is critical for safety because:
//
//  1. Session lifecycle: The Session persists across connection pool checkouts,
//     but must only be used while the PooledBackend is acquired. Exposing the
//     Session directly would allow callers to hold references after Release(),
//     leading to use-after-release bugs.
//
//  2. Connection validity: All operations must check if the connection has been
//     released (panicIfReleased). Direct Session access would bypass these checks.
//
//  3. Error handling: Write operations automatically mark the connection for
//     destruction on error. Direct Session access would bypass this.
//
// When adding new functionality, prefer adding methods to PooledBackend that
// delegate to Session, rather than exposing Session directly.
type PooledBackend struct {
	conn            *MultiPoolConn
	session         *Session
	released        bool
	cursor          *pgwire.Cursor
	netConnAcquired bool // for pgproxy.Backend interface
}

var _ pgproxy.Backend = (*PooledBackend)(nil)

func (c *PooledBackend) TrackedParameters() []string {
	return c.session.TrackedParameters
}

// HasStatement returns true if the named prepared statement exists on this backend connection.
// This includes statements that are pending creation (Parse in-flight but ParseComplete not yet received).
func (c *PooledBackend) HasStatement(name string) bool {
	c.panicIfReleased()
	return c.session.State.Statements.Alive[name] || c.session.State.Statements.PendingCreate[name]
}

func (c *PooledBackend) String() string {
	return fmt.Sprintf("%s&pooledBackend=%p", c.session.String(), c)
}

func (c *PooledBackend) PgConn() *pgconn.PgConn {
	c.panicIfReleased()
	return c.conn.Value().Conn().PgConn()
}

func (c *PooledBackend) Flush() error {
	c.panicIfReleased()
	return c.session.Flush()
}

func (c *PooledBackend) Cursor() *pgwire.Cursor {
	c.panicIfReleased()
	if c.cursor == nil {
		c.cursor = pgwire.NewServerCursor(c.session.RingBuffer())
	}
	return c.cursor
}

// NewCursor creates a fresh cursor over the backend's ring buffer.
// Use this when you need an independent cursor that won't interfere with
// the main pooled cursor (e.g., for internal queries).
func (c *PooledBackend) NewCursor() *pgwire.Cursor {
	c.panicIfReleased()
	return pgwire.NewServerCursor(c.session.RingBuffer())
}

// StartRingBuffer begins the ring buffer reader goroutine.
// Must be called after acquiring the backend and before using the Cursor.
// Any internal queries (like varcache SET statements) should be run
// via PgConn() BEFORE calling this method.
func (c *PooledBackend) StartRingBuffer() {
	c.panicIfReleased()
	c.session.StartRingBuffer()
}

// PauseRingBuffer stops the ring buffer reader goroutine, allowing direct
// access to the connection via PgConn(). Must call ResumeRingBuffer() after.
func (c *PooledBackend) PauseRingBuffer() error {
	c.panicIfReleased()
	ring := c.session.RingBuffer()
	if ring != nil && ring.Running() {
		return ring.StopNetConnReader()
	}
	return nil
}

// ResumeRingBuffer restarts the ring buffer reader goroutine after it was
// paused with PauseRingBuffer().
func (c *PooledBackend) ResumeRingBuffer() {
	c.panicIfReleased()
	ring := c.session.RingBuffer()
	if ring != nil && !ring.Running() {
		ring.StartNetConnReader(context.Background(), c.conn.Value().Conn().PgConn().Conn())
	}
}

// RingBuffer returns the ring buffer for this backend connection.
func (c *PooledBackend) RingBuffer() *pgwire.RingBuffer {
	c.panicIfReleased()
	return c.session.RingBuffer()
}

func (c *PooledBackend) WriteRange(r *pgwire.RingRange) (int64, error) {
	c.panicIfReleased()
	n, err := c.session.WriteRange(r)
	if err != nil {
		c.MarkForDestroy(fmt.Errorf("failed to write message batch %s: %w", r.String(), err))
	}
	return n, err
}

func (c *PooledBackend) WriteMsg(msg pgproto3.FrontendMessage) error {
	c.panicIfReleased()
	err := c.session.WriteMsg(msg)
	if err != nil {
		c.MarkForDestroy(fmt.Errorf("failed to write message %T: %w", msg, err))
	}
	return err
}

func (c *PooledBackend) ParameterStatusChanges(keys []string, since pgwire.ParameterStatuses) pgwire.ParameterStatusDiff {
	c.panicIfReleased()
	return c.session.ParameterStatusChanges(keys, since)
}

// UpdateState should be called for each server message received from the backend.
// TODO: handle this internally in Session somehow for cursor batches.
func (c *PooledBackend) UpdateState(msg pgwire.Message) {
	c.panicIfReleased()
	c.session.State.Update(msg)
}

// OutstandingRequestCount returns the number of pending requests sent to the backend.
// This is the BACKEND state (requests we sent), not CLIENT state (requests client sent us).
func (c *PooledBackend) OutstandingRequestCount() int {
	c.panicIfReleased()
	return c.session.State.OutstandingRequestCount()
}

// ParameterStatuses returns the backend's current parameter statuses.
// This is the BACKEND state (what postgres told us), not CLIENT state (what we told client).
func (c *PooledBackend) ParameterStatuses() pgwire.ParameterStatuses {
	c.panicIfReleased()
	return c.session.State.ParameterStatuses
}

// SyncParameterStatusesFromPgConn updates the backend session's parameter statuses
// from pgconn's internal tracking. Call this after using PgConn().Exec() directly
// to keep the session state in sync.
func (c *PooledBackend) SyncParameterStatusesFromPgConn() {
	c.panicIfReleased()
	c.session.SyncParameterStatusesFromPgConn()
}

// Release returns the connection to the pool.
// It is safe to call Release multiple times.
func (c *PooledBackend) Release() {
	if c.released {
		return
	}
	c.released = true

	// Check if the backend connection is in a clean state.
	// If the connection is in a transaction or failed transaction state,
	// it must be destroyed rather than returned to the pool, because
	// subsequent queries on this connection would fail with
	// "current transaction is aborted".
	//
	// Note: We check session.State.TxStatus (pglink's tracked state) rather than
	// conn.TxStatus() (pgx's tracked state) because pglink bypasses pgx for
	// zero-copy message proxying, so pgx doesn't know the actual transaction state.
	if c.session.State.TxStatus != pgwire.TxIdle {
		c.session.logger.Warn("releasing backend in non-idle transaction state, marking for destruction",
			"txStatus", c.session.State.TxStatus)
		c.conn.MarkForDestroy()
	}

	// Release session BEFORE releasing connection to avoid race condition:
	// another goroutine could acquire the same connection before we release
	// the session, causing "session already acquired" errors.
	c.session.Release()
	c.conn.Release()
}

func (c *PooledBackend) MarkForDestroy(err error) {
	c.session.logger.Error("marking for destruction due to error", "error", err)
	c.conn.MarkForDestroy()
}

func (c *PooledBackend) ReleaseAndDestroy(err error) {
	if c.released {
		c.session.logger.Error("LogicError: already released, refusing to mark for destruction", "error", err)
		return
	}
	c.session.logger.Error("marking for destruction due to error", "error", err)
	c.conn.ReleaseAndDestroy()
	c.Release()
}

func (c *PooledBackend) panicIfReleased() {
	if c.released {
		panic(fmt.Errorf("PooledConn: already released: %s", c.String()))
	}
}

// ============================================================================
// pgproxy.Backend interface implementation
// ============================================================================

// AcquireNetConn implements pgproxy.Conn.
// Takes exclusive ownership of the underlying net.Conn for use with pgproxy.Session.
func (c *PooledBackend) AcquireNetConn(ctx context.Context) (net.Conn, error) {
	c.panicIfReleased()
	if c.netConnAcquired {
		return nil, fmt.Errorf("net.Conn already acquired")
	}
	c.netConnAcquired = true

	conn := c.conn.Value().Conn().PgConn().Conn()
	c.session.logger.Debug("acquired net.Conn", "localAddr", conn.LocalAddr(), "remoteAddr", conn.RemoteAddr())
	return conn, nil
}

// ReleaseNetConn implements pgproxy.Conn.
// Releases the underlying net.Conn back to PooledBackend.
func (c *PooledBackend) ReleaseNetConn() error {
	c.panicIfReleased()
	if !c.netConnAcquired {
		return fmt.Errorf("net.Conn not acquired")
	}
	c.netConnAcquired = false
	return nil
}

// Terminate implements pgproxy.Conn.
// Marks the backend connection for destruction due to an error.
func (c *PooledBackend) Terminate(ctx context.Context, err error) error {
	c.panicIfReleased()
	c.MarkForDestroy(err)
	return nil
}

// MessageTrackers implements pgproxy.Conn.
// Returns trackers for messages sent to/from this backend.
func (c *PooledBackend) MessageTrackers() []pgproxy.MessageTracker {
	c.panicIfReleased()
	// Return the OutstandingRequestQueue as a tracker.
	// Additional trackers (TransactionFlow, CopyFlow, etc.) will be added in Phase 2.
	return []pgproxy.MessageTracker{&c.session.OutstandingRequests}
}

// OutstandingRequests implements pgproxy.Backend.
// Returns the queue of requests sent to this backend awaiting responses.
func (c *PooledBackend) OutstandingRequests() *pgproxy.OutstandingRequestQueue {
	c.panicIfReleased()
	return &c.session.OutstandingRequests
}

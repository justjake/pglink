package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
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
	conn     *MultiPoolConn
	session  *Session
	released bool
	cursor   *pgwire.Cursor
}

func (c *PooledBackend) TrackedParameters() []string {
	return c.session.TrackedParameters
}

// HasStatement returns true if the named prepared statement exists on this backend connection.
func (c *PooledBackend) HasStatement(name string) bool {
	c.panicIfReleased()
	return c.session.State.Statements.Alive[name]
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

func (c *PooledBackend) WriteRange(r *pgwire.RingRange) error {
	c.panicIfReleased()
	err := c.session.WriteRange(r)
	if err != nil {
		c.MarkForDestroy(fmt.Errorf("failed to write message batch %s: %w", r.String(), err))
	}
	return err
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

// TryFill attempts to read data directly into the ring buffer (for single-goroutine mode).
// Returns true if data was read, false if no data available (timeout).
func (c *PooledBackend) TryFill(ctx context.Context, timeout time.Duration) (bool, error) {
	c.panicIfReleased()
	return c.session.TryFill(ctx, timeout)
}

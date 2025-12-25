package backend

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/pgwire"
)

// PooledBackend wraps a session connected to a backend database server.
type PooledBackend struct {
	conn     *MultiPoolConn
	session  *Session
	released bool
	cursor   *pgwire.Cursor
}

func (c *PooledBackend) TrackedParameters() []string {
	return c.session.TrackedParameters
}

func (c *PooledBackend) PgConn() *pgconn.PgConn {
	return c.conn.Value().Conn().PgConn()
}

func (c *PooledBackend) Name() string {
	return fmt.Sprintf("%s&pooledBackend=%p", c.session.Name(), c)
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
	return c.session.WriteRange(r)
}

func (c *PooledBackend) WriteMsg(msg pgproto3.FrontendMessage) error {
	c.panicIfReleased()
	return c.session.WriteMsg(msg)
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
		panic(fmt.Errorf("PooledConn: already released: %s", c.Name()))
	}
}

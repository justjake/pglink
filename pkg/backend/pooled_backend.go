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

func (c *PooledBackend) ReadingChan() <-chan ReadResult[pgwire.ServerMessage] {
	c.panicIfReleased()
	return c.session.reader.ReadingChan()
}

func (c *PooledBackend) Flush() error {
	c.panicIfReleased()
	return c.session.Flush()
}

func (c *PooledBackend) Continue() {
	c.panicIfReleased()
	c.session.reader.Continue()
}

func (c *PooledBackend) Send(msg pgproto3.FrontendMessage) error {
	c.panicIfReleased()
	return c.session.Send(msg)
}

func (c *PooledBackend) ParameterStatusChanges(keys []string, since pgwire.ParameterStatuses) pgwire.ParameterStatusDiff {
	c.panicIfReleased()
	return c.session.ParameterStatusChanges(keys, since)
}

// Release returns the connection to the pool.
// It is safe to call Release multiple times.
func (c *PooledBackend) Release() {
	if c.released {
		return
	}
	c.released = true
	c.conn.Release()
	c.session.Release()
	// Note: We don't release dbConns here because the connection goes back
	// to the pool as idle. dbConns is only decremented in BeforeClose when
	// the connection is actually closed.
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

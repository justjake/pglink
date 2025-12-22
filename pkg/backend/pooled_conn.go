package backend

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/justjake/pglink/pkg/pgwire"
)

// PooledConn wraps a pgxpool connection with automatic release tracking.
type PooledConn struct {
	conn     *pgxpool.Conn
	session  *Session
	released bool
}

func (c *PooledConn) TrackedParameters() []string {
	return c.session.TrackedParameters
}

func (c *PooledConn) PgConn() *pgconn.PgConn {
	c.panicIfReleased()
	return c.conn.Conn().PgConn()
}

func (c *PooledConn) Name() string {
	return fmt.Sprintf("%s&pooledConn=%p", c.session.Name(), c)
}

func (c *PooledConn) ReadingChan() <-chan ReadResult[pgwire.ServerMessage] {
	c.panicIfReleased()
	return c.session.reader.ReadingChan()
}

func (c *PooledConn) Flush() error {
	c.panicIfReleased()
	return c.session.Flush()
}

func (c *PooledConn) Continue() {
	c.panicIfReleased()
	c.session.reader.Continue()
}

func (c *PooledConn) Send(msg pgproto3.FrontendMessage) error {
	c.panicIfReleased()
	return c.session.Send(msg)
}

func (c *PooledConn) ParameterStatusChanges(keys []string, since pgwire.ParameterStatuses) pgwire.ParameterStatusDiff {
	c.panicIfReleased()
	return c.session.ParameterStatusChanges(keys, since)
}

func (c *PooledConn) Kill(ctx context.Context, err error) {
	c.panicIfReleased()
	c.session.logger.Error("killing connection", "error", err)
	if closeErr := c.conn.Hijack().Close(ctx); closeErr != nil {
		c.session.logger.Error("failed to kill connection", "error", closeErr)
	}
	// c.session.Kill(err)
}

// Release returns the connection to the pool.
// It is safe to call Release multiple times.
func (c *PooledConn) Release() {
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

func (c *PooledConn) panicIfReleased() {
	if c.released {
		panic(fmt.Errorf("PooledConn: already released: %s", c.Name()))
	}
}

package backend

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/justjake/pglink/pkg/pgwire"
)

// PooledConn wraps a pgxpool connection with automatic release tracking.
type PooledConn struct {
	*pgxpool.Conn
	session  *Session
	released bool
}

func (c *PooledConn) TrackedParameters() []string {
	return c.session.TrackedParameters
}

func (c *PooledConn) PgConn() *pgconn.PgConn {
	c.panicIfReleased()
	return c.Conn.Conn().PgConn()
}

func (c *PooledConn) Name() string {
	return fmt.Sprintf("%s&pooledConn=%p", c.session.Name(), c)
}

func (c *PooledConn) ReadingChan() <-chan ReadResult[pgwire.ServerMessage] {
	c.panicIfReleased()
	return c.session.reader.ReadingChan()
}

// Release returns the connection to the pool.
// It is safe to call Release multiple times.
func (c *PooledConn) Release() {
	if c.released {
		return
	}
	c.released = true
	c.Conn.Release()
	// Note: We don't release dbConns here because the connection goes back
	// to the pool as idle. dbConns is only decremented in BeforeClose when
	// the connection is actually closed.
}

func (c *PooledConn) panicIfReleased() {
	if c.released {
		panic(fmt.Errorf("PooledConn: already released: %s", c.Name()))
	}
}

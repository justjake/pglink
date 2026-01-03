package pgserver

import (
	"errors"
	"fmt"
	"iter"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/pgwire"
)

var ErrConnAlreadyExists = errors.New("connection already exists")

// ConnMap tracks client connections by their [pgwire.ProcessID] and [pgwire.SecretKey].
type ConnMap struct {
	mu    sync.RWMutex
	conns map[ConnKey]*ClientConn
}

type ConnKey struct {
	ProcessID pgwire.ProcessID
	SecretKey pgwire.SecretKey
}

func CancelMessageConnKey(msg *pgproto3.CancelRequest) ConnKey {
	return ConnKey{
		ProcessID: pgwire.ProcessID(msg.ProcessID),
		SecretKey: pgwire.SecretKey(msg.SecretKey),
	}
}

func (c *ConnMap) Get(key ConnKey) (*ClientConn, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	conn, ok := c.conns[key]
	return conn, ok
}

func (c *ConnMap) Add(conn *ClientConn) (ConnKey, error) {
	key := c.key(conn)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.conns[key]; ok {
		return ConnKey{}, fmt.Errorf("%w: ProcessId=%v SecretKey=<redacted>", ErrConnAlreadyExists, key.ProcessID)
	}
	c.conns[key] = conn
	return key, nil
}

func (c *ConnMap) Remove(key ConnKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.conns, key)
}

func (c *ConnMap) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.conns)
}

func (c *ConnMap) All() iter.Seq2[ConnKey, *ClientConn] {
	return func(yield func(ConnKey, *ClientConn) bool) {
		c.mu.RLock()
		defer c.mu.RUnlock()
		for key, conn := range c.conns {
			if !yield(key, conn) {
				return
			}
		}
	}
}

func (c *ConnMap) key(conn *ClientConn) ConnKey {
	return ConnKey{
		ProcessID: conn.ProcessID,
		SecretKey: conn.SecretKey,
	}
}

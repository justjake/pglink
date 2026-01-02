package pgserver

import (
	"errors"
	"fmt"
	"iter"
	"sync"
)

var ErrConnAlreadyExists = errors.New("connection already exists")

type ConnMap struct {
	mu    sync.RWMutex
	conns map[ConnKey]*ClientConn
}

type ConnKey struct {
	ProcessID ProcessID
	SecretKey SecretKey
}

func (c *ConnMap) Get(key ConnKey) (*ClientConn, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	conn, ok := c.conns[key]
	return conn, ok
}

func (c *ConnMap) Add(conn *ClientConn) (ConnKey, error) {
	key := c.key(conn)
	if _, ok := c.Get(key); ok {
		return ConnKey{}, fmt.Errorf("%w: ProcessId=%v, SecretKey=<redacted>", ErrConnAlreadyExists, key.ProcessID)
	}
	c.set(key, conn)
	return key, nil
}

func (c *ConnMap) Remove(key ConnKey) {
	c.delete(key)
}

func (c *ConnMap) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.conns)
}

func (c *ConnMap) Iter() iter.Seq2[ConnKey, *ClientConn] {
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

func (c *ConnMap) set(key ConnKey, conn *ClientConn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conns == nil {
		c.conns = make(map[ConnKey]*ClientConn)
	}
	c.conns[key] = conn
}

func (c *ConnMap) delete(key ConnKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.conns, key)
}

func (c *ConnMap) key(conn *ClientConn) ConnKey {
	return ConnKey{
		ProcessID: conn.ProcessID,
		SecretKey: conn.SecretKey,
	}
}

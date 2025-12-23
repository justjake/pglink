package backend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/puddle/v2"
)

var ErrMaxConnsReached = errors.New("max conns reached")
var ErrNoIdleConnections = fmt.Errorf("%w, no idle connections to close", ErrMaxConnsReached)

// MultiPool controls a set of pgxpool.Pool connection pools, while enforcing a
// global max connections limit.
//
// The total number of connections may temporarily exceed the MaxConns limit.
// It is a bug if the total number of connections grows beyond MaxConns without bound.
//
// MultiPool will run in production environments with MaxConns=1000+. It's okay
// to overshoot a MaxConns of 1000 by about 20 connections at a time. Beyond
// that it's prefereable to error even if a connection was recently released but
// hasn't been destroyed or made available for Acquire yet.
type MultiPool[K comparable] struct {
	MaxConns int32
	pools    map[K]*multiPoolMember[K]
	started  bool
	tickets  *puddle.Pool[ticket]

	closeOnce sync.Once

	// Maintinence
	closeChan         chan struct{}
	logger            *slog.Logger
	healthCheckPeriod time.Duration

	totalConns      atomic.Int32
	totalIdleConns  atomic.Int32
	pendingCreates  atomic.Int32
	pendingDestroys atomic.Int32
}

func NewMultiPool[K comparable](maxConns int32, logger *slog.Logger) *MultiPool[K] {
	tickets, err := puddle.NewPool(&puddle.Config[ticket]{
		Constructor: func(ctx context.Context) (ticket, error) {
			return ticket{}, nil
		},
		Destructor: func(ticket) {},
		MaxSize:    maxConns,
	})
	if err != nil {
		panic(err)
	}

	return &MultiPool[K]{
		MaxConns:          maxConns,
		pools:             make(map[K]*multiPoolMember[K]),
		tickets:           tickets,
		closeChan:         make(chan struct{}),
		logger:            logger,
		healthCheckPeriod: time.Second * 10,
	}
}

type ticket struct{}

var acquireContextKey = ticket{}

type acquireContextReq struct {
	createAttempts int
	created        bool
}

// Not thread-safe.
func (p *MultiPool[K]) AddPool(ctx context.Context, key K, givenConfig *pgxpool.Config) error {
	if p.started {
		panic("multi pool already started")
	}

	stuff := &multiPoolMember[K]{
		key:    key,
		config: givenConfig,
		parent: p,
	}

	withCallbacks := givenConfig.Copy()
	withCallbacks.BeforeConnect = stuff.beforeConnect
	withCallbacks.AfterConnect = stuff.afterConnect
	withCallbacks.PrepareConn = stuff.prepareConn
	withCallbacks.AfterRelease = stuff.afterRelease
	withCallbacks.BeforeClose = stuff.beforeClose

	pool, err := pgxpool.NewWithConfig(ctx, withCallbacks)
	if err != nil {
		return err
	}

	stuff.pool = pool
	p.pools[key] = stuff
	return nil
}

// Not thread-safe. Setup complete, now you can call Acquire.
func (p *MultiPool[K]) Start() {
	if p.started {
		panic("multi pool already started")
	}
	p.started = true
	go p.backgroundHealthCheck()
}

// Acquire acquires a connection from the pool for the given name.
//
// It will wait until the global number of connections is below the total max
// connections before attempting to acquire from the pool.
//
// This may close an idle connection from another pool to make room for the new
// connection.
//
// Safe to call from multiple goroutines.
//
// Connection limitations are best-effort to avoid unnecessary locking.
func (p *MultiPool[K]) Acquire(ctx context.Context, key K) (*MultiPoolConn, error) {
	if !p.started {
		panic("multi pool not started")
	}
	r, err := p.tickets.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	// The BeforeConnect hook will handle destroying idle connections if needed
	// when creating new connections at the global limit.
	req := &acquireContextReq{}
	conn, err := p.pools[key].pool.Acquire(context.WithValue(ctx, acquireContextKey, req))
	if err != nil {
		if req.createAttempts > 0 && !req.created {
			// We increment createAttempts before attempting to create;
			// but there's no callback on creation error, so we do that book-keeping here.
			p.pendingCreates.Add(int32(-req.createAttempts))
		}
		r.ReleaseUnused()
		return nil, err
	}
	// In the success case, pendingCreates is handled by the AfterConnect callback.

	return &MultiPoolConn{
		conn:     conn,
		resource: r,
		pool:     p,
	}, nil
}

// acquireConnSlot attempts to find an unused idle connection and mark it for
// deletion, to allow the creation of a new connection in another pool.
//
// Returns nil if under the MaxConns limit or if an idle connection was marked for deletion.
// Otherwise, returns an error where errors.Is(err, ErrNoIdleConnections) is true.
func (p *MultiPool[K]) acquireConnSlot(ctx context.Context, forPool *multiPoolMember[K]) error {
	existing := p.totalConns.Load()
	pendingCreates := p.pendingCreates.Load()
	pendingDestroys := p.pendingDestroys.Load()
	total := existing + pendingCreates - pendingDestroys
	// Use <= because pendingCreates now includes the current request (incremented
	// before this check). If total == MaxConns, this request is already counted.
	if total <= p.MaxConns {
		return nil
	}

	// If there are pending destroys, room will be made soon. Allow this create
	// to proceed, which may briefly exceed MaxConns. The shouldDestroyConnection
	// check will ensure we settle back to MaxConns.
	if pendingDestroys > 0 {
		return nil
	}

	idle := p.totalIdleConns.Load()
	if idle == 0 {
		return fmt.Errorf("%w: max %d: %d created, %d pending create, %d pending destroy, %d total", ErrNoIdleConnections, p.MaxConns, existing, pendingCreates, pendingDestroys, total)
	}

	// Try to mark an idle connection in OTHER pools for destruction.
	alreadyMarked := 0
	pools := 0
	for _, otherMember := range p.pools {
		if otherMember == forPool {
			continue
		}
		pools++
		idle := otherMember.pool.AcquireAllIdle(ctx)
		found := false
		for _, c := range idle {
			if !found {
				if IsMarkedForDestroy(c.Conn().PgConn()) {
					alreadyMarked++
				} else {
					p.markForDestroy(c.Conn().PgConn())
					found = true
				}
			}
			c.Release()
		}
		if found {
			return nil
		}
	}

	// If there are no other pools (single-pool case), or other pools have no
	// idle connections to reclaim, check if the CURRENT pool has idle connections.
	// If so, allow the creation - the idle connections will be destroyed by
	// shouldDestroyConnection when released, settling back to MaxConns.
	// This handles the race where pgxpool decides to create a new connection
	// before a just-released connection is fully available in the idle pool.
	if idle > 0 {
		return nil
	}

	return fmt.Errorf("%w: max %d: searched %d pools, %d already marked for destroy", ErrNoIdleConnections, p.MaxConns, pools, alreadyMarked)
}

func (p *MultiPool[K]) markIdle(conn *pgx.Conn, isIdle bool) {
	if isIdle {
		p.totalIdleConns.Add(1)
		conn.PgConn().CustomData()[idleKey] = true
	} else if _, ok := conn.PgConn().CustomData()[idleKey]; ok {
		p.totalIdleConns.Add(-1)
		delete(conn.PgConn().CustomData(), idleKey)
	}
}

// Close all pools and release all connections.
func (p *MultiPool[K]) Close() {
	p.closeOnce.Do(func() {
		close(p.closeChan)
		p.tickets.Close()
		for _, pool := range p.pools {
			pool.pool.Close()
		}
	})
}

const destroyKey = "xxx"
const idleKey = "idle"

func IsMarkedForDestroy(conn *pgconn.PgConn) bool {
	customData := conn.CustomData()
	if destroy, ok := customData[destroyKey].(bool); ok && destroy {
		return true
	}
	return false
}

func (p *MultiPool[K]) shouldDestroyConnection(_ *multiPoolMember[K], conn *pgx.Conn) bool {
	if IsMarkedForDestroy(conn.PgConn()) {
		return true
	}

	// Only destroy if we're actually over the limit. Don't consider pendingCreates
	// here because those creates might fail, and destroying connections preemptively
	// can starve the pending creates of idle connections to reclaim.
	// The pendingDestroys already account for connections that will be destroyed.
	totalConns := p.totalConns.Load() - p.pendingDestroys.Load()
	return totalConns > p.MaxConns
}

func (p *MultiPool[K]) markForDestroy(conn *pgconn.PgConn) {
	if IsMarkedForDestroy(conn) {
		return // Already marked, don't double-count pendingDestroys
	}
	conn.CustomData()[destroyKey] = true
	p.pendingDestroys.Add(1)
}

func (p *MultiPool[K]) backgroundHealthCheck() {
	ticker := time.NewTicker(p.healthCheckPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-p.closeChan:
			return
		// case <-p.healthCheckChan:
		// 	p.checkHealth()
		case <-ticker.C:
			p.checkHealth()
		}
	}
}

func (p *MultiPool[K]) checkHealth() {
	for p.checkMaxConns() {
		// Technically Destroy is asynchronous but 500ms should be enough for it to
		// remove it from the underlying pool
		select {
		case <-p.closeChan:
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (p *MultiPool[K]) checkMaxConns() (destroyed bool) {
	// TotalConns can include ones that are being destroyed but we should have
	// sleep(500ms) around all of the destroys to help prevent that from throwing
	// off this check
	totalIdleConns := p.totalIdleConns.Load()
	if totalIdleConns == 0 {
		return false
	}

	totalConns := p.totalConns.Load()
	toClose := totalConns - p.MaxConns

	destroyedAny := false
	if toClose > 0 && totalIdleConns > 0 {
		// Destroy the number of connections needed to get to the max conns
		// We rely on random map iteration order to distribute fairness.
		for _, stuff := range p.pools {
			idle := stuff.pool.AcquireAllIdle(context.Background())
			for _, c := range idle {
				// Release will trigger our AfterRelease callback,
				// which will decide to destroy the connection if needed.
				c.Release()
				// Assume it would be destroyed if necessary.
				destroyedAny = true
			}
		}
	}

	return destroyedAny
}

type destroyMarker interface {
	markForDestroy(conn *pgconn.PgConn)
}

type MultiPoolConn struct {
	conn                    *pgxpool.Conn
	resource                *puddle.Resource[ticket]
	pool                    destroyMarker
	released                bool
	markForDestroyOnRelease bool
}

// Release returns the connection to the pool.
func (c *MultiPoolConn) Release() {
	if c.released {
		return
	}
	c.released = true
	if c.markForDestroyOnRelease {
		c.pool.markForDestroy(c.conn.Conn().PgConn())
	}
	// Must release conn before resource
	c.conn.Release()
	c.resource.Release()
}

// Value returns the connection.
func (c *MultiPoolConn) Value() *pgxpool.Conn {
	// This will panic if released.
	c.resource.Value()
	return c.conn
}

// MarkForDestroy marks the connection for destruction after it is released.
func (c *MultiPoolConn) MarkForDestroy() {
	c.markForDestroyOnRelease = true
}

func (c *MultiPoolConn) ReleaseAndDestroy() {
	c.MarkForDestroy()
	c.Release()
}

type multiPoolMember[K comparable] struct {
	key    K
	pool   *pgxpool.Pool
	config *pgxpool.Config
	parent *MultiPool[K]
}

// BeforeConnect is called before a new connection is made. It is passed a copy of the underlying pgx.ConnConfig and
// will not impact any existing open connections.
func (m *multiPoolMember[K]) beforeConnect(ctx context.Context, connCfg *pgx.ConnConfig) error {
	if m.config.BeforeConnect != nil {
		if err := m.config.BeforeConnect(ctx, connCfg); err != nil {
			return err
		}
	}

	// Track pending creates BEFORE acquireConnSlot so that concurrent callers
	// see an accurate count. This bounds the overshoot to approximately the
	// number of goroutines that can increment pendingCreates before the first
	// one reaches acquireConnSlot's check.
	req, hasReq := ctx.Value(acquireContextKey).(*acquireContextReq)
	if hasReq {
		req.createAttempts++
		m.parent.pendingCreates.Add(1)
	}

	if err := m.parent.acquireConnSlot(ctx, m); err != nil {
		if hasReq {
			m.parent.pendingCreates.Add(-1)
			req.createAttempts-- // Prevent double-decrement in Acquire
		}
		return err
	}

	// TODO: if !hasReq, trigger health check, since we may be creating idle conns
	// without incrementing pendingCreates (e.g., from MinIdleConns).
	//
	// TODO: we could refuse to acquireConnSlot here unless we have a valid ticket,
	// and dedicate re-balancing MinIdleConns between pools entirely to the
	// healthcheck thread/process, which may help prevent bursting beyond MaxConns
	// due to untracked pendingCreates not matching the tracked pendingDestroys.

	return nil
}

// AfterConnect is called after a connection is established, but before it is added to the pool.
func (m *multiPoolMember[K]) afterConnect(ctx context.Context, conn *pgx.Conn) error {
	if m.config.AfterConnect != nil {
		if err := m.config.AfterConnect(ctx, conn); err != nil {
			return err
		}
	}

	if req, ok := ctx.Value(acquireContextKey).(*acquireContextReq); ok {
		req.created = true
		m.parent.pendingCreates.Add(int32(-req.createAttempts))
	}
	m.parent.totalConns.Add(1)
	m.parent.markIdle(conn, true)
	return nil
}

// PrepareConn is called before a connection is acquired from the pool. If this function returns true, the connection
// is considered valid, otherwise the connection is destroyed. If the function returns a non-nil error, the instigating
// query will fail with the returned error.
//
// Specifically, this means that:
//
//   - If it returns true and a nil error, the query proceeds as normal.
//   - If it returns true and an error, the connection will be returned to the pool, and the instigating query will fail with the returned error.
//   - If it returns false, and an error, the connection will be destroyed, and the query will fail with the returned error.
//   - If it returns false and a nil error, the connection will be destroyed, and the instigating query will be retried on a new connection.
func (m *multiPoolMember[K]) prepareConn(ctx context.Context, conn *pgx.Conn) (bool, error) {
	if m.config.PrepareConn != nil {
		if ok, err := m.config.PrepareConn(ctx, conn); !ok || err != nil {
			if !ok {
				m.parent.markForDestroy(conn.PgConn())
			}

			return ok, err
		}
	}

	m.parent.markIdle(conn, false)
	return true, nil
}

// AfterRelease is called after a connection is released, but before it is returned to the pool. It must return true to return the connection to the pool or false to destroy the connection.
func (m *multiPoolMember[K]) afterRelease(conn *pgx.Conn) bool {
	m.parent.markIdle(conn, true)

	if m.config.AfterRelease != nil {
		if !m.config.AfterRelease(conn) {
			m.parent.markForDestroy(conn.PgConn())
			return false
		}
	}

	if m.parent.shouldDestroyConnection(m, conn) {
		m.parent.markForDestroy(conn.PgConn())
		return false
	}

	return true
}

// BeforeClose is called right before a connection is closed and removed from the pool.
func (m *multiPoolMember[K]) beforeClose(conn *pgx.Conn) {
	m.parent.totalConns.Add(-1)
	if IsMarkedForDestroy(conn.PgConn()) {
		m.parent.pendingDestroys.Add(-1)
	}
	m.parent.markIdle(conn, false)
}

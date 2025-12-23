package backend

import (
	"context"
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

type MultiPool[K comparable] struct {
	MaxConns int32
	pools    map[K]*poolStuff[K]
	stuff    []*poolStuff[K]
	started  bool
	tickets  *puddle.Pool[ticket]

	closeOnce sync.Once

	// Maintinence
	closeChan         chan struct{}
	logger            *slog.Logger
	healthCheckPeriod time.Duration

	totalConns     atomic.Int32
	totalIdleConns atomic.Int32
}

func NewMultiPool[K comparable](maxConns int32, logger *slog.Logger) *MultiPool[K] {
	tickets, err := puddle.NewPool[ticket](&puddle.Config[ticket]{
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
		pools:             make(map[K]*poolStuff[K]),
		tickets:           tickets,
		closeChan:         make(chan struct{}),
		logger:            logger,
		healthCheckPeriod: time.Second * 10,
	}
}

type ticket struct{}

// Not thread-safe.
func (p *MultiPool[K]) AddPool(ctx context.Context, key K, givenConfig *pgxpool.Config) error {
	if p.started {
		panic("multi pool already started")
	}

	stuff := &poolStuff[K]{
		key:    key,
		config: givenConfig.Copy(),
	}

	// BeforeConnect: prevent new connections to be created if we're at the max
	// conns limit
	if givenConfig.BeforeConnect != nil {
		stuff.config.BeforeConnect = func(ctx context.Context, connCfg *pgx.ConnConfig) error {
			if err := givenConfig.BeforeConnect(ctx, connCfg); err != nil {
				return err
			}

			totalConns := p.totalConns.Load()
			if totalConns >= p.MaxConns {
				return fmt.Errorf("connection limit reached: %d >= %d", totalConns, p.MaxConns)
			}

			return nil
		}
	} else {
		stuff.config.BeforeConnect = func(ctx context.Context, connCfg *pgx.ConnConfig) error {
			totalConns := p.totalConns.Load()
			if totalConns >= p.MaxConns {
				return fmt.Errorf("connection limit reached: %d >= %d", totalConns, p.MaxConns)
			}
			return nil
		}
	}

	// AfterConnect: increment the total conns count
	//               increment the idle conns count
	if givenConfig.AfterConnect != nil {
		stuff.config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			p.totalConns.Add(1)
			p.totalIdleConns.Add(1)
			return givenConfig.AfterConnect(ctx, conn)
		}
	} else {
		stuff.config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			p.totalConns.Add(1)
			p.totalIdleConns.Add(1)
			return nil
		}
	}

	// BeforeAcquire(aka PrepareConn): decrement the idle conns count
	if givenConfig.PrepareConn != nil {
		stuff.config.PrepareConn = func(ctx context.Context, conn *pgx.Conn) (bool, error) {
			p.totalIdleConns.Add(-1)
			return givenConfig.PrepareConn(ctx, conn)
		}
	} else {
		stuff.config.PrepareConn = func(ctx context.Context, conn *pgx.Conn) (bool, error) {
			p.totalIdleConns.Add(-1)
			return true, nil
		}
	}

	// AfterRelease is called after a connection is released, but before it is
	// returned to the pool. It must return true to return the connection to the
	// pool or false to destroy the connection.
	if givenConfig.AfterRelease != nil {
		stuff.config.AfterRelease = func(conn *pgx.Conn) bool {
			p.totalIdleConns.Add(1)

			if !givenConfig.AfterRelease(conn) {
				return false
			}

			if p.shouldDestroyConnection(stuff, conn) {
				return false
			}

			return true
		}
	} else {
		stuff.config.AfterRelease = func(conn *pgx.Conn) bool {
			p.totalIdleConns.Add(1)
			return !p.shouldDestroyConnection(stuff, conn)
		}
	}

	if givenConfig.BeforeClose != nil {
		stuff.config.BeforeClose = func(conn *pgx.Conn) {
			p.totalConns.Add(-1)
			p.totalIdleConns.Add(-1)
			givenConfig.BeforeClose(conn)
		}
	} else {
		stuff.config.BeforeClose = func(conn *pgx.Conn) {
			p.totalConns.Add(-1)
			p.totalIdleConns.Add(-1)
		}
	}

	pool, err := pgxpool.NewWithConfig(ctx, stuff.config)
	if err != nil {
		return err
	}

	stuff.pool = pool
	p.pools[key] = stuff
	p.stuff = append(p.stuff, stuff)
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

	conn, err := p.pools[key].pool.Acquire(ctx)
	if err != nil {
		r.ReleaseUnused()
		return nil, err
	}

	return &MultiPoolConn{
		conn:     conn,
		resource: r,
	}, nil
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

const destroyKey = "XXX"

func MarkForDestroy(conn *pgconn.PgConn) {
	conn.CustomData()[destroyKey] = true
}

func IsMarkedForDestroy(conn *pgconn.PgConn) bool {
	customData := conn.CustomData()
	if destroy, ok := customData[destroyKey].(bool); ok && destroy {
		return true
	}
	return false
}

func (p *MultiPool[K]) shouldDestroyConnection(stuff *poolStuff[K], conn *pgx.Conn) bool {
	if IsMarkedForDestroy(conn.PgConn()) {
		return true
	}

	totalConns := p.totalConns.Load()
	return totalConns > p.MaxConns
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

type MultiPoolConn struct {
	conn     *pgxpool.Conn
	resource *puddle.Resource[ticket]
}

// Release returns the connection to the pool.
func (c *MultiPoolConn) Release() {
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
	c.conn.Conn().PgConn().CustomData()[destroyKey] = true
}

func (c *MultiPoolConn) ReleaseAndDestroy() {
	c.MarkForDestroy()
	c.Release()
}

type poolStuff[K comparable] struct {
	key    K
	pool   *pgxpool.Pool
	config *pgxpool.Config
}

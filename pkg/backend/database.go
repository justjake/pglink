package backend

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/justjake/pglink/pkg/config"
)

// Database manages connection pools to a backend PostgreSQL server.
// It coordinates multiple per-user pools under a global connection limit,
// providing fair scheduling and connection stealing when at capacity.
type Database struct {
	config  config.DatabaseConfig
	secrets *config.SecretCache

	mu        sync.RWMutex
	userPools map[config.UserConfig]*pgxpool.Pool

	// Connection coordination
	connMgr *connManager[config.UserConfig]
}

// NewDatabase creates a new Database with the given database configuration.
// The global max connections is taken from cfg.Backend.PoolMaxConns.
// All user pools are created eagerly to respect MinIdleConns settings.
func NewDatabase(ctx context.Context, cfg config.DatabaseConfig, secrets *config.SecretCache) (*Database, error) {
	db := &Database{
		config:    cfg,
		secrets:   secrets,
		userPools: make(map[config.UserConfig]*pgxpool.Pool),
		connMgr:   newConnManager(cfg.Backend.PoolMaxConns, cfg.Users),
	}

	// Set up connection stealing callback
	db.connMgr.setStealFunc(db.tryStealIdleConnection)

	// Eagerly create all user pools to respect MinIdleConns
	for _, user := range cfg.Users {
		pool, err := db.createPool(ctx, user)
		if err != nil {
			// Close any pools we already created
			db.Close()
			return nil, fmt.Errorf("failed to create pool for user: %w", err)
		}
		db.userPools[user] = pool
	}

	return db, nil
}

// GetPool returns the connection pool for the given user, or nil if none exists.
func (d *Database) GetPool(user config.UserConfig) *pgxpool.Pool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.userPools[user]
}

// SetPool sets the connection pool for the given user.
func (d *Database) SetPool(user config.UserConfig, pool *pgxpool.Pool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.userPools[user] = pool
}

// getPool returns the pool for the user.
// Since pools are created eagerly in NewDatabase, this should always succeed
// for valid users.
func (d *Database) getPool(user config.UserConfig) (*pgxpool.Pool, error) {
	pool := d.GetPool(user)
	if pool == nil {
		return nil, fmt.Errorf("no pool for user (this should not happen)")
	}
	return pool, nil
}

// createPool creates a new connection pool for the given user.
func (d *Database) createPool(ctx context.Context, user config.UserConfig) (*pgxpool.Pool, error) {
	poolCfg, err := d.poolConfigForUser(ctx, user)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}

	return pool, nil
}

// poolConfigForUser builds a pgxpool.Config for the given user.
func (d *Database) poolConfigForUser(ctx context.Context, user config.UserConfig) (*pgxpool.Config, error) {
	cfg, err := d.config.Backend.PoolConfig()
	if err != nil {
		return nil, err
	}

	// Resolve and set user credentials
	username, err := d.secrets.Get(ctx, user.Username)
	if err != nil {
		return nil, fmt.Errorf("failed to get username: %w", err)
	}
	password, err := d.secrets.Get(ctx, user.Password)
	if err != nil {
		return nil, fmt.Errorf("failed to get password: %w", err)
	}

	cfg.ConnConfig.User = username
	cfg.ConnConfig.Password = password

	// Install callbacks to track actual database connections
	d.installConnectionCallbacks(cfg)

	return cfg, nil
}

// installConnectionCallbacks sets up BeforeConnect and BeforeClose callbacks
// to track actual database connections across all pools.
func (d *Database) installConnectionCallbacks(cfg *pgxpool.Config) {
	// Chain with any existing BeforeConnect
	existingBeforeConnect := cfg.BeforeConnect
	cfg.BeforeConnect = func(ctx context.Context, connCfg *pgx.ConnConfig) error {
		// Check global limit before allowing new connection
		if !d.connMgr.tryReserveDBConn() {
			return ErrConnectionLimitReached
		}

		if existingBeforeConnect != nil {
			if err := existingBeforeConnect(ctx, connCfg); err != nil {
				// Release the reservation since connection won't be created
				d.connMgr.releaseDBConn()
				return err
			}
		}
		return nil
	}

	// Chain with any existing BeforeClose
	existingBeforeClose := cfg.BeforeClose
	cfg.BeforeClose = func(conn *pgx.Conn) {
		// Release the connection slot
		d.connMgr.releaseDBConn()

		if existingBeforeClose != nil {
			existingBeforeClose(conn)
		}
	}
}

// PoolConfig returns a pgxpool.Config for the given user.
// If a pool already exists for this user, returns its config.
// Otherwise, builds a new config from the backend configuration and user credentials.
func (d *Database) PoolConfig(ctx context.Context, user config.UserConfig) (*pgxpool.Config, error) {
	// Check if pool already exists
	if pool := d.GetPool(user); pool != nil {
		return pool.Config(), nil
	}

	return d.poolConfigForUser(ctx, user)
}

// PooledConn wraps a pgxpool connection with automatic release tracking.
type PooledConn struct {
	*pgxpool.Conn
	db       *Database
	user     config.UserConfig
	released bool
}

// Release returns the connection to the pool.
// It is safe to call Release multiple times.
func (pc *PooledConn) Release() {
	if pc.released {
		return
	}
	pc.released = true
	pc.Conn.Release()
	// Note: We don't release dbConns here because the connection goes back
	// to the pool as idle. dbConns is only decremented in BeforeClose when
	// the connection is actually closed.
}

// Acquire acquires a connection from the pool for the given user.
// It respects the global connection limit and uses fair scheduling when
// at capacity, ensuring that users with many pending requests don't
// starve users with few pending requests.
//
// The returned PooledConn must be released by calling Release() when done.
// Failing to release connections will exhaust the connection pool.
func (d *Database) Acquire(ctx context.Context, user config.UserConfig) (*PooledConn, error) {
	// Get the user's pool (created eagerly in NewDatabase)
	pool, err := d.getPool(user)
	if err != nil {
		return nil, err
	}

	// Acquire with retry loop for handling connection limit
	for {
		// Wait for fair turn if at capacity
		if err := d.connMgr.waitForTurn(ctx, user); err != nil {
			return nil, err
		}

		// Try to acquire from the user's pool
		conn, err := pool.Acquire(ctx)
		if err == nil {
			return &PooledConn{
				Conn: conn,
				db:   d,
				user: user,
			}, nil
		}

		// Check if we hit the global connection limit
		if errors.Is(err, ErrConnectionLimitReached) {
			// Try to steal an idle connection from another pool
			if d.tryStealIdleConnection(user) {
				// Successfully stole a connection, retry acquire
				continue
			}

			// Couldn't steal, wait for a connection to be released
			// Use a short timeout to avoid blocking forever
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Millisecond):
				// Retry
				continue
			}
		}

		// Some other error
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
}

// tryStealIdleConnection attempts to close an idle connection from any pool
// except the excluded user. Returns true if a connection was stolen.
func (d *Database) tryStealIdleConnection(exclude config.UserConfig) bool {
	// Collect candidate pools under the lock, then release before blocking operations
	var candidates []*pgxpool.Pool

	d.mu.RLock()
	for user, pool := range d.userPools {
		if user == exclude {
			continue
		}
		stat := pool.Stat()
		if stat.IdleConns() > 0 {
			candidates = append(candidates, pool)
		}
	}
	d.mu.RUnlock()

	// Try to steal from each candidate (outside the lock to avoid deadlock)
	for _, pool := range candidates {
		if d.tryHijackIdleConnection(pool) {
			return true
		}
	}

	return false
}

// tryHijackIdleConnection tries to acquire an idle connection and close it.
// This makes room in the global connection count.
// Returns true if successful.
func (d *Database) tryHijackIdleConnection(pool *pgxpool.Pool) bool {
	// Use a very short timeout - we only want to grab an idle connection,
	// not wait for one to become available
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return false
	}

	// Got a connection - hijack and close it
	// Hijack removes it from the pool, then we close the underlying connection
	pgConn := conn.Hijack()
	_ = pgConn.Close(context.Background())
	// Note: BeforeClose will be called, decrementing dbConns

	return true
}

// Stats returns statistics about the database's connection pools.
func (d *Database) Stats() DatabaseStats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	stats := DatabaseStats{
		ConnManager: d.connMgr.stats(),
		Pools:       make(map[string]PoolStats),
	}

	for user, pool := range d.userPools {
		// Use username as key (need to resolve it)
		key := fmt.Sprintf("user_%p", &user) // Use pointer as simple key
		poolStat := pool.Stat()
		stats.Pools[key] = PoolStats{
			TotalConns:    poolStat.TotalConns(),
			AcquiredConns: poolStat.AcquiredConns(),
			IdleConns:     poolStat.IdleConns(),
			MaxConns:      poolStat.MaxConns(),
		}
	}

	return stats
}

// TotalDBConnections returns the total number of actual database connections
// across all pools. This is the authoritative count tracked via callbacks.
func (d *Database) TotalDBConnections() int32 {
	return d.connMgr.currentDBConns()
}

// DatabaseStats contains statistics about the database and its pools.
type DatabaseStats struct {
	ConnManager connManagerStats
	Pools       map[string]PoolStats
}

// PoolStats contains statistics about a single connection pool.
type PoolStats struct {
	TotalConns    int32
	AcquiredConns int32
	IdleConns     int32
	MaxConns      int32
}

// Close closes all connection pools.
func (d *Database) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, pool := range d.userPools {
		pool.Close()
	}
	d.userPools = make(map[config.UserConfig]*pgxpool.Pool)
}

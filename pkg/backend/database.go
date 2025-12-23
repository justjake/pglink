package backend

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/justjake/pglink/pkg/config"
)

// Database manages connection pools to a backend PostgreSQL server.
// It coordinates multiple per-user pools under a global connection limit,
// providing fair scheduling and connection stealing when at capacity.
//
// All pools are created eagerly in NewDatabase and the userPools map is
// immutable after construction, so no locking is needed.
type Database struct {
	config  *config.DatabaseConfig
	secrets *config.SecretCache
	pools   *MultiPool[config.UserConfig]
	logger  *slog.Logger
}

// NewDatabase creates a new Database with the given database configuration.
// The global max connections is taken from cfg.Backend.PoolMaxConns.
// All user pools are created eagerly to respect MinIdleConns settings.
func NewDatabase(ctx context.Context, cfg *config.DatabaseConfig, secrets *config.SecretCache, logger *slog.Logger) (*Database, error) {
	db := &Database{
		config:  cfg,
		secrets: secrets,
		pools:   NewMultiPool[config.UserConfig](cfg.Backend.PoolMaxConns),
		logger:  logger,
	}

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

// GetPool returns the connection pool for the given user.
// Panics if the user is not known (all valid users have pools created in NewDatabase).
func (d *Database) GetPool(user config.UserConfig) *pgxpool.Pool {
	pool := d.userPools[user]
	if pool == nil {
		panic("Database: unknown user")
	}
	return pool
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

// Acquire acquires a connection from the pool for the given user.
// It respects the global connection limit and uses fair scheduling when
// at capacity, ensuring that users with many pending requests don't
// starve users with few pending requests.
//
// The returned PooledConn must be released by calling Release() when done.
// Failing to release connections will exhaust the connection pool.
func (d *Database) Acquire(ctx context.Context, user config.UserConfig) (*PooledConn, error) {
	// Get the user's pool (created eagerly in NewDatabase)
	pool := d.GetPool(user)

	// Acquire with retry loop for handling connection limit
	for {
		// Try to acquire from the user's pool first.
		// This will succeed immediately if there's an idle connection available.
		// Only if the pool needs to create a NEW connection will BeforeConnect
		// be called, which may return ErrConnectionLimitReached.
		conn, err := pool.Acquire(ctx)
		if err == nil {
			session, err := GetOrCreateSession(conn.Conn().PgConn(), d, user)
			if err != nil {
				// Drop the connection from the pool.
				d.logger.Error("failed to create session, dropping connection", "error", err, "pid", conn.Conn().PgConn().PID())
				closeErr := conn.Hijack().Close(context.Background())
				if closeErr != nil {
					err = errors.Join(err, closeErr)
				}
				return nil, err
			}

			// Acquire the session so it can be used for reading/writing
			if err := session.Acquire(); err != nil {
				d.logger.Error("failed to acquire session", "error", err)
				conn.Release()
				return nil, err
			}

			return &PooledConn{
				conn:    conn,
				session: session,
			}, nil
		}

		// Check if we hit the global connection limit (no idle connections,
		// and can't create new ones because we're at the limit)
		if errors.Is(err, ErrConnectionLimitReached) {
			// Try to steal an idle connection from another pool
			if d.tryStealIdleConnection(user) {
				// Successfully stole a connection, retry acquire
				continue
			}

			// No idle connections available and at the global limit.
			// Poll for an idle connection to become available.
			// In transaction pooling mode, connections are released back to the pool
			// (not closed), so we can't rely on waitForTurn which only wakes on close.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(5 * time.Millisecond):
				// Retry - an idle connection may have been released
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
	for user, pool := range d.userPools {
		if user == exclude {
			continue
		}
		if pool.Stat().IdleConns() > 0 {
			if d.tryHijackIdleConnection(pool) {
				return true
			}
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
	stats := DatabaseStats{
		ConnManager: d.connMgr.stats(),
		Pools:       make(map[string]PoolStats),
	}

	for user, pool := range d.userPools {
		poolStat := pool.Stat()
		stats.Pools[user.String()] = PoolStats{
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
	for _, pool := range d.userPools {
		pool.Close()
	}
}

func (d *Database) Name() string {
	return fmt.Sprintf("%s:%d/%s", d.config.Backend.Host, d.config.Backend.Port, d.config.Backend.Database)
}

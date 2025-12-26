package backend

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/pgwire"
)

// Database manages connection pools to a backend PostgreSQL server.
// It coordinates multiple per-user pools under a global connection limit,
// providing fair scheduling and connection stealing when at capacity.
//
// All pools are created eagerly in NewDatabase and the userPools map is
// immutable after construction, so no locking is needed.
type Database struct {
	config         *config.DatabaseConfig
	secrets        *config.SecretCache
	pool           *MultiPool[config.UserConfig]
	logger         *slog.Logger
	tracingEnabled bool

	// stmtCache caches prepared statement metadata for reuse across backend connections.
	// This allows statements to be re-created on backends that don't have them yet.
	stmtCache *pgwire.PreparedStatementCache

	destroyedConns atomic.Int32
}

// NewDatabase creates a new Database with the given database configuration.
// The global max connections is taken from cfg.Backend.PoolMaxConns.
// All user pools are created eagerly to respect MinIdleConns settings.
// If tracingEnabled is true, otelpgx tracing will be added to connections
// (assumes the global tracer provider has been configured).
func NewDatabase(ctx context.Context, cfg *config.DatabaseConfig, secrets *config.SecretCache, logger *slog.Logger, tracingEnabled bool) (*Database, error) {
	db := &Database{
		config:         cfg,
		secrets:        secrets,
		tracingEnabled: tracingEnabled,
		stmtCache:      pgwire.NewPreparedStatementCache(cfg.Backend.GetPreparedStatementCacheSize()),
	}
	db.logger = logger.With("backend", db.Name())

	db.pool = NewMultiPool[config.UserConfig](cfg.Backend.PoolMaxConns, db.logger)
	for _, user := range cfg.Users {
		cfg, err := db.poolConfigForUser(ctx, user)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to create pool for user %s: %w", user.String(), err)
		}
		err = db.pool.AddPool(ctx, user, cfg)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to add pool for user %s: %w", user.String(), err)
		}
	}
	db.pool.Start()

	return db, nil
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

	// Set tracer if enabled (uses global tracer provider set by observability.NewTracerProvider)
	if d.tracingEnabled {
		cfg.ConnConfig.Tracer = otelpgx.NewTracer()
	}

	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		d.logger.Debug("connected to backend", "user", username, "pid", conn.PgConn().PID())
		return nil
	}
	cfg.BeforeClose = func(conn *pgx.Conn) {
		d.logger.Debug("disconnecting from backend", "user", username, "pid", conn.PgConn().PID())
	}

	return cfg, nil
}

// Acquire acquires a connection from the pool for the given user.
// It respects the global connection limit and uses fair scheduling when
// at capacity, ensuring that users with many pending requests don't
// starve users with few pending requests.
//
// The returned PooledConn must be released by calling Release() when done.
// Failing to release connections will exhaust the connection pool.
func (d *Database) Acquire(ctx context.Context, user config.UserConfig) (*PooledBackend, error) {
	conn, err := d.pool.Acquire(ctx, user)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	session, err := GetOrCreateSession(conn.Value().Conn().PgConn(), d, user)
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	if err := session.Acquire(); err != nil {
		conn.Release()
		return nil, fmt.Errorf("failed to acquire session: %w", err)
	}

	return &PooledBackend{
		conn:    conn,
		session: session,
	}, nil
}

// Stats returns statistics about the database's connection pools.
func (d *Database) Stats() DatabaseStats {
	// TODO: move to MultiPool, private access violation here:
	stats := DatabaseStats{
		MaxConns:       d.pool.MaxConns,
		TotalConns:     d.pool.totalConns.Load(),
		IdleConns:      d.pool.totalIdleConns.Load(),
		DestroyedConns: d.destroyedConns.Load(),
		Pools:          make(map[string]PoolStats, len(d.pool.pools)),
	}

	for user, stuff := range d.pool.pools {
		poolStat := stuff.pool.Stat()
		stats.Pools[user.String()] = PoolStats{
			TotalConns:    poolStat.TotalConns(),
			AcquiredConns: poolStat.AcquiredConns(),
			IdleConns:     poolStat.IdleConns(),
			MaxConns:      poolStat.MaxConns(),
		}
	}

	return stats
}

// DatabaseStats contains statistics about the database and its pools.
type DatabaseStats struct {
	MaxConns       int32
	TotalConns     int32
	IdleConns      int32
	DestroyedConns int32
	Pools          map[string]PoolStats
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
	d.pool.Close()
}

func (d *Database) Name() string {
	return fmt.Sprintf("%s:%d/%s", d.config.Backend.Host, d.config.Backend.Port, d.config.Backend.Database)
}

// StatementCache returns the prepared statement cache for this database.
// The cache stores metadata for re-creating statements on backend connections.
func (d *Database) StatementCache() *pgwire.PreparedStatementCache {
	return d.stmtCache
}

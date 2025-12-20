package backend

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/justjake/pglink/pkg/config"
)

// Database manages connection pools to a backend PostgreSQL server.
type Database struct {
	config  config.BackendConfig
	secrets *config.SecretCache

	mu        sync.RWMutex
	userPools map[config.UserConfig]*pgxpool.Pool
}

// NewDatabase creates a new Database with the given backend configuration.
func NewDatabase(cfg config.BackendConfig, secrets *config.SecretCache) *Database {
	return &Database{
		config:    cfg,
		secrets:   secrets,
		userPools: make(map[config.UserConfig]*pgxpool.Pool),
	}
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

// PoolConfig returns a pgxpool.Config for the given user.
// If a pool already exists for this user, returns its config.
// Otherwise, builds a new config from the backend configuration and user credentials.
func (d *Database) PoolConfig(ctx context.Context, user config.UserConfig) (*pgxpool.Config, error) {
	// Check if pool already exists
	if pool := d.GetPool(user); pool != nil {
		return pool.Config(), nil
	}

	// Get base config from backend
	cfg, err := d.config.PoolConfig()
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

	return cfg, nil
}

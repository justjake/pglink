// Package e2e provides end-to-end testing infrastructure for pglink.
// It manages docker compose backend databases and pglink service lifecycle,
// providing a clean test environment for comprehensive integration testing.
package e2e

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/justjake/pglink/pkg/config"
	pkge2e "github.com/justjake/pglink/pkg/e2e"
	"github.com/justjake/pglink/pkg/frontend"
)

const (
	// ServiceStartTimeout is how long to wait for pglink to start
	ServiceStartTimeout = 30 * time.Second
)

// Re-export types and values from pkg/e2e for convenience in tests.
type TestUser = pkge2e.TestUser
type TestDatabase = pkge2e.TestDatabase

var PredefinedUsers = pkge2e.PredefinedUsers
var PredefinedDatabases = pkge2e.PredefinedDatabases

// ConfigModifier is a function that modifies a config before starting the service.
// Used by StartWithConfig to customize timeout settings, pool sizes, etc.
type ConfigModifier func(*config.Config)

// Harness manages the test infrastructure lifecycle
type Harness struct {
	t           *testing.T
	projectDir  string // Current working directory (may be a worktree)
	mainRepoDir string // Main git repo (for shared docker-compose)
	configPath  string

	// Algo is the session algorithm to use ("default" or "ring").
	// Set this before calling Start().
	Algo string

	// ConfigModifier is called to modify the config before starting the service.
	// Set this before calling Start() or use StartWithConfig().
	ConfigModifier ConfigModifier

	// pglinkPort is dynamically allocated to avoid conflicts between worktrees
	pglinkPort int

	service   *frontend.Service
	serviceWg sync.WaitGroup
	cancel    context.CancelFunc

	// Track whether we started docker compose (so we know whether to stop it)
	startedDockerCompose bool

	logger *slog.Logger
}

// NewHarness creates a new test harness. Call Start() to initialize infrastructure.
func NewHarness(t *testing.T) *Harness {
	t.Helper()

	h := NewHarnessForMain()
	h.t = t
	return h
}

// NewHarnessForMain creates a harness for use in TestMain (without a *testing.T).
// Errors will cause a panic instead of t.Fatalf.
func NewHarnessForMain() *Harness {
	// Find project root (worktree root containing docker-compose.yaml)
	projectDir, err := pkge2e.CurrentWorktreePath()
	if err != nil {
		panic(fmt.Sprintf("failed to find project root: %v", err))
	}

	// Find main repo for docker-compose (shared across worktrees)
	mainRepoDir, err := pkge2e.MainWorktreePath(projectDir)
	if err != nil {
		panic(fmt.Sprintf("failed to find main repo: %v", err))
	}

	// Allocate a free port for pglink to avoid conflicts between worktrees
	pglinkPort, err := pkge2e.FindFreePort()
	if err != nil {
		panic(fmt.Sprintf("failed to find free port: %v", err))
	}

	configPath := filepath.Join(projectDir, "pglink.json")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		panic(fmt.Sprintf("pglink.json not found at %s", configPath))
	}

	logLevel := slog.LevelInfo // Use INFO to avoid pgproto3 tracing overhead
	if levelStr := os.Getenv("PGLINK_LOG_LEVEL"); levelStr != "" {
		switch levelStr {
		case "DEBUG", "debug":
			logLevel = slog.LevelDebug
		case "INFO", "info":
			logLevel = slog.LevelInfo
		case "WARN", "warn":
			logLevel = slog.LevelWarn
		case "ERROR", "error":
			logLevel = slog.LevelError
		}
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	return &Harness{
		t:           nil,
		projectDir:  projectDir,
		mainRepoDir: mainRepoDir,
		configPath:  configPath,
		pglinkPort:  pglinkPort,
		logger:      logger,
	}
}

// Start initializes the test infrastructure:
// 1. Ensures docker-compose is running
// 2. Waits for all backend databases to be healthy
// 3. Starts the pglink service
func (h *Harness) Start(ctx context.Context) {
	if h.t != nil {
		h.t.Helper()
	}

	h.logger.Info("starting e2e test harness", "projectDir", h.projectDir)

	// Ensure docker-compose is running
	h.ensureDockerCompose(ctx)

	// Wait for backends to be healthy
	h.waitForBackends(ctx)

	// Start pglink service
	h.startService(ctx)

	// Wait for pglink to be ready
	h.waitForService(ctx)

	h.logger.Info("e2e test harness ready")
}

// Stop shuts down the test infrastructure gracefully
func (h *Harness) Stop() {
	h.logger.Info("stopping e2e test harness")

	// Shutdown service with a timeout
	if h.service != nil {
		h.cancel()

		// Wait for service shutdown with a timeout
		done := make(chan struct{})
		go func() {
			h.serviceWg.Wait()
			close(done)
		}()

		select {
		case <-done:
			h.logger.Info("pglink service stopped")
		case <-time.After(3 * time.Second):
			h.logger.Warn("pglink service shutdown timed out, exiting anyway")
		}
	}

	// Note: We intentionally do NOT stop docker-compose after tests
	// to avoid disrupting other test runs or development work.
	// Docker-compose containers are left running for efficiency.
}

// fatalf reports a fatal error, using t.Fatalf if available or panicking otherwise
func (h *Harness) fatalf(format string, args ...any) {
	if h.t != nil {
		h.t.Fatalf(format, args...)
	} else {
		panic(fmt.Sprintf(format, args...))
	}
}

// ensureDockerCompose starts docker compose if not already running.
// Docker compose runs from the main repo directory so containers are shared
// across all worktrees.
func (h *Harness) ensureDockerCompose(ctx context.Context) {
	wasRunning := pkge2e.IsDockerComposeRunning(ctx, h.mainRepoDir)
	if err := pkge2e.EnsureDockerCompose(ctx, h.mainRepoDir, false, h.logger); err != nil {
		h.fatalf("failed to start docker compose: %v", err)
	}
	h.startedDockerCompose = !wasRunning
}

// waitForBackends waits for all backend databases to accept connections
func (h *Harness) waitForBackends(ctx context.Context) {
	if err := pkge2e.WaitForBackends(ctx, h.logger); err != nil {
		h.fatalf("failed to connect to backends: %v", err)
	}
}

// startService starts the pglink service
func (h *Harness) startService(ctx context.Context) {
	cfg, err := config.ReadConfigFile(h.configPath)
	if err != nil {
		h.fatalf("failed to read config: %v", err)
	}

	// Override listen port to use dynamically allocated port
	cfg.SetListenAddr(fmt.Sprintf(":%d", h.pglinkPort))

	// Apply algo setting if specified
	if h.Algo != "" {
		cfg.SetAlgo(h.Algo)
		h.logger.Info("using session algorithm", "algo", h.Algo)
	}

	// Apply config modifier if specified
	if h.ConfigModifier != nil {
		h.ConfigModifier(cfg)
	}

	secrets, err := config.NewSecretCacheFromEnv(ctx)
	if err != nil {
		h.fatalf("failed to create secrets cache: %v", err)
	}

	fsys := os.DirFS(cfg.Dir())

	if err := cfg.Validate(ctx, fsys, secrets, h.logger); err != nil {
		h.fatalf("config validation failed: %v", err)
	}

	// Create a cancellable context for the service that won't be cancelled
	// when the caller's context is cancelled (the caller may use a timeout
	// for setup, but we want the service to run until Stop() is called)
	svcCtx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel

	svc, err := frontend.NewService(svcCtx, cfg, fsys, secrets, h.logger, false, nil)
	if err != nil {
		h.fatalf("failed to create service: %v", err)
	}
	h.service = svc

	// Run service in background
	h.serviceWg.Add(1)
	go func() {
		defer h.serviceWg.Done()
		if err := svc.Listen(); err != nil && svcCtx.Err() == nil {
			h.logger.Error("service error", "error", err)
		}
	}()

	h.logger.Info("pglink service starting", "port", h.pglinkPort, "algo", cfg.GetAlgo())
}

// waitForService waits for pglink to accept connections
func (h *Harness) waitForService(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, ServiceStartTimeout)
	defer cancel()

	addr := fmt.Sprintf("localhost:%d", h.pglinkPort)
	if err := pkge2e.WaitForTCPPort(ctx, addr, h.logger); err != nil {
		h.fatalf("pglink service did not start in time: %v", err)
	}
}

// ConnectWithUser creates a connection pool through pglink with the specified user
func (h *Harness) ConnectWithUser(ctx context.Context, database string, user TestUser) (*pgxpool.Pool, error) {
	connStr := fmt.Sprintf(
		"postgres://%s:%s@localhost:%d/%s?sslmode=prefer",
		user.Username,
		user.Password,
		h.pglinkPort,
		database,
	)

	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pool config: %w", err)
	}

	// Configure pool for testing
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 1

	// Disable statement caching for transaction pooling compatibility.
	// In transaction pooling mode, the proxy changes statement name prefixes
	// each time a backend is acquired, so cached statements don't work.
	poolConfig.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	poolConfig.ConnConfig.StatementCacheCapacity = 0
	poolConfig.ConnConfig.DescriptionCacheCapacity = 0

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}

	// Don't ping - it creates backend acquisition that can interfere with
	// the test's expected acquisition patterns.

	return pool, nil
}

// Connect creates a connection pool through pglink with the default app user
func (h *Harness) Connect(ctx context.Context, database string) (*pgxpool.Pool, error) {
	return h.ConnectWithUser(ctx, database, PredefinedUsers.App)
}

// ConnectWithExecMode creates a connection pool with a specific query exec mode.
// This is useful for tests that need to use a different mode than the default
// DescribeExec mode (e.g., SimpleProtocol for stress tests).
func (h *Harness) ConnectWithExecMode(ctx context.Context, database string, mode pgx.QueryExecMode) (*pgxpool.Pool, error) {
	connStr := fmt.Sprintf(
		"postgres://%s:%s@localhost:%d/%s?sslmode=prefer",
		PredefinedUsers.App.Username,
		PredefinedUsers.App.Password,
		h.pglinkPort,
		database,
	)

	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pool config: %w", err)
	}

	poolConfig.MaxConns = 10
	poolConfig.MinConns = 1
	poolConfig.ConnConfig.DefaultQueryExecMode = mode
	poolConfig.ConnConfig.StatementCacheCapacity = 0
	poolConfig.ConnConfig.DescriptionCacheCapacity = 0

	return pgxpool.NewWithConfig(ctx, poolConfig)
}

// ConnectSingle creates a single connection through pglink (not a pool)
func (h *Harness) ConnectSingle(ctx context.Context, database string, user TestUser) (*pgx.Conn, error) {
	connStr := fmt.Sprintf(
		"postgres://%s:%s@localhost:%d/%s?sslmode=prefer",
		user.Username,
		user.Password,
		h.pglinkPort,
		database,
	)

	config, err := pgx.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Disable statement caching for transaction pooling compatibility.
	// In transaction pooling mode, the proxy changes statement name prefixes
	// each time a backend is acquired, so cached statements don't work.
	config.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	config.StatementCacheCapacity = 0
	config.DescriptionCacheCapacity = 0

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	return conn, nil
}

// ConnectSingleExec creates a single pgx.Conn using QueryExecModeExec.
// This mode sends Parse+Bind+Execute in a single pipelined message, avoiding
// the two-round-trip behavior of QueryExecModeDescribeExec that can cause issues
// with transaction pooling when the proxy reassigns backends between rounds.
func (h *Harness) ConnectSingleExec(ctx context.Context, database string, user TestUser) (*pgx.Conn, error) {
	connStr := fmt.Sprintf(
		"postgres://%s:%s@localhost:%d/%s?sslmode=prefer",
		user.Username,
		user.Password,
		h.pglinkPort,
		database,
	)

	config, err := pgx.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// QueryExecModeExec sends Parse+Bind+Execute+Sync as a single pipelined
	// message, which keeps the entire query on one backend connection.
	config.DefaultQueryExecMode = pgx.QueryExecModeExec
	config.StatementCacheCapacity = 0
	config.DescriptionCacheCapacity = 0

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	return conn, nil
}

// ConnectDirect creates a direct connection to a backend database, bypassing pglink
func (h *Harness) ConnectDirect(ctx context.Context, db TestDatabase) (*pgx.Conn, error) {
	connStr := fmt.Sprintf(
		"postgres://postgres:postgres@%s:%d/%s?sslmode=disable",
		db.BackendHost,
		db.BackendPort,
		db.BackendDB,
	)

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect directly: %w", err)
	}

	return conn, nil
}

// ProjectDir returns the project root directory
func (h *Harness) ProjectDir() string {
	return h.projectDir
}

// ConfigPath returns the path to pglink.json
func (h *Harness) ConfigPath() string {
	return h.configPath
}

// FileSystem returns the filesystem rooted at the project directory
func (h *Harness) FileSystem() fs.FS {
	return os.DirFS(h.projectDir)
}

// Logger returns the test logger
func (h *Harness) Logger() *slog.Logger {
	return h.logger
}

// ExecDirect executes SQL directly on a backend database as the postgres superuser.
// This is useful for test setup and teardown (creating tables, granting permissions, etc.).
func (h *Harness) ExecDirect(ctx context.Context, db TestDatabase, sql string) error {
	conn, err := h.ConnectDirect(ctx, db)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, sql)
	return err
}

// GetTestDatabase returns the TestDatabase config for the given database name.
// Panics if the database is not found.
func (h *Harness) GetTestDatabase(name string) TestDatabase {
	db := pkge2e.GetTestDatabase(name)
	if db == nil {
		panic(fmt.Sprintf("database %q not found in PredefinedDatabases", name))
	}
	return *db
}

// GetAlgo returns the session algorithm being used by this harness.
// Returns "default" if not explicitly set.
func (h *Harness) GetAlgo() string {
	if h.Algo == "" {
		return config.SessionAlgoDefault
	}
	return h.Algo
}

// Port returns the port pglink is listening on.
// This is dynamically allocated to avoid conflicts between worktrees.
func (h *Harness) Port() int {
	return h.pglinkPort
}

// AllSessionAlgos returns all valid session algorithm names.
// Use this to iterate over all algos in tests:
//
//	for _, algo := range AllSessionAlgos() { ... }
func AllSessionAlgos() []string {
	return config.AllSessionAlgos()
}

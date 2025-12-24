// Package e2e provides end-to-end testing infrastructure for pglink.
// It manages docker-compose backend databases and pglink service lifecycle,
// providing a clean test environment for comprehensive integration testing.
package e2e

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/frontend"
)

const (
	// DefaultPglinkPort is the port pglink listens on during tests
	DefaultPglinkPort = 16432

	// DockerComposeStartTimeout is how long to wait for docker-compose services
	DockerComposeStartTimeout = 2 * time.Minute

	// BackendHealthCheckInterval is how often to check backend health
	BackendHealthCheckInterval = 500 * time.Millisecond

	// ServiceStartTimeout is how long to wait for pglink to start
	ServiceStartTimeout = 30 * time.Second
)

// TestUser represents credentials for connecting through pglink
type TestUser struct {
	Username string
	Password string
}

// PredefinedUsers are the users configured in the test setup
var PredefinedUsers = struct {
	App       TestUser
	Admin     TestUser
	Developer TestUser
}{
	App:       TestUser{Username: "app", Password: "app_password"},
	Admin:     TestUser{Username: "admin", Password: "admin_password"},
	Developer: TestUser{Username: "developer", Password: "developer_password"},
}

// TestDatabase represents a database accessible through pglink
type TestDatabase struct {
	Name        string // Name as exposed through pglink (e.g., "alpha_uno")
	BackendHost string // Backend postgres host
	BackendPort int    // Backend postgres port
	BackendDB   string // Actual database name on backend (e.g., "uno")
}

// PredefinedDatabases are the databases configured in pglink.json
var PredefinedDatabases = []TestDatabase{
	{Name: "alpha_uno", BackendHost: "localhost", BackendPort: 15432, BackendDB: "uno"},
	{Name: "alpha_dos", BackendHost: "localhost", BackendPort: 15432, BackendDB: "dos"},
	{Name: "bravo_uno", BackendHost: "localhost", BackendPort: 15433, BackendDB: "uno"},
	{Name: "bravo_dos", BackendHost: "localhost", BackendPort: 15433, BackendDB: "dos"},
	{Name: "charlie_uno", BackendHost: "localhost", BackendPort: 15434, BackendDB: "uno"},
	{Name: "charlie_dos", BackendHost: "localhost", BackendPort: 15434, BackendDB: "dos"},
}

// Harness manages the test infrastructure lifecycle
type Harness struct {
	t          *testing.T
	projectDir string
	configPath string

	service   *frontend.Service
	serviceWg sync.WaitGroup
	cancel    context.CancelFunc

	// Track whether we started docker-compose (so we know whether to stop it)
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
	// Find project root (directory containing docker-compose.yaml)
	projectDir, err := findProjectRoot()
	if err != nil {
		panic(fmt.Sprintf("failed to find project root: %v", err))
	}

	configPath := filepath.Join(projectDir, "pglink.json")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		panic(fmt.Sprintf("pglink.json not found at %s", configPath))
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	return &Harness{
		t:          nil,
		projectDir: projectDir,
		configPath: configPath,
		logger:     logger,
	}
}

// findProjectRoot locates the project root by looking for docker-compose.yaml
func findProjectRoot() (string, error) {
	// Start from current working directory and walk up
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "docker-compose.yaml")); err == nil {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			// Reached root
			break
		}
		dir = parent
	}

	return "", fmt.Errorf("could not find docker-compose.yaml in any parent directory")
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

// ensureDockerCompose starts docker-compose if not already running
func (h *Harness) ensureDockerCompose(ctx context.Context) {
	// Check if containers are already running
	if h.isDockerComposeRunning(ctx) {
		h.logger.Info("docker-compose already running")
		return
	}

	h.logger.Info("starting docker-compose")
	h.startedDockerCompose = true

	cmd := exec.CommandContext(ctx, "docker-compose", "up", "-d", "--wait")
	cmd.Dir = h.projectDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		h.fatalf("failed to start docker-compose: %v", err)
	}

	h.logger.Info("docker-compose started")
}

// isDockerComposeRunning checks if all required containers are running
func (h *Harness) isDockerComposeRunning(ctx context.Context) bool {
	cmd := exec.CommandContext(ctx, "docker-compose", "ps", "--format", "{{.State}}")
	cmd.Dir = h.projectDir
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	// Check that we have running containers
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	runningCount := 0
	for _, line := range lines {
		if strings.Contains(line, "running") {
			runningCount++
		}
	}

	// We need all 3 containers (alpha, bravo, charlie) running
	return runningCount >= 3
}

// waitForBackends waits for all backend databases to accept connections
func (h *Harness) waitForBackends(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, DockerComposeStartTimeout)
	defer cancel()

	backends := []struct {
		name string
		port int
	}{
		{"alpha", 15432},
		{"bravo", 15433},
		{"charlie", 15434},
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(backends))

	for _, b := range backends {
		wg.Add(1)
		go func(name string, port int) {
			defer wg.Done()
			if err := h.waitForBackend(ctx, name, port); err != nil {
				errCh <- fmt.Errorf("backend %s: %w", name, err)
			}
		}(b.name, b.port)
	}

	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		h.fatalf("failed to connect to backends: %v", errs)
	}

	h.logger.Info("all backends healthy")
}

// waitForBackend waits for a single backend to be ready
func (h *Harness) waitForBackend(ctx context.Context, name string, port int) error {
	connStr := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", port)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := pgx.Connect(ctx, connStr)
		if err == nil {
			_ = conn.Close(ctx)
			h.logger.Info("backend ready", "name", name, "port", port)
			return nil
		}

		h.logger.Debug("waiting for backend", "name", name, "port", port, "error", err)
		time.Sleep(BackendHealthCheckInterval)
	}
}

// startService starts the pglink service
func (h *Harness) startService(ctx context.Context) {
	cfg, err := config.ReadConfigFile(h.configPath)
	if err != nil {
		h.fatalf("failed to read config: %v", err)
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

	h.logger.Info("pglink service starting")
}

// waitForService waits for pglink to accept connections
func (h *Harness) waitForService(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, ServiceStartTimeout)
	defer cancel()

	addr := fmt.Sprintf("localhost:%d", DefaultPglinkPort)

	for {
		select {
		case <-ctx.Done():
			h.fatalf("pglink service did not start in time")
		default:
		}

		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			h.logger.Info("pglink service ready", "addr", addr)
			return
		}

		h.logger.Debug("waiting for pglink service", "addr", addr, "error", err)
		time.Sleep(100 * time.Millisecond)
	}
}

// ConnectWithUser creates a connection pool through pglink with the specified user
func (h *Harness) ConnectWithUser(ctx context.Context, database string, user TestUser) (*pgxpool.Pool, error) {
	connStr := fmt.Sprintf(
		"postgres://%s:%s@localhost:%d/%s?sslmode=prefer",
		user.Username,
		user.Password,
		DefaultPglinkPort,
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

// ConnectSingle creates a single connection through pglink (not a pool)
func (h *Harness) ConnectSingle(ctx context.Context, database string, user TestUser) (*pgx.Conn, error) {
	connStr := fmt.Sprintf(
		"postgres://%s:%s@localhost:%d/%s?sslmode=prefer",
		user.Username,
		user.Password,
		DefaultPglinkPort,
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
	for _, db := range PredefinedDatabases {
		if db.Name == name {
			return db
		}
	}
	panic(fmt.Sprintf("database %q not found in PredefinedDatabases", name))
}

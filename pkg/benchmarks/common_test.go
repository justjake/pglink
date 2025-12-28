// Package benchmarks contains standard Go benchmarks for pglink performance testing.
//
// These benchmarks are designed to be run via the orchestrator (cmd/bench) which sets
// up the appropriate environment variables. Configuration is defined in BenchConfig
// (see config.go) using struct tags:
//
//	env:"BENCH_FOO"     - environment variable to read from
//	path:"foo"          - include in benchmark path as foo=<value> (for benchstat filtering)
//	header:"foo"        - name to use in output header (defaults to snake_case of field name)
//	default:"value"     - default value if env var is not set
//
// Concurrency is controlled via the -cpu flag (GOMAXPROCS), not connection pools.
// Each parallel worker creates and holds its own dedicated connection.
package benchmarks

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// opTimeout is the maximum time any single benchmark operation should take.
// If an operation exceeds this, something is wrong (deadlock, network issue, etc.)
const opTimeout = 10 * time.Second

// Op wraps a benchmark operation with timeout and error handling.
type Op struct {
	Ctx    context.Context
	Cancel context.CancelFunc
	name   string
	idx    int
}

// NewOp creates a new benchmark operation with a timeout.
// Call op.Done() when the operation completes, or op.Fatal(b, err) on error.
func NewOp(benchCtx context.Context, name string, idx int) *Op {
	ctx, cancel := context.WithTimeout(benchCtx, opTimeout)
	return &Op{
		Ctx:    ctx,
		Cancel: cancel,
		name:   name,
		idx:    idx,
	}
}

// Done cancels the operation's context. Call this when the operation succeeds.
func (o *Op) Done() {
	o.Cancel()
}

// Failed formats an error message for the operation and cancels the context.
func (o *Op) Failed(err error) string {
	o.Cancel()
	return fmt.Sprintf("%s [iter %d]: %v", o.name, o.idx, err)
}

var benchConfig BenchConfig

func TestMain(m *testing.M) {
	// Load configuration from environment using reflection
	if err := benchConfig.LoadFromEnv(); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if benchConfig.ConnString == "" {
		// Skip benchmarks if not configured (running outside orchestrator)
		log.Println("BENCH_CONN_STRING not set, skipping benchmarks")
		os.Exit(0)
	}

	// Verify we can connect
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	conn, err := pgx.Connect(ctx, benchConfig.ConnString)
	cancel()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	conn.Close(context.Background())

	log.Printf("Connected to database, target=%s, GOMAXPROCS=%d",
		benchConfig.Target, runtime.GOMAXPROCS(0))

	// Print benchstat-compatible configuration header
	benchConfig.PrintHeader()

	// Run benchmarks
	os.Exit(m.Run())
}

// connect creates a new database connection for a benchmark worker.
// Each parallel worker should call this once and reuse the connection.
func connect(ctx context.Context) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(benchConfig.ConnString)
	if err != nil {
		return nil, err
	}

	// When using simple query protocol, disable prepared statements entirely
	if benchConfig.Protocol == "simple" {
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}

	return pgx.ConnectConfig(ctx, cfg)
}

// TestConn wraps a pgx.Conn with a Release method compatible with pgxpool.
type TestConn struct {
	*pgx.Conn
	release func()
}

// Release returns the connection to the pool or closes it, depending on pool mode.
func (c *TestConn) Release() {
	if c.release != nil {
		c.release()
	}
}

// TestPool provides pooled access to database connections.
// The interface is compatible with pgxpool.Pool's Acquire/Release pattern.
type TestPool interface {
	// Acquire gets a connection from the pool.
	// The returned TestConn must be Released when done.
	Acquire(ctx context.Context) (*TestConn, error)

	// Close releases all resources held by the pool.
	// Returns an error if closing fails.
	Close() error
}

// GetTestPool creates a TestPool based on the current benchConfig.ConnectMode.
// For ConnectPerWorker: creates one connection at init, reuses it for all Acquire calls.
// For ConnectPerOp: creates a new connection on each Acquire call.
func GetTestPool(b *testing.B, benchCtx context.Context) (TestPool, error) {
	switch benchConfig.ConnectMode {
	case ConnectPerOp:
		return &loopPool{}, nil
	case ConnectPerWorker:
		fallthrough
	default:
		return newWorkerPool(benchCtx)
	}
}

// workerPool holds a single connection for the worker's lifetime.
// Acquire returns the same connection each time; Release is a no-op.
// This pool is only used by a single goroutine, so no locking is needed.
type workerPool struct {
	conn *pgx.Conn
}

func newWorkerPool(ctx context.Context) (*workerPool, error) {
	op := NewOp(ctx, "connect", 0)
	defer op.Done()

	conn, err := connect(op.Ctx)
	if err != nil {
		return nil, err
	}
	return &workerPool{conn: conn}, nil
}

func (p *workerPool) Acquire(_ context.Context) (*TestConn, error) {
	// Return the same connection; Release is a no-op
	return &TestConn{Conn: p.conn, release: func() {}}, nil
}

func (p *workerPool) Close() error {
	if p.conn != nil {
		err := p.conn.Close(context.Background())
		p.conn = nil
		return err
	}
	return nil
}

// loopPool creates a new connection on each Acquire call.
// Release closes the connection immediately.
type loopPool struct{}

func (p *loopPool) Acquire(ctx context.Context) (*TestConn, error) {
	conn, err := connect(ctx)
	if err != nil {
		return nil, err
	}
	return &TestConn{
		Conn: conn,
		release: func() {
			conn.Close(context.Background())
		},
	}, nil
}

func (p *loopPool) Close() error {
	// Nothing to do - connections are closed on Release
	return nil
}

// getBenchName returns a benchmark sub-name with path-tagged config fields.
// Fields with `path:"name"` tags are included as name=value in the path.
func getBenchName() string {
	return benchConfig.BuildBenchPath()
}

// connectAsAdmin creates a connection using admin credentials for DDL operations.
// This is used for setup/teardown that requires CREATE/DROP permissions.
func connectAsAdmin(ctx context.Context) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(benchConfig.ConnString)
	if err != nil {
		return nil, err
	}

	// Replace credentials with admin user
	cfg.User = "admin"
	cfg.Password = "admin_password"

	return pgx.ConnectConfig(ctx, cfg)
}

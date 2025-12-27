// Package benchmarks contains standard Go benchmarks for pglink performance testing.
//
// These benchmarks are designed to be run via the orchestrator (cmd/bench) which sets
// up the appropriate environment variables. Configuration is defined in BenchConfig
// using struct tags:
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
	"reflect"
	"runtime"
	"strconv"
	"strings"
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

// BenchConfig holds configuration loaded from environment.
// Use struct tags to control behavior:
//   - env:"VAR"      - environment variable name
//   - path:"name"    - include in benchmark path (for benchstat filtering)
//   - header:"name"  - header output name (default: snake_case of field name)
//   - default:"val"  - default value
//
// PoolMode controls how connections are managed in benchmarks.
type PoolMode string

const (
	// PoolModeWorker creates one connection per parallel worker, held for the worker's lifetime.
	// This is efficient for benchmarks that focus on query throughput.
	PoolModeWorker PoolMode = "worker"

	// PoolModeLoop creates a new connection for each loop iteration.
	// This tests connection establishment overhead.
	PoolModeLoop PoolMode = "loop"
)

type BenchConfig struct {
	ConnString  string        `env:"BENCH_CONN_STRING"`
	Target      string        `env:"BENCH_TARGET" path:"target" default:"unknown"`
	Duration    time.Duration `env:"BENCH_DURATION" default:"15s"`
	Warmup      time.Duration `env:"BENCH_WARMUP" default:"5s"`
	Protocol    string        `env:"BENCH_SIMPLE_QUERY" header:"protocol" default:"extended"` // "simple" or "extended"
	PoolMode    PoolMode      `env:"BENCH_POOL_MODE" path:"pool" default:"worker"`            // "worker" or "loop"
	Seed        int64         `env:"BENCH_SEED"`
	RunID       string        `env:"BENCH_RUN_ID" header:"run_id"`
	Round       int           `env:"BENCH_ROUND"`
	TotalRounds int           `env:"BENCH_TOTAL_ROUNDS" header:"total_rounds"`
}

var benchConfig BenchConfig

func TestMain(m *testing.M) {
	// Load configuration from environment using reflection
	if err := loadBenchConfig(&benchConfig); err != nil {
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
	printBenchConfig(&benchConfig)

	// Run benchmarks
	os.Exit(m.Run())
}

// connect creates a new database connection for a benchmark worker.
// Each parallel worker should call this once and reuse the connection.
func connect(ctx context.Context) (*pgx.Conn, error) {
	return pgx.Connect(ctx, benchConfig.ConnString)
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

// GetTestPool creates a TestPool based on the current benchConfig.PoolMode.
// For PoolModeWorker: creates one connection at init, reuses it for all Acquire calls.
// For PoolModeLoop: creates a new connection on each Acquire call.
func GetTestPool(b *testing.B, benchCtx context.Context) (TestPool, error) {
	switch benchConfig.PoolMode {
	case PoolModeLoop:
		return &loopPool{connString: benchConfig.ConnString}, nil
	case PoolModeWorker:
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
type loopPool struct {
	connString string
}

func (p *loopPool) Acquire(ctx context.Context) (*TestConn, error) {
	conn, err := pgx.Connect(ctx, p.connString)
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
	return buildBenchPath(&benchConfig)
}

// loadBenchConfig loads configuration from environment variables using struct tags.
func loadBenchConfig(cfg *BenchConfig) error {
	v := reflect.ValueOf(cfg).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldVal := v.Field(i)

		envVar := field.Tag.Get("env")
		if envVar == "" {
			continue
		}

		envVal := os.Getenv(envVar)
		defaultVal := field.Tag.Get("default")

		// Use default if env var is empty
		if envVal == "" {
			envVal = defaultVal
		}

		if envVal == "" {
			continue
		}

		// Parse based on field type
		// Check time.Duration first (it has Kind int64 but needs special parsing)
		if field.Type == reflect.TypeOf(time.Duration(0)) {
			if d, err := time.ParseDuration(envVal); err == nil {
				fieldVal.Set(reflect.ValueOf(d))
			}
			continue
		}

		switch fieldVal.Kind() {
		case reflect.String:
			// Special handling for Protocol field (BENCH_SIMPLE_QUERY is a bool env var)
			if field.Name == "Protocol" {
				if envVal == "true" {
					fieldVal.SetString("simple")
				} else if envVal != "simple" && envVal != "extended" {
					fieldVal.SetString(defaultVal)
				} else {
					fieldVal.SetString(envVal)
				}
			} else {
				fieldVal.SetString(envVal)
			}
		case reflect.Int, reflect.Int64:
			if n, err := strconv.ParseInt(envVal, 10, 64); err == nil {
				fieldVal.SetInt(n)
			}
		case reflect.Bool:
			fieldVal.SetBool(envVal == "true")
		}
	}

	return nil
}

// buildBenchPath builds the benchmark sub-test path from path-tagged fields.
func buildBenchPath(cfg *BenchConfig) string {
	var parts []string

	v := reflect.ValueOf(cfg).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		pathName := field.Tag.Get("path")
		if pathName == "" {
			continue
		}

		fieldVal := v.Field(i)
		var valStr string

		switch fieldVal.Kind() {
		case reflect.String:
			valStr = fieldVal.String()
		case reflect.Int, reflect.Int64:
			valStr = strconv.FormatInt(fieldVal.Int(), 10)
		case reflect.Bool:
			valStr = strconv.FormatBool(fieldVal.Bool())
		default:
			if field.Type == reflect.TypeOf(time.Duration(0)) {
				valStr = fieldVal.Interface().(time.Duration).String()
			} else {
				valStr = fmt.Sprintf("%v", fieldVal.Interface())
			}
		}

		if valStr != "" && valStr != "0" && valStr != "0s" {
			parts = append(parts, fmt.Sprintf("%s=%s", pathName, valStr))
		}
	}

	return strings.Join(parts, "/")
}

// printBenchConfig prints benchmark configuration in benchstat-compatible format.
func printBenchConfig(cfg *BenchConfig) {
	v := reflect.ValueOf(cfg).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldVal := v.Field(i)

		// Skip ConnString (contains credentials)
		if field.Name == "ConnString" {
			continue
		}

		// Get header name (use header tag, or convert field name to snake_case)
		headerName := field.Tag.Get("header")
		if headerName == "" {
			headerName = toSnakeCase(field.Name)
		}

		// Get value as string
		var valStr string

		// Check time.Duration first (it has Kind int64 but needs special formatting)
		if field.Type == reflect.TypeOf(time.Duration(0)) {
			d := time.Duration(fieldVal.Int())
			if d == 0 {
				continue
			}
			valStr = d.String()
		} else {
			switch fieldVal.Kind() {
			case reflect.String:
				valStr = fieldVal.String()
			case reflect.Int, reflect.Int64:
				n := fieldVal.Int()
				if n == 0 {
					continue // Skip zero values
				}
				valStr = strconv.FormatInt(n, 10)
			case reflect.Bool:
				if !fieldVal.Bool() {
					continue // Skip false values
				}
				valStr = "true"
			default:
				valStr = fmt.Sprintf("%v", fieldVal.Interface())
			}
		}

		if valStr != "" {
			fmt.Printf("%s: %s\n", headerName, valStr)
		}
	}
}

// toSnakeCase converts CamelCase to snake_case.
func toSnakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteByte('_')
		}
		result.WriteRune(r)
	}
	return strings.ToLower(result.String())
}

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
package benchmarks

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// BenchConfig holds configuration loaded from environment.
// Use struct tags to control behavior:
//   - env:"VAR"      - environment variable name
//   - path:"name"    - include in benchmark path (for benchstat filtering)
//   - header:"name"  - header output name (default: snake_case of field name)
//   - default:"val"  - default value
type BenchConfig struct {
	ConnString  string        `env:"BENCH_CONN_STRING"`
	Target      string        `env:"BENCH_TARGET" path:"target" default:"unknown"`
	MaxConns    int           `env:"BENCH_MAX_CONNS" path:"conns" default:"100"`
	Concurrency int           `env:"BENCH_CONCURRENCY" path:"workers" default:"100"`
	Duration    time.Duration `env:"BENCH_DURATION" default:"15s"`
	Warmup      time.Duration `env:"BENCH_WARMUP" default:"5s"`
	Protocol    string        `env:"BENCH_SIMPLE_QUERY" header:"protocol" default:"extended"` // "simple" or "extended"
	Seed        int64         `env:"BENCH_SEED"`
	RunID       string        `env:"BENCH_RUN_ID" header:"run_id"`
	Round       int           `env:"BENCH_ROUND"`
	TotalRounds int           `env:"BENCH_TOTAL_ROUNDS" header:"total_rounds"`
}

var (
	// pool is the shared connection pool for all benchmarks.
	pool *pgxpool.Pool

	// benchConfig holds the loaded configuration.
	benchConfig BenchConfig
)

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

	// Create connection pool
	ctx := context.Background()
	poolConfig, err := pgxpool.ParseConfig(benchConfig.ConnString)
	if err != nil {
		log.Fatalf("Failed to parse connection string: %v", err)
	}

	poolConfig.MaxConns = int32(benchConfig.MaxConns)

	pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("Failed to create connection pool: %v", err)
	}
	defer pool.Close()

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	log.Printf("Connected to database, target=%s, max_conns=%d, concurrency=%d",
		benchConfig.Target, benchConfig.MaxConns, benchConfig.Concurrency)

	// Print benchstat-compatible configuration header
	printBenchConfig(&benchConfig)

	// Run benchmarks
	os.Exit(m.Run())
}

// getPool returns the shared connection pool.
func getPool() *pgxpool.Pool {
	return pool
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

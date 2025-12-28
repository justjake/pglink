// Package benchmarks provides benchmark configuration and utilities.
//
// The BenchConfig type is shared between the benchmark orchestrator (pkg/e2e)
// and benchmark tests (pkg/benchmarks/*_test.go). Configuration is passed via
// environment variables, using struct tags to define the mapping.
package benchmarks

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ConnectMode controls how connections are managed in benchmarks.
type ConnectMode string

const (
	// ConnectPerWorker creates one connection per parallel worker, held for the worker's lifetime.
	// This is efficient for benchmarks that focus on query throughput.
	ConnectPerWorker ConnectMode = "per-worker"

	// ConnectPerOp creates a new connection for each loop iteration.
	// This tests connection establishment overhead.
	ConnectPerOp ConnectMode = "per-op"
)

// BenchConfig holds configuration for benchmark runs.
//
// Configuration is passed via environment variables. Use struct tags to control behavior:
//   - env:"VAR"      - environment variable name (required for serialization)
//   - path:"name"    - include in benchmark path (for benchstat filtering)
//   - header:"name"  - header output name (default: snake_case of field name)
//   - default:"val"  - default value when loading from env
type BenchConfig struct {
	// ConnString is the database connection string.
	ConnString string `env:"BENCH_CONN_STRING"`

	// Target is the target name (e.g., "pglink", "direct", "pgbouncer").
	Target string `env:"BENCH_TARGET" path:"target" default:"unknown"`

	// Duration is the benchmark duration.
	Duration time.Duration `env:"BENCH_DURATION" default:"15s"`

	// Warmup is the warmup duration before measuring.
	Warmup time.Duration `env:"BENCH_WARMUP" default:"5s"`

	// Protocol is "simple" or "extended" query protocol.
	// When loading from env, "true" is converted to "simple".
	Protocol string `env:"BENCH_SIMPLE_QUERY" header:"protocol" default:"extended"`

	// ConnectMode controls connection lifecycle: "per-worker" or "per-op".
	ConnectMode ConnectMode `env:"BENCH_CONNECT_MODE" path:"connect" default:"per-worker"`

	// Seed is the random seed for workload generation (0 = time-based).
	Seed int64 `env:"BENCH_SEED"`

	// RunID is a unique identifier for this benchmark run.
	RunID string `env:"BENCH_RUN_ID" header:"run_id"`

	// Round is the current round number (1-indexed).
	Round int `env:"BENCH_ROUND"`

	// TotalRounds is the total number of rounds.
	TotalRounds int `env:"BENCH_TOTAL_ROUNDS" header:"total_rounds"`
}

// ToEnv serializes the config to environment variable format.
// Returns a slice of "KEY=VALUE" strings suitable for exec.Cmd.Env.
func (c *BenchConfig) ToEnv() []string {
	var env []string

	v := reflect.ValueOf(c).Elem()
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldVal := v.Field(i)

		envVar := field.Tag.Get("env")
		if envVar == "" {
			continue
		}

		var valStr string

		// Handle special case for Protocol field
		if field.Name == "Protocol" {
			val := fieldVal.String()
			if val == "simple" {
				valStr = "true"
			} else {
				continue // Don't set BENCH_SIMPLE_QUERY for extended mode
			}
		} else {
			// Check time.Duration first (it has Kind int64 but needs special formatting)
			if field.Type == reflect.TypeOf(time.Duration(0)) {
				d := time.Duration(fieldVal.Int())
				if d == 0 {
					continue // Skip zero durations
				}
				valStr = d.String()
			} else {
				switch fieldVal.Kind() {
				case reflect.String:
					valStr = fieldVal.String()
					if valStr == "" {
						continue // Skip empty strings
					}
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
					if valStr == "" || valStr == "0" {
						continue
					}
				}
			}
		}

		env = append(env, fmt.Sprintf("%s=%s", envVar, valStr))
	}

	return env
}

// LoadFromEnv loads configuration from environment variables.
// Returns an error if required fields are missing or invalid.
func (c *BenchConfig) LoadFromEnv() error {
	v := reflect.ValueOf(c).Elem()
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

// BuildBenchPath builds the benchmark sub-test path from path-tagged fields.
// Fields with `path:"name"` tags are included as name=value in the path.
func (c *BenchConfig) BuildBenchPath() string {
	var parts []string

	v := reflect.ValueOf(c).Elem()
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

// PrintHeader prints benchmark configuration in benchstat-compatible format.
// Outputs to stdout as "header_name: value" lines.
func (c *BenchConfig) PrintHeader() {
	v := reflect.ValueOf(c).Elem()
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

// ReportThroughput reports QPS and Ops/s custom metrics for benchmarks.
// - b: the benchmark (must be called after benchmark loop completes)
// - queriesPerOp: number of PostgreSQL queries executed per benchmark iteration
//
// Call this after b.RunParallel() returns or at the end of the benchmark loop.
func ReportThroughput(b *testing.B, queriesPerOp int) {
	elapsed := b.Elapsed()
	if elapsed <= 0 {
		return
	}
	elapsedSec := elapsed.Seconds()
	opsPerSec := float64(b.N) / elapsedSec
	qps := float64(b.N*queriesPerOp) / elapsedSec
	b.ReportMetric(opsPerSec, "ops/s")
	b.ReportMetric(qps, "qps")
}

// ReportThroughputWithQueries reports QPS and Ops/s using an exact query count.
// Use this when the number of queries per operation varies.
// - b: the benchmark (must be called after benchmark loop completes)
// - totalQueries: total number of PostgreSQL queries executed across all iterations
//
// Call this after b.RunParallel() returns or at the end of the benchmark loop.
func ReportThroughputWithQueries(b *testing.B, totalQueries int64) {
	elapsed := b.Elapsed()
	if elapsed <= 0 {
		return
	}
	elapsedSec := elapsed.Seconds()
	opsPerSec := float64(b.N) / elapsedSec
	qps := float64(totalQueries) / elapsedSec
	b.ReportMetric(opsPerSec, "ops/s")
	b.ReportMetric(qps, "qps")
}

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// PgbenchRunner runs benchmarks using the pgbench tool.
type PgbenchRunner struct {
	// BinaryPath is the path to the pgbench binary. If empty, uses "pgbench" from PATH.
	BinaryPath string

	// Clients is the number of concurrent clients (-c flag). Default: 100.
	Clients int

	// Threads is the number of threads (-j flag). Default: min(Clients, NumCPU).
	Threads int

	// ScaleFactor is the scale factor for pgbench init (-s flag). Default: 10.
	ScaleFactor int

	// Protocol is the query protocol to use (-M flag): simple, extended, or prepared.
	// Default: extended.
	Protocol string

	// BackendConnString is the connection string for direct backend access.
	// Used for pgbench initialization (must connect directly, not through proxy).
	BackendConnString string

	// initialized tracks whether pgbench tables have been set up.
	initialized bool
}

// NewPgbenchRunner creates a new PgbenchRunner with default settings.
func NewPgbenchRunner() *PgbenchRunner {
	return &PgbenchRunner{
		Clients:     100,
		Threads:     0, // Will be set to min(Clients, NumCPU) at runtime
		ScaleFactor: 10,
		Protocol:    "extended",
	}
}

// Name returns the runner name.
func (r *PgbenchRunner) Name() string {
	return "pgbench"
}

// SupportedCases returns the benchmark cases supported by pgbench.
func (r *PgbenchRunner) SupportedCases() []string {
	return []string{"pgbench_select", "pgbench_tpcb", "pgbench_simple_update"}
}

// pgbenchCaseInfo maps case names to pgbench flags and descriptions.
var pgbenchCaseInfo = map[string]struct {
	flag string
	desc string
}{
	"pgbench_select":        {"-b select-only", "SELECT-only workload"},
	"pgbench_tpcb":          {"-b tpcb-like", "TPC-B-like workload (default)"},
	"pgbench_simple_update": {"-b simple-update", "Simple UPDATE workload"},
}

// Run executes pgbench benchmarks for the given configuration.
func (r *PgbenchRunner) Run(ctx context.Context, cfg BenchRunConfig) (*BenchRunResult, error) {
	pgbenchPath := r.BinaryPath
	if pgbenchPath == "" {
		pgbenchPath = "pgbench"
	}

	// Verify pgbench is available
	if _, err := exec.LookPath(pgbenchPath); err != nil {
		return nil, fmt.Errorf("pgbench binary not found: %w", err)
	}

	// Ensure pgbench tables are initialized
	if !r.initialized {
		backendConn := r.BackendConnString
		if backendConn == "" {
			// Fall back to trying to initialize through the target connection
			// This works for direct targets but may fail for proxies
			backendConn = cfg.ConnString
		}
		if err := r.ensurePgbenchTables(ctx, pgbenchPath, backendConn); err != nil {
			return nil, fmt.Errorf("failed to initialize pgbench tables: %w", err)
		}
		r.initialized = true
	}

	// Run each case and collect results
	var allMetrics []BenchMetric
	var allOutput bytes.Buffer
	var totalDuration time.Duration

	for _, caseName := range cfg.Cases {
		caseInfo, ok := pgbenchCaseInfo[strings.ToLower(caseName)]
		if !ok {
			continue // Skip unknown cases
		}

		startTime := time.Now()

		// Build pgbench command
		args := r.buildArgs(cfg, caseInfo.flag)
		cmd := exec.CommandContext(ctx, pgbenchPath, args...)

		// Set up pipes for streaming output
		stdoutPipe, err := cmd.StdoutPipe()
		if err != nil {
			return nil, fmt.Errorf("failed to create stdout pipe: %w", err)
		}
		stderrPipe, err := cmd.StderrPipe()
		if err != nil {
			return nil, fmt.Errorf("failed to create stderr pipe: %w", err)
		}

		// Start the command
		if err := cmd.Start(); err != nil {
			return nil, fmt.Errorf("failed to start pgbench: %w", err)
		}

		// Stream output in real-time while capturing it
		var stdout, stderr bytes.Buffer
		var wg sync.WaitGroup
		wg.Add(2)

		// Stream stdout to stderr for visibility and capture
		go func() {
			defer wg.Done()
			buf := make([]byte, 1024)
			for {
				n, err := stdoutPipe.Read(buf)
				if n > 0 {
					_, _ = stdout.Write(buf[:n])
					_, _ = os.Stderr.Write(buf[:n]) // Stream to stderr for real-time visibility
				}
				if err != nil {
					break
				}
			}
		}()

		// Capture stderr
		go func() {
			defer wg.Done()
			_, _ = io.Copy(&stderr, stderrPipe)
		}()

		// Wait for output streaming to complete
		wg.Wait()

		// Wait for command to finish
		err = cmd.Wait()
		duration := time.Since(startTime)
		totalDuration += duration

		if err != nil {
			// Log the error but continue with other cases
			fmt.Fprintf(&allOutput, "# pgbench %s failed: %v\n%s\n", caseName, err, stderr.String())
			continue
		}

		// Parse metrics from output
		metric := parsePgbenchOutput(stdout.Bytes(), caseName)
		allMetrics = append(allMetrics, metric)

		// Add output to combined buffer (in go benchmark format for consistency)
		fmt.Fprintf(&allOutput, "# pgbench: %s\n%s\n", caseName, stdout.String())
	}

	return &BenchRunResult{
		Output:   allOutput.Bytes(),
		Metrics:  allMetrics,
		Duration: totalDuration,
	}, nil
}

// buildArgs constructs the pgbench command-line arguments.
func (r *PgbenchRunner) buildArgs(cfg BenchRunConfig, caseFlag string) []string {
	args := []string{}

	// Number of clients
	clients := r.Clients
	if clients == 0 {
		clients = cfg.CPU
	}
	if clients > 0 {
		args = append(args, "-c", strconv.Itoa(clients))
	}

	// Number of threads
	threads := r.Threads
	if threads == 0 && clients > 0 {
		// Default to clients or runtime.NumCPU(), whichever is smaller
		threads = clients
	}
	if threads > 0 {
		args = append(args, "-j", strconv.Itoa(threads))
	}

	// Duration (in seconds)
	duration := int(cfg.Duration.Seconds())
	if duration > 0 {
		args = append(args, "-T", strconv.Itoa(duration))
	}

	// Protocol mode
	protocol := r.Protocol
	if cfg.SimpleQueryMode {
		protocol = "simple"
	}
	if protocol != "" {
		args = append(args, "-M", protocol)
	}

	// Built-in script flag (e.g., "-b select-only")
	if caseFlag != "" {
		// Split the flag since it might be "-b select-only"
		parts := strings.Fields(caseFlag)
		args = append(args, parts...)
	}

	// Progress reporting every 5 seconds
	args = append(args, "-P", "5")

	// Report latency statistics
	args = append(args, "-r")

	// Connection string (pgbench uses libpq-style connection)
	args = append(args, cfg.ConnString)

	return args
}

// ensurePgbenchTables initializes pgbench tables if they don't exist.
func (r *PgbenchRunner) ensurePgbenchTables(ctx context.Context, pgbenchPath, connString string) error {
	// Check if tables already exist by trying to connect and query
	conn, err := pgconn.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer func() {
		_ = conn.Close(ctx)
	}()

	// Check for pgbench_accounts table
	results, err := conn.Exec(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_name = 'pgbench_accounts'
		)
	`).ReadAll()
	if err != nil {
		return fmt.Errorf("failed to check for pgbench tables: %w", err)
	}

	var exists bool
	for _, result := range results {
		if result.Err != nil {
			return fmt.Errorf("failed to check for pgbench tables: %w", result.Err)
		}
		for _, row := range result.Rows {
			if len(row) > 0 && len(row[0]) > 0 {
				exists = row[0][0] == 't'
			}
		}
	}

	if exists {
		// Tables already exist
		return nil
	}

	// Initialize pgbench tables
	fmt.Fprintf(os.Stderr, "Initializing pgbench tables (scale factor: %d)...\n", r.ScaleFactor)

	args := []string{
		"-i",                              // Initialize
		"-s", strconv.Itoa(r.ScaleFactor), // Scale factor
		"-q", // Quiet mode
		connString,
	}

	cmd := exec.CommandContext(ctx, pgbenchPath, args...)
	cmd.Stdout = os.Stderr // Show progress on stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("pgbench init failed: %w", err)
	}

	fmt.Fprintf(os.Stderr, "pgbench tables initialized successfully\n")
	return nil
}

// Regex patterns for parsing pgbench output
var (
	// tps = 896.967014 (without initial connection time)
	pgbenchTpsRegex = regexp.MustCompile(`tps = ([\d.]+)`)
	// latency average = 11.013 ms
	pgbenchLatAvgRegex = regexp.MustCompile(`latency average = ([\d.]+) ms`)
	// number of transactions actually processed: 10000/10000
	pgbenchTxnRegex = regexp.MustCompile(`number of transactions actually processed: (\d+)/(\d+)`)
)

// parsePgbenchOutput extracts metrics from pgbench output.
func parsePgbenchOutput(output []byte, caseName string) BenchMetric {
	metric := BenchMetric{
		Name: fmt.Sprintf("Pgbench%s", toCamelCase(caseName)),
	}

	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()

		// Parse TPS
		if m := pgbenchTpsRegex.FindStringSubmatch(line); m != nil {
			if tps, err := strconv.ParseFloat(m[1], 64); err == nil {
				metric.OpsPerSec = tps
				metric.QPS = tps // TPS and QPS are equivalent for pgbench
			}
		}

		// Parse latency average (convert ms to ns/op)
		if m := pgbenchLatAvgRegex.FindStringSubmatch(line); m != nil {
			if latMs, err := strconv.ParseFloat(m[1], 64); err == nil {
				metric.NsPerOp = latMs * 1_000_000 // ms to ns
			}
		}

		// Parse transactions processed as iterations
		if m := pgbenchTxnRegex.FindStringSubmatch(line); m != nil {
			if txn, err := strconv.ParseInt(m[1], 10, 64); err == nil {
				metric.Iterations = txn
			}
		}
	}

	return metric
}

// toCamelCase converts a snake_case string to CamelCase.
func toCamelCase(s string) string {
	s = strings.TrimPrefix(s, "pgbench_")
	parts := strings.Split(s, "_")
	for i, p := range parts {
		if len(p) > 0 {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, "")
}

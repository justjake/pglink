package e2e

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// psqlResult holds the output and error from a psql command
type psqlResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// runPsql executes a psql command through pglink and returns the result.
// The command is executed with the specified database and user.
func runPsql(ctx context.Context, database string, user TestUser, command string) (*psqlResult, error) {
	connStr := fmt.Sprintf(
		"postgres://%s:%s@localhost:%d/%s?sslmode=prefer",
		user.Username,
		user.Password,
		testHarness.Port(),
		database,
	)

	cmd := exec.CommandContext(ctx, "psql", connStr, "-c", command)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			return nil, fmt.Errorf("failed to run psql: %w", err)
		}
	}

	return &psqlResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exitCode,
	}, nil
}

// runPsqlFile executes a psql command file through pglink.
func runPsqlFile(ctx context.Context, database string, user TestUser, filePath string) (*psqlResult, error) {
	connStr := fmt.Sprintf(
		"postgres://%s:%s@localhost:%d/%s?sslmode=prefer",
		user.Username,
		user.Password,
		testHarness.Port(),
		database,
	)

	cmd := exec.CommandContext(ctx, "psql", connStr, "-f", filePath)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			return nil, fmt.Errorf("failed to run psql: %w", err)
		}
	}

	return &psqlResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exitCode,
	}, nil
}

// runPsqlDirect executes a psql command directly to a backend database (bypassing pglink).
// Used for test setup and teardown.
func runPsqlDirect(ctx context.Context, db TestDatabase, command string) (*psqlResult, error) {
	connStr := fmt.Sprintf(
		"postgres://postgres:postgres@%s:%d/%s?sslmode=disable",
		db.BackendHost,
		db.BackendPort,
		db.BackendDB,
	)

	cmd := exec.CommandContext(ctx, "psql", connStr, "-c", command)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			return nil, fmt.Errorf("failed to run psql: %w", err)
		}
	}

	return &psqlResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exitCode,
	}, nil
}

// =============================================================================
// Basic psql Connectivity Tests
// =============================================================================

// TestPsqlBasicConnect verifies that the psql CLI can connect through pglink
func TestPsqlBasicConnect(t *testing.T) {
	_ = getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	result, err := runPsql(ctx, "alpha_uno", PredefinedUsers.App, "SELECT 1 AS test_value")
	require.NoError(t, err)

	t.Logf("stdout: %s", result.Stdout)
	t.Logf("stderr: %s", result.Stderr)

	assert.Equal(t, 0, result.ExitCode, "psql should succeed")
	assert.Contains(t, result.Stdout, "1", "output should contain the result")
}

// TestPsqlAllUsers verifies that psql can connect with all configured users
func TestPsqlAllUsers(t *testing.T) {
	_ = getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	users := []TestUser{
		PredefinedUsers.App,
		PredefinedUsers.Admin,
		PredefinedUsers.Developer,
	}

	for _, user := range users {
		t.Run(user.Username, func(t *testing.T) {
			result, err := runPsql(ctx, "alpha_uno", user, "SELECT current_user")
			require.NoError(t, err)

			assert.Equal(t, 0, result.ExitCode, "psql should succeed")
			assert.Contains(t, result.Stdout, user.Username, "output should show the correct user")
		})
	}
}

// TestPsqlMultipleQueries verifies that psql can execute multiple queries
func TestPsqlMultipleQueries(t *testing.T) {
	_ = getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	result, err := runPsql(ctx, "alpha_uno", PredefinedUsers.App, "SELECT 1; SELECT 2; SELECT 3")
	require.NoError(t, err)

	t.Logf("stdout: %s", result.Stdout)

	assert.Equal(t, 0, result.ExitCode, "psql should succeed")
}

// =============================================================================
// COPY OUT Tests (using \copy pseudo-command)
// =============================================================================

// TestPsqlCopyOutToStdout tests \copy ... TO STDOUT
func TestPsqlCopyOutToStdout(t *testing.T) {
	_ = getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	// Use \copy which is a psql meta-command that handles COPY on the client side
	result, err := runPsql(ctx, "alpha_uno", PredefinedUsers.App,
		`\copy (SELECT id, name, value FROM schema1.example ORDER BY id) TO STDOUT WITH CSV HEADER`)
	require.NoError(t, err)

	t.Logf("stdout: %s", result.Stdout)
	t.Logf("stderr: %s", result.Stderr)

	assert.Equal(t, 0, result.ExitCode, "psql should succeed")
	assert.Contains(t, result.Stdout, "id,name,value", "output should contain CSV header")
	assert.Contains(t, result.Stdout, "alpha-uno-s1", "output should contain data")
	assert.Contains(t, result.Stdout, "beta-uno-s1", "output should contain data")
}

// TestPsqlCopyOutToFile tests \copy ... TO 'filename'
func TestPsqlCopyOutToFile(t *testing.T) {
	_ = getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	// Create a temp file for the output
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "copy_out.csv")

	// Use \copy to export data to a file
	copyCmd := fmt.Sprintf(`\copy (SELECT id, name, value FROM schema1.example ORDER BY id) TO '%s' WITH CSV HEADER`, outputFile)
	result, err := runPsql(ctx, "alpha_uno", PredefinedUsers.App, copyCmd)
	require.NoError(t, err)

	t.Logf("stdout: %s", result.Stdout)
	t.Logf("stderr: %s", result.Stderr)

	assert.Equal(t, 0, result.ExitCode, "psql should succeed")

	// Verify the file was created and contains expected data
	content, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	t.Logf("File content:\n%s", string(content))

	assert.Contains(t, string(content), "id,name,value", "file should contain CSV header")
	assert.Contains(t, string(content), "alpha-uno-s1", "file should contain data")
	assert.Contains(t, string(content), "beta-uno-s1", "file should contain data")
}

// TestPsqlCopyOutLargeDataset tests COPY OUT with a larger dataset
func TestPsqlCopyOutLargeDataset(t *testing.T) {
	_ = getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	// Use generate_series to create a larger dataset
	result, err := runPsql(ctx, "alpha_uno", PredefinedUsers.App,
		`\copy (SELECT x AS id, 'row_' || x AS name, x * 10 AS value FROM generate_series(1, 1000) AS x) TO STDOUT WITH CSV`)
	require.NoError(t, err)

	t.Logf("stderr: %s", result.Stderr)

	assert.Equal(t, 0, result.ExitCode, "psql should succeed")

	// Count the number of lines (should be 1000)
	lines := strings.Split(strings.TrimSpace(result.Stdout), "\n")
	assert.Equal(t, 1000, len(lines), "should have 1000 rows")

	// Verify first and last rows
	assert.Contains(t, lines[0], "1,row_1,10", "first row should match")
	assert.Contains(t, lines[999], "1000,row_1000,10000", "last row should match")
}

// TestPsqlCopyOutBinary tests COPY OUT with binary format
func TestPsqlCopyOutBinary(t *testing.T) {
	_ = getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	// Create a temp file for binary output
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "copy_out.bin")

	// Use \copy to export data in binary format
	copyCmd := fmt.Sprintf(`\copy (SELECT id, name FROM schema1.example ORDER BY id) TO '%s' WITH BINARY`, outputFile)
	result, err := runPsql(ctx, "alpha_uno", PredefinedUsers.App, copyCmd)
	require.NoError(t, err)

	t.Logf("stderr: %s", result.Stderr)

	assert.Equal(t, 0, result.ExitCode, "psql should succeed")

	// Verify the file was created (binary files start with PGCOPY signature)
	content, err := os.ReadFile(outputFile)
	require.NoError(t, err)

	// PostgreSQL binary copy format starts with "PGCOPY\n\xff\r\n\0"
	assert.True(t, len(content) > 11, "binary file should have content")
	assert.Equal(t, "PGCOPY\n", string(content[0:7]), "binary file should have PGCOPY header")
}

// =============================================================================
// COPY IN Tests (using \copy pseudo-command)
// =============================================================================

// TestPsqlCopyInFromFile tests \copy ... FROM 'filename'
func TestPsqlCopyInFromFile(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	// Find the alpha database config for direct connection
	var alphaDB TestDatabase
	for _, db := range PredefinedDatabases {
		if db.Name == "alpha_uno" {
			alphaDB = db
			break
		}
	}

	// Create a test table using direct connection (as postgres superuser)
	_, err := runPsqlDirect(ctx, alphaDB, `
		DROP TABLE IF EXISTS schema1.copy_test;
		CREATE TABLE schema1.copy_test (id INT, name TEXT, value INT);
		GRANT SELECT, INSERT, UPDATE, DELETE ON schema1.copy_test TO app;
	`)
	require.NoError(t, err)

	// Cleanup at end of test
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		runPsqlDirect(cleanupCtx, alphaDB, "DROP TABLE IF EXISTS schema1.copy_test")
	}()

	// Create input CSV file
	tmpDir := t.TempDir()
	inputFile := filepath.Join(tmpDir, "copy_in.csv")
	csvData := "1,alice,100\n2,bob,200\n3,charlie,300\n"
	err = os.WriteFile(inputFile, []byte(csvData), 0644)
	require.NoError(t, err)

	// Use \copy to import data from file through pglink
	copyCmd := fmt.Sprintf(`\copy schema1.copy_test (id, name, value) FROM '%s' WITH CSV`, inputFile)
	result, err := runPsql(ctx, "alpha_uno", PredefinedUsers.App, copyCmd)
	require.NoError(t, err)

	t.Logf("stdout: %s", result.Stdout)
	t.Logf("stderr: %s", result.Stderr)

	assert.Equal(t, 0, result.ExitCode, "psql should succeed")

	// Verify data was inserted by querying through pglink
	pool, err := h.Connect(ctx, "alpha_uno")
	require.NoError(t, err)
	defer pool.Close()

	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM schema1.copy_test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count, "should have 3 rows")

	// Verify specific data
	var name string
	var value int
	err = pool.QueryRow(ctx, "SELECT name, value FROM schema1.copy_test WHERE id = 2").Scan(&name, &value)
	require.NoError(t, err)
	assert.Equal(t, "bob", name)
	assert.Equal(t, 200, value)
}

// TestPsqlCopyInFromStdin tests \copy ... FROM STDIN using a psql script
func TestPsqlCopyInFromStdin(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	// Find the alpha database config for direct connection
	var alphaDB TestDatabase
	for _, db := range PredefinedDatabases {
		if db.Name == "alpha_uno" {
			alphaDB = db
			break
		}
	}

	// Create a test table using direct connection
	_, err := runPsqlDirect(ctx, alphaDB, `
		DROP TABLE IF EXISTS schema1.copy_stdin_test;
		CREATE TABLE schema1.copy_stdin_test (id INT, name TEXT, value INT);
		GRANT SELECT, INSERT, UPDATE, DELETE ON schema1.copy_stdin_test TO app;
	`)
	require.NoError(t, err)

	// Cleanup at end of test
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		runPsqlDirect(cleanupCtx, alphaDB, "DROP TABLE IF EXISTS schema1.copy_stdin_test")
	}()

	// Create a SQL script that uses COPY ... FROM STDIN
	tmpDir := t.TempDir()
	scriptFile := filepath.Join(tmpDir, "copy_stdin.sql")

	// psql supports inline COPY data after FROM stdin
	scriptContent := `COPY schema1.copy_stdin_test (id, name, value) FROM stdin;
10	stdin_alice	1000
20	stdin_bob	2000
30	stdin_charlie	3000
\.
`
	err = os.WriteFile(scriptFile, []byte(scriptContent), 0644)
	require.NoError(t, err)

	// Execute the script through pglink
	result, err := runPsqlFile(ctx, "alpha_uno", PredefinedUsers.App, scriptFile)
	require.NoError(t, err)

	t.Logf("stdout: %s", result.Stdout)
	t.Logf("stderr: %s", result.Stderr)

	assert.Equal(t, 0, result.ExitCode, "psql should succeed")

	// Verify data was inserted
	pool, err := h.Connect(ctx, "alpha_uno")
	require.NoError(t, err)
	defer pool.Close()

	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM schema1.copy_stdin_test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 3, count, "should have 3 rows")

	// Verify specific data
	var name string
	var value int
	err = pool.QueryRow(ctx, "SELECT name, value FROM schema1.copy_stdin_test WHERE id = 20").Scan(&name, &value)
	require.NoError(t, err)
	assert.Equal(t, "stdin_bob", name)
	assert.Equal(t, 2000, value)
}

// TestPsqlCopyInLargeDataset tests COPY IN with a large number of rows
func TestPsqlCopyInLargeDataset(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	// Find the alpha database config for direct connection
	var alphaDB TestDatabase
	for _, db := range PredefinedDatabases {
		if db.Name == "alpha_uno" {
			alphaDB = db
			break
		}
	}

	// Create a test table using direct connection
	_, err := runPsqlDirect(ctx, alphaDB, `
		DROP TABLE IF EXISTS schema1.copy_large_test;
		CREATE TABLE schema1.copy_large_test (id INT, name TEXT, value INT);
		GRANT SELECT, INSERT, UPDATE, DELETE ON schema1.copy_large_test TO app;
	`)
	require.NoError(t, err)

	// Cleanup at end of test
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		runPsqlDirect(cleanupCtx, alphaDB, "DROP TABLE IF EXISTS schema1.copy_large_test")
	}()

	// Create a large CSV file
	tmpDir := t.TempDir()
	inputFile := filepath.Join(tmpDir, "copy_large.csv")

	const numRows = 5000
	var csvBuilder strings.Builder
	for i := 1; i <= numRows; i++ {
		csvBuilder.WriteString(strconv.Itoa(i))
		csvBuilder.WriteString(",row_")
		csvBuilder.WriteString(strconv.Itoa(i))
		csvBuilder.WriteString(",")
		csvBuilder.WriteString(strconv.Itoa(i * 10))
		csvBuilder.WriteString("\n")
	}
	err = os.WriteFile(inputFile, []byte(csvBuilder.String()), 0644)
	require.NoError(t, err)

	// Use \copy to import the large dataset
	copyCmd := fmt.Sprintf(`\copy schema1.copy_large_test (id, name, value) FROM '%s' WITH CSV`, inputFile)
	result, err := runPsql(ctx, "alpha_uno", PredefinedUsers.App, copyCmd)
	require.NoError(t, err)

	t.Logf("stderr: %s", result.Stderr)

	assert.Equal(t, 0, result.ExitCode, "psql should succeed")

	// Verify all data was inserted
	pool, err := h.Connect(ctx, "alpha_uno")
	require.NoError(t, err)
	defer pool.Close()

	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM schema1.copy_large_test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, numRows, count, "should have all rows")

	// Verify sum to ensure data integrity
	var sum int64
	err = pool.QueryRow(ctx, "SELECT SUM(value) FROM schema1.copy_large_test").Scan(&sum)
	require.NoError(t, err)

	// Sum of (10 + 20 + ... + 50000) = 10 * (1 + 2 + ... + 5000) = 10 * (5000 * 5001 / 2)
	expectedSum := int64(10 * numRows * (numRows + 1) / 2)
	assert.Equal(t, expectedSum, sum, "sum should match expected value")
}

// =============================================================================
// COPY Roundtrip Tests
// =============================================================================

// TestPsqlCopyRoundtrip tests exporting data and re-importing it
func TestPsqlCopyRoundtrip(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	// Find the alpha database config for direct connection
	var alphaDB TestDatabase
	for _, db := range PredefinedDatabases {
		if db.Name == "alpha_uno" {
			alphaDB = db
			break
		}
	}

	// Create source and destination tables using direct connection
	_, err := runPsqlDirect(ctx, alphaDB, `
		DROP TABLE IF EXISTS schema1.copy_roundtrip_src;
		DROP TABLE IF EXISTS schema1.copy_roundtrip_dst;
		CREATE TABLE schema1.copy_roundtrip_src (id INT PRIMARY KEY, name TEXT, value INT);
		CREATE TABLE schema1.copy_roundtrip_dst (id INT PRIMARY KEY, name TEXT, value INT);
		INSERT INTO schema1.copy_roundtrip_src VALUES (1, 'one', 100), (2, 'two', 200), (3, 'three', 300);
		GRANT SELECT ON schema1.copy_roundtrip_src TO app;
		GRANT SELECT, INSERT ON schema1.copy_roundtrip_dst TO app;
	`)
	require.NoError(t, err)

	// Cleanup at end of test
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		runPsqlDirect(cleanupCtx, alphaDB, `
			DROP TABLE IF EXISTS schema1.copy_roundtrip_src;
			DROP TABLE IF EXISTS schema1.copy_roundtrip_dst;
		`)
	}()

	tmpDir := t.TempDir()
	dataFile := filepath.Join(tmpDir, "roundtrip.csv")

	// Export data from source table through pglink
	copyOutCmd := fmt.Sprintf(`\copy schema1.copy_roundtrip_src TO '%s' WITH CSV HEADER`, dataFile)
	result, err := runPsql(ctx, "alpha_uno", PredefinedUsers.App, copyOutCmd)
	require.NoError(t, err)
	assert.Equal(t, 0, result.ExitCode, "copy out should succeed")

	// Verify the file was created
	content, err := os.ReadFile(dataFile)
	require.NoError(t, err)
	t.Logf("Exported data:\n%s", string(content))

	// Import data into destination table through pglink
	copyInCmd := fmt.Sprintf(`\copy schema1.copy_roundtrip_dst FROM '%s' WITH CSV HEADER`, dataFile)
	result, err = runPsql(ctx, "alpha_uno", PredefinedUsers.App, copyInCmd)
	require.NoError(t, err)
	assert.Equal(t, 0, result.ExitCode, "copy in should succeed")

	// Verify data matches
	pool, err := h.Connect(ctx, "alpha_uno")
	require.NoError(t, err)
	defer pool.Close()

	// Check row count
	var srcCount, dstCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM schema1.copy_roundtrip_src").Scan(&srcCount)
	require.NoError(t, err)
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM schema1.copy_roundtrip_dst").Scan(&dstCount)
	require.NoError(t, err)
	assert.Equal(t, srcCount, dstCount, "row counts should match")

	// Check data matches exactly (using EXCEPT)
	var diffCount int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM (
			SELECT * FROM schema1.copy_roundtrip_src
			EXCEPT
			SELECT * FROM schema1.copy_roundtrip_dst
		) diff
	`).Scan(&diffCount)
	require.NoError(t, err)
	assert.Equal(t, 0, diffCount, "data should match exactly")
}

// TestPsqlCopyInWithTransaction tests COPY IN within a transaction
func TestPsqlCopyInWithTransaction(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	// Find the alpha database config for direct connection
	var alphaDB TestDatabase
	for _, db := range PredefinedDatabases {
		if db.Name == "alpha_uno" {
			alphaDB = db
			break
		}
	}

	// Create a test table using direct connection
	_, err := runPsqlDirect(ctx, alphaDB, `
		DROP TABLE IF EXISTS schema1.copy_tx_test;
		CREATE TABLE schema1.copy_tx_test (id INT, name TEXT);
		GRANT SELECT, INSERT, UPDATE, DELETE ON schema1.copy_tx_test TO app;
	`)
	require.NoError(t, err)

	// Cleanup at end of test
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		runPsqlDirect(cleanupCtx, alphaDB, "DROP TABLE IF EXISTS schema1.copy_tx_test")
	}()

	// Create a script that does COPY within a transaction and commits
	tmpDir := t.TempDir()
	scriptFile := filepath.Join(tmpDir, "copy_tx.sql")

	scriptContent := `BEGIN;
COPY schema1.copy_tx_test (id, name) FROM stdin;
1	tx_one
2	tx_two
\.
COMMIT;
`
	err = os.WriteFile(scriptFile, []byte(scriptContent), 0644)
	require.NoError(t, err)

	// Execute the script through pglink
	result, err := runPsqlFile(ctx, "alpha_uno", PredefinedUsers.App, scriptFile)
	require.NoError(t, err)

	t.Logf("stdout: %s", result.Stdout)
	t.Logf("stderr: %s", result.Stderr)

	assert.Equal(t, 0, result.ExitCode, "psql should succeed")

	// Verify data was committed
	pool, err := h.Connect(ctx, "alpha_uno")
	require.NoError(t, err)
	defer pool.Close()

	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM schema1.copy_tx_test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count, "should have 2 rows after commit")
}

// TestPsqlCopyInRollback tests that COPY IN is properly rolled back
func TestPsqlCopyInRollback(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	// Find the alpha database config for direct connection
	var alphaDB TestDatabase
	for _, db := range PredefinedDatabases {
		if db.Name == "alpha_uno" {
			alphaDB = db
			break
		}
	}

	// Create a test table using direct connection
	_, err := runPsqlDirect(ctx, alphaDB, `
		DROP TABLE IF EXISTS schema1.copy_rollback_test;
		CREATE TABLE schema1.copy_rollback_test (id INT, name TEXT);
		GRANT SELECT, INSERT, UPDATE, DELETE ON schema1.copy_rollback_test TO app;
	`)
	require.NoError(t, err)

	// Cleanup at end of test
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		runPsqlDirect(cleanupCtx, alphaDB, "DROP TABLE IF EXISTS schema1.copy_rollback_test")
	}()

	// Create a script that does COPY within a transaction and rolls back
	tmpDir := t.TempDir()
	scriptFile := filepath.Join(tmpDir, "copy_rollback.sql")

	scriptContent := `BEGIN;
COPY schema1.copy_rollback_test (id, name) FROM stdin;
1	rb_one
2	rb_two
\.
ROLLBACK;
`
	err = os.WriteFile(scriptFile, []byte(scriptContent), 0644)
	require.NoError(t, err)

	// Execute the script through pglink
	result, err := runPsqlFile(ctx, "alpha_uno", PredefinedUsers.App, scriptFile)
	require.NoError(t, err)

	t.Logf("stdout: %s", result.Stdout)
	t.Logf("stderr: %s", result.Stderr)

	assert.Equal(t, 0, result.ExitCode, "psql should succeed")

	// Verify data was NOT committed
	pool, err := h.Connect(ctx, "alpha_uno")
	require.NoError(t, err)
	defer pool.Close()

	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM schema1.copy_rollback_test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "should have 0 rows after rollback")
}

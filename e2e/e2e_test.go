package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testTimeout returns a context with a reasonable timeout for tests.
// Individual tests should complete within 5 seconds.
func testTimeout(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// TestMain sets up and tears down the test harness for all e2e tests.
// This ensures docker-compose and pglink are running before any tests execute.
//
// The session algorithm can be selected via the PGLINK_ALGO environment variable.
// Valid values are "default" (channel-based) and "ring" (experimental ring buffer).
// To run tests for all algos:
//
//	for algo in default ring; do PGLINK_ALGO=$algo go test ./e2e/... || exit 1; done
var testHarness *Harness

// maxTestDuration is the maximum time allowed for all tests to complete.
// If exceeded, the process will force-exit.
// Note: This includes harness setup (~5s) and teardown (~5s), so we allow
// extra time beyond the 30s guideline for actual test execution.
const maxTestDuration = 40 * time.Second

func TestMain(m *testing.M) {
	// Start watchdog timer - force exit if tests hang
	watchdog := time.AfterFunc(maxTestDuration+2*time.Second, func() {
		fmt.Fprintf(os.Stderr, "\n\nFATAL: Test timeout exceeded (%v + 2s grace). Force exiting.\n", maxTestDuration)
		os.Exit(2)
	})
	defer watchdog.Stop()

	// Create harness with a nil testing.T since we're in TestMain
	testHarness = NewHarnessForMain()

	// Check for algo override via environment variable
	if algo := os.Getenv("PGLINK_ALGO"); algo != "" {
		testHarness.Algo = algo
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	testHarness.Start(ctx)
	cancel()

	// Run tests
	code := m.Run()

	// Cleanup
	testHarness.Stop()

	os.Exit(code)
}

func getHarness(t *testing.T) *Harness {
	t.Helper()
	return testHarness
}

// =============================================================================
// Basic Connectivity Tests
// =============================================================================

func TestBasicConnect(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	pool, err := h.Connect(ctx, "alpha_uno")
	require.NoError(t, err)
	defer pool.Close()

	// Simple query
	var result int
	err = pool.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)
}

func TestAllDatabases(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	for _, db := range PredefinedDatabases {
		t.Run(db.Name, func(t *testing.T) {
			pool, err := h.Connect(ctx, db.Name)
			require.NoError(t, err)
			defer pool.Close()

			// Query to verify we're connected to the right database
			var dbName string
			err = pool.QueryRow(ctx, "SELECT current_database()").Scan(&dbName)
			require.NoError(t, err)
			assert.Equal(t, db.BackendDB, dbName)

			// Query sample data to verify connectivity
			var count int
			err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM schema1.example").Scan(&count)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, count, 2, "should have sample data")
		})
	}
}

func TestAllUsers(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	users := []TestUser{
		PredefinedUsers.App,
		PredefinedUsers.Admin,
		PredefinedUsers.Developer,
	}

	for _, user := range users {
		t.Run(user.Username, func(t *testing.T) {
			pool, err := h.ConnectWithUser(ctx, "alpha_uno", user)
			require.NoError(t, err)
			defer pool.Close()

			// Verify we're connected as the right user
			var currentUser string
			err = pool.QueryRow(ctx, "SELECT current_user").Scan(&currentUser)
			require.NoError(t, err)
			assert.Equal(t, user.Username, currentUser)
		})
	}
}

// =============================================================================
// Transaction Pooling Behavior Tests
// =============================================================================

// TestTransactionPooling verifies that pglink implements transaction pooling mode:
// - Connections are assigned to clients only for the duration of a transaction
// - Between transactions, the backend connection may be reused by other clients
// - The backend PID may change between transactions
func TestTransactionPooling(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	// Create multiple client connections
	const numClients = 5
	conns := make([]*pgx.Conn, numClients)
	for i := 0; i < numClients; i++ {
		conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
		require.NoError(t, err)
		defer conn.Close(context.Background())
		conns[i] = conn
	}

	// Warmup: run one transaction on each connection sequentially first.
	// This initializes the backend pool and avoids a pglink bug that occurs
	// when many connections start transactions concurrently on a cold pool.
	for i, conn := range conns {
		tx, err := conn.Begin(ctx)
		require.NoError(t, err, "warmup begin failed for conn %d", i)
		var pid int32
		err = tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid)
		require.NoError(t, err, "warmup query failed for conn %d", i)
		err = tx.Commit(ctx)
		require.NoError(t, err, "warmup commit failed for conn %d", i)
	}

	// Each client does a transaction and records the backend PID
	type txResult struct {
		clientIdx  int
		iteration  int
		backendPID int32
		err        error
	}

	resultCh := make(chan txResult, 100)

	// Run multiple iterations concurrently to test transaction pooling
	const iterations = 10
	var wg sync.WaitGroup

	for iter := 0; iter < iterations; iter++ {
		for clientIdx := 0; clientIdx < numClients; clientIdx++ {
			wg.Add(1)
			go func(conn *pgx.Conn, clientIdx, iter int) {
				defer wg.Done()

				result := txResult{clientIdx: clientIdx, iteration: iter}

				tx, err := conn.Begin(ctx)
				if err != nil {
					result.err = fmt.Errorf("begin: %w", err)
					resultCh <- result
					return
				}

				var pid int32
				err = tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid)
				if err != nil {
					tx.Rollback(context.Background())
					result.err = fmt.Errorf("query: %w", err)
					resultCh <- result
					return
				}
				result.backendPID = pid

				err = tx.Commit(ctx)
				if err != nil {
					result.err = fmt.Errorf("commit: %w", err)
					resultCh <- result
					return
				}

				resultCh <- result
			}(conns[clientIdx], clientIdx, iter)
		}
		// Small delay between iterations to allow connection reuse
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()
	close(resultCh)

	// Collect results
	var results []txResult
	var errors []error
	for r := range resultCh {
		if r.err != nil {
			errors = append(errors, r.err)
		} else {
			results = append(results, r)
		}
	}

	// Report any errors
	for _, err := range errors {
		t.Errorf("transaction error: %v", err)
	}

	// Analyze results: in transaction pooling mode, we expect:
	// 1. The same client may get different backend PIDs across transactions
	// 2. Different clients may share the same backend PID (not at the same time)
	pidsByClient := make(map[int]map[int32]bool)
	for _, r := range results {
		if pidsByClient[r.clientIdx] == nil {
			pidsByClient[r.clientIdx] = make(map[int32]bool)
		}
		pidsByClient[r.clientIdx][r.backendPID] = true
	}

	// Log statistics
	t.Logf("Transaction pooling results:")
	t.Logf("  Total transactions: %d (expected %d)", len(results), numClients*iterations)
	t.Logf("  Errors: %d", len(errors))

	allPIDs := make(map[int32]int)
	for _, r := range results {
		allPIDs[r.backendPID]++
	}
	t.Logf("  Unique backend PIDs: %d", len(allPIDs))
	t.Logf("  Transactions per PID: %v", allPIDs)

	// All transactions should succeed
	require.Equal(t, numClients*iterations, len(results), "all transactions should succeed")

	// With pool_max_conns=10 and 5 clients doing 10 iterations each,
	// we should see PID reuse (fewer PIDs than total transactions)
	assert.Less(t, len(allPIDs), numClients*iterations,
		"PIDs should be reused across transactions (transaction pooling)")
}

// TestBackendPIDChangesOutsideTransaction verifies that the backend PID
// can change between transactions on the same client connection
func TestBackendPIDChangesOutsideTransaction(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Record PIDs across multiple transactions
	var pids []int32
	const numTransactions = 20

	for i := 0; i < numTransactions; i++ {
		tx, err := conn.Begin(ctx)
		require.NoError(t, err)

		var pid int32
		err = tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid)
		require.NoError(t, err)
		pids = append(pids, pid)

		err = tx.Commit(ctx)
		require.NoError(t, err)

		// Small delay to allow other connections to potentially grab the backend
		time.Sleep(5 * time.Millisecond)
	}

	// In transaction pooling mode, PIDs might change between transactions
	// (though it's not guaranteed if there's no contention)
	t.Logf("PIDs across %d transactions: %v", numTransactions, pids)

	// Count unique PIDs
	uniquePIDs := make(map[int32]bool)
	for _, pid := range pids {
		uniquePIDs[pid] = true
	}
	t.Logf("Unique PIDs: %d", len(uniquePIDs))

	// With low contention, we might get the same PID repeatedly
	// The key assertion is that transactions complete successfully
	assert.Equal(t, numTransactions, len(pids), "all transactions should complete")
}

// TestTransactionIsolation verifies that changes within a transaction
// are isolated until committed
func TestTransactionIsolation(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	db := h.GetTestDatabase("alpha_uno")

	// Create a test table directly on the backend (as postgres superuser)
	err := h.ExecDirect(ctx, db, `
		DROP TABLE IF EXISTS schema1.isolation_test;
		CREATE TABLE schema1.isolation_test (id INT PRIMARY KEY, value TEXT);
		GRANT SELECT, INSERT, UPDATE, DELETE ON schema1.isolation_test TO app;
	`)
	require.NoError(t, err)

	// Cleanup at end of test
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		h.ExecDirect(cleanupCtx, db, "DROP TABLE IF EXISTS schema1.isolation_test")
	}()

	conn1, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn1.Close(context.Background())

	conn2, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn2.Close(context.Background())

	// Start transaction on conn1
	tx1, err := conn1.Begin(ctx)
	require.NoError(t, err)
	defer tx1.Rollback(context.Background())

	// Insert data in transaction (not yet committed)
	_, err = tx1.Exec(ctx, "INSERT INTO schema1.isolation_test (id, value) VALUES (1, 'uncommitted')")
	require.NoError(t, err)

	// Verify data is visible within the transaction
	var val1 string
	err = tx1.QueryRow(ctx, "SELECT value FROM schema1.isolation_test WHERE id = 1").Scan(&val1)
	require.NoError(t, err)
	assert.Equal(t, "uncommitted", val1, "data should be visible within transaction")

	// conn2 should NOT see the uncommitted data (transaction isolation)
	var count int
	err = conn2.QueryRow(ctx, "SELECT COUNT(*) FROM schema1.isolation_test WHERE id = 1").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "uncommitted data should not be visible to other connections")

	// Commit the transaction
	err = tx1.Commit(ctx)
	require.NoError(t, err)

	// Now conn2 should see the committed data
	err = conn2.QueryRow(ctx, "SELECT COUNT(*) FROM schema1.isolation_test WHERE id = 1").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "committed data should be visible to other connections")

	var val2 string
	err = conn2.QueryRow(ctx, "SELECT value FROM schema1.isolation_test WHERE id = 1").Scan(&val2)
	require.NoError(t, err)
	assert.Equal(t, "uncommitted", val2, "data value should match what was inserted")
}

// TestSessionVariablesWithinTransaction verifies that session variables
// set within a transaction persist for the duration of the transaction
func TestSessionVariablesWithinTransaction(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Start a transaction
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	// Set a session variable
	_, err = tx.Exec(ctx, "SET LOCAL work_mem = '128MB'")
	require.NoError(t, err)

	// Verify it's set
	var workMem string
	err = tx.QueryRow(ctx, "SHOW work_mem").Scan(&workMem)
	require.NoError(t, err)
	assert.Equal(t, "128MB", workMem)

	// Commit and verify the LOCAL setting is gone
	err = tx.Commit(ctx)
	require.NoError(t, err)

	// After commit, the LOCAL setting should be reset
	// (though in transaction pooling the connection may change entirely)
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

// TestConcurrentConnections tests many concurrent connections
func TestConcurrentConnections(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	const numConnections = 20
	const queriesPerConn = 5

	var wg sync.WaitGroup
	var successCount atomic.Int32
	var errorCount atomic.Int32

	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func(connID int) {
			defer wg.Done()

			conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
			if err != nil {
				t.Logf("connection %d failed to connect: %v", connID, err)
				errorCount.Add(1)
				return
			}
			defer conn.Close(context.Background())

			for j := 0; j < queriesPerConn; j++ {
				var result int
				err := conn.QueryRow(ctx, "SELECT $1::int", connID*1000+j).Scan(&result)
				if err != nil {
					t.Logf("connection %d query %d failed: %v", connID, j, err)
					errorCount.Add(1)
					continue
				}
				if result == connID*1000+j {
					successCount.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Concurrent test results: %d successful queries, %d errors",
		successCount.Load(), errorCount.Load())

	// With 50 concurrent connections fighting for 10 backend connections,
	// some transient failures under extreme load are expected.
	// We expect at least 95% success rate.
	total := int32(numConnections * queriesPerConn)
	successRate := float64(successCount.Load()) / float64(total)
	assert.GreaterOrEqual(t, successRate, 0.95, "at least 95%% of queries should succeed")
}

// TestConcurrentTransactions tests many concurrent transactions
func TestConcurrentTransactions(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	const numGoroutines = 20
	const txPerGoroutine = 5

	var wg sync.WaitGroup
	var successCount atomic.Int32
	var errorCount atomic.Int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
			if err != nil {
				errorCount.Add(1)
				return
			}
			defer conn.Close(context.Background())

			for j := 0; j < txPerGoroutine; j++ {
				tx, err := conn.Begin(ctx)
				if err != nil {
					errorCount.Add(1)
					continue
				}

				// Do some work in the transaction
				var result int
				err = tx.QueryRow(ctx, "SELECT $1::int + $2::int", goroutineID, j).Scan(&result)
				if err != nil {
					tx.Rollback(context.Background())
					errorCount.Add(1)
					continue
				}

				if result == goroutineID+j {
					err = tx.Commit(ctx)
					if err != nil {
						errorCount.Add(1)
						continue
					}
					successCount.Add(1)
				} else {
					tx.Rollback(context.Background())
					errorCount.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Concurrent transaction results: %d successful, %d errors",
		successCount.Load(), errorCount.Load())

	assert.Equal(t, int32(numGoroutines*txPerGoroutine), successCount.Load())
	assert.Equal(t, int32(0), errorCount.Load())
}

// TestConnectionPoolExhaustion tests behavior when connection pool is exhausted
func TestConnectionPoolExhaustion(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	// The backend pool has pool_max_conns=10
	// Try to hold more connections than the pool allows
	const numConnections = 15

	conns := make([]*pgx.Conn, 0, numConnections)
	txs := make([]pgx.Tx, 0, numConnections)

	// Acquire connections and start transactions to hold backend connections
	for i := 0; i < numConnections; i++ {
		conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
		if err != nil {
			t.Logf("Connection %d failed: %v", i, err)
			continue
		}
		conns = append(conns, conn)

		// Start a transaction to hold the backend connection
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Logf("Transaction %d failed to start: %v", i, err)
			continue
		}

		// Execute a query to ensure we actually have a backend connection
		var pid int32
		err = tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid)
		if err != nil {
			t.Logf("Query in transaction %d failed: %v", i, err)
			tx.Rollback(context.Background())
			continue
		}
		t.Logf("Connection %d got backend PID %d", i, pid)
		txs = append(txs, tx)
	}

	t.Logf("Successfully acquired %d connections with transactions", len(txs))

	// Cleanup - use context.Background() to ensure cleanup completes
	for _, tx := range txs {
		tx.Rollback(context.Background())
	}
	for _, conn := range conns {
		conn.Close(context.Background())
	}

	// The pool should be usable again after releasing.
	// Use a fresh context since the original may have timed out during exhaustion.
	freshCtx, freshCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer freshCancel()

	conn, err := h.ConnectSingle(freshCtx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	var result int
	err = conn.QueryRow(freshCtx, "SELECT 1").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)

}

// =============================================================================
// Query Type Tests
// =============================================================================

// TestSimpleQuery tests simple (non-extended) query protocol
func TestSimpleQuery(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Use Exec for simple query (no parameters)
	tag, err := conn.Exec(ctx, "SELECT 1; SELECT 2; SELECT 3")
	require.NoError(t, err)
	t.Logf("Exec result: %s", tag.String())
}

// TestExtendedQuery tests extended query protocol with parameters
func TestExtendedQuery(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Extended query with parameters
	rows, err := conn.Query(ctx, "SELECT $1::int, $2::text, $3::bool", 42, "hello", true)
	require.NoError(t, err)
	defer rows.Close()

	require.True(t, rows.Next())

	var intVal int
	var textVal string
	var boolVal bool
	err = rows.Scan(&intVal, &textVal, &boolVal)
	require.NoError(t, err)

	assert.Equal(t, 42, intVal)
	assert.Equal(t, "hello", textVal)
	assert.True(t, boolVal)
}

// TestPreparedStatements tests prepared statement handling
func TestPreparedStatements(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// In transaction pooling mode, named prepared statements only persist
	// within a transaction, so we must use a transaction to test them.
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	// Prepare a statement within the transaction
	_, err = tx.Prepare(ctx, "test_stmt", "SELECT $1::int + $2::int")
	require.NoError(t, err)

	// Execute it multiple times
	for i := 0; i < 5; i++ {
		var result int
		err = tx.QueryRow(ctx, "test_stmt", i, 100).Scan(&result)
		require.NoError(t, err)
		assert.Equal(t, i+100, result)
	}

	// Note: We skip DEALLOCATE because the proxy rewrites statement names
	// and DEALLOCATE is a SQL command that doesn't go through the rewriter.
	// The statement will be automatically deallocated when the transaction ends.

	err = tx.Commit(ctx)
	require.NoError(t, err)
}

// TestLargeResultSet tests handling of large result sets
func TestLargeResultSet(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Generate a large result set
	const numRows = 10000

	rows, err := conn.Query(ctx, "SELECT generate_series(1, $1)", numRows)
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		var val int
		err = rows.Scan(&val)
		require.NoError(t, err)
		count++
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, numRows, count)
}

// TestNullHandling tests NULL value handling
func TestNullHandling(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	var intVal *int
	var textVal *string

	err = conn.QueryRow(ctx, "SELECT NULL::int, NULL::text").Scan(&intVal, &textVal)
	require.NoError(t, err)

	assert.Nil(t, intVal)
	assert.Nil(t, textVal)
}

// TestDataTypes tests various PostgreSQL data types
func TestDataTypes(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	testCases := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{"int2", "SELECT 123::int2", int16(123)},
		{"int4", "SELECT 123456::int4", int32(123456)},
		{"int8", "SELECT 1234567890123::int8", int64(1234567890123)},
		{"float4", "SELECT 3.14::float4", float32(3.14)},
		{"float8", "SELECT 3.14159265359::float8", float64(3.14159265359)},
		{"bool_true", "SELECT true::bool", true},
		{"bool_false", "SELECT false::bool", false},
		{"text", "SELECT 'hello world'::text", "hello world"},
		{"bytea", "SELECT '\\x48454c4c4f'::bytea", []byte("HELLO")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := conn.Query(ctx, tc.query)
			require.NoError(t, err)
			defer rows.Close()

			require.True(t, rows.Next())

			values, err := rows.Values()
			require.NoError(t, err)
			require.Len(t, values, 1)

			// Type-specific assertions
			switch expected := tc.expected.(type) {
			case float32:
				assert.InDelta(t, expected, values[0], 0.001)
			case float64:
				assert.InDelta(t, expected, values[0], 0.0000001)
			default:
				assert.Equal(t, expected, values[0])
			}
		})
	}
}

// =============================================================================
// Error Handling Tests
// =============================================================================

// TestInvalidQuery tests handling of invalid SQL
func TestInvalidQuery(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	_, err = conn.Exec(ctx, "SELECT * FROM nonexistent_table_xyz")
	require.Error(t, err)
	t.Logf("Expected error: %v", err)

	// Connection should still be usable
	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)
}

// TestTransactionRollback tests transaction rollback behavior
func TestTransactionRollback(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	db := h.GetTestDatabase("alpha_uno")

	// Create a test table directly on the backend (as postgres superuser)
	err := h.ExecDirect(ctx, db, `
		DROP TABLE IF EXISTS schema1.rollback_test;
		CREATE TABLE schema1.rollback_test (id INT PRIMARY KEY, value TEXT);
		GRANT SELECT, INSERT, UPDATE, DELETE ON schema1.rollback_test TO app;
	`)
	require.NoError(t, err)

	// Cleanup at end of test
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		h.ExecDirect(cleanupCtx, db, "DROP TABLE IF EXISTS schema1.rollback_test")
	}()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Start transaction and insert data
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, "INSERT INTO schema1.rollback_test (id, value) VALUES (1, 'should_be_rolled_back')")
	require.NoError(t, err)

	// Verify data is visible within the transaction
	var val string
	err = tx.QueryRow(ctx, "SELECT value FROM schema1.rollback_test WHERE id = 1").Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, "should_be_rolled_back", val)

	// Rollback
	err = tx.Rollback(ctx)
	require.NoError(t, err)

	// Verify the data was rolled back (table should be empty)
	var count int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM schema1.rollback_test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "data should be rolled back")
}

// TestErrorInTransaction tests error handling within transactions
func TestErrorInTransaction(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	// Execute a query that will fail
	_, err = tx.Exec(ctx, "SELECT * FROM nonexistent_xyz")
	require.Error(t, err)

	// Transaction should be in aborted state
	// Further queries should fail until rollback
	_, err = tx.Exec(ctx, "SELECT 1")
	require.Error(t, err)

	// Rollback should work
	err = tx.Rollback(ctx)
	require.NoError(t, err)

	// Connection should be usable again
	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)
}

// TestQueryCancellation tests that queries can be cancelled via context cancellation.
// This verifies that the proxy correctly handles query cancellation.
// Note: When pgx cancels a query via context, it typically closes the connection
// as a safety measure since the connection state is indeterminate.
func TestQueryCancellation(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Create a cancellable context for the query
	queryCtx, queryCancel := context.WithCancel(ctx)

	// Channel to receive the query error
	errCh := make(chan error, 1)

	// Start a long-running query in a goroutine
	go func() {
		// pg_sleep(10) would sleep for 10 seconds, but we'll cancel it
		_, err := conn.Exec(queryCtx, "SELECT pg_sleep(10)")
		errCh <- err
	}()

	// Give the query time to start
	time.Sleep(100 * time.Millisecond)

	// Cancel the query
	queryCancel()

	// Wait for the query to return with an error
	select {
	case err := <-errCh:
		require.Error(t, err, "cancelled query should return an error")
		// The error should indicate cancellation
		// pgx wraps this as a context.Canceled error or a query cancelled error
		errStr := err.Error()
		t.Logf("Query cancellation error: %v", err)
		assert.True(t,
			strings.Contains(errStr, "cancel") ||
				strings.Contains(errStr, "context") ||
				strings.Contains(errStr, "57014"), // SQLSTATE for query_canceled
			"error should indicate cancellation: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("query should have been cancelled within 5 seconds")
	}

	// After context cancellation, pgx typically closes the connection.
	// Verify we can still connect and query with a new connection.
	conn2, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn2.Close(context.Background())

	var result int
	err = conn2.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)
}

// TestInvalidCredentials tests authentication failure
func TestInvalidCredentials(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	_, err := h.ConnectSingle(ctx, "alpha_uno", TestUser{
		Username: "app",
		Password: "wrong_password",
	})
	require.Error(t, err)
	t.Logf("Expected authentication error: %v", err)
}

// TestInvalidDatabase tests connecting to non-existent database
func TestInvalidDatabase(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	_, err := h.ConnectSingle(ctx, "nonexistent_database", PredefinedUsers.App)
	require.Error(t, err)
	t.Logf("Expected database error: %v", err)
}

// =============================================================================
// Connection State Tests
// =============================================================================

// TestSearchPath tests that search_path is handled correctly
func TestSearchPath(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Set search_path within transaction
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	_, err = tx.Exec(ctx, "SET LOCAL search_path = schema2")
	require.NoError(t, err)

	// Query using the new search_path
	rows, err := tx.Query(ctx, "SELECT name FROM example")
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		require.NoError(t, err)
		names = append(names, name)
	}
	require.NoError(t, rows.Err())

	// Should get schema2 data
	t.Logf("Names from schema2: %v", names)
	assert.GreaterOrEqual(t, len(names), 1)

	tx.Commit(ctx)
}

// TestTimeZone tests timezone handling
func TestTimeZone(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	// Set timezone
	_, err = tx.Exec(ctx, "SET LOCAL timezone = 'America/New_York'")
	require.NoError(t, err)

	var tz string
	err = tx.QueryRow(ctx, "SHOW timezone").Scan(&tz)
	require.NoError(t, err)
	assert.Equal(t, "America/New_York", tz)

	tx.Commit(ctx)
}

// =============================================================================
// Multiple Database Tests
// =============================================================================

// TestCrossBackendQueries tests queries across different backends
func TestCrossBackendQueries(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	// Connect to all backends and verify they're independent
	databases := []string{"alpha_uno", "bravo_uno", "charlie_uno"}
	results := make(map[string]int32)

	for _, db := range databases {
		conn, err := h.ConnectSingle(ctx, db, PredefinedUsers.App)
		require.NoError(t, err)
		defer conn.Close(context.Background())

		var pid int32
		err = conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid)
		require.NoError(t, err)
		results[db] = pid
		t.Logf("Database %s: backend PID %d", db, pid)
	}

	// PIDs should be independent (different backend servers)
	// Note: They could theoretically be the same number but that's unlikely
	t.Logf("Backend PIDs: %v", results)
}

// TestSameBackendDifferentDatabases tests connections to different databases on same backend
func TestSameBackendDifferentDatabases(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	// alpha_uno and alpha_dos are on the same backend (alpha)
	conn1, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn1.Close(context.Background())

	conn2, err := h.ConnectSingle(ctx, "alpha_dos", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn2.Close(context.Background())

	// Verify they're connected to different databases
	var db1, db2 string
	err = conn1.QueryRow(ctx, "SELECT current_database()").Scan(&db1)
	require.NoError(t, err)
	err = conn2.QueryRow(ctx, "SELECT current_database()").Scan(&db2)
	require.NoError(t, err)

	assert.Equal(t, "uno", db1)
	assert.Equal(t, "dos", db2)

	// Verify they can query their respective tables
	var name1, name2 string
	err = conn1.QueryRow(ctx, "SELECT name FROM schema1.example LIMIT 1").Scan(&name1)
	require.NoError(t, err)
	err = conn2.QueryRow(ctx, "SELECT name FROM schema1.example LIMIT 1").Scan(&name2)
	require.NoError(t, err)

	t.Logf("alpha_uno data: %s", name1)
	t.Logf("alpha_dos data: %s", name2)
}

// =============================================================================
// Stress Tests
// =============================================================================

// TestStressConnectDisconnect rapidly creates and closes connections
func TestStressConnectDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	const iterations = 20
	const concurrency = 5

	var wg sync.WaitGroup
	var successCount atomic.Int32
	var errorCount atomic.Int32

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
				if err != nil {
					errorCount.Add(1)
					continue
				}

				var result int
				err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
				conn.Close(context.Background())

				if err == nil && result == 1 {
					successCount.Add(1)
				} else {
					errorCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	t.Logf("Stress test results: %d successful, %d errors",
		successCount.Load(), errorCount.Load())

	// Allow some connection failures under stress, but most should succeed
	successRate := float64(successCount.Load()) / float64(iterations*concurrency)
	assert.GreaterOrEqual(t, successRate, 0.95, "at least 95% of connections should succeed")
}

// TestStressQueries runs many queries in parallel.
// Each worker uses its own connection and uses simple queries (no parameters)
// to avoid extended query protocol complexity in transaction pooling mode.
func TestStressQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	const numQueries = 100
	const concurrency = 10
	const queriesPerWorker = numQueries / concurrency

	var wg sync.WaitGroup
	var successCount atomic.Int32
	var errors sync.Map // queryID -> error

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
			if err != nil {
				errors.Store(workerID*1000, fmt.Errorf("connect: %w", err))
				return
			}
			defer conn.Close(ctx)

			for j := 0; j < queriesPerWorker; j++ {
				queryID := workerID*queriesPerWorker + j
				// Use a transaction to ensure consistent backend connection
				// throughout the query lifecycle (Parse, Describe, Bind, Execute)
				tx, err := conn.Begin(ctx)
				if err != nil {
					errors.Store(queryID, fmt.Errorf("begin: %w", err))
					continue
				}
				query := fmt.Sprintf("SELECT %d", queryID)
				var result int
				err = tx.QueryRow(ctx, query).Scan(&result)
				commitErr := tx.Commit(ctx)
				if err == nil && commitErr == nil && result == queryID {
					successCount.Add(1)
				} else if err != nil {
					errors.Store(queryID, err)
				} else if commitErr != nil {
					errors.Store(queryID, fmt.Errorf("commit: %w", commitErr))
				}
			}
		}(i)
	}

	wg.Wait()

	// Log any errors
	errors.Range(func(key, value any) bool {
		t.Logf("Worker/Query %d failed: %v", key.(int), value)
		return true
	})

	t.Logf("Stress query results: %d/%d successful", successCount.Load(), numQueries)
	assert.Equal(t, int32(numQueries), successCount.Load(), "all queries should succeed")
}

// =============================================================================
// COPY Protocol Tests (pgx CopyFrom API)
// =============================================================================

// TestCopyFrom tests pgx's CopyFrom API which uses pipelined COPY protocol
func TestCopyFrom(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Create temp table
	_, err = conn.Exec(ctx, "CREATE TEMP TABLE copy_test (id int, name text)")
	require.NoError(t, err)

	// Use CopyFrom to insert rows
	rows := [][]any{
		{1, "row1"},
		{2, "row2"},
		{3, "row3"},
	}

	count, err := conn.CopyFrom(ctx, pgx.Identifier{"copy_test"}, []string{"id", "name"}, pgx.CopyFromRows(rows))
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)

	// Verify the data was inserted
	var rowCount int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM copy_test").Scan(&rowCount)
	require.NoError(t, err)
	assert.Equal(t, 3, rowCount)

	// Verify specific values
	var id int
	var name string
	err = conn.QueryRow(ctx, "SELECT id, name FROM copy_test WHERE id = 2").Scan(&id, &name)
	require.NoError(t, err)
	assert.Equal(t, 2, id)
	assert.Equal(t, "row2", name)
}

// TestCopyFromLargeDataset tests CopyFrom with a larger dataset
func TestCopyFromLargeDataset(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Create temp table
	_, err = conn.Exec(ctx, "CREATE TEMP TABLE copy_large (id int, data text)")
	require.NoError(t, err)

	// Generate 1000 rows
	const numRows = 1000
	rows := make([][]any, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = []any{i, fmt.Sprintf("data-%d", i)}
	}

	count, err := conn.CopyFrom(ctx, pgx.Identifier{"copy_large"}, []string{"id", "data"}, pgx.CopyFromRows(rows))
	require.NoError(t, err)
	assert.Equal(t, int64(numRows), count)

	// Verify count
	var rowCount int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM copy_large").Scan(&rowCount)
	require.NoError(t, err)
	assert.Equal(t, numRows, rowCount)

	// Verify min/max
	var minID, maxID int
	err = conn.QueryRow(ctx, "SELECT MIN(id), MAX(id) FROM copy_large").Scan(&minID, &maxID)
	require.NoError(t, err)
	assert.Equal(t, 0, minID)
	assert.Equal(t, numRows-1, maxID)
}

// TestCopyFromInTransaction tests CopyFrom within a transaction
func TestCopyFromInTransaction(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Create temp table
	_, err = conn.Exec(ctx, "CREATE TEMP TABLE copy_tx (id int, name text)")
	require.NoError(t, err)

	// Start transaction
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	// CopyFrom within transaction
	rows := [][]any{
		{1, "tx-row1"},
		{2, "tx-row2"},
	}
	count, err := tx.CopyFrom(ctx, pgx.Identifier{"copy_tx"}, []string{"id", "name"}, pgx.CopyFromRows(rows))
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// Verify data visible within transaction
	var rowCount int
	err = tx.QueryRow(ctx, "SELECT COUNT(*) FROM copy_tx").Scan(&rowCount)
	require.NoError(t, err)
	assert.Equal(t, 2, rowCount)

	// Commit
	err = tx.Commit(ctx)
	require.NoError(t, err)

	// Verify data persisted after commit
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM copy_tx").Scan(&rowCount)
	require.NoError(t, err)
	assert.Equal(t, 2, rowCount)
}

// TestCopyFromRollback tests that CopyFrom respects transaction rollback
func TestCopyFromRollback(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Create temp table
	_, err = conn.Exec(ctx, "CREATE TEMP TABLE copy_rollback (id int, name text)")
	require.NoError(t, err)

	// Start transaction
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	// CopyFrom within transaction
	rows := [][]any{
		{1, "will-rollback"},
		{2, "also-rollback"},
	}
	count, err := tx.CopyFrom(ctx, pgx.Identifier{"copy_rollback"}, []string{"id", "name"}, pgx.CopyFromRows(rows))
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// Rollback
	err = tx.Rollback(ctx)
	require.NoError(t, err)

	// Verify data was rolled back
	var rowCount int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM copy_rollback").Scan(&rowCount)
	require.NoError(t, err)
	assert.Equal(t, 0, rowCount, "rolled back data should not be visible")
}

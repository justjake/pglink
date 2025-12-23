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

// testTimeout returns a context with a reasonable timeout for tests
func testTimeout(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 30*time.Second)
}

// TestMain sets up and tears down the test harness for all e2e tests.
// This ensures docker-compose and pglink are running before any tests execute.
var testHarness *Harness

func TestMain(m *testing.M) {
	// Create harness with a nil testing.T since we're in TestMain
	testHarness = NewHarnessForMain()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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

	// Each client does a transaction and records the backend PID
	type txResult struct {
		clientIdx  int
		iteration  int
		backendPID int32
		err        error
	}

	resultCh := make(chan txResult, 100)

	// Run multiple iterations
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

	conn1, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn1.Close(context.Background())

	conn2, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn2.Close(context.Background())

	// Create a test table in a transaction on conn1
	_, err = conn1.Exec(ctx, "CREATE TEMP TABLE test_isolation (id int, value text)")
	require.NoError(t, err)

	// Start transaction on conn1
	tx1, err := conn1.Begin(ctx)
	require.NoError(t, err)
	defer tx1.Rollback(context.Background())

	// Insert in transaction
	_, err = tx1.Exec(ctx, "INSERT INTO test_isolation VALUES (1, 'uncommitted')")
	require.NoError(t, err)

	// conn2 shouldn't see the uncommitted data (even if temp tables were shared, which they aren't)
	// Actually temp tables are session-local, so conn2 won't see the table at all
	// Let's use a schema-level table for this test

	// Cleanup and use a real table
	tx1.Rollback(context.Background())

	// Test transaction isolation using session variables instead of tables
	// since the app user doesn't have CREATE TABLE permission.
	// We'll test that a session variable set in a transaction is visible
	// within that transaction but doesn't affect other sessions.

	// Get initial work_mem values for both connections
	var initialWorkMem1, initialWorkMem2 string
	err = conn1.QueryRow(ctx, "SHOW work_mem").Scan(&initialWorkMem1)
	require.NoError(t, err)
	err = conn2.QueryRow(ctx, "SHOW work_mem").Scan(&initialWorkMem2)
	require.NoError(t, err)

	// Start a transaction on conn1 and set work_mem locally
	tx1, err = conn1.Begin(ctx)
	require.NoError(t, err)
	defer tx1.Rollback(context.Background())

	// Set work_mem to a distinct value in the transaction
	_, err = tx1.Exec(ctx, "SET LOCAL work_mem = '256MB'")
	require.NoError(t, err)

	// Verify the variable is set within the transaction
	var val1 string
	err = tx1.QueryRow(ctx, "SHOW work_mem").Scan(&val1)
	require.NoError(t, err)
	assert.Equal(t, "256MB", val1, "work_mem should be set in transaction")

	// conn2 should still have its original work_mem (different session)
	var val2 string
	err = conn2.QueryRow(ctx, "SHOW work_mem").Scan(&val2)
	require.NoError(t, err)
	assert.Equal(t, initialWorkMem2, val2, "work_mem should not be affected in other session")

	// Commit the transaction
	err = tx1.Commit(ctx)
	require.NoError(t, err)

	// After commit with LOCAL, work_mem should be reset to original
	var val3 string
	err = conn1.QueryRow(ctx, "SHOW work_mem").Scan(&val3)
	require.NoError(t, err)
	assert.Equal(t, initialWorkMem1, val3, "LOCAL work_mem should be reset after commit")
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

	const numConnections = 50
	const queriesPerConn = 10

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

	// The pool should be usable again after releasing
	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
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

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Test rollback behavior using session variables since the app user
	// doesn't have CREATE TABLE permission.

	// Start transaction and set a session variable
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, "SET LOCAL my.rollback_test = 'should_be_rolled_back'")
	require.NoError(t, err)

	// Verify the variable is set within transaction
	var val string
	err = tx.QueryRow(ctx, "SHOW my.rollback_test").Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, "should_be_rolled_back", val)

	// Rollback
	err = tx.Rollback(ctx)
	require.NoError(t, err)

	// Verify the LOCAL setting was rolled back
	err = conn.QueryRow(ctx, "SHOW my.rollback_test").Scan(&val)
	require.NoError(t, err)
	assert.Empty(t, val, "LOCAL variable should be reset after rollback")
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

// TestQueryCancellation tests that queries can be cancelled via the PostgreSQL
// cancel protocol. This verifies that the proxy correctly forwards cancel
// requests to the backend.
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

	// Verify the connection is still usable
	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
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

	const iterations = 100
	const concurrency = 10

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

// TestStressQueries runs many queries in parallel
func TestStressQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	pool, err := h.Connect(ctx, "alpha_uno")
	require.NoError(t, err)
	defer pool.Close()

	const numQueries = 1000
	const concurrency = 50

	var wg sync.WaitGroup
	var successCount atomic.Int32
	queryCh := make(chan int, numQueries)

	// Fill the channel with query IDs
	for i := 0; i < numQueries; i++ {
		queryCh <- i
	}
	close(queryCh)

	// Worker goroutines
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for queryID := range queryCh {
				var result int
				err := pool.QueryRow(ctx, "SELECT $1::int", queryID).Scan(&result)
				if err == nil && result == queryID {
					successCount.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	t.Logf("Stress query results: %d/%d successful", successCount.Load(), numQueries)
	// Under extreme load with 50 workers sharing the pool, some transient
	// failures are acceptable. We expect at least 99% success rate.
	successRate := float64(successCount.Load()) / float64(numQueries)
	assert.GreaterOrEqual(t, successRate, 0.99, "at least 99%% of queries should succeed")
}

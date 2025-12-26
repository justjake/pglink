package e2e

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Prepared Statement Caching Tests
//
// These tests verify that pglink handles prepared statements like pgbouncer,
// ensuring transparent statement management across pooled connections.
//
// Key behaviors tested:
// 1. Named prepared statements work within transactions
// 2. Unnamed prepared statements (pgx default) work correctly
// 3. Same query from different connections shares cache entry
// 4. Statement reuse after connection release (pgbouncer's key feature)
// 5. Parameter type info is preserved
// 6. Row descriptions are preserved
// 7. Concurrent access to cached statements
// =============================================================================

// TestPreparedStmt_NamedWithinTransaction tests that named prepared statements
// work correctly when used within a transaction. This is the baseline behavior
// that must always work.
func TestPreparedStmt_NamedWithinTransaction(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Start transaction to ensure we keep the backend connection
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	// Prepare a named statement
	_, err = tx.Prepare(ctx, "add_numbers", "SELECT $1::int + $2::int AS sum")
	require.NoError(t, err)

	// Execute it multiple times with different parameters
	testCases := []struct {
		a, b, expected int
	}{
		{1, 2, 3},
		{10, 20, 30},
		{-5, 5, 0},
		{100, 200, 300},
	}

	for _, tc := range testCases {
		var result int
		err := tx.QueryRow(ctx, "add_numbers", tc.a, tc.b).Scan(&result)
		require.NoError(t, err, "a=%d, b=%d", tc.a, tc.b)
		assert.Equal(t, tc.expected, result, "a=%d, b=%d", tc.a, tc.b)
	}
}

// TestPreparedStmt_UnnamedStatement tests that unnamed prepared statements
// (the default in pgx when using QueryRow/Query) work correctly.
func TestPreparedStmt_UnnamedStatement(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// pgx by default uses unnamed prepared statements for parameterized queries
	// This tests that the proxy handles them correctly
	var result int
	err = conn.QueryRow(ctx, "SELECT $1::int * 2", 21).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 42, result)

	// Multiple queries should work
	for i := 1; i <= 5; i++ {
		var r int
		err = conn.QueryRow(ctx, "SELECT $1::int * $1::int", i).Scan(&r)
		require.NoError(t, err)
		assert.Equal(t, i*i, r, "i=%d", i)
	}
}

// TestPreparedStmt_DifferentQueriesSameConnection tests that multiple different
// prepared statements can coexist on the same connection.
func TestPreparedStmt_DifferentQueriesSameConnection(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	// Prepare multiple statements
	_, err = tx.Prepare(ctx, "stmt_add", "SELECT $1::int + $2::int")
	require.NoError(t, err)

	_, err = tx.Prepare(ctx, "stmt_mult", "SELECT $1::int * $2::int")
	require.NoError(t, err)

	_, err = tx.Prepare(ctx, "stmt_concat", "SELECT $1::text || $2::text")
	require.NoError(t, err)

	// Execute each
	var intResult int
	var textResult string

	err = tx.QueryRow(ctx, "stmt_add", 10, 5).Scan(&intResult)
	require.NoError(t, err)
	assert.Equal(t, 15, intResult)

	err = tx.QueryRow(ctx, "stmt_mult", 10, 5).Scan(&intResult)
	require.NoError(t, err)
	assert.Equal(t, 50, intResult)

	err = tx.QueryRow(ctx, "stmt_concat", "hello", "world").Scan(&textResult)
	require.NoError(t, err)
	assert.Equal(t, "helloworld", textResult)
}

// TestPreparedStmt_ParameterTypes tests that prepared statements with various
// parameter types work correctly and type info is preserved.
func TestPreparedStmt_ParameterTypes(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	// Statement with multiple parameter types
	_, err = tx.Prepare(ctx, "multi_type",
		"SELECT $1::int, $2::text, $3::boolean, $4::float8")
	require.NoError(t, err)

	var intVal int
	var textVal string
	var boolVal bool
	var floatVal float64

	err = tx.QueryRow(ctx, "multi_type", 42, "hello", true, 3.14).Scan(
		&intVal, &textVal, &boolVal, &floatVal)
	require.NoError(t, err)

	assert.Equal(t, 42, intVal)
	assert.Equal(t, "hello", textVal)
	assert.Equal(t, true, boolVal)
	assert.InDelta(t, 3.14, floatVal, 0.001)
}

// TestPreparedStmt_RowDescription tests that prepared statements returning
// multiple columns have their row descriptions preserved correctly.
func TestPreparedStmt_RowDescription(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	// Create a table for testing
	_, err = tx.Exec(ctx, `
		CREATE TEMP TABLE test_rows (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value INT,
			created_at TIMESTAMP DEFAULT NOW()
		)
	`)
	require.NoError(t, err)

	// Insert some data
	_, err = tx.Exec(ctx, `
		INSERT INTO test_rows (name, value) VALUES
		('foo', 100),
		('bar', 200),
		('baz', 300)
	`)
	require.NoError(t, err)

	// Prepare a statement that returns multiple columns
	_, err = tx.Prepare(ctx, "get_rows", "SELECT id, name, value FROM test_rows WHERE value > $1 ORDER BY id")
	require.NoError(t, err)

	// Execute and verify
	rows, err := tx.Query(ctx, "get_rows", 150)
	require.NoError(t, err)
	defer rows.Close()

	var results []struct {
		ID    int
		Name  string
		Value int
	}

	for rows.Next() {
		var r struct {
			ID    int
			Name  string
			Value int
		}
		err := rows.Scan(&r.ID, &r.Name, &r.Value)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, rows.Err())

	assert.Len(t, results, 2) // bar (200) and baz (300)
	assert.Equal(t, "bar", results[0].Name)
	assert.Equal(t, "baz", results[1].Name)
}

// TestPreparedStmt_ConcurrentSameQuery tests that multiple connections can
// use the same query concurrently, and the cache handles this correctly.
func TestPreparedStmt_ConcurrentSameQuery(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const numWorkers = 5
	const queriesPerWorker = 10

	var wg sync.WaitGroup
	var successCount atomic.Int32
	var errorCount atomic.Int32
	errors := make(chan error, numWorkers*queriesPerWorker)

	// All workers use the same query
	query := "SELECT $1::int + $2::int AS sum"

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
			if err != nil {
				errors <- fmt.Errorf("worker %d connect: %w", workerID, err)
				errorCount.Add(1)
				return
			}
			defer conn.Close(context.Background())

			for j := 0; j < queriesPerWorker; j++ {
				var result int
				a, b := workerID*10+j, j
				expected := a + b

				err := conn.QueryRow(ctx, query, a, b).Scan(&result)
				if err != nil {
					errors <- fmt.Errorf("worker %d query %d: %w", workerID, j, err)
					errorCount.Add(1)
					continue
				}

				if result != expected {
					errors <- fmt.Errorf("worker %d query %d: got %d, want %d", workerID, j, result, expected)
					errorCount.Add(1)
					continue
				}

				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Collect errors for reporting
	var errMsgs []string
	for err := range errors {
		errMsgs = append(errMsgs, err.Error())
	}

	assert.Empty(t, errMsgs, "errors occurred: %v", errMsgs)
	assert.Equal(t, int32(numWorkers*queriesPerWorker), successCount.Load())
}

// TestPreparedStmt_ReuseAfterTransactionEnd tests the key pgbouncer behavior:
// a prepared statement should remain usable across transaction boundaries
// when the same logical connection is used.
//
// In transaction pooling mode, when a transaction ends, the backend connection
// may be returned to the pool. If the client tries to use the same prepared
// statement later, the proxy should handle this gracefully.
func TestPreparedStmt_ReuseAfterTransactionEnd(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// First transaction: prepare and use statement
	tx1, err := conn.Begin(ctx)
	require.NoError(t, err)

	_, err = tx1.Prepare(ctx, "reusable_stmt", "SELECT $1::int * 2")
	require.NoError(t, err)

	var result1 int
	err = tx1.QueryRow(ctx, "reusable_stmt", 21).Scan(&result1)
	require.NoError(t, err)
	assert.Equal(t, 42, result1)

	err = tx1.Commit(ctx)
	require.NoError(t, err)

	// Second transaction: try to prepare same statement name again
	// In pgbouncer's transaction pooling mode, this is expected behavior
	tx2, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx2.Rollback(context.Background())

	// Re-prepare the statement (same name, same query)
	// This should work - the proxy should handle that the statement
	// may or may not exist on the new backend connection
	_, err = tx2.Prepare(ctx, "reusable_stmt", "SELECT $1::int * 2")
	require.NoError(t, err)

	var result2 int
	err = tx2.QueryRow(ctx, "reusable_stmt", 50).Scan(&result2)
	require.NoError(t, err)
	assert.Equal(t, 100, result2)
}

// TestPreparedStmt_DescribeStatement tests that DESCRIBE (pgx's statement
// description) works correctly with cached statements.
func TestPreparedStmt_DescribeStatement(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	// Prepare a statement
	sd, err := tx.Prepare(ctx, "describe_test", "SELECT $1::int AS num, $2::text AS str")
	require.NoError(t, err)

	// Verify statement description
	require.NotNil(t, sd)

	// Check parameter OIDs (int4 and text)
	// OID 23 = int4, OID 25 = text
	assert.Len(t, sd.ParamOIDs, 2)

	// Check field descriptions
	assert.Len(t, sd.Fields, 2)
	assert.Equal(t, "num", sd.Fields[0].Name)
	assert.Equal(t, "str", sd.Fields[1].Name)
}

// TestPreparedStmt_ErrorRecovery tests that the proxy correctly handles
// errors during prepared statement execution and recovers gracefully.
func TestPreparedStmt_ErrorRecovery(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	// Prepare a statement
	_, err = tx.Prepare(ctx, "error_test", "SELECT $1::int / $2::int")
	require.NoError(t, err)

	// Execute successfully
	var result int
	err = tx.QueryRow(ctx, "error_test", 10, 2).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 5, result)

	// Execute with error (division by zero)
	err = tx.QueryRow(ctx, "error_test", 10, 0).Scan(&result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "division by zero")

	// Transaction should be aborted after error
	err = tx.Rollback(ctx)
	require.NoError(t, err)

	// Start a new transaction and verify connection still works
	tx2, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx2.Rollback(context.Background())

	// Should be able to prepare and execute again
	_, err = tx2.Prepare(ctx, "recovery_test", "SELECT 1")
	require.NoError(t, err)

	err = tx2.QueryRow(ctx, "recovery_test").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 1, result)
}

// TestPreparedStmt_LargeNumberOfStatements tests that many different prepared
// statements can be used, exercising the LRU cache eviction.
func TestPreparedStmt_LargeNumberOfStatements(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	// Create and use many statements to test cache behavior
	const numStatements = 50

	for i := 0; i < numStatements; i++ {
		stmtName := fmt.Sprintf("stmt_%d", i)
		// Each statement has a slightly different query
		query := fmt.Sprintf("SELECT $1::int + %d", i)

		_, err := tx.Prepare(ctx, stmtName, query)
		require.NoError(t, err, "prepare stmt_%d", i)

		var result int
		err = tx.QueryRow(ctx, stmtName, 100).Scan(&result)
		require.NoError(t, err, "exec stmt_%d", i)
		assert.Equal(t, 100+i, result, "stmt_%d result", i)
	}

	// Go back and re-execute some earlier statements to verify they still work
	// (tests that cache eviction doesn't break in-transaction statements)
	for i := 0; i < 10; i++ {
		stmtName := fmt.Sprintf("stmt_%d", i)
		var result int
		err := tx.QueryRow(ctx, stmtName, 200).Scan(&result)
		require.NoError(t, err, "re-exec stmt_%d", i)
		assert.Equal(t, 200+i, result, "re-exec stmt_%d result", i)
	}
}

// TestPreparedStmt_NoRowsResult tests prepared statements that return no rows.
func TestPreparedStmt_NoRowsResult(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	// Create temp table
	_, err = tx.Exec(ctx, "CREATE TEMP TABLE empty_test (id INT)")
	require.NoError(t, err)

	// Prepare a SELECT that will return no rows
	_, err = tx.Prepare(ctx, "empty_select", "SELECT id FROM empty_test WHERE id = $1")
	require.NoError(t, err)

	// Execute - should return pgx.ErrNoRows
	var result int
	err = tx.QueryRow(ctx, "empty_select", 999).Scan(&result)
	assert.ErrorIs(t, err, pgx.ErrNoRows)
}

// TestPreparedStmt_DML tests prepared statements with INSERT/UPDATE/DELETE.
func TestPreparedStmt_DML(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	// Create temp table
	_, err = tx.Exec(ctx, `
		CREATE TEMP TABLE dml_test (
			id SERIAL PRIMARY KEY,
			value INT
		)
	`)
	require.NoError(t, err)

	// Prepare INSERT
	_, err = tx.Prepare(ctx, "insert_stmt", "INSERT INTO dml_test (value) VALUES ($1) RETURNING id")
	require.NoError(t, err)

	// Prepare UPDATE
	_, err = tx.Prepare(ctx, "update_stmt", "UPDATE dml_test SET value = $2 WHERE id = $1")
	require.NoError(t, err)

	// Prepare DELETE
	_, err = tx.Prepare(ctx, "delete_stmt", "DELETE FROM dml_test WHERE id = $1")
	require.NoError(t, err)

	// Insert
	var id int
	err = tx.QueryRow(ctx, "insert_stmt", 100).Scan(&id)
	require.NoError(t, err)
	assert.Equal(t, 1, id)

	// Update
	tag, err := tx.Exec(ctx, "update_stmt", id, 200)
	require.NoError(t, err)
	assert.Equal(t, int64(1), tag.RowsAffected())

	// Verify update
	var value int
	err = tx.QueryRow(ctx, "SELECT value FROM dml_test WHERE id = $1", id).Scan(&value)
	require.NoError(t, err)
	assert.Equal(t, 200, value)

	// Delete
	tag, err = tx.Exec(ctx, "delete_stmt", id)
	require.NoError(t, err)
	assert.Equal(t, int64(1), tag.RowsAffected())

	// Verify delete
	err = tx.QueryRow(ctx, "SELECT value FROM dml_test WHERE id = $1", id).Scan(&value)
	assert.ErrorIs(t, err, pgx.ErrNoRows)
}

// TestPreparedStmt_NullParameters tests prepared statements with NULL parameters.
func TestPreparedStmt_NullParameters(t *testing.T) {
	h := getHarness(t)
	ctx, cancel := testTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	// Prepare statement that handles NULLs
	_, err = tx.Prepare(ctx, "null_test", "SELECT COALESCE($1::int, 0) AS result")
	require.NoError(t, err)

	// With non-NULL
	var result int
	err = tx.QueryRow(ctx, "null_test", 42).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 42, result)

	// With NULL
	err = tx.QueryRow(ctx, "null_test", nil).Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, 0, result)
}

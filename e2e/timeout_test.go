package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/justjake/pglink/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// timeoutTestTimeout returns a context with a longer timeout for timeout tests.
// These tests intentionally wait for timeouts to fire, so they need more time.
func timeoutTestTimeout(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 10*time.Second)
}

// newTimeoutHarness creates a new harness with timeout settings configured.
// Each timeout test gets its own harness to avoid interference.
func newTimeoutHarness(t *testing.T, modifier ConfigModifier) *Harness {
	t.Helper()

	h := NewHarness(t)
	h.ConfigModifier = modifier

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h.Start(ctx)

	t.Cleanup(func() {
		h.Stop()
	})

	return h
}

// =============================================================================
// Query Timeout Tests
// =============================================================================

// TestQueryTimeout_Terminate verifies that query_timeout terminates connections
// when a query exceeds the timeout duration.
func TestQueryTimeout_Terminate(t *testing.T) {
	// Configure a 500ms query timeout
	h := newTimeoutHarness(t, func(cfg *config.Config) {
		for _, dbCfg := range cfg.Databases {
			dbCfg.QueryTimeout = config.Duration(500 * time.Millisecond)
			dbCfg.TimeoutAction = config.TimeoutActionTerminate
		}
	})

	ctx, cancel := timeoutTestTimeout(t)
	defer cancel()

	// Connect to the database
	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Execute a query that exceeds the timeout (pg_sleep for 2 seconds)
	_, err = conn.Exec(ctx, "SELECT pg_sleep(2)")

	// Should get an error - the connection should be terminated
	require.Error(t, err, "expected timeout error")

	// The error should contain timeout-related information
	errStr := err.Error()
	assert.True(t,
		strings.Contains(errStr, "timeout") ||
			strings.Contains(errStr, "57014") || // query_canceled error code
			strings.Contains(errStr, "cancel"),
		"error should indicate timeout: %v", err)
}

// TestQueryTimeout_ShortQuery verifies that queries completing within the
// timeout are not affected.
func TestQueryTimeout_ShortQuery(t *testing.T) {
	// Configure a 1 second query timeout
	h := newTimeoutHarness(t, func(cfg *config.Config) {
		for _, dbCfg := range cfg.Databases {
			dbCfg.QueryTimeout = config.Duration(1 * time.Second)
			dbCfg.TimeoutAction = config.TimeoutActionTerminate
		}
	})

	ctx, cancel := timeoutTestTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Execute a quick query that should complete within timeout
	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err, "short query should succeed")
	assert.Equal(t, 1, result)

	// Execute another query to verify connection is still usable
	err = conn.QueryRow(ctx, "SELECT 2").Scan(&result)
	require.NoError(t, err, "second query should succeed")
	assert.Equal(t, 2, result)
}

// =============================================================================
// Idle Transaction Timeout Tests
// =============================================================================

// TestIdleTransactionTimeout verifies that idle_transaction_timeout terminates
// connections that are idle within a transaction for too long.
func TestIdleTransactionTimeout(t *testing.T) {
	// Configure a 500ms idle transaction timeout
	h := newTimeoutHarness(t, func(cfg *config.Config) {
		for _, dbCfg := range cfg.Databases {
			dbCfg.IdleTransactionTimeout = config.Duration(500 * time.Millisecond)
			dbCfg.TimeoutAction = config.TimeoutActionTerminate
		}
	})

	ctx, cancel := timeoutTestTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Start a transaction
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	// Run a quick query to establish we're in a transaction
	var result int
	err = tx.QueryRow(ctx, "SELECT 1").Scan(&result)
	require.NoError(t, err)

	// Sleep longer than the idle transaction timeout
	time.Sleep(700 * time.Millisecond)

	// Try to execute another query - should fail due to timeout
	_, err = tx.Exec(ctx, "SELECT 2")

	// Should get an error - the connection should be terminated
	require.Error(t, err, "expected idle transaction timeout error")

	errStr := err.Error()
	assert.True(t,
		strings.Contains(errStr, "timeout") ||
			strings.Contains(errStr, "25P03") || // idle_in_transaction_session_timeout error code
			strings.Contains(errStr, "idle"),
		"error should indicate idle transaction timeout: %v", err)
}

// TestIdleTransactionTimeout_ActiveQuery verifies that idle_transaction_timeout
// does NOT fire while a query is actively executing.
func TestIdleTransactionTimeout_ActiveQuery(t *testing.T) {
	// Configure a 300ms idle transaction timeout
	h := newTimeoutHarness(t, func(cfg *config.Config) {
		for _, dbCfg := range cfg.Databases {
			dbCfg.IdleTransactionTimeout = config.Duration(300 * time.Millisecond)
			dbCfg.TimeoutAction = config.TimeoutActionTerminate
		}
	})

	ctx, cancel := timeoutTestTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Start a transaction
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	// Execute a query that takes longer than idle_transaction_timeout
	// but should succeed because we're ACTIVELY querying, not idle
	var result int
	err = tx.QueryRow(ctx, "SELECT pg_sleep(0.5), 42").Scan(new(any), &result)
	require.NoError(t, err, "active query should not be affected by idle_transaction_timeout")
	assert.Equal(t, 42, result)

	// Commit the transaction
	err = tx.Commit(ctx)
	require.NoError(t, err, "commit should succeed")
}

// =============================================================================
// Transaction Timeout Tests
// =============================================================================

// TestTransactionTimeout verifies that transaction_timeout terminates
// transactions that exceed the total allowed time.
func TestTransactionTimeout(t *testing.T) {
	// Configure a 800ms transaction timeout
	h := newTimeoutHarness(t, func(cfg *config.Config) {
		for _, dbCfg := range cfg.Databases {
			dbCfg.TransactionTimeout = config.Duration(800 * time.Millisecond)
			dbCfg.TimeoutAction = config.TimeoutActionTerminate
		}
	})

	ctx, cancel := timeoutTestTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Start a transaction
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	// Execute multiple quick queries with sleeps to exceed total transaction time
	for i := 0; i < 3; i++ {
		var result int
		err = tx.QueryRow(ctx, "SELECT $1::int", i).Scan(&result)
		if err != nil {
			// Transaction timeout may have fired
			break
		}
		time.Sleep(300 * time.Millisecond)
	}

	// At this point, either the loop broke with an error, or we can check commit
	if err == nil {
		// Try to commit - should fail due to transaction timeout
		err = tx.Commit(ctx)
	}

	require.Error(t, err, "expected transaction timeout error")

	errStr := err.Error()
	assert.True(t,
		strings.Contains(errStr, "timeout") ||
			strings.Contains(errStr, "57014") || // query_canceled
			strings.Contains(errStr, "transaction"),
		"error should indicate transaction timeout: %v", err)
}

// TestTransactionTimeout_ShortTransaction verifies that transactions completing
// within the timeout are not affected.
func TestTransactionTimeout_ShortTransaction(t *testing.T) {
	// Configure a 2 second transaction timeout
	h := newTimeoutHarness(t, func(cfg *config.Config) {
		for _, dbCfg := range cfg.Databases {
			dbCfg.TransactionTimeout = config.Duration(2 * time.Second)
			dbCfg.TimeoutAction = config.TimeoutActionTerminate
		}
	})

	ctx, cancel := timeoutTestTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Start a transaction
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	// Execute multiple quick queries
	for i := 0; i < 5; i++ {
		var result int
		err = tx.QueryRow(ctx, "SELECT $1::int", i).Scan(&result)
		require.NoError(t, err, "query %d should succeed", i)
		assert.Equal(t, i, result)
	}

	// Commit should succeed
	err = tx.Commit(ctx)
	require.NoError(t, err, "commit should succeed")
}

// =============================================================================
// Timeout Priority Tests
// =============================================================================

// TestTimeoutPriority_QueryOverTransaction verifies that query_timeout takes
// priority over transaction_timeout when both would fire.
func TestTimeoutPriority_QueryOverTransaction(t *testing.T) {
	// Configure both timeouts - query shorter than transaction
	h := newTimeoutHarness(t, func(cfg *config.Config) {
		for _, dbCfg := range cfg.Databases {
			dbCfg.QueryTimeout = config.Duration(500 * time.Millisecond)
			dbCfg.TransactionTimeout = config.Duration(2 * time.Second)
			dbCfg.TimeoutAction = config.TimeoutActionTerminate
		}
	})

	ctx, cancel := timeoutTestTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Start a transaction
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	// Execute a long query - should hit query_timeout first
	_, err = tx.Exec(ctx, "SELECT pg_sleep(3)")
	require.Error(t, err, "expected query timeout error")

	// The error should indicate query timeout (57014/query_canceled), not transaction timeout
	errStr := err.Error()
	assert.True(t,
		strings.Contains(errStr, "query") ||
			strings.Contains(errStr, "57014"),
		"should be query timeout error, got: %v", err)
}

// =============================================================================
// Edge Cases
// =============================================================================

// TestTimeout_NoTimeoutConfigured verifies that connections work normally
// when no timeouts are configured.
func TestTimeout_NoTimeoutConfigured(t *testing.T) {
	// Use default harness (no timeout modifier)
	h := getHarness(t)

	ctx, cancel := timeoutTestTimeout(t)
	defer cancel()

	conn, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Start a transaction
	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	// Execute a slow query - should succeed since no timeout configured
	var result int
	err = tx.QueryRow(ctx, "SELECT pg_sleep(0.1), 42").Scan(new(any), &result)
	require.NoError(t, err, "query should succeed without timeout")
	assert.Equal(t, 42, result)

	err = tx.Commit(ctx)
	require.NoError(t, err, "commit should succeed")
}

// TestTimeout_ConnectionReuse verifies that after a timeout terminates a connection,
// subsequent connections still work.
func TestTimeout_ConnectionReuse(t *testing.T) {
	// Configure a short query timeout
	h := newTimeoutHarness(t, func(cfg *config.Config) {
		for _, dbCfg := range cfg.Databases {
			dbCfg.QueryTimeout = config.Duration(300 * time.Millisecond)
			dbCfg.TimeoutAction = config.TimeoutActionTerminate
		}
	})

	ctx, cancel := timeoutTestTimeout(t)
	defer cancel()

	// First connection - trigger timeout
	conn1, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)

	_, err = conn1.Exec(ctx, "SELECT pg_sleep(1)")
	require.Error(t, err, "expected timeout")
	conn1.Close(context.Background())

	// Second connection - should work fine
	conn2, err := h.ConnectSingle(ctx, "alpha_uno", PredefinedUsers.App)
	require.NoError(t, err)
	defer conn2.Close(context.Background())

	var result int
	err = conn2.QueryRow(ctx, "SELECT 123").Scan(&result)
	require.NoError(t, err, "new connection should work")
	assert.Equal(t, 123, result)
}

// TestTimeout_SimpleProtocol verifies that timeouts work with simple query protocol.
func TestTimeout_SimpleProtocol(t *testing.T) {
	h := newTimeoutHarness(t, func(cfg *config.Config) {
		for _, dbCfg := range cfg.Databases {
			dbCfg.QueryTimeout = config.Duration(500 * time.Millisecond)
			dbCfg.TimeoutAction = config.TimeoutActionTerminate
		}
	})

	ctx, cancel := timeoutTestTimeout(t)
	defer cancel()

	// Connect with simple protocol mode
	connStr := fmt.Sprintf(
		"postgres://app:app_password@localhost:%d/alpha_uno?sslmode=prefer",
		h.Port(),
	)

	connConfig, err := pgx.ParseConfig(connStr)
	require.NoError(t, err)
	connConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	// Execute a query that exceeds the timeout
	_, err = conn.Exec(ctx, "SELECT pg_sleep(2)")
	require.Error(t, err, "expected timeout error with simple protocol")
}

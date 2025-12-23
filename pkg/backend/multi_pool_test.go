// Package backend provides tests for the MultiPool connection pool.
//
// These tests require PostgreSQL to be running. Start the docker-compose
// infrastructure with: docker-compose up -d
//
// The tests use the same backends as the e2e tests (ports 15432, 15433, 15434).
package backend

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testConnStr returns a connection string for the test PostgreSQL server.
// Uses the alpha backend from docker-compose (port 15432).
func testConnStr() string {
	return "postgres://postgres:postgres@localhost:15432/postgres?sslmode=disable"
}

// newTestLogger creates a logger for tests that discards output by default.
func newTestLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestPool creates a MultiPool for testing with the given max connections.
func newTestPool(t *testing.T, maxConns int32) *MultiPool[string] {
	t.Helper()
	logger := newTestLogger(t)
	return NewMultiPool[string](maxConns, logger)
}

// addTestPoolUser adds a user pool to the MultiPool with default config.
func addTestPoolUser(t *testing.T, ctx context.Context, mp *MultiPool[string], userKey string) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(testConnStr())
	require.NoError(t, err, "failed to parse pool config")

	// Configure pgxpool to defer to MultiPool for connection management.
	cfg.MinConns = 0
	cfg.MaxConns = 100 // High limit - let MultiPool control the actual limit
	cfg.MaxConnIdleTime = time.Hour
	cfg.MaxConnLifetime = time.Hour

	err = mp.AddPool(ctx, userKey, cfg)
	require.NoError(t, err, "failed to add pool for user %s", userKey)
}

// =============================================================================
// MaxConns Enforcement Tests
// =============================================================================

// TestMultiPool_MaxConns_BlocksWhenAtLimit verifies that Acquire blocks when
// all connections are in use and the pool is at its max connection limit.
func TestMultiPool_MaxConns_BlocksWhenAtLimit(t *testing.T) {
	ctx := context.Background()
	const maxConns = 3

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1")
	mp.Start()
	defer mp.Close()

	// Acquire all available connections
	conns := make([]*MultiPoolConn, maxConns)
	for i := 0; i < maxConns; i++ {
		conn, err := mp.Acquire(ctx, "user1")
		require.NoError(t, err, "failed to acquire connection %d", i)
		conns[i] = conn
	}

	// Verify we have max connections
	assert.Equal(t, int32(maxConns), mp.totalConns.Load(), "should have maxConns total connections")

	// Attempt to acquire another connection with a short timeout - should block and timeout
	shortCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	_, err := mp.Acquire(shortCtx, "user1")
	assert.ErrorIs(t, err, context.DeadlineExceeded, "should timeout when pool is exhausted")

	// Release all connections
	for _, c := range conns {
		c.Release()
	}
}

// TestMultiPool_MaxConns_SharedAcrossUsers verifies that the max connection
// limit is shared across all user pools.
func TestMultiPool_MaxConns_SharedAcrossUsers(t *testing.T) {
	ctx := context.Background()
	const maxConns = 4

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1")
	addTestPoolUser(t, ctx, mp, "user2")
	mp.Start()
	defer mp.Close()

	// Acquire 2 connections from each user (total 4 = maxConns)
	var conns []*MultiPoolConn
	for i := 0; i < 2; i++ {
		conn, err := mp.Acquire(ctx, "user1")
		require.NoError(t, err)
		conns = append(conns, conn)

		conn, err = mp.Acquire(ctx, "user2")
		require.NoError(t, err)
		conns = append(conns, conn)
	}

	assert.Equal(t, int32(maxConns), mp.totalConns.Load(), "should have maxConns total connections")

	// Either user should be blocked now
	shortCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	_, err := mp.Acquire(shortCtx, "user1")
	assert.ErrorIs(t, err, context.DeadlineExceeded, "user1 should be blocked")

	shortCtx2, cancel2 := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel2()

	_, err = mp.Acquire(shortCtx2, "user2")
	assert.ErrorIs(t, err, context.DeadlineExceeded, "user2 should be blocked")

	// Release all connections
	for _, c := range conns {
		c.Release()
	}
}

// TestMultiPool_MaxConns_ReleaseMakesRoom verifies that releasing a connection
// allows a blocked Acquire to proceed.
func TestMultiPool_MaxConns_ReleaseMakesRoom(t *testing.T) {
	ctx := context.Background()
	const maxConns = 2

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1")
	mp.Start()
	defer mp.Close()

	// Acquire all connections
	conn1, err := mp.Acquire(ctx, "user1")
	require.NoError(t, err)
	conn2, err := mp.Acquire(ctx, "user1")
	require.NoError(t, err)

	// Start a goroutine that will try to acquire and block
	acquireDone := make(chan *MultiPoolConn, 1)
	acquireErr := make(chan error, 1)
	go func() {
		conn, err := mp.Acquire(ctx, "user1")
		if err != nil {
			acquireErr <- err
		} else {
			acquireDone <- conn
		}
	}()

	// Give the goroutine time to block
	time.Sleep(50 * time.Millisecond)

	// Release one connection
	conn1.Release()

	// The blocked acquire should now succeed
	select {
	case conn := <-acquireDone:
		conn.Release()
	case err := <-acquireErr:
		t.Fatalf("expected acquire to succeed after release, got error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("acquire should have succeeded within 2 seconds")
	}

	conn2.Release()
}

// =============================================================================
// Connection Counter Tests
// =============================================================================

// TestMultiPool_Counters_AccurateOnAcquireRelease verifies that totalConns and
// totalIdleConns are accurately tracked through acquire/release cycles.
func TestMultiPool_Counters_AccurateOnAcquireRelease(t *testing.T) {
	ctx := context.Background()
	const maxConns = 5

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1")
	mp.Start()
	defer mp.Close()

	// Initially no connections
	assert.Equal(t, int32(0), mp.totalConns.Load(), "initial totalConns should be 0")
	assert.Equal(t, int32(0), mp.totalIdleConns.Load(), "initial totalIdleConns should be 0")

	// Acquire a connection
	conn1, err := mp.Acquire(ctx, "user1")
	require.NoError(t, err)

	// Should have 1 total, 0 idle (it's in use)
	assert.Equal(t, int32(1), mp.totalConns.Load(), "totalConns after first acquire")
	assert.Equal(t, int32(0), mp.totalIdleConns.Load(), "totalIdleConns after first acquire")

	// Acquire another
	conn2, err := mp.Acquire(ctx, "user1")
	require.NoError(t, err)

	assert.Equal(t, int32(2), mp.totalConns.Load(), "totalConns after second acquire")
	assert.Equal(t, int32(0), mp.totalIdleConns.Load(), "totalIdleConns after second acquire")

	// Release the first connection
	conn1.Release()
	time.Sleep(20 * time.Millisecond) // Allow async operations

	assert.Equal(t, int32(2), mp.totalConns.Load(), "totalConns after first release")
	assert.Equal(t, int32(1), mp.totalIdleConns.Load(), "totalIdleConns after first release")

	// Release the second connection
	conn2.Release()
	time.Sleep(20 * time.Millisecond)

	assert.Equal(t, int32(2), mp.totalConns.Load(), "totalConns after second release")
	assert.Equal(t, int32(2), mp.totalIdleConns.Load(), "totalIdleConns after second release")
}

// TestMultiPool_Counters_MultipleUsers verifies counter accuracy across multiple user pools.
func TestMultiPool_Counters_MultipleUsers(t *testing.T) {
	ctx := context.Background()
	const maxConns = 10

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1")
	addTestPoolUser(t, ctx, mp, "user2")
	addTestPoolUser(t, ctx, mp, "user3")
	mp.Start()
	defer mp.Close()

	// Acquire connections from each user
	conn1, err := mp.Acquire(ctx, "user1")
	require.NoError(t, err)
	conn2, err := mp.Acquire(ctx, "user2")
	require.NoError(t, err)
	conn3, err := mp.Acquire(ctx, "user3")
	require.NoError(t, err)

	assert.Equal(t, int32(3), mp.totalConns.Load(), "totalConns across users")
	assert.Equal(t, int32(0), mp.totalIdleConns.Load(), "all connections in use")

	// Release all
	conn1.Release()
	conn2.Release()
	conn3.Release()
	time.Sleep(30 * time.Millisecond)

	assert.Equal(t, int32(3), mp.totalConns.Load(), "totalConns after release")
	assert.Equal(t, int32(3), mp.totalIdleConns.Load(), "all connections idle")
}

// =============================================================================
// Connection Destruction Tests
// =============================================================================

// TestMultiPool_MarkForDestroy verifies that marked connections are destroyed on release.
func TestMultiPool_MarkForDestroy(t *testing.T) {
	ctx := context.Background()
	const maxConns = 5

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1")
	mp.Start()
	defer mp.Close()

	// Acquire a connection
	conn, err := mp.Acquire(ctx, "user1")
	require.NoError(t, err)

	assert.Equal(t, int32(1), mp.totalConns.Load())

	// Mark for destruction and release
	conn.ReleaseAndDestroy()

	// Wait for async destruction to complete
	time.Sleep(100 * time.Millisecond)

	// Connection should be destroyed
	assert.Equal(t, int32(0), mp.totalConns.Load(), "connection should be destroyed")
	assert.Equal(t, int32(0), mp.totalIdleConns.Load(), "no idle connections")
}

// =============================================================================
// Cross-User Connection Acquisition Tests
// =============================================================================

// TestMultiPool_CrossUserAcquisition verifies that when one user has idle
// connections filling the pool, another user can still acquire by reclaiming
// an idle connection.
func TestMultiPool_CrossUserAcquisition(t *testing.T) {
	ctx := context.Background()
	const maxConns = 2

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1")
	addTestPoolUser(t, ctx, mp, "user2")
	mp.Start()
	defer mp.Close()

	// User1 acquires all connections
	conn1, err := mp.Acquire(ctx, "user1")
	require.NoError(t, err)
	conn2, err := mp.Acquire(ctx, "user1")
	require.NoError(t, err)

	assert.Equal(t, int32(maxConns), mp.totalConns.Load())

	// User1 releases - connections become idle in user1's pool
	conn1.Release()
	conn2.Release()
	time.Sleep(20 * time.Millisecond)

	assert.Equal(t, int32(maxConns), mp.totalConns.Load())
	assert.Equal(t, int32(maxConns), mp.totalIdleConns.Load())

	// User2 should be able to acquire by reclaiming user1's idle connection
	conn3, err := mp.Acquire(ctx, "user2")
	require.NoError(t, err, "user2 should be able to acquire by reclaiming user1's idle connection")

	conn3.Release()
}

// TestMultiPool_SingleMaxConn_Reacquire verifies that with maxConns=1, the same
// user can release and reacquire the connection.
func TestMultiPool_SingleMaxConn_Reacquire(t *testing.T) {
	ctx := context.Background()
	const maxConns = 1

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1")
	mp.Start()
	defer mp.Close()

	// Acquire the single connection
	conn1, err := mp.Acquire(ctx, "user1")
	require.NoError(t, err)

	assert.Equal(t, int32(1), mp.totalConns.Load())

	// Release it
	conn1.Release()
	time.Sleep(20 * time.Millisecond)

	// Should be able to reacquire
	conn2, err := mp.Acquire(ctx, "user1")
	require.NoError(t, err, "should be able to reacquire after release")

	conn2.Release()
}

// TestMultiPool_SingleMaxConn_CrossUser verifies that with maxConns=1, user2
// can acquire after user1 releases.
func TestMultiPool_SingleMaxConn_CrossUser(t *testing.T) {
	ctx := context.Background()
	const maxConns = 1

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1")
	addTestPoolUser(t, ctx, mp, "user2")
	mp.Start()
	defer mp.Close()

	// User1 acquires the single connection
	conn1, err := mp.Acquire(ctx, "user1")
	require.NoError(t, err)

	// User2 should block
	shortCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	_, err = mp.Acquire(shortCtx, "user2")
	cancel()
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// User1 releases
	conn1.Release()
	time.Sleep(20 * time.Millisecond)

	// User2 should now be able to acquire (reclaiming user1's idle connection)
	conn2, err := mp.Acquire(ctx, "user2")
	require.NoError(t, err, "user2 should acquire after user1 releases")

	conn2.Release()
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

// TestMultiPool_ConcurrentAcquireRelease tests concurrent access to the pool.
func TestMultiPool_ConcurrentAcquireRelease(t *testing.T) {
	ctx := context.Background()
	const maxConns = 10
	const numGoroutines = 20
	const iterationsPerGoroutine = 10

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1")
	addTestPoolUser(t, ctx, mp, "user2")
	mp.Start()
	defer mp.Close()

	var wg sync.WaitGroup
	var successCount atomic.Int32
	var errorCount atomic.Int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			user := "user1"
			if goroutineID%2 == 0 {
				user = "user2"
			}

			for j := 0; j < iterationsPerGoroutine; j++ {
				shortCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				conn, err := mp.Acquire(shortCtx, user)
				cancel()

				if err != nil {
					errorCount.Add(1)
					continue
				}

				// Do some work
				_, err = conn.Value().Exec(ctx, "SELECT 1")
				if err != nil {
					conn.ReleaseAndDestroy()
					errorCount.Add(1)
					continue
				}

				conn.Release()
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	t.Logf("Concurrent test: %d successes, %d errors", successCount.Load(), errorCount.Load())

	// Most operations should succeed
	totalOps := int32(numGoroutines * iterationsPerGoroutine)
	successRate := float64(successCount.Load()) / float64(totalOps)
	assert.GreaterOrEqual(t, successRate, 0.9, "at least 90%% should succeed")

	// Counters should be consistent
	assert.LessOrEqual(t, mp.totalConns.Load(), int32(maxConns), "should not exceed maxConns")
	assert.GreaterOrEqual(t, mp.totalIdleConns.Load(), int32(0), "idle should not be negative")
}

// TestMultiPool_ConcurrentAcquire_RespectMaxConns verifies that concurrent
// acquires never exceed MaxConns connections due to the mutex serialization
// of the destroy-and-create operation.
func TestMultiPool_ConcurrentAcquire_RespectMaxConns(t *testing.T) {
	ctx := context.Background()
	const maxConns = 5
	const numGoroutines = 50

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1")
	mp.Start()
	defer mp.Close()

	var wg sync.WaitGroup
	var maxObserved atomic.Int32

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			shortCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			conn, err := mp.Acquire(shortCtx, "user1")
			if err != nil {
				return
			}
			defer conn.Release()

			current := mp.totalConns.Load()
			for {
				old := maxObserved.Load()
				if current <= old {
					break
				}
				if maxObserved.CompareAndSwap(old, current) {
					break
				}
			}

			time.Sleep(10 * time.Millisecond)
		}()
	}

	wg.Wait()

	// With the mutex serialization, totalConns should never exceed maxConns
	assert.LessOrEqual(t, maxObserved.Load(), int32(maxConns),
		"should never observe more than maxConns connections")

	// After all operations complete, connections should settle to <= maxConns
	time.Sleep(100 * time.Millisecond) // Allow async cleanup
	finalConns := mp.totalConns.Load()
	assert.LessOrEqual(t, finalConns, int32(maxConns),
		"connections should settle to maxConns after operations complete")
}

// =============================================================================
// Edge Cases
// =============================================================================

// TestMultiPool_AcquireAfterClose verifies behavior when acquiring after close.
func TestMultiPool_AcquireAfterClose(t *testing.T) {
	ctx := context.Background()

	mp := newTestPool(t, 5)
	addTestPoolUser(t, ctx, mp, "user1")
	mp.Start()
	mp.Close()

	_, err := mp.Acquire(ctx, "user1")
	assert.Error(t, err, "acquire after close should fail")
}

// TestMultiPool_ReleaseConnection verifies basic release behavior.
func TestMultiPool_ReleaseConnection(t *testing.T) {
	ctx := context.Background()

	mp := newTestPool(t, 5)
	addTestPoolUser(t, ctx, mp, "user1")
	mp.Start()
	defer mp.Close()

	conn, err := mp.Acquire(ctx, "user1")
	require.NoError(t, err)

	assert.Equal(t, int32(1), mp.totalConns.Load())
	assert.Equal(t, int32(0), mp.totalIdleConns.Load())

	conn.Release()
	time.Sleep(20 * time.Millisecond)

	assert.Equal(t, int32(1), mp.totalConns.Load())
	assert.Equal(t, int32(1), mp.totalIdleConns.Load())
}

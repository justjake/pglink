// Package backend provides tests for the MultiPool connection pool.
//
// These tests require PostgreSQL to be running. Start the docker-compose
// infrastructure with: docker-compose up -d
//
// The tests use the same backends as the e2e tests (ports 15432, 15433, 15434).
package backend

import (
	"context"
	"fmt"
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

// addTestPoolUser adds a user pool with a specified per-pool max connections.
func addTestPoolUser(t *testing.T, ctx context.Context, mp *MultiPool[string], userKey string, poolMaxConns int32) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(testConnStr())
	require.NoError(t, err, "failed to parse pool config")

	// Configure pgxpool to defer to MultiPool for connection management.
	cfg.MinConns = 0
	cfg.MaxConns = poolMaxConns
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
	const maxConns = 50

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
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
	const maxConns = 50

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
	addTestPoolUser(t, ctx, mp, "user2", maxConns)
	mp.Start()
	defer mp.Close()

	// Acquire maxConns/2 connections from each user (total = maxConns)
	var conns []*MultiPoolConn
	for i := 0; i < maxConns/2; i++ {
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
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
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
	const maxConns = 50

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
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
	const maxConns = 50

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
	addTestPoolUser(t, ctx, mp, "user2", maxConns)
	addTestPoolUser(t, ctx, mp, "user3", maxConns)
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
	const maxConns = 50

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
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
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
	addTestPoolUser(t, ctx, mp, "user2", maxConns)
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
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
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
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
	addTestPoolUser(t, ctx, mp, "user2", maxConns)
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
	const maxConns = 50
	const numGoroutines = 20
	const iterationsPerGoroutine = 10

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
	addTestPoolUser(t, ctx, mp, "user2", maxConns)
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

	// Most operations should succeed - some failures are expected under high contention
	totalOps := int32(numGoroutines * iterationsPerGoroutine)
	successRate := float64(successCount.Load()) / float64(totalOps)
	assert.GreaterOrEqual(t, successRate, 0.8, "at least 80%% should succeed under high contention")

	// Counters should be consistent
	assert.LessOrEqual(t, mp.totalConns.Load(), int32(maxConns), "should not exceed maxConns")
	assert.GreaterOrEqual(t, mp.totalIdleConns.Load(), int32(0), "idle should not be negative")
}

// TestMultiPool_ConcurrentAcquire_SettlesToMaxConns verifies that concurrent
// acquires eventually settle to MaxConns or below. Temporary overshoot is allowed
// during concurrent operations.
func TestMultiPool_ConcurrentAcquire_SettlesToMaxConns(t *testing.T) {
	ctx := context.Background()
	const maxConns = 50
	const numGoroutines = 100
	// Allow temporary overshoot during concurrent operations (10% of maxConns)
	const maxAllowedOvershoot = 10

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
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

	t.Logf("Max observed: %d, MaxConns: %d", maxObserved.Load(), maxConns)

	// Allow temporary overshoot, but not unbounded growth
	assert.LessOrEqual(t, maxObserved.Load(), int32(maxConns+maxAllowedOvershoot),
		"should not grow significantly beyond MaxConns during concurrent operations")

	// After all operations complete, connections should settle to <= maxConns
	time.Sleep(200 * time.Millisecond) // Allow async cleanup
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
	const maxConns = 5

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
	mp.Start()
	mp.Close()

	_, err := mp.Acquire(ctx, "user1")
	assert.Error(t, err, "acquire after close should fail")
}

// TestMultiPool_ReleaseConnection verifies basic release behavior.
func TestMultiPool_ReleaseConnection(t *testing.T) {
	ctx := context.Background()
	const maxConns = 5

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
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

// =============================================================================
// MaxConns Limit Behavior Tests (Temporary Overshoot Allowed)
// =============================================================================

// TestMultiPool_MaxConns_EventuallySettles verifies that even if connections
// briefly exceed MaxConns during concurrent acquisition, they eventually settle
// back to MaxConns or below.
func TestMultiPool_MaxConns_EventuallySettles(t *testing.T) {
	ctx := context.Background()
	const maxConns = 50
	const numGoroutines = 100

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
	mp.Start()
	defer mp.Close()

	var wg sync.WaitGroup
	var maxObserved atomic.Int32

	// Barrier to start all goroutines at roughly the same time
	startChan := make(chan struct{})

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startChan

			shortCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			conn, err := mp.Acquire(shortCtx, "user1")
			if err != nil {
				return
			}

			// Record the max observed connections
			current := mp.totalConns.Load()
			for {
				old := maxObserved.Load()
				if current <= old || maxObserved.CompareAndSwap(old, current) {
					break
				}
			}

			// Hold connection briefly
			time.Sleep(20 * time.Millisecond)
			conn.Release()
		}()
	}

	// Start all goroutines simultaneously
	close(startChan)
	wg.Wait()

	// Allow time for cleanup
	time.Sleep(200 * time.Millisecond)

	t.Logf("Max observed: %d, MaxConns: %d", maxObserved.Load(), maxConns)

	// After all operations complete and cleanup happens, connections should be at or below MaxConns
	finalConns := mp.totalConns.Load()
	assert.LessOrEqual(t, finalConns, int32(maxConns),
		"connections should settle to at most MaxConns after all operations complete")

	// Verify counters are consistent
	assert.GreaterOrEqual(t, mp.totalIdleConns.Load(), int32(0), "idle should not be negative")
	assert.Equal(t, int32(0), mp.pendingCreates.Load(), "pendingCreates should be 0 at rest")
	assert.Equal(t, int32(0), mp.pendingDestroys.Load(), "pendingDestroys should be 0 at rest")
}

// TestMultiPool_MaxConns_NoUnboundedGrowth is a stress test that verifies
// connections do not grow without bound under sustained concurrent load.
func TestMultiPool_MaxConns_NoUnboundedGrowth(t *testing.T) {
	ctx := context.Background()
	const maxConns = 50
	const numWorkers = 20
	const opsPerWorker = 50
	const maxAllowedOvershoot = 10 // Allow some temporary overshoot (20% of maxConns)

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
	addTestPoolUser(t, ctx, mp, "user2", maxConns)
	mp.Start()
	defer mp.Close()

	var wg sync.WaitGroup
	var maxObserved atomic.Int32
	var successCount atomic.Int32
	stopMonitor := make(chan struct{})

	// Monitor goroutine to track max connections over time
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopMonitor:
				return
			case <-ticker.C:
				current := mp.totalConns.Load()
				for {
					old := maxObserved.Load()
					if current <= old || maxObserved.CompareAndSwap(old, current) {
						break
					}
				}
			}
		}
	}()

	// Worker goroutines that continuously acquire and release
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			user := "user1"
			if workerID%2 == 0 {
				user = "user2"
			}

			for j := 0; j < opsPerWorker; j++ {
				shortCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				conn, err := mp.Acquire(shortCtx, user)
				cancel()

				if err != nil {
					continue
				}

				// Simulate some work
				time.Sleep(time.Duration(5+workerID%10) * time.Millisecond)
				conn.Release()
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()
	close(stopMonitor)
	<-monitorDone

	// Allow cleanup
	time.Sleep(200 * time.Millisecond)

	t.Logf("Stress test: %d successes, max observed: %d, MaxConns: %d",
		successCount.Load(), maxObserved.Load(), maxConns)

	// Key assertion: connections should never grow excessively beyond MaxConns
	assert.LessOrEqual(t, maxObserved.Load(), int32(maxConns+maxAllowedOvershoot),
		"connections should not grow significantly beyond MaxConns")

	// After workload completes, connections must settle to MaxConns or below
	finalConns := mp.totalConns.Load()
	assert.LessOrEqual(t, finalConns, int32(maxConns),
		"connections should settle to MaxConns after workload completes")

	// Verify counter consistency
	assert.GreaterOrEqual(t, mp.totalIdleConns.Load(), int32(0), "idle should not be negative")
	assert.LessOrEqual(t, mp.totalIdleConns.Load(), mp.totalConns.Load(), "idle should not exceed total")
}

// =============================================================================
// Production-Scale Tests (MaxConns=1000)
// =============================================================================

// TestMultiPool_ProductionScale_OvershootBound verifies that at production scale
// (MaxConns=1000), the overshoot is bounded when the pool is fully saturated.
// This test holds all connections simultaneously to force the pool to its limit.
func TestMultiPool_ProductionScale_OvershootBound(t *testing.T) {
	ctx := context.Background()
	const maxConns = 1000
	// At production scale, we expect overshoot to be a small percentage
	const maxAllowedOvershoot = 50 // 5% of 1000

	mp := newTestPool(t, maxConns)
	// Each pool needs enough capacity to hold ~1/3 of maxConns
	// Use 500 per pool to allow for imbalanced distribution
	addTestPoolUser(t, ctx, mp, "user1", 500)
	addTestPoolUser(t, ctx, mp, "user2", 500)
	addTestPoolUser(t, ctx, mp, "user3", 500)
	mp.Start()
	defer mp.Close()

	var wg sync.WaitGroup
	var maxObserved atomic.Int32
	var successCount atomic.Int32
	stopMonitor := make(chan struct{})

	// Monitor goroutine to track max connections over time
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopMonitor:
				return
			case <-ticker.C:
				current := mp.totalConns.Load()
				for {
					old := maxObserved.Load()
					if current <= old || maxObserved.CompareAndSwap(old, current) {
						break
					}
				}
			}
		}
	}()

	// First, saturate the pool by acquiring maxConns connections in parallel
	// Serial acquisition is too slow with 1000 real PostgreSQL connections
	users := []string{"user1", "user2", "user3"}
	holdConns := make([]*MultiPoolConn, maxConns)
	var holdMu sync.Mutex
	var holdCount atomic.Int32
	var satWg sync.WaitGroup

	satCtx, satCancel := context.WithTimeout(ctx, 60*time.Second)
	defer satCancel()

	for i := 0; i < int(maxConns); i++ {
		satWg.Add(1)
		go func(idx int) {
			defer satWg.Done()
			user := users[idx%len(users)]
			conn, err := mp.Acquire(satCtx, user)
			if err != nil {
				return
			}
			holdMu.Lock()
			holdConns[idx] = conn
			holdMu.Unlock()
			holdCount.Add(1)
		}(i)
	}
	satWg.Wait()

	t.Logf("Acquired %d connections to saturate pool", holdCount.Load())

	// Now try to acquire more while at the limit - this tests overshoot
	// We'll release connections and have goroutines race to acquire
	const numRacers = 200

	// Release all connections and immediately have racers try to acquire
	releaseStart := make(chan struct{})

	for i := 0; i < numRacers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-releaseStart

			user := users[workerID%len(users)]
			shortCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			conn, err := mp.Acquire(shortCtx, user)
			cancel()

			if err != nil {
				return
			}
			successCount.Add(1)
			time.Sleep(10 * time.Millisecond)
			conn.Release()
		}(i)
	}

	// Release held connections and start racers simultaneously
	close(releaseStart)
	for _, conn := range holdConns {
		if conn != nil {
			conn.Release()
		}
	}

	wg.Wait()
	close(stopMonitor)
	<-monitorDone

	// Allow cleanup
	time.Sleep(300 * time.Millisecond)

	overshoot := maxObserved.Load() - maxConns
	if overshoot < 0 {
		overshoot = 0
	}
	overshootPercent := float64(overshoot) / float64(maxConns) * 100

	t.Logf("Production scale test (MaxConns=%d):", maxConns)
	t.Logf("  Held connections: %d", holdCount.Load())
	t.Logf("  Racer successes: %d / %d", successCount.Load(), numRacers)
	t.Logf("  Max observed: %d", maxObserved.Load())
	t.Logf("  Overshoot: %d (%.2f%%)", overshoot, overshootPercent)

	// Should have held close to maxConns
	assert.GreaterOrEqual(t, int(holdCount.Load()), int(maxConns)-10, "should hold near maxConns connections")

	// Key assertion: overshoot should be bounded
	assert.LessOrEqual(t, maxObserved.Load(), int32(maxConns+maxAllowedOvershoot),
		"overshoot should be bounded to %d%% at production scale", maxAllowedOvershoot*100/maxConns)

	// After workload completes, connections must settle to MaxConns or below
	finalConns := mp.totalConns.Load()
	assert.LessOrEqual(t, finalConns, int32(maxConns),
		"connections should settle to MaxConns after workload completes")

	// Verify counter consistency
	assert.Equal(t, int32(0), mp.pendingCreates.Load(), "pendingCreates should be 0 at rest")
	assert.Equal(t, int32(0), mp.pendingDestroys.Load(), "pendingDestroys should be 0 at rest")
}

// TestMultiPool_ProductionScale_BurstRecovery tests that after a burst of activity
// at production scale, the pool properly recovers and settles to MaxConns.
func TestMultiPool_ProductionScale_BurstRecovery(t *testing.T) {
	ctx := context.Background()
	const maxConns = 1000
	const burstSize = 500 // Half of max, all at once

	mp := newTestPool(t, maxConns)
	// Single pool needs capacity for the full burst
	addTestPoolUser(t, ctx, mp, "user1", 1000)
	mp.Start()
	defer mp.Close()

	var wg sync.WaitGroup
	var maxDuringBurst atomic.Int32
	var successCount atomic.Int32

	// Create a burst of concurrent acquires
	for i := 0; i < burstSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			shortCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			conn, err := mp.Acquire(shortCtx, "user1")
			if err != nil {
				return
			}
			successCount.Add(1)

			current := mp.totalConns.Load()
			for {
				old := maxDuringBurst.Load()
				if current <= old || maxDuringBurst.CompareAndSwap(old, current) {
					break
				}
			}

			// Hold connection briefly
			time.Sleep(20 * time.Millisecond)
			conn.Release()
		}()
	}

	wg.Wait()

	t.Logf("Burst test (MaxConns=%d, burstSize=%d):", maxConns, burstSize)
	t.Logf("  Successes: %d", successCount.Load())
	t.Logf("  Max during burst: %d", maxDuringBurst.Load())

	// All burst requests should succeed (we're under MaxConns)
	assert.Equal(t, int32(burstSize), successCount.Load(), "all burst requests should succeed")

	// Max during burst should not exceed MaxConns significantly
	assert.LessOrEqual(t, maxDuringBurst.Load(), int32(maxConns),
		"burst should not exceed MaxConns when under limit")

	// After burst, wait for idle period cleanup
	time.Sleep(300 * time.Millisecond)

	finalConns := mp.totalConns.Load()
	t.Logf("  Final connections: %d", finalConns)

	// Connections should settle
	assert.LessOrEqual(t, finalConns, int32(maxConns),
		"connections should settle to MaxConns after burst")
}

// TestMultiPool_ProductionScale_CrossPoolContention tests cross-pool connection
// reclamation at production scale with high contention.
func TestMultiPool_ProductionScale_CrossPoolContention(t *testing.T) {
	ctx := context.Background()
	const maxConns = 1000
	const numPools = 10
	const numGoroutinesPerPool = 20
	const opsPerGoroutine = 25
	const maxAllowedOvershoot = 50

	mp := newTestPool(t, maxConns)
	for i := 0; i < numPools; i++ {
		// Each pool needs enough capacity for its goroutines plus headroom
		addTestPoolUser(t, ctx, mp, fmt.Sprintf("pool%d", i), 200)
	}
	mp.Start()
	defer mp.Close()

	var wg sync.WaitGroup
	var successCount atomic.Int32
	var maxObserved atomic.Int32
	stopMonitor := make(chan struct{})

	// Monitor goroutine
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopMonitor:
				return
			case <-ticker.C:
				current := mp.totalConns.Load()
				for {
					old := maxObserved.Load()
					if current <= old || maxObserved.CompareAndSwap(old, current) {
						break
					}
				}
			}
		}
	}()

	for poolID := 0; poolID < numPools; poolID++ {
		for g := 0; g < numGoroutinesPerPool; g++ {
			wg.Add(1)
			go func(poolName string) {
				defer wg.Done()

				for j := 0; j < opsPerGoroutine; j++ {
					shortCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
					conn, err := mp.Acquire(shortCtx, poolName)
					cancel()

					if err != nil {
						continue
					}

					// Simulate work
					time.Sleep(5 * time.Millisecond)
					conn.Release()
					successCount.Add(1)
				}
			}(fmt.Sprintf("pool%d", poolID))
		}
	}

	wg.Wait()
	close(stopMonitor)
	<-monitorDone
	time.Sleep(300 * time.Millisecond)

	totalOps := int32(numPools * numGoroutinesPerPool * opsPerGoroutine)
	successRate := float64(successCount.Load()) / float64(totalOps) * 100
	overshoot := maxObserved.Load() - maxConns

	t.Logf("Cross-pool contention (MaxConns=%d, pools=%d):", maxConns, numPools)
	t.Logf("  Successes: %d / %d (%.1f%%)", successCount.Load(), totalOps, successRate)
	t.Logf("  Max observed: %d", maxObserved.Load())
	t.Logf("  Overshoot: %d", overshoot)

	// Most operations should succeed
	assert.GreaterOrEqual(t, successRate, 50.0, "at least 50%% should succeed")

	// Overshoot should be bounded
	assert.LessOrEqual(t, maxObserved.Load(), int32(maxConns+maxAllowedOvershoot),
		"overshoot should be bounded")

	// Connections should settle
	assert.LessOrEqual(t, mp.totalConns.Load(), int32(maxConns),
		"connections should settle to MaxConns")
}

// TestMultiPool_PendingCounters verifies that pendingCreates and pendingDestroys
// counters are properly maintained and return to zero after operations complete.
func TestMultiPool_PendingCounters(t *testing.T) {
	ctx := context.Background()
	const maxConns = 50

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
	mp.Start()
	defer mp.Close()

	// Acquire all connections
	conns := make([]*MultiPoolConn, maxConns)
	for i := 0; i < maxConns; i++ {
		conn, err := mp.Acquire(ctx, "user1")
		require.NoError(t, err)
		conns[i] = conn
	}

	// Pending counters should be zero when all creates are complete
	assert.Equal(t, int32(0), mp.pendingCreates.Load(), "pendingCreates should be 0")
	assert.Equal(t, int32(0), mp.pendingDestroys.Load(), "pendingDestroys should be 0")

	// Mark all for destruction and release
	for _, conn := range conns {
		conn.ReleaseAndDestroy()
	}

	// Wait for destruction to complete
	time.Sleep(200 * time.Millisecond)

	// All counters should be back to zero
	assert.Equal(t, int32(0), mp.totalConns.Load(), "all connections should be destroyed")
	assert.Equal(t, int32(0), mp.totalIdleConns.Load(), "no idle connections")
	assert.Equal(t, int32(0), mp.pendingCreates.Load(), "pendingCreates should be 0 at rest")
	assert.Equal(t, int32(0), mp.pendingDestroys.Load(), "pendingDestroys should be 0 at rest")
}

// TestMultiPool_CrossPool_IdleReclamation verifies that when pool A has idle
// connections and pool B needs a connection at the MaxConns limit, pool A's
// idle connection is reclaimed to make room.
func TestMultiPool_CrossPool_IdleReclamation(t *testing.T) {
	ctx := context.Background()
	const maxConns = 50

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "userA", maxConns)
	addTestPoolUser(t, ctx, mp, "userB", maxConns)
	mp.Start()
	defer mp.Close()

	// UserA fills the pool with connections
	connsA := make([]*MultiPoolConn, maxConns)
	for i := 0; i < maxConns; i++ {
		conn, err := mp.Acquire(ctx, "userA")
		require.NoError(t, err)
		connsA[i] = conn
	}

	assert.Equal(t, int32(maxConns), mp.totalConns.Load())

	// Release userA's connections - they become idle
	for _, conn := range connsA {
		conn.Release()
	}
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, int32(maxConns), mp.totalConns.Load())
	assert.Equal(t, int32(maxConns), mp.totalIdleConns.Load())

	// UserB should be able to acquire by reclaiming userA's idle connections
	connsB := make([]*MultiPoolConn, 0, maxConns)
	for i := 0; i < maxConns; i++ {
		shortCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		conn, err := mp.Acquire(shortCtx, "userB")
		cancel()
		require.NoError(t, err, "userB should be able to acquire connection %d", i)
		connsB = append(connsB, conn)
	}

	// Allow time for destruction of userA's connections
	time.Sleep(100 * time.Millisecond)

	t.Logf("After userB acquired: totalConns=%d, totalIdleConns=%d",
		mp.totalConns.Load(), mp.totalIdleConns.Load())

	// All connections should now belong to userB, total should be at MaxConns
	assert.LessOrEqual(t, mp.totalConns.Load(), int32(maxConns+2),
		"total connections should be at or slightly above MaxConns during transition")

	// Release userB's connections
	for _, conn := range connsB {
		conn.Release()
	}
	time.Sleep(100 * time.Millisecond)

	// Eventually should settle to MaxConns or below
	assert.LessOrEqual(t, mp.totalConns.Load(), int32(maxConns),
		"connections should settle to MaxConns")
}

// TestMultiPool_CrossPool_ConcurrentReclamation tests concurrent acquisition
// from multiple pools when all are competing for the same limited slots.
// This is a stress test that verifies the pool doesn't grow unboundedly under
// high contention. Some failures are expected during extreme contention.
func TestMultiPool_CrossPool_ConcurrentReclamation(t *testing.T) {
	ctx := context.Background()
	const maxConns = 50
	const numPools = 5
	const numGoroutinesPerPool = 10
	const opsPerGoroutine = 10
	// Allow temporary overshoot during high contention (20% of maxConns)
	const maxAllowedOvershoot = 10

	mp := newTestPool(t, maxConns)
	for i := 0; i < numPools; i++ {
		addTestPoolUser(t, ctx, mp, fmt.Sprintf("pool%d", i), maxConns)
	}
	mp.Start()
	defer mp.Close()

	var wg sync.WaitGroup
	var successCount atomic.Int32
	var maxObserved atomic.Int32

	for poolID := 0; poolID < numPools; poolID++ {
		for g := 0; g < numGoroutinesPerPool; g++ {
			wg.Add(1)
			go func(poolName string) {
				defer wg.Done()

				for j := 0; j < opsPerGoroutine; j++ {
					shortCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
					conn, err := mp.Acquire(shortCtx, poolName)
					cancel()

					if err != nil {
						continue
					}

					// Track max observed
					current := mp.totalConns.Load()
					for {
						old := maxObserved.Load()
						if current <= old || maxObserved.CompareAndSwap(old, current) {
							break
						}
					}

					// Simulate work
					time.Sleep(10 * time.Millisecond)
					conn.Release()
					successCount.Add(1)
				}
			}(fmt.Sprintf("pool%d", poolID))
		}
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	t.Logf("Cross-pool concurrent: %d successes, max observed: %d, MaxConns: %d",
		successCount.Load(), maxObserved.Load(), maxConns)

	// Some operations will fail during high contention - that's expected
	// Just verify some progress was made
	totalOps := int32(numPools * numGoroutinesPerPool * opsPerGoroutine)
	successRate := float64(successCount.Load()) / float64(totalOps)
	assert.GreaterOrEqual(t, successRate, 0.3, "at least 30%% should succeed under high contention")

	// Key assertions: max observed should be bounded
	assert.LessOrEqual(t, maxObserved.Load(), int32(maxConns+maxAllowedOvershoot),
		"should not grow significantly beyond MaxConns")

	// Connections should settle to MaxConns
	assert.LessOrEqual(t, mp.totalConns.Load(), int32(maxConns),
		"connections should settle to MaxConns")
}

// TestMultiPool_AcquireFailure_CounterCleanup verifies that when a connection
// fails to be created, the pendingCreates counter is properly decremented.
func TestMultiPool_AcquireFailure_CounterCleanup(t *testing.T) {
	ctx := context.Background()
	const maxConns = 50

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
	mp.Start()
	defer mp.Close()

	// First, verify normal operation works
	conn, err := mp.Acquire(ctx, "user1")
	require.NoError(t, err)
	conn.Release()
	time.Sleep(20 * time.Millisecond)

	// Counters should be zero
	assert.Equal(t, int32(0), mp.pendingCreates.Load())
	assert.Equal(t, int32(0), mp.pendingDestroys.Load())

	// Try to acquire from a non-existent pool (should panic or error)
	// Instead, we'll use context cancellation to simulate failure
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel() // Cancel immediately

	_, err = mp.Acquire(canceledCtx, "user1")
	assert.Error(t, err)

	// Counters should still be zero or properly cleaned up
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(0), mp.pendingCreates.Load(), "pendingCreates should be cleaned up on failure")
}

// TestMultiPool_RapidAcquireRelease tests rapid cycles of acquire/release
// to verify no resource leaks occur.
//
// Note: With a single pool, when we're at maxConns but the connections are in
// use by the current pool (not idle in OTHER pools), acquireConnSlot will fail.
// This test validates that sequential acquire/release properly reuses connections.
func TestMultiPool_RapidAcquireRelease(t *testing.T) {
	ctx := context.Background()
	const maxConns = 50
	const iterations = 200

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
	mp.Start()
	defer mp.Close()

	var lastPID uint32
	for i := 0; i < iterations; i++ {
		conn, err := mp.Acquire(ctx, "user1")
		if err != nil {
			t.Logf("iteration %d FAILED: totalConns=%d, totalIdleConns=%d, pendingCreates=%d, pendingDestroys=%d, err=%v",
				i, mp.totalConns.Load(), mp.totalIdleConns.Load(),
				mp.pendingCreates.Load(), mp.pendingDestroys.Load(), err)
		}
		require.NoError(t, err, "iteration %d", i)

		// Track PIDs to detect connection reuse vs creation
		pid := conn.Value().Conn().PgConn().PID()
		if lastPID != 0 && pid != lastPID {
			t.Logf("iteration %d: DIFFERENT connection PID (was %d, now %d, totalConns=%d)",
				i, lastPID, pid, mp.totalConns.Load())
		}
		lastPID = pid

		conn.Release()
		// Small sleep to allow async operations to complete
		time.Sleep(5 * time.Millisecond)
	}

	// Allow cleanup
	time.Sleep(100 * time.Millisecond)

	// Verify counters are sane
	assert.LessOrEqual(t, mp.totalConns.Load(), int32(maxConns))
	assert.GreaterOrEqual(t, mp.totalIdleConns.Load(), int32(0))
	assert.LessOrEqual(t, mp.totalIdleConns.Load(), mp.totalConns.Load())
	assert.Equal(t, int32(0), mp.pendingCreates.Load())
	assert.Equal(t, int32(0), mp.pendingDestroys.Load())
}

// TestMultiPool_BurstThenIdle tests a burst of activity followed by idle period
// to verify connection cleanup works correctly.
func TestMultiPool_BurstThenIdle(t *testing.T) {
	ctx := context.Background()
	const maxConns = 50
	const burstSize = 100

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
	mp.Start()
	defer mp.Close()

	var wg sync.WaitGroup
	var maxDuringBurst atomic.Int32

	// Create a burst of concurrent acquires
	for i := 0; i < burstSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			shortCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()

			conn, err := mp.Acquire(shortCtx, "user1")
			if err != nil {
				return
			}

			current := mp.totalConns.Load()
			for {
				old := maxDuringBurst.Load()
				if current <= old || maxDuringBurst.CompareAndSwap(old, current) {
					break
				}
			}

			time.Sleep(50 * time.Millisecond)
			conn.Release()
		}()
	}

	wg.Wait()

	t.Logf("During burst: maxObserved=%d", maxDuringBurst.Load())

	// After burst, wait for idle period cleanup
	time.Sleep(500 * time.Millisecond)

	finalConns := mp.totalConns.Load()
	t.Logf("After idle: totalConns=%d", finalConns)

	// Connections should be at or below MaxConns
	assert.LessOrEqual(t, finalConns, int32(maxConns),
		"connections should not exceed MaxConns after settling")

	// Verify counter sanity
	assert.Equal(t, int32(0), mp.pendingCreates.Load())
	assert.Equal(t, int32(0), mp.pendingDestroys.Load())
}

// TestMultiPool_AllConnectionsInUse_NewUserBlocks verifies that when all
// connections are actively in use (not idle), a new user cannot acquire.
func TestMultiPool_AllConnectionsInUse_NewUserBlocks(t *testing.T) {
	ctx := context.Background()
	const maxConns = 2

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
	addTestPoolUser(t, ctx, mp, "user2", maxConns)
	mp.Start()
	defer mp.Close()

	// User1 acquires all connections and holds them (not idle)
	conns := make([]*MultiPoolConn, maxConns)
	for i := 0; i < maxConns; i++ {
		conn, err := mp.Acquire(ctx, "user1")
		require.NoError(t, err)
		conns[i] = conn
	}

	assert.Equal(t, int32(maxConns), mp.totalConns.Load())
	assert.Equal(t, int32(0), mp.totalIdleConns.Load(), "no idle connections")

	// User2 should block and timeout (no idle connections to reclaim)
	shortCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	_, err := mp.Acquire(shortCtx, "user2")
	assert.Error(t, err, "user2 should fail when all connections are in use")

	// Release user1's connections
	for _, c := range conns {
		c.Release()
	}
}

// TestMultiPool_ConnectionReuse verifies that released connections are properly
// reused rather than creating new ones.
func TestMultiPool_ConnectionReuse(t *testing.T) {
	ctx := context.Background()
	const maxConns = 2

	mp := newTestPool(t, maxConns)
	addTestPoolUser(t, ctx, mp, "user1", maxConns)
	mp.Start()
	defer mp.Close()

	// Acquire and release the same connection multiple times
	var lastPID uint32
	for i := 0; i < 10; i++ {
		conn, err := mp.Acquire(ctx, "user1")
		require.NoError(t, err)

		// Get the backend PID to verify same connection is reused
		pid := conn.Value().Conn().PgConn().PID()
		if lastPID != 0 {
			assert.Equal(t, lastPID, pid, "should reuse the same connection")
		}
		lastPID = pid

		conn.Release()
		time.Sleep(10 * time.Millisecond)
	}

	// Should still only have 1 connection
	assert.Equal(t, int32(1), mp.totalConns.Load())
}

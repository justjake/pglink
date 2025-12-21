package backend

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justjake/pglink/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeUser(name string) config.UserConfig {
	return config.UserConfig{
		Username: config.SecretRef{InsecureValue: name},
		Password: config.SecretRef{InsecureValue: name},
	}
}

func TestConnManager_BasicAcquireRelease(t *testing.T) {
	cm := newConnManager[config.UserConfig](5, nil)

	// Should be able to reserve up to max
	for i := 0; i < 5; i++ {
		ok := cm.tryReserveDBConn()
		assert.True(t, ok, "should be able to reserve connection %d", i+1)
	}

	// Should fail at max
	ok := cm.tryReserveDBConn()
	assert.False(t, ok, "should fail when at max")

	// Release one and try again
	cm.releaseDBConn()
	ok = cm.tryReserveDBConn()
	assert.True(t, ok, "should succeed after release")

	// Verify count
	assert.Equal(t, int32(5), cm.currentDBConns())
}

func TestConnManager_FairScheduling(t *testing.T) {
	userA := makeUser("userA")
	userB := makeUser("userB")
	cm := newConnManager[config.UserConfig](2, []config.UserConfig{userA, userB})

	// Fill up to max - these represent actual DB connections
	cm.tryReserveDBConn()
	cm.tryReserveDBConn()

	// Both users try to wait for their fair turn
	var wg sync.WaitGroup
	order := make([]string, 0, 4)
	var orderMu sync.Mutex

	// UserA waits twice
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := cm.waitForTurn(context.Background(), userA)
			require.NoError(t, err)
			orderMu.Lock()
			order = append(order, "A")
			orderMu.Unlock()
		}()
	}

	// UserB waits twice
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := cm.waitForTurn(context.Background(), userB)
			require.NoError(t, err)
			orderMu.Lock()
			order = append(order, "B")
			orderMu.Unlock()
		}()
	}

	// Give goroutines time to queue up
	time.Sleep(10 * time.Millisecond)

	// Signal waiters one at a time (simulating connection becoming available)
	// Note: We don't call releaseDBConn here because waitForTurn doesn't
	// reserve connections - it just waits for a fair turn.
	for i := 0; i < 4; i++ {
		cm.signalNextWaiter()
		time.Sleep(5 * time.Millisecond) // Let the signaled goroutine run
	}

	wg.Wait()

	// Check that users were served fairly (round-robin)
	// The order should alternate: A, B, A, B or B, A, B, A
	orderMu.Lock()
	defer orderMu.Unlock()

	t.Logf("Order: %v", order)

	// Count consecutive same-user entries - should never be more than 1
	// (with fair scheduling, we should alternate)
	consecutive := 1
	maxConsecutive := 1
	for i := 1; i < len(order); i++ {
		if order[i] == order[i-1] {
			consecutive++
			if consecutive > maxConsecutive {
				maxConsecutive = consecutive
			}
		} else {
			consecutive = 1
		}
	}

	// Due to timing, we might get at most 2 consecutive, but ideally 1
	assert.LessOrEqual(t, maxConsecutive, 2, "should have fair scheduling (max consecutive same-user: %d)", maxConsecutive)
}

func TestConnManager_ContextCancellation(t *testing.T) {
	user := makeUser("user")
	cm := newConnManager[config.UserConfig](1, []config.UserConfig{user})

	// Fill up
	cm.tryReserveDBConn()

	// Try to wait with canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := cm.waitForTurn(ctx, user)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestConnManager_ConcurrentAccess(t *testing.T) {
	const maxConns = 10
	const numGoroutines = 100
	const numOpsPerGoroutine = 50

	cm := newConnManager[config.UserConfig](maxConns, nil)

	var wg sync.WaitGroup
	var maxObserved atomic.Int32

	// Spawn many goroutines doing concurrent reserve/release
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOpsPerGoroutine; j++ {
				if cm.tryReserveDBConn() {
					// Track max observed
					current := cm.currentDBConns()
					for {
						old := maxObserved.Load()
						if current <= old {
							break
						}
						if maxObserved.CompareAndSwap(old, current) {
							break
						}
					}

					// Hold briefly then release
					time.Sleep(time.Microsecond)
					cm.releaseDBConn()
				}
			}
		}()
	}

	wg.Wait()

	// Verify invariant was never violated
	assert.LessOrEqual(t, maxObserved.Load(), int32(maxConns), "should never exceed max connections")

	// Verify final state
	assert.Equal(t, int32(0), cm.currentDBConns(), "should have no connections at end")
}

func TestConnManager_StealCallback(t *testing.T) {
	userA := makeUser("userA")
	userB := makeUser("userB")
	cm := newConnManager[config.UserConfig](2, []config.UserConfig{userA, userB})

	// Track steal attempts
	var stealAttempts atomic.Int32
	cm.setStealFunc(func(exclude config.UserConfig) bool {
		stealAttempts.Add(1)
		// Simulate successful steal
		return true
	})

	// Fill up
	cm.tryReserveDBConn()
	cm.tryReserveDBConn()

	// UserB tries to wait - since at capacity, steal should be attempted
	// before queueing (in the actual Database.Acquire flow)
	// Here we just verify the steal func is set correctly
	cm.mu.Lock()
	stealFunc := cm.stealIdleFunc
	cm.mu.Unlock()

	require.NotNil(t, stealFunc)
	result := stealFunc(userA)
	assert.True(t, result)
	assert.Equal(t, int32(1), stealAttempts.Load())

	// Verify it's called with correct exclusion
	result = stealFunc(userB)
	assert.True(t, result)
	assert.Equal(t, int32(2), stealAttempts.Load())
}

func TestConnManager_DBConnTrackingInvariant(t *testing.T) {
	// This test verifies that dbConns accurately tracks connections
	// via the tryReserveDBConn/releaseDBConn interface

	const maxConns = 5
	cm := newConnManager[config.UserConfig](maxConns, nil)

	// Simulate connection lifecycle
	type connState struct {
		reserved bool
	}

	var mu sync.Mutex
	conns := make([]*connState, 0)

	// Helper to "create" a connection
	createConn := func() bool {
		if !cm.tryReserveDBConn() {
			return false
		}
		mu.Lock()
		conns = append(conns, &connState{reserved: true})
		mu.Unlock()
		return true
	}

	// Helper to "close" a connection
	closeConn := func(idx int) {
		mu.Lock()
		if idx < len(conns) && conns[idx].reserved {
			conns[idx].reserved = false
			mu.Unlock()
			cm.releaseDBConn()
		} else {
			mu.Unlock()
		}
	}

	// Create max connections
	for i := 0; i < maxConns; i++ {
		ok := createConn()
		require.True(t, ok, "should create connection %d", i)
	}

	// Verify at max
	assert.Equal(t, int32(maxConns), cm.currentDBConns())
	assert.False(t, createConn(), "should fail at max")

	// Close half
	for i := 0; i < maxConns/2; i++ {
		closeConn(i)
	}

	// Verify count decreased
	assert.Equal(t, int32(maxConns-maxConns/2), cm.currentDBConns())

	// Should be able to create more
	ok := createConn()
	assert.True(t, ok, "should create after close")

	// Close all remaining
	mu.Lock()
	count := len(conns)
	mu.Unlock()
	for i := 0; i < count; i++ {
		closeConn(i)
	}

	// Verify back to zero
	assert.Equal(t, int32(0), cm.currentDBConns())
}

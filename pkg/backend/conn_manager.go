package backend

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ErrConnectionLimitReached is returned when the global connection limit is reached.
var ErrConnectionLimitReached = errors.New("backend connection limit reached")

// connManager coordinates connection acquisition across multiple user pools,
// enforcing a global connection limit with fair scheduling across users.
//
// Key properties:
// - Global max connections limit across all user pools (actual DB connections)
// - Fair scheduling: users take turns when waiting (round-robin)
// - Connection stealing: idle connections from one user can be reclaimed for another
//
// The manager tracks two things:
// 1. dbConns: actual database connections across all pools (via AfterConnect/BeforeClose)
// 2. Fair wait queues: ensures users take turns when at capacity
//
// The type parameter T is the user identifier type, which must be comparable
// for use as a map key.
type connManager[T comparable] struct {
	maxConns int32

	// Actual database connections across all pools.
	// Incremented in AfterConnect, decremented in BeforeClose.
	dbConns atomic.Int32

	mu sync.Mutex

	// Per-user wait queues for fair scheduling.
	// When at max capacity, requests are queued per-user and serviced round-robin.
	waiting map[T]*list.List // list of *connWaiter

	// Round-robin scheduling: fixed order of users provided at construction.
	// When waking, we skip users not currently in the waiting map.
	users   []T
	nextIdx int // index into users for round-robin

	// Callback to find and steal idle connections
	stealIdleFunc func(exclude T) bool
}

// connWaiter represents a goroutine waiting to acquire a connection slot.
type connWaiter struct {
	ready    chan struct{} // Closed when slot is granted
	canceled atomic.Bool   // Set to true if context was canceled
}

// newConnManager creates a new connection manager with the given global max connections.
// The users slice defines the set of valid users and their round-robin order.
// Using a user not in this slice will panic.
func newConnManager[T comparable](maxConns int32, users []T) *connManager[T] {
	return &connManager[T]{
		maxConns: maxConns,
		waiting:  make(map[T]*list.List),
		users:    users,
	}
}

// setStealFunc sets the callback used to steal idle connections from other pools.
// The callback should attempt to close an idle connection from any pool except
// the excluded user, returning true if successful.
func (cm *connManager[T]) setStealFunc(f func(exclude T) bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.stealIdleFunc = f
}

// tryReserveDBConn attempts to reserve a slot for a new database connection.
// Called from BeforeConnect callback. Returns true if under limit.
func (cm *connManager[T]) tryReserveDBConn() bool {
	for {
		current := cm.dbConns.Load()
		if current >= cm.maxConns {
			return false
		}
		if cm.dbConns.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

// releaseDBConn releases a database connection slot.
// Called from BeforeClose callback.
func (cm *connManager[T]) releaseDBConn() {
	newVal := cm.dbConns.Add(-1)
	if newVal < 0 {
		panic("connManager: dbConns went negative")
	}

	// Wake up the next waiter in round-robin order
	cm.signalNextWaiter()
}

// currentDBConns returns the current number of actual database connections.
func (cm *connManager[T]) currentDBConns() int32 {
	return cm.dbConns.Load()
}

// waitForTurn waits for this user's fair turn to attempt acquisition.
// This implements fair scheduling - users take turns in round-robin order.
// Returns nil when it's the user's turn, or ctx.Err() if canceled.
func (cm *connManager[T]) waitForTurn(ctx context.Context, user T) error {
	cm.mu.Lock()

	// Fast path: if under limit and no one waiting, go immediately
	if cm.dbConns.Load() < cm.maxConns && len(cm.waiting) == 0 {
		cm.mu.Unlock()
		return nil
	}

	// Need to wait for fair turn
	w := &connWaiter{
		ready: make(chan struct{}),
	}

	// Validate user is known
	if !cm.isValidUser(user) {
		panic("connManager: unknown user")
	}

	// Get or create wait list for this user
	waitList := cm.waiting[user]
	if waitList == nil {
		waitList = list.New()
		cm.waiting[user] = waitList
	}

	elem := waitList.PushBack(w)
	cm.mu.Unlock()

	// Wait for our turn or context cancellation
	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		// Mark as canceled and clean up
		w.canceled.Store(true)
		cm.removeWaiter(user, elem)
		return ctx.Err()
	}
}

// removeWaiter removes a waiter from the queue (used on cancellation).
func (cm *connManager[T]) removeWaiter(user T, elem *list.Element) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	waitList := cm.waiting[user]
	if waitList == nil {
		return
	}

	waitList.Remove(elem)

	// Clean up empty wait list from the map (but keep user in userOrder)
	if waitList.Len() == 0 {
		delete(cm.waiting, user)
	}
}

// signalNextWaiter wakes the next waiter using round-robin scheduling.
// Called when a connection slot becomes available.
func (cm *connManager[T]) signalNextWaiter() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.wakeNextWaiter()
}

// isValidUser checks if a user is in the known users list.
// Must be called with cm.mu held.
func (cm *connManager[T]) isValidUser(user T) bool {
	for _, u := range cm.users {
		if u == user {
			return true
		}
	}
	return false
}

// wakeNextWaiter wakes the next waiter using round-robin scheduling across users.
// Must be called with cm.mu held.
func (cm *connManager[T]) wakeNextWaiter() {
	if len(cm.users) == 0 {
		return
	}

	// Try each user in round-robin order, starting from nextIdx
	for i := 0; i < len(cm.users); i++ {
		idx := (cm.nextIdx + i) % len(cm.users)
		user := cm.users[idx]

		// Skip users with no pending waiters
		waitList := cm.waiting[user]
		if waitList == nil || waitList.Len() == 0 {
			continue
		}

		// Find the first non-canceled waiter
		for e := waitList.Front(); e != nil; {
			w := e.Value.(*connWaiter)
			next := e.Next()

			if w.canceled.Load() {
				// Remove canceled waiter
				waitList.Remove(e)
				e = next
				continue
			}

			// Found a valid waiter - wake it
			close(w.ready)
			waitList.Remove(e)

			// Clean up empty wait list
			if waitList.Len() == 0 {
				delete(cm.waiting, user)
			}

			// Advance round-robin to next user
			cm.nextIdx = (idx + 1) % len(cm.users)
			return
		}

		// All waiters for this user were canceled, clean up the empty list
		if waitList.Len() == 0 {
			delete(cm.waiting, user)
		}
	}
}

// stats returns current connection manager statistics.
func (cm *connManager[T]) stats() connManagerStats {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	stats := connManagerStats{
		MaxConns:     cm.maxConns,
		DBConns:      cm.dbConns.Load(),
		WaitingUsers: int32(len(cm.waiting)),
	}

	for _, waitList := range cm.waiting {
		stats.TotalWaiters += int32(waitList.Len())
	}

	return stats
}

// connManagerStats contains connection manager statistics.
type connManagerStats struct {
	MaxConns     int32
	DBConns      int32 // Actual database connections
	WaitingUsers int32
	TotalWaiters int32
}

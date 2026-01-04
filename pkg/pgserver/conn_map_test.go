package pgserver

import (
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnMap_AddGetRemove(t *testing.T) {
	cm := &ConnMap{}

	conn := &ClientConn{
		ProcessID: 123,
		SecretKey: 456,
		User:      "testuser",
		Database:  "testdb",
	}

	// Add
	key, err := cm.Add(conn)
	require.NoError(t, err)
	assert.Equal(t, pgwire.ProcessID(123), key.ProcessID)
	assert.Equal(t, pgwire.SecretKey(456), key.SecretKey)
	assert.Equal(t, 1, cm.Len())

	// Get
	got, ok := cm.Get(key)
	require.True(t, ok)
	assert.Same(t, conn, got)

	// Get non-existent
	_, ok = cm.Get(ConnKey{ProcessID: 999, SecretKey: 888})
	assert.False(t, ok)

	// Remove
	cm.Remove(key)
	assert.Equal(t, 0, cm.Len())

	// Get after remove
	_, ok = cm.Get(key)
	assert.False(t, ok)
}

func TestConnMap_DuplicateKey(t *testing.T) {
	cm := &ConnMap{}

	conn1 := &ClientConn{
		ProcessID: 100,
		SecretKey: 200,
	}
	conn2 := &ClientConn{
		ProcessID: 100,
		SecretKey: 200,
	}

	_, err := cm.Add(conn1)
	require.NoError(t, err)

	// Adding same key should fail
	_, err = cm.Add(conn2)
	require.ErrorIs(t, err, ErrConnAlreadyExists)
	assert.Equal(t, 1, cm.Len())

	// Different keys should work
	conn3 := &ClientConn{
		ProcessID: 100,
		SecretKey: 201, // different secret
	}
	_, err = cm.Add(conn3)
	require.NoError(t, err)
	assert.Equal(t, 2, cm.Len())
}

func TestConnMap_Iterator(t *testing.T) {
	cm := &ConnMap{}

	conns := []*ClientConn{
		{ProcessID: 1, SecretKey: 10, User: "user1"},
		{ProcessID: 2, SecretKey: 20, User: "user2"},
		{ProcessID: 3, SecretKey: 30, User: "user3"},
	}

	for _, conn := range conns {
		_, err := cm.Add(conn)
		require.NoError(t, err)
	}

	// Collect all via iterator
	collected := make(map[pgwire.ProcessID]*ClientConn)
	for key, conn := range cm.All() {
		collected[key.ProcessID] = conn
	}

	assert.Len(t, collected, 3)
	for _, conn := range conns {
		got, ok := collected[conn.ProcessID]
		require.True(t, ok)
		assert.Same(t, conn, got)
	}
}

func TestConnMap_IteratorEarlyBreak(t *testing.T) {
	cm := &ConnMap{}

	for i := 0; i < 10; i++ {
		_, err := cm.Add(&ClientConn{
			ProcessID: pgwire.ProcessID(i),
			SecretKey: pgwire.SecretKey(i * 10),
		})
		require.NoError(t, err)
	}

	// Break after first iteration
	count := 0
	for range cm.All() {
		count++
		break
	}
	assert.Equal(t, 1, count)

	// Map should still be intact
	assert.Equal(t, 10, cm.Len())
}

func TestConnMap_ConcurrentAccess(t *testing.T) {
	cm := &ConnMap{}
	const numGoroutines = 50
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			baseID := goroutineID * numOperations

			for i := 0; i < numOperations; i++ {
				conn := &ClientConn{
					ProcessID: pgwire.ProcessID(baseID + i),
					SecretKey: pgwire.SecretKey(baseID + i),
				}

				key, err := cm.Add(conn)
				if err != nil {
					// Duplicate - that's fine for this test
					continue
				}

				// Read it back
				got, ok := cm.Get(key)
				if ok {
					assert.Same(t, conn, got)
				}

				// Remove it
				cm.Remove(key)
			}
		}(g)
	}

	wg.Wait()
	// All should be removed
	assert.Equal(t, 0, cm.Len())
}

func TestCancelMessageConnKey(t *testing.T) {
	key := CancelMessageConnKey(&pgproto3.CancelRequest{
		ProcessID: 12345,
		SecretKey: 67890,
	})
	assert.Equal(t, pgwire.ProcessID(12345), key.ProcessID)
	assert.Equal(t, pgwire.SecretKey(67890), key.SecretKey)
}

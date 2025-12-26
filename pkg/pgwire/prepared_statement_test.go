package pgwire

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashQuery(t *testing.T) {
	// Same query should produce same hash
	h1 := HashQuery("SELECT 1")
	h2 := HashQuery("SELECT 1")
	assert.Equal(t, h1, h2)

	// Different queries should produce different hashes
	h3 := HashQuery("SELECT 2")
	assert.NotEqual(t, h1, h3)

	// Empty string should work
	h4 := HashQuery("")
	assert.NotEqual(t, uint64(0), h4) // FNV has a non-zero offset basis
}

func TestPreparedStatementCache_PutGet(t *testing.T) {
	cache := NewPreparedStatementCache(100)

	stmt := &PreparedStatement{
		Query:     "SELECT $1",
		QueryHash: HashQuery("SELECT $1"),
	}
	cache.Put(stmt)

	got, ok := cache.Get(stmt.QueryHash)
	require.True(t, ok)
	assert.Equal(t, stmt.Query, got.Query)
	assert.Equal(t, stmt.QueryHash, got.QueryHash)
}

func TestPreparedStatementCache_GetByQuery(t *testing.T) {
	cache := NewPreparedStatementCache(100)

	query := "SELECT * FROM users WHERE id = $1"
	stmt := &PreparedStatement{
		Query:     query,
		QueryHash: HashQuery(query),
	}
	cache.Put(stmt)

	got, ok := cache.GetByQuery(query)
	require.True(t, ok)
	assert.Equal(t, query, got.Query)
}

func TestPreparedStatementCache_GetMiss(t *testing.T) {
	cache := NewPreparedStatementCache(100)

	_, ok := cache.Get(HashQuery("nonexistent"))
	assert.False(t, ok)
}

func TestPreparedStatementCache_LRUEviction(t *testing.T) {
	cache := NewPreparedStatementCache(3) // Small cache

	// Add 3 statements
	for i := 0; i < 3; i++ {
		query := fmt.Sprintf("SELECT %d", i)
		cache.Put(&PreparedStatement{
			Query:     query,
			QueryHash: HashQuery(query),
		})
	}

	assert.Equal(t, 3, cache.Len())

	// All 3 should be present
	for i := 0; i < 3; i++ {
		query := fmt.Sprintf("SELECT %d", i)
		_, ok := cache.Get(HashQuery(query))
		assert.True(t, ok, "query %d should be present", i)
	}

	// Add 4th - should evict oldest (SELECT 0)
	cache.Put(&PreparedStatement{
		Query:     "SELECT 3",
		QueryHash: HashQuery("SELECT 3"),
	})

	assert.Equal(t, 3, cache.Len())

	_, ok := cache.Get(HashQuery("SELECT 0"))
	assert.False(t, ok, "SELECT 0 should have been evicted")

	_, ok = cache.Get(HashQuery("SELECT 3"))
	assert.True(t, ok, "SELECT 3 should be present")
}

func TestPreparedStatementCache_Touch(t *testing.T) {
	cache := NewPreparedStatementCache(2)

	// Add 2 statements
	cache.Put(&PreparedStatement{Query: "A", QueryHash: HashQuery("A")})
	cache.Put(&PreparedStatement{Query: "B", QueryHash: HashQuery("B")})

	// Touch A to make it recently used
	cache.Touch(HashQuery("A"))

	// Add C - should evict B (oldest), not A
	cache.Put(&PreparedStatement{Query: "C", QueryHash: HashQuery("C")})

	_, ok := cache.Get(HashQuery("A"))
	assert.True(t, ok, "A should still be present (was touched)")

	_, ok = cache.Get(HashQuery("B"))
	assert.False(t, ok, "B should have been evicted")

	_, ok = cache.Get(HashQuery("C"))
	assert.True(t, ok, "C should be present")
}

func TestPreparedStatementCache_TouchMiss(t *testing.T) {
	cache := NewPreparedStatementCache(100)

	// Touch nonexistent - should not panic
	assert.NotPanics(t, func() {
		cache.Touch(HashQuery("nonexistent"))
	})
}

func TestPreparedStatementCache_Update(t *testing.T) {
	cache := NewPreparedStatementCache(100)

	query := "SELECT 1"
	hash := HashQuery(query)

	// Add initial statement
	cache.Put(&PreparedStatement{
		Query:         query,
		QueryHash:     hash,
		ParameterOIDs: []uint32{1, 2, 3},
	})

	// Update with new data
	cache.Put(&PreparedStatement{
		Query:         query,
		QueryHash:     hash,
		ParameterOIDs: []uint32{4, 5, 6},
	})

	assert.Equal(t, 1, cache.Len()) // Should not duplicate

	got, ok := cache.Get(hash)
	require.True(t, ok)
	assert.Equal(t, []uint32{4, 5, 6}, got.ParameterOIDs)
}

func TestPreparedStatementCache_Delete(t *testing.T) {
	cache := NewPreparedStatementCache(100)

	query := "SELECT 1"
	hash := HashQuery(query)

	cache.Put(&PreparedStatement{Query: query, QueryHash: hash})
	assert.Equal(t, 1, cache.Len())

	cache.Delete(hash)
	assert.Equal(t, 0, cache.Len())

	_, ok := cache.Get(hash)
	assert.False(t, ok)
}

func TestPreparedStatementCache_DeleteMiss(t *testing.T) {
	cache := NewPreparedStatementCache(100)

	// Delete nonexistent - should not panic
	assert.NotPanics(t, func() {
		cache.Delete(HashQuery("nonexistent"))
	})
}

func TestPreparedStatementCache_Clear(t *testing.T) {
	cache := NewPreparedStatementCache(100)

	for i := 0; i < 10; i++ {
		query := fmt.Sprintf("SELECT %d", i)
		cache.Put(&PreparedStatement{Query: query, QueryHash: HashQuery(query)})
	}

	assert.Equal(t, 10, cache.Len())

	cache.Clear()

	assert.Equal(t, 0, cache.Len())
}

func TestPreparedStatementCache_UnlimitedSize(t *testing.T) {
	cache := NewPreparedStatementCache(0) // Unlimited

	// Add many statements
	for i := 0; i < 1000; i++ {
		query := fmt.Sprintf("SELECT %d", i)
		cache.Put(&PreparedStatement{Query: query, QueryHash: HashQuery(query)})
	}

	// All should be present
	assert.Equal(t, 1000, cache.Len())

	for i := 0; i < 1000; i++ {
		query := fmt.Sprintf("SELECT %d", i)
		_, ok := cache.Get(HashQuery(query))
		assert.True(t, ok)
	}
}

func TestPreparedStatementCache_Concurrent(t *testing.T) {
	cache := NewPreparedStatementCache(100)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				query := fmt.Sprintf("SELECT %d_%d", i, j)
				hash := HashQuery(query)
				cache.Put(&PreparedStatement{Query: query, QueryHash: hash})
				cache.Get(hash)
				cache.Touch(hash)
			}
		}(i)
	}
	wg.Wait()
	// Test passes if no race conditions or panics
}

func TestPreparedStatementCache_All(t *testing.T) {
	cache := NewPreparedStatementCache(100)

	queries := []string{"A", "B", "C"}
	for _, q := range queries {
		cache.Put(&PreparedStatement{Query: q, QueryHash: HashQuery(q)})
	}

	// Collect all via iterator
	found := make(map[string]bool)
	for _, stmt := range cache.All() {
		found[stmt.Query] = true
	}

	assert.Equal(t, len(queries), len(found))
	for _, q := range queries {
		assert.True(t, found[q], "should have found %s", q)
	}
}

func TestPreparedStatementCache_AllEmpty(t *testing.T) {
	cache := NewPreparedStatementCache(100)

	count := 0
	for range cache.All() {
		count++
	}

	assert.Equal(t, 0, count)
}

func TestPreparedStatementCache_WithRowDescription(t *testing.T) {
	cache := NewPreparedStatementCache(100)

	rowDesc := []byte{0x54, 0x00, 0x00, 0x00, 0x21} // Mock RowDescription wire format
	stmt := &PreparedStatement{
		Query:          "SELECT id, name FROM users",
		QueryHash:      HashQuery("SELECT id, name FROM users"),
		ParameterOIDs:  []uint32{23}, // int4
		RowDescription: rowDesc,
	}
	cache.Put(stmt)

	got, ok := cache.Get(stmt.QueryHash)
	require.True(t, ok)
	assert.Equal(t, rowDesc, got.RowDescription)
	assert.Equal(t, []uint32{23}, got.ParameterOIDs)
}

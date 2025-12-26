package pgwire

import (
	"container/list"
	"hash/fnv"
	"iter"
	"sync"
)

// PreparedStatement holds cached metadata for a prepared statement.
// This is stored in the database-wide cache and contains all information
// needed to re-create the statement on a backend connection.
type PreparedStatement struct {
	// Query is the SQL query text
	Query string

	// QueryHash is a hash of the query for fast comparison and lookup
	QueryHash uint64

	// ParameterOIDs are the OIDs of the query parameters from ParameterDescription
	ParameterOIDs []uint32

	// RowDescription is the wire-format RowDescription message (for faking Describe responses)
	// This is nil if the statement returns no rows (e.g., INSERT without RETURNING)
	RowDescription []byte
}

// HashQuery computes a hash of a query string for use as a cache key.
// Uses FNV-1a which is fast and has good distribution.
func HashQuery(query string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(query))
	return h.Sum64()
}

// PreparedStatementCache is a thread-safe LRU cache for prepared statement metadata.
// It stores statement definitions that can be used to re-create statements on
// backend connections that don't have them yet.
//
// The cache is keyed by query hash. Multiple statements with different names
// but the same query text share the same cache entry.
type PreparedStatementCache struct {
	mu sync.RWMutex

	// statements maps query hash to statement metadata
	statements map[uint64]*PreparedStatement

	// lru is a doubly-linked list for LRU ordering (front = most recent)
	lru *list.List

	// lruMap maps query hash to the list element for O(1) removal
	lruMap map[uint64]*list.Element

	// maxSize is the maximum number of statements to cache (0 = unlimited)
	maxSize int
}

// NewPreparedStatementCache creates a new cache with the given maximum size.
// If maxSize is 0, the cache is unlimited.
func NewPreparedStatementCache(maxSize int) *PreparedStatementCache {
	return &PreparedStatementCache{
		statements: make(map[uint64]*PreparedStatement),
		lru:        list.New(),
		lruMap:     make(map[uint64]*list.Element),
		maxSize:    maxSize,
	}
}

// Get retrieves a statement from the cache by query hash.
// Returns (statement, true) if found, (nil, false) if not.
// This does NOT update LRU order - call Touch() separately if needed.
func (c *PreparedStatementCache) Get(queryHash uint64) (*PreparedStatement, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stmt, ok := c.statements[queryHash]
	return stmt, ok
}

// GetByQuery retrieves a statement from the cache by query text.
// This is a convenience method that hashes the query first.
func (c *PreparedStatementCache) GetByQuery(query string) (*PreparedStatement, bool) {
	return c.Get(HashQuery(query))
}

// Put adds or updates a statement in the cache.
// If the cache is at capacity, the least recently used entry is evicted.
func (c *PreparedStatementCache) Put(stmt *PreparedStatement) {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash := stmt.QueryHash

	// If already exists, update and move to front
	if elem, ok := c.lruMap[hash]; ok {
		c.statements[hash] = stmt
		c.lru.MoveToFront(elem)
		return
	}

	// Evict if at capacity
	if c.maxSize > 0 && len(c.statements) >= c.maxSize {
		c.evictOldest()
	}

	// Add new entry
	c.statements[hash] = stmt
	elem := c.lru.PushFront(hash)
	c.lruMap[hash] = elem
}

// Touch updates the LRU order for a statement, marking it as recently used.
// Does nothing if the statement is not in the cache.
func (c *PreparedStatementCache) Touch(queryHash uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.lruMap[queryHash]; ok {
		c.lru.MoveToFront(elem)
	}
}

// Delete removes a statement from the cache.
// Does nothing if the statement is not in the cache.
func (c *PreparedStatementCache) Delete(queryHash uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.lruMap[queryHash]; ok {
		c.lru.Remove(elem)
		delete(c.lruMap, queryHash)
		delete(c.statements, queryHash)
	}
}

// Len returns the number of statements in the cache.
func (c *PreparedStatementCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.statements)
}

// Clear removes all statements from the cache.
func (c *PreparedStatementCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.statements = make(map[uint64]*PreparedStatement)
	c.lru.Init()
	c.lruMap = make(map[uint64]*list.Element)
}

// All returns an iterator over all statements in the cache.
// The iteration order is undefined.
func (c *PreparedStatementCache) All() iter.Seq2[uint64, *PreparedStatement] {
	return func(yield func(uint64, *PreparedStatement) bool) {
		c.mu.RLock()
		defer c.mu.RUnlock()

		for hash, stmt := range c.statements {
			if !yield(hash, stmt) {
				return
			}
		}
	}
}

// evictOldest removes the least recently used entry.
// Caller must hold the write lock.
func (c *PreparedStatementCache) evictOldest() {
	elem := c.lru.Back()
	if elem == nil {
		return
	}

	hash := elem.Value.(uint64)
	c.lru.Remove(elem)
	delete(c.lruMap, hash)
	delete(c.statements, hash)
}

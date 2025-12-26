# pglink Performance Analysis

## Summary

Benchmark testing reveals pglink is **2.5x slower** than pgbouncer for simple queries
(SELECT 1), with a **16% error rate** under high contention. This document analyzes
the root causes and proposes three solutions.

## Benchmark Results (50 concurrent connections, 20 seconds)

| Target | SELECT 1 ops/sec | Latency p50 | Error Rate |
|--------|------------------|-------------|------------|
| direct | 35,666 | 1.3ms | 0% |
| pgbouncer | 35,076 | 1.3ms | 0% |
| pglink | 14,365 | 3.1ms | 16% |

Error messages from pglink:
- `ERROR: current transaction is aborted, commands ignored until end of transaction block (SQLSTATE 25P02)`

## Diagnostic Findings

### 1. CPU Overhead is Minimal

Micro-benchmarks in `pkg/pgwire/benchmark_test.go` show the CPU work per SELECT 1 cycle
is approximately **180ns** with **4 allocations (96 bytes)**:

```
BenchmarkProtocolState_UpdateSimpleQuery-16     18.24 ns/op    16 B/op    1 allocs/op
BenchmarkClientMessageHandlers_AllocPerCall-16  15.89 ns/op    16 B/op    1 allocs/op
BenchmarkFullPath_SimpleQuery-16               181.3 ns/op    96 B/op    4 allocs/op
```

At 35,000 ops/sec, this represents only ~0.6% CPU utilization. **CPU overhead is NOT
the bottleneck.**

### 2. MultiPool Overhead is Minimal

Comparison tests in `pkg/backend/multi_pool_test.go` show MultiPool adds only ~1%
overhead compared to stock pgxpool.Pool:

- Stock pgxpool.Pool: 37,100 ops/sec
- MultiPool: 36,637 ops/sec

**MultiPool is NOT the bottleneck.**

### 3. The Real Issues

#### A. Error Rate Under Contention (Correctness Bug)

The 16% error rate indicates a **correctness bug**, not just a performance issue. The
`current transaction is aborted` error occurs when:

1. A query fails while in a transaction
2. Subsequent queries are sent without rolling back
3. PostgreSQL rejects them with SQLSTATE 25P02

This suggests pglink may be:
- Returning backend connections to the pool while in a failed transaction state
- Not properly resetting connection state before reuse
- Having race conditions in the polling loop under high contention

#### B. Architecture Overhead

The ring buffer architecture with dual cursors adds complexity:

1. **Goroutine per connection**: Each frontend/backend pair requires background
   goroutines for ring buffer filling
2. **Channel signaling**: Ready()/Done() channels add scheduling overhead
3. **Polling loop**: TryNextBatch() on both cursors every iteration even when only
   one side has data
4. **Memory copies**: io.Copy for each message batch

#### C. State Update Allocations

CPU profiling shows `ProtocolState.Update` consumes 26% of CPU time. The `wrapVoid`
helper allocates closures on every call:

```go
func wrapVoid[T any](fn func(T)) func(T) (struct{}, error) {
    return func(t T) (struct{}, error) {  // Allocates every call
        fn(t)
        return struct{}{}, nil
    }
}
```

## Root Cause Summary

1. **Primary**: Correctness bug causing 16% error rate under contention
2. **Secondary**: Architecture overhead from goroutines, channels, and polling
3. **Tertiary**: Per-call allocations in state update path

---

## Proposed Solutions

### Option 1: Fix Correctness Bug First (Recommended Starting Point)

**Approach**: Investigate and fix the transaction state corruption issue before
optimizing performance.

**Implementation**:
1. Add connection state validation before returning to pool
2. Send `ROLLBACK` if transaction is in failed state before release
3. Add debug logging to track state transitions
4. Create stress test specifically for this scenario

**Pros**:
- Addresses the immediate reliability issue
- May reveal other hidden bugs
- Low risk, focused change

**Cons**:
- May not fully close the performance gap
- Adds latency if ROLLBACK is frequently needed

**Estimated Impact**: Eliminate 16% error rate; may improve throughput by ~20%

---

### Option 2: Optimize Polling Loop (Direct Forwarding Mode)

**Approach**: Add a fast path for simple queries that bypasses the ring buffer
complexity.

**Implementation**:
1. Detect "simple forward" scenarios (single message, no rewriting needed)
2. Use direct syscall forwarding (splice/sendfile where available)
3. Skip ring buffer parsing for pass-through messages
4. Fall back to full ring buffer path for complex flows

**Pros**:
- Reduces latency for common case (80%+ of queries)
- Maintains full functionality for complex flows
- Can be incrementally adopted

**Cons**:
- Adds code complexity (two paths)
- May have edge cases that fall through
- Platform-specific optimizations (splice is Linux-only)

**Estimated Impact**: 30-50% latency reduction for simple queries

---

### Option 3: Replace Ring Buffer with Direct I/O (Major Refactor)

**Approach**: Replace the ring buffer architecture with direct I/O using a
single-threaded event loop per connection.

**Implementation**:
1. Remove background goroutines for ring buffer filling
2. Use non-blocking I/O with epoll/kqueue directly
3. Process messages inline in the event loop
4. Maintain zero-copy forwarding where possible

**Pros**:
- Eliminates goroutine scheduling overhead
- Reduces memory usage per connection
- Potentially matches pgbouncer's architecture

**Cons**:
- Major refactor requiring significant testing
- May need platform-specific code
- Risk of introducing new bugs

**Estimated Impact**: Potential to match pgbouncer performance (2.5x improvement)

---

## Recommendation

**Start with Option 1** to fix the correctness bug. A proxy with 16% error rate
is not production-ready regardless of performance.

After fixing correctness:
- If performance is within 30% of pgbouncer: Ship it
- If still significantly slower: Implement Option 2's fast path

Option 3 should only be considered if Options 1+2 don't close the gap sufficiently,
as it carries the highest risk and cost.

## Files Modified in This Analysis

- `pkg/pgwire/benchmark_test.go` - CPU overhead benchmarks
- `pkg/backend/multi_pool_test.go` - Pool overhead benchmarks (existing, added comparison)
- `cmd/bench/main.go` - Added simple query mode support
- `bin/bench-simple` - Script to run simple query benchmarks

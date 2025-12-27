# pglink Benchmarks

Generated: 2025-12-26T21:53:58-05:00

## Configuration

- **Rounds per target:** 2 (shuffled order to mitigate ordering effects)
- **Duration per round:** 10s measurement + 3s warmup
- **Pool:** max_conns=100, concurrency=100
- **Query mode:** extended protocol
- **Workload seed:** -4421386466632173043 (for reproducibility)

## Results

### SELECT 1 Latency

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 47523 | - | 0% | 1.9ms | 4.7ms |
| pgbouncer | 45797 (4% slower) | - | 0% | 2.0ms (+5%) | 4.6ms |
| pglink (GOMAXPROCS=16) | 20513 (2.3x slower) | - | 0% | 4.8ms (2.5x) | 7.4ms (+57%) |

### Mixed Workload

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 29346 | - | 0% | 67.1ms | 104.4ms |
| pgbouncer | 5188 (5.7x slower) | - | 0% | 362.8ms (5.4x) | 743.5ms (7.1x) |
| pglink (GOMAXPROCS=16) | 17250 (41% slower) | - | 0% | 113.1ms (+69%) | 172.2ms (+65%) |

### COPY OUT (1000 rows × 5 per task)

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 11325 | 180.1 | 0% | 43.6ms | 61.2ms |
| pgbouncer | 11282 | 179.5 | 0% | 44.1ms | 56.7ms |
| pglink (GOMAXPROCS=16) | 10097 (11% slower) | 160.6 (11% slower) | <0.01% | 48.8ms (+12%) | 69.7ms (+14%) |

### COPY IN (1000 rows × 5 per task)

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 3581 | 57.0 | 0% | 133.2ms | 305.1ms |
| pgbouncer | 3427 (4% slower) | 54.5 (4% slower) | 1.17% | 133.6ms | 440.4ms (+44%) |
| pglink (GOMAXPROCS=16) | 1376 (2.6x slower) | 21.9 (2.6x slower) | **94.4%** | 11.1ms | 188.8ms |

## Analysis

High error rates typically indicate pool exhaustion (concurrency > max_conns) or backend overload.


### Sample Errors


- `ERROR: canceling statement due to user request (SQLSTATE 57014)`

- `ERROR: relation "bench_copy_temp" does not exist (SQLSTATE 42P01)`

- `unexpected EOF`



## Test Environment

- Go version: go1.25.5 X:jsonv2
- OS: darwin/arm64
- CPUs: 16

# pglink Benchmarks

Generated: 2025-12-26T22:42:50-05:00

## Configuration

- **Rounds per target:** 1 (shuffled order to mitigate ordering effects)
- **Duration per round:** 5s measurement + 2s warmup
- **Pool:** max_conns=100, concurrency=100
- **Query mode:** extended protocol
- **Workload seed:** -4421386466632173043 (for reproducibility)

## Results

### SELECT 1 Latency

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 48687 | - | 0% | 1.9ms | 4.1ms |
| pgbouncer | 43516 (11% slower) | - | 0% | 2.1ms (+10%) | 5.4ms (+31%) |
| pglink (GOMAXPROCS=16) | 20077 (2.4x slower) | - | <0.01% | 4.9ms (2.5x) | 7.6ms (+85%) |

### Mixed Workload

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 28355 | - | 0% | 69.7ms | 106.5ms |
| pgbouncer | 4991 (5.7x slower) | - | 0% | 357.6ms (5.1x) | 876.6ms (8.2x) |
| pglink (GOMAXPROCS=16) | 17620 (38% slower) | - | 0.05% | 109.2ms (+57%) | 170.9ms (+60%) |

### COPY OUT (1000 rows × 5 per task)

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 11308 | 179.9 | 0% | 43.3ms | 66.9ms |
| pgbouncer | 11192 | 178.0 | 0% | 44.1ms | 62.7ms |
| pglink (GOMAXPROCS=16) | 10150 (10% slower) | 161.5 (10% slower) | 0% | 48.4ms (+12%) | 68.7ms (+3%) |

### COPY IN (1000 rows × 5 per task)

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 3595 | 57.2 | 0% | 132.8ms | 271.5ms |
| pgbouncer | 3430 (5% slower) | 54.6 (5% slower) | 2.07% | 133.6ms | 318.8ms (+17%) |
| pglink (GOMAXPROCS=16) | 3358 (7% slower) | 53.4 (7% slower) | 0.68% | 142.5ms (+7%) | 233.0ms |

## Analysis

High error rates typically indicate pool exhaustion (concurrency > max_conns) or backend overload.


### Sample Errors


- `ERROR: canceling statement due to user request (SQLSTATE 57014)`

- `ERROR: relation "bench_copy_temp" does not exist (SQLSTATE 42P01)`



## Test Environment

- Go version: go1.25.5 X:jsonv2
- OS: darwin/arm64
- CPUs: 16

# pglink Benchmarks

Generated: 2025-12-26T15:54:34-05:00

## Configuration

- **Rounds per target:** 2 (shuffled order to mitigate ordering effects)
- **Duration per round:** 10s measurement + 3s warmup
- **Pool:** max_conns=50, concurrency=50
- **Query mode:** simple protocol
- **Workload seed:** -4421386466632173043 (for reproducibility)

## Results

### SELECT 1 Latency

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 35667 | - | 0% | 1.3ms | 3.1ms |
| pgbouncer | 35077 | - | 0% | 1.3ms | 3.2ms |
| pglink (GOMAXPROCS=16) | 14366 (2.5x slower) | - | 15.96% | 3.1ms (2.4x) | 12.0ms (3.8x) |

### COPY OUT (1000 rows × 5 per task)

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 10619 | 168.9 | 0% | 22.9ms | 39.2ms |
| pgbouncer | 10756 | 171.1 | 0% | 23.0ms | 31.4ms |
| pglink (GOMAXPROCS=16) | 9787 (8% slower) | 155.7 (8% slower) | 0% | 19.9ms | 31.4ms |

### COPY IN (1000 rows × 5 per task)

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 2822 | 44.9 | 0% | 81.5ms | 285.5ms |
| pgbouncer | 2964 | 47.1 | 0.64% | 78.5ms | 213.6ms |
| pglink (GOMAXPROCS=16) | 2815 | 44.8 | 14.82% | 80.6ms | 181.4ms |

## Analysis

High error rates typically indicate pool exhaustion (concurrency > max_conns) or backend overload.


### Sample Errors


- `ERROR: current transaction is aborted, commands ignored until end of transaction block (SQLSTATE 25P02)`

- `ERROR: relation "bench_copy_temp" does not exist (SQLSTATE 42P01)`

- `ERROR: canceling statement due to user request (SQLSTATE 57014)`



## Test Environment

- Go version: go1.25.5 X:jsonv2
- OS: darwin/arm64
- CPUs: 16

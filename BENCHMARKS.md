# pglink Benchmarks

Generated: 2025-12-24T15:44:18-05:00

## Configuration

- **Rounds per target:** 1 (shuffled order to mitigate ordering effects)
- **Duration per round:** 5s measurement + 1s warmup
- **Pool:** max_conns=10, concurrency=10
- **Workload seed:** -4421388665655429465 (for reproducibility)

## Results

### Mixed Workload

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 11760 | - | 0% | 17.1ms | 25.5ms |
| pgbouncer | 5232 (2.2x slower) | - | 0% | 36.7ms (2.1x) | 64.8ms (2.5x) |
| pglink (GOMAXPROCS=16) | 0 ❌ | - | **100.0%** | 1.00s (58.5x) | 1.01s (39.5x) |

### COPY OUT (1000 rows × 5 per task)

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 9041 | 143.8 | 0% | 5.3ms | 9.0ms |
| pgbouncer | 8439 (7% slower) | 134.2 (7% slower) | 0% | 5.8ms (+8%) | 8.5ms |
| pglink (GOMAXPROCS=16) | 53 (170.6x slower) | 0.8 (170.6x slower) | 0% | 853.9ms (160.0x) | 2.41s (268.4x) |

### COPY IN (1000 rows × 5 per task)

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 1937 | 30.8 | 0% | 24.9ms | 45.3ms |
| pgbouncer | 1856 (4% slower) | 29.5 (4% slower) | 0.27% | 26.0ms (+5%) | 42.3ms |
| pglink (GOMAXPROCS=16) | 1658 (14% slower) | 26.4 (14% slower) | 0% | 29.8ms (+20%) | 37.8ms |

## Analysis

High error rates typically indicate pool exhaustion (concurrency > max_conns) or backend overload.


### Sample Errors


- `FATAL: failed to acquire backend (SQLSTATE 57P03)`

- `ERROR: relation "bench_copy_temp" does not exist (SQLSTATE 42P01)`



## Test Environment

- Go version: go1.25.5 X:jsonv2
- OS: darwin/arm64
- CPUs: 16

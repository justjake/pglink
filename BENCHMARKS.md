# pglink Benchmarks

Generated: 2025-12-27T00:34:59-05:00

## Configuration

- **Rounds per target:** 1 (shuffled order to mitigate ordering effects)
- **Duration per round:** 5s measurement + 2s warmup
- **Pool:** max_conns=100, concurrency=100
- **Query mode:** extended protocol
- **Workload seed:** -4421387566143801254 (for reproducibility)

## Results

### COPY IN (1000 rows × 5 per task)

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 3635 | 57.8 | 0% | 132.0ms | 244.7ms |
| pgbouncer | 3450 (5% slower) | 54.9 (5% slower) | 2.23% | 133.6ms | 338.5ms (+38%) |
| pglink (GOMAXPROCS=16) | 3343 (8% slower) | 53.2 (8% slower) | 0% | 142.9ms (+8%) | 266.2ms (+9%) |

## Analysis

High error rates typically indicate pool exhaustion (concurrency > max_conns) or backend overload.


### Sample Errors


- `ERROR: relation "bench_copy_temp" does not exist (SQLSTATE 42P01)`



## Test Environment

- Go version: go1.25.5 X:jsonv2
- OS: darwin/arm64
- CPUs: 16

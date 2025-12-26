# pglink Benchmarks

Generated: 2025-12-26T17:06:14-05:00

## Configuration

- **Rounds per target:** 2 (shuffled order to mitigate ordering effects)
- **Duration per round:** 10s measurement + 3s warmup
- **Pool:** max_conns=100, concurrency=100
- **Query mode:** simple protocol
- **Workload seed:** -4421386466632173043 (for reproducibility)

## Results

### SELECT 1 Latency

| Target | QPS | MB/s | Error Rate | P50 (μs) | P99 (μs) |
|--------|-----|------|------------|----------|----------|

| direct | 48229 | - | 0% | 2.0ms | 4.1ms |
| pgbouncer | 44604 (8% slower) | - | 0% | 2.1ms (+7%) | 4.7ms (+16%) |
| pglink (GOMAXPROCS=16) | 33075 (31% slower) | - | <0.01% | 2.9ms (+48%) | 5.3ms (+31%) |

## Analysis

High error rates typically indicate pool exhaustion (concurrency > max_conns) or backend overload.


### Sample Errors


- `ERROR: canceling statement due to user request (SQLSTATE 57014)`



## Test Environment

- Go version: go1.25.5 X:jsonv2
- OS: darwin/arm64
- CPUs: 16

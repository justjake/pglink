# pglink Benchmarks

Generated: 2025-12-26T17:40:44-05:00

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

| direct | 46425 | - | 0% | 2.0ms | 4.4ms |
| pgbouncer | 44778 (4% slower) | - | 0% | 2.1ms (+4%) | 4.4ms |
| pglink (GOMAXPROCS=16) | 31928 (31% slower) | - | <0.01% | 3.0ms (+47%) | 5.9ms (+33%) |

## Analysis

High error rates typically indicate pool exhaustion (concurrency > max_conns) or backend overload.


### Sample Errors


- `ERROR: canceling statement due to user request (SQLSTATE 57014)`



## Test Environment

- Go version: go1.25.5 X:jsonv2
- OS: darwin/arm64
- CPUs: 16

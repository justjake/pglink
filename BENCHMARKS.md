# pglink Benchmarks

Generated: 2025-12-23T16:24:31-05:00

## Configuration

- **Duration per test:** 2s
- **Warmup:** 1s
- **Max connections tested:** [100]
- **Concurrency levels:** [5]
- **GOMAXPROCS (pglink):** [4]

## Results

### SELECT 1 (max_conns=100, concurrency=5)

| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (us) | P99 (us) |
|--------|-----------|-------------|-----|------------|----------|----------|
| direct | 100 | 5 | 5455 | 0.00% | 779 | 1973 |
| pgbouncer | 100 | 5 | 4910 | 0.00% | 884 | 2111 |
| pglink (GOMAXPROCS=4) | 100 | 5 | 3336 | 0.00% | 1371 | 2655 |


### SELECT 100 Rows (max_conns=100, concurrency=5)

| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (us) | P99 (us) |
|--------|-----------|-------------|-----|------------|----------|----------|
| direct | 100 | 5 | 4759 | 0.00% | 879 | 2235 |
| pgbouncer | 100 | 5 | 4704 | 0.00% | 922 | 2150 |
| pglink (GOMAXPROCS=4) | 100 | 5 | 1424 | 0.00% | 3399 | 4844 |


### Transaction (max_conns=100, concurrency=5)

| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (us) | P99 (us) |
|--------|-----------|-------------|-----|------------|----------|----------|
| direct | 100 | 5 | 2741 | 0.00% | 1637 | 3270 |
| pgbouncer | 100 | 5 | 2358 | 0.00% | 1909 | 4162 |
| pglink (GOMAXPROCS=4) | 100 | 5 | 1868 | 0.00% | 2549 | 5062 |


### COPY OUT pgx (1000 rows, max_conns=100, concurrency=5)

| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (us) | P99 (us) |
|--------|-----------|-------------|-----|------------|----------|----------|
| direct | 100 | 5 | 6073 | 0.00% | 723 | 1604 |
| pgbouncer | 100 | 5 | 5275 | 0.00% | 875 | 1981 |
| pglink (GOMAXPROCS=4) | 100 | 5 | 209 | 0.00% | 23537 | 36518 |


### COPY IN pgx (1000 rows, max_conns=100, concurrency=5)

| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (us) | P99 (us) |
|--------|-----------|-------------|-----|------------|----------|----------|
| direct | 100 | 5 | 1667 | 0.00% | 2849 | 6184 |
| pgbouncer | 100 | 5 | 1504 | 0.00% | 3108 | 7950 |
| pglink (GOMAXPROCS=4) | 100 | 5 | 0 | 0.00% | 0 | 0 |


### COPY OUT psql (1000 rows, concurrency=5)

| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (us) | P99 (us) |
|--------|-----------|-------------|-----|------------|----------|----------|
| direct (psql) | - | 5 | 145 | 0.00% | 33283 | 49897 |
| pgbouncer (psql) | - | 5 | 190 | 0.00% | 25869 | 36022 |
| pglink (psql, GOMAXPROCS=4) | - | 5 | 111 | 0.00% | 44383 | 58213 |


### COPY IN psql (1000 rows, concurrency=5)

| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (us) | P99 (us) |
|--------|-----------|-------------|-----|------------|----------|----------|
| direct (psql) | - | 5 | 133 | 56.00% | 35742 | 53431 |


## Analysis

### Error Rates Under Load

High error rates typically indicate:
- Pool exhaustion (concurrency > max_conns)
- Connection timeouts
- Backend overload


### Sample Errors


- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 24815 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24814.
Process 24814 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24815.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 24817 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24814.
Process 24814 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24817.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 24816 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24814.
Process 24814 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24816.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 24821 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24819.
Process 24819 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24821.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 24822 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24819.
Process 24819 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24822.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 24820 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24819.
Process 24819 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24820.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 24826 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24825.
Process 24825 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24826.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 24827 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24828.
Process 24828 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24827.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 24831 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24829.
Process 24829 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24831.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 24830 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24829.
Process 24829 waits for AccessExclusiveLock on relation 20027 of database 16387; blocked by process 24830.
HINT:  See server log for query details.
`



## Test Environment

- Go version: go1.25.5 X:jsonv2
- OS: darwin/arm64
- CPUs: 16

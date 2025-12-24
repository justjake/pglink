# pglink Benchmarks

Generated: 2025-12-23T23:53:05-05:00

## Configuration

- **Duration per test:** 5s
- **Warmup:** 2s
- **Max connections tested:** [100]
- **Concurrency levels:** [100]
- **GOMAXPROCS (pglink):** [8]

## Results

### SELECT 1 (max_conns=100, concurrency=100)

| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (us) | P99 (us) |
|--------|-----------|-------------|-----|------------|----------|----------|
| direct | 100 | 100 | 22435 | 0.00% | 4259 | 7720 |
| pgbouncer | 100 | 100 | 22629 | 0.00% | 4214 | 8068 |
| pglink (GOMAXPROCS=8) | 100 | 100 | 8965 | 0.00% | 10959 | 15443 |


### SELECT 100 Rows (max_conns=100, concurrency=100)

| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (us) | P99 (us) |
|--------|-----------|-------------|-----|------------|----------|----------|
| direct | 100 | 100 | 21379 | 0.00% | 4374 | 9387 |
| pgbouncer | 100 | 100 | 21294 | 0.00% | 4485 | 8501 |
| pglink (GOMAXPROCS=8) | 100 | 100 | 1888 | 0.00% | 53348 | 69799 |


### Transaction (max_conns=100, concurrency=100)

| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (us) | P99 (us) |
|--------|-----------|-------------|-----|------------|----------|----------|
| direct | 100 | 100 | 11551 | 0.00% | 8310 | 14176 |
| pgbouncer | 100 | 100 | 10371 | 0.00% | 8777 | 23130 |
| pglink (GOMAXPROCS=8) | 100 | 100 | 4552 | 0.00% | 21502 | 28585 |


### COPY OUT pgx (1000 rows, max_conns=100, concurrency=100)

| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (us) | P99 (us) |
|--------|-----------|-------------|-----|------------|----------|----------|
| direct | 100 | 100 | 11171 | 0.00% | 9115 | 15242 |
| pgbouncer | 100 | 100 | 10785 | 0.00% | 9554 | 16203 |
| pglink (GOMAXPROCS=8) | 100 | 100 | 168 | 0.36% | 391322 | 2148166 |


### COPY IN pgx (1000 rows, max_conns=100, concurrency=100)

| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (us) | P99 (us) |
|--------|-----------|-------------|-----|------------|----------|----------|
| direct | 100 | 100 | 2690 | 0.00% | 35982 | 57893 |
| pgbouncer | 100 | 100 | 2481 | 0.00% | 37500 | 84341 |
| pglink (GOMAXPROCS=8) | 100 | 100 | 748 | 0.03% | 132451 | 145973 |


### COPY OUT psql (1000 rows, concurrency=100)

| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (us) | P99 (us) |
|--------|-----------|-------------|-----|------------|----------|----------|
| direct (psql) | - | 100 | 201 | 0.00% | 379749 | 1515610 |
| pgbouncer (psql) | - | 100 | 232 | 0.00% | 334006 | 1380997 |
| pglink (psql, GOMAXPROCS=8) | - | 100 | 303 | 100.00% | 227733 | 1242341 |


### COPY IN psql (1000 rows, concurrency=100)

| Target | Max Conns | Concurrency | QPS | Error Rate | P50 (us) | P99 (us) |
|--------|-----------|-------------|-----|------------|----------|----------|
| direct (psql) | - | 100 | 172 | 61.97% | 421432 | 1532974 |
| pgbouncer (psql) | - | 100 | 202 | 55.94% | 366750 | 1680342 |
| pglink (psql, GOMAXPROCS=8) | - | 100 | 1 | 100.00% | 1036326 | 1655037 |


## Analysis

### Error Rates Under Load

High error rates typically indicate:
- Pool exhaustion (concurrency > max_conns)
- Connection timeouts
- Backend overload


### Sample Errors


- `ERROR: canceling statement due to user request (SQLSTATE 57014)`

- `FATAL: failed to acquire backend (SQLSTATE 57P03)`

- `psql copy out failed: exit status 2: psql: error: connection to server at "localhost" (::1), port 16432 failed: Connection refused
	Is the server running on that host and accepting TCP/IP connections?
connection to server at "localhost" (127.0.0.1), port 16432 failed: Connection refused
	Is the server running on that host and accepting TCP/IP connections?
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 1260 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1261.
Process 1261 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1260.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 1263 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1262.
Process 1262 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1263.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 1267 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1266.
Process 1266 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1267.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 1277 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1276.
Process 1276 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1277.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 1280 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1278.
Process 1278 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1280.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 1281 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1279.
Process 1279 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1281.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 1284 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1286.
Process 1286 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1284.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 1288 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1290.
Process 1290 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1288.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 1282 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1290.
Process 1290 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1282.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 1287 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1289.
Process 1289 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 1287.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 2171 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 98555.
Process 98555 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 2171.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 98555 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 2174.
Process 2174 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 98555.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 2174 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 98555.
Process 98555 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 2174.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 772 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 2171.
Process 2171 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 772.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 2175 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 2171.
Process 2171 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 2175.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 2174 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 2171.
Process 2171 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 2174.
HINT:  See server log for query details.
`

- `psql copy in failed: exit status 1: COPY 1000
ERROR:  deadlock detected
DETAIL:  Process 98555 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 2171.
Process 2171 waits for AccessExclusiveLock on relation 25078 of database 16387; blocked by process 98555.
HINT:  See server log for query details.
`



## Test Environment

- Go version: go1.25.5 X:jsonv2
- OS: darwin/arm64
- CPUs: 16

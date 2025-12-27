# pglink

```
                  __ _         __   
    ____   ____ _/ /(_)____   / /__ 
   / __ \ / __ '/ // // __ \ / //_/ 
  / /_/ // /_/ / // // / / // ,<    
 / .___/ \__, /_//_//_/ /_//_/|_|   
/_/     /____/                      
```

PostgreSQL wire protocol proxy with transaction-based connection pooling.

pglink supports SCRAM-SHA-256 authentication with TLS channel binding (SCRAM-SHA-256-PLUS). Plaintext authentication is suppoted only with TLS connections. MD5-hashed password authentication is supported but strongly discouraged.

## Performance

Results from arbitrary and ill-considered benchmarks:

| Benchmark | direct         | pgbouncer            | pglink               |
|-----------|----------------|----------------------|----------------------|
| SELECT 1  | 48,687 qps     | 43,516 qps           | 20,077 qps           |
| Mixed     | 28,355 qps     | 4,991 qps            | 17,620 qps           |
| COPY OUT  | 179.9 MB/s     | 178.0 MB/s           | 161.5 MB/s           |
| COPY IN   | 57.2 MB/s (0%) | 54.6 MB/s (2.1% err) | 53.4 MB/s (0.7% err) |

## Installation

```bash
go install github.com/justjake/pglink/cmd/pglink@latest
```

Or build from source:

```bash
git clone https://github.com/justjake/pglink
cd pglink
bin/build
```

## Usage

```
pglink -config /etc/pglink/pglink.json
pglink -config pglink.json -json
```

### Command Line Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-config` | string | *required* | Path to pglink.json config file (required) |
| `-json` | bool | `false` | Output logs in JSON format |
| `-help` | bool | `false` | Show full documentation |

## Configuration Reference

pglink is configured via a JSON file (`pglink.json`).

### Example

```json
{
  "listen": [":5432"],
  "auth_method": "scram-sha-256",
  "max_client_connections": 1000,
  "tls": {
    "sslmode": "prefer",
    "generate_cert": true
  },
  "databases": {
    "mydb": {
      "users": [
        {
          "username": {"env_var": "PG_USER"},
          "password": {"env_var": "PG_PASSWORD"}
        }
      ],
      "backend": {
        "host": "postgres.example.com",
        "port": 5432,
        "database": "mydb",
        "pool_max_conns": 20,
        "pool_min_idle_conns": 5,
        "sslmode": "require"
      }
    }
  }
}
```


### Config

The pglink configuration.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `listen` | string | No | The network address to listen on. Examples: "5432", ":5432", "127.0.0.1:5432", "0.0.0.0:5432" Default: `":16432"` |
| `tls` | [TLSConfig](#tlsconfig) | No | TLS for incoming client connections. If not specified, a self-signed certificate is generated in memory. |
| `databases` | map[string][DatabaseConfig](#databaseconfig) | Yes | Database names to their configurations. Clients connect by specifying a database name; the proxy routes the connection to the corresponding backend. |
| `auth_method` | string | No | The authentication method for client connections. Valid values: "plaintext", "md5_password", "scram-sha-256" Default: `"scram-sha-256"` |
| `scram_iterations` | integer | No | The number of iterations for SCRAM-SHA-256 authentication. Higher values provide better protection against brute-force attacks but make authentication slower. Default: `4096` |
| `max_client_connections` | integer | No | The maximum number of concurrent client connections the proxy will accept. New connections beyond this limit are immediately rejected with an error. Default: `1000` |
| `opentelemetry` | OpenTelemetryConfig | No | Distributed tracing via `opentelemetry`. If nil, tracing is disabled. |
| `prometheus` | PrometheusConfig | No | Prometheus metrics export. If nil, metrics are disabled. Presence of this config enables metrics. |
| `flight_recorder` | FlightRecorderConfig | No | The runtime/trace flight recorder. If nil, flight recording is disabled. |


### TLSConfig

JsonTLSConfig configures TLS for incoming client connections.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sslmode` | string | No | Whether TLS is required, preferred, or disabled. See the SSLMode type for valid values. |
| `cert_path` | string | No | The path to the TLS certificate file in PEM format. |
| `cert_private_key_path` | string | No | The path to the TLS private key file in PEM format. |
| `generate_cert` | boolean | No | Automatic generation of a self-signed certificate. If `cert_path` and `cert_private_key_path` are also set, the certificate is written to those paths (unless they already exist). |


### DatabaseConfig

A single database that clients can connect to.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `users` | [][UserConfig](#userconfig) | Yes | The list of user credentials that can connect to this database. Each user can authenticate with their own username and password. |
| `backend` | [BackendConfig](#backendconfig) | Yes | The PostgreSQL server to proxy connections to. |
| `pool_acquire_timeout_milliseconds` | integer | No | PoolAcquireTimeout specifies the max time to wait for a connection from the backend connection pool. Default: `1` |
| `track_extra_parameters` | []string | No | A list of additional PostgreSQL startup parameters to track and forward to the backend. By default, only standard parameters like client_encoding and application_name are tracked. |


### UserConfig

Authentication credentials for a user.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `username` | [SecretRef](#secretref) | Yes | The username for this user, loaded from a secret source. |
| `password` | [SecretRef](#secretref) | Yes | The password for this user, loaded from a secret source. |


### SecretRef

A secret value from one of several sources.
Exactly one of `aws_secret_arn`, `insecure_value`, or `env_var` must be set.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `aws_secret_arn` | string | No | The ARN of an AWS Secrets Manager secret. When using this, `key` must also be set to specify which JSON key to extract. |
| `key` | string | No | The JSON key to extract from an AWS Secrets Manager secret. Required when using `aws_secret_arn`. |
| `insecure_value` | string | No | A plaintext secret value embedded directly in the config. Only use this for development; prefer `env_var` or `aws_secret_arn` for production. |
| `env_var` | string | No | The name of an environment variable containing the secret value. |


### BackendConfig

The backend PostgreSQL server to proxy to.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `host` | string | Yes | The hostname or IP address of the backend PostgreSQL server. |
| `port` | integer | No | The port number of the backend PostgreSQL server. Default: `5432` |
| `database` | string | Yes | The name of the database on the backend server. |
| `connect_timeout` | string | No | The connection timeout in seconds (e.g., "5"). |
| `sslmode` | string | No | SSL/TLS for backend connections. Values: disable, allow, prefer, require, verify-ca, verify-full |
| `sslkey` | string | No | The path to the client private key file for SSL authentication. |
| `sslcert` | string | No | The path to the client certificate file for SSL authentication. |
| `sslrootcert` | string | No | The path to the root CA certificate file for SSL verification. |
| `sslpassword` | string | No | The password for the encrypted client private key. |
| `statement_cache_capacity` | integer | No | The prepared statement cache size. Set to 0 to disable caching. Default: `512` |
| `description_cache_capacity` | integer | No | The statement description cache size. Set to 0 to disable caching. Default: `512` |
| `prepared_statement_cache_size` | integer | No | The maximum number of prepared statements to cache per database for statement reuse across backend connections. This is pglink's own cache, separate from pgx's statement cache. Set to 0 to disable caching. Default: `1000` |
| `pool_max_conns` | integer | Yes | The maximum number of connections in the pool. This is required and must be greater than 0. |
| `pool_min_idle_conns` | integer | No | The minimum number of idle connections to maintain. The total across all users must not exceed `pool_max_conns`. |
| `pool_max_conn_lifetime` | string | No | The maximum lifetime of a connection (e.g., "1h"). Connections older than this are closed and replaced. |
| `pool_max_conn_lifetime_jitter` | string | No | Randomness to connection lifetimes (e.g., "5m"). Prevents all connections from expiring simultaneously. |
| `pool_max_conn_idle_time` | string | No | The maximum time a connection can be idle (e.g., "30m"). Idle connections older than this are closed. |
| `pool_health_check_period` | string | No | The interval between health checks (e.g., "1m"). Unhealthy connections are closed and replaced. |
| `default_startup_parameters` | map[string]string | No | PostgreSQL parameters sent when connecting. These override client-provided parameters for keys present here. |


### FlightRecorderConfig

The runtime/trace flight recorder.
The flight recorder continuously records execution traces in a ring buffer,
allowing snapshots to be captured on demand for post-mortem analysis.

The presence of this config enables the flight recorder. To disable,
remove the flight_recorder key from the config entirely.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `min_age` | Duration | No | The minimum duration of trace data to retain in the ring buffer. The flight recorder will keep at least this much recent trace data available. Default: "10s". For production debugging, set to 2x the expected problem duration. |
| `max_bytes` | int64 | No | The maximum memory (in bytes) for the trace buffer. This bounds memory usage regardless of `min_age` setting. Expect 2-10 MB/s of trace data for busy services. Default: 10485760 (10 MiB). |
| `output_dir` | string | Yes | The directory where trace snapshots are written. Required. |
| `periodic_interval` | Duration | No | Periodic snapshot capture at the specified interval. Set to 0 or omit to disable periodic snapshots. Example: "5m" captures a snapshot every 5 minutes. |
| `triggers` | FlightRecorderTriggers | No | Automatic snapshot triggers. If nil, only manual triggers (signal, HTTP) and periodic (if configured) are available. |


### FlightRecorderTriggers

Automatic snapshot triggers.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `on_slow_query_ms` | integer | No | `on_slow_query_ms` captures a snapshot when a query exceeds this duration in milliseconds. Set to 0 to disable. Default: 0 (disabled). |
| `on_error` | boolean | No | `on_error` captures a snapshot when a protocol error occurs. Default: false. |
| `on_signal` | boolean | No | `on_signal` captures a snapshot when SIGUSR1 is received. Default: true. |
| `cooldown` | Duration | No | The minimum time between automatic trigger captures. This prevents flooding with snapshots during sustained issues. Does not affect manual triggers (signal, HTTP). Default: "60s". |


### OpenTelemetryConfig

`opentelemetry` distributed tracing.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `enabled` | boolean | No | `opentelemetry` tracing. Default: false. |
| `service_name` | string | No | The service name to use in traces. Default: "pglink". |
| `otlp_endpoint` | string | No | The OTLP collector endpoint. If not set, the OTEL_EXPORTER_OTLP_ENDPOINT environment variable is used. |
| `otlp_protocol` | string | No | The OTLP protocol to use: "grpc" or "http". Default: "grpc". |
| `sampling_rate` | float64 | No | The sampling rate from 0.0 to 1.0. Default: 1.0 (sample all). |
| `include_query_text` | boolean | No | `include_query_text` includes SQL query text in spans. Default: false. Warning: This may expose sensitive data in traces. |
| `traceparent_startup_parameter` | string | No | The name of the startup parameter used to pass W3C trace context from clients. Default: "traceparent". |
| `traceparent_sql_regex` | BoolOrString | Yes | Extraction of W3C trace context from SQL comments. - true: use default regex /*traceparent='([^']+)'*/ - false: disable SQL comment extraction - string: custom regex with capture group for traceparent value This field is REQUIRED when `opentelemetry` is enabled. |
| `application_name_sql_regex` | BoolOrString | No | `application_name_sql_regex` extracts per-query application_name from SQL. - true: use default regex matching comments and SET statements - false: disable (default) - string: custom regex with capture group for application_name value |


### PrometheusConfig

Prometheus metrics export.
If this config is present in the config file, Prometheus metrics are enabled.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `listen` | string | No | The address to listen on for the metrics HTTP server. Format: "host:port" or ":port" Default: ":9090" |
| `path` | string | No | The HTTP path for the metrics endpoint. Default: "/metrics" |



### AuthMethod

The authentication method to use for client connections.

| Value | Description |
|-------|-------------|
| `plaintext` | Cleartext password authentication. Requires TLS to be enabled. |
| `md5_password` | MD5-hashed password authentication. TLS is strongly recommended but not required. |
| `scram-sha-256` | SCRAM-SHA-256 authentication. This is the default and most secure option. When TLS is available, SCRAM-SHA-256-PLUS with channel binding will be offered. |
| `scram-sha-256-plus` | SCRAM-SHA-256 with channel binding. Requires TLS. This is not directly configurable; use "scram-sha-256" and PLUS will be offered automatically when TLS is available. |


### SSLMode

The SSL mode for incoming client connections.
These mirror PostgreSQL's sslmode settings but apply to the proxy as a server.

| Value | Description |
|-------|-------------|
| `disable` | TLS is disabled entirely. Only plaintext connections are accepted. |
| `allow` | Both TLS and plaintext connections are accepted from clients. |
| `prefer` | TLS is preferred but plaintext connections are accepted if the client doesn't support TLS. |
| `require` | TLS is required for all connections. Plaintext connections are rejected. |


## License

MIT

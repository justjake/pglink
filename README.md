# pglink

```
                  __ _         __   
    ____   ____ _/ /(_)____   / /__ 
   / __ \ / __ '/ // // __ \ / //_/ 
  / /_/ // /_/ / // // / / // ,<    
 / .___/ \__, /_//_//_/ /_//_/|_|   
/_/     /____/                      
```

A high-performance PostgreSQL wire protocol proxy with connection pooling,
authentication, and TLS support.

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

## Configuration

pglink is configured via a JSON file (`pglink.json`). Below is the complete
reference for all configuration options.

### Example Configuration

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

Config holds the pglink configuration.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `listen` | array | Yes |  |
| `tls` | JsonTLSConfig | No |  |
| `databases` | object | Yes |  |
| `auth_method` | string | No | AuthMethod specifies the authentication method for client connections.
Valid values: "plaintext", "md5_password", "scram-sha-256"
Defaults to "scram-sha-256" if not specified. Default: `"scram-sha-256"` |
| `scram_iterations` | integer | No | SCRAMIterations is the number of iterations for SCRAM-SHA-256 authentication.
Higher values provide better protection against brute-force attacks but
make authentication slower. Defaults to 4096 (PostgreSQL default). Default: `4096` |
| `max_client_connections` | integer | No | MaxClientConnections is the maximum number of concurrent client connections
the proxy will accept. New connections beyond this limit are immediately
rejected with an error. If nil or 0, defaults to 1000. Default: `1000.` |


### DatabaseConfig

DatabaseConfig configures a single database that clients can connect to.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `users` | array | Yes |  |
| `backend` | BackendConfig | Yes |  |
| `track_extra_parameters` | array | No |  |


### UserConfig

UserConfig configures authentication credentials for a user.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `username` | string | Yes |  |
| `password` | string | Yes |  |


### BackendConfig

BackendConfig configures the backend PostgreSQL server to proxy to.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `host` | string | Yes | Connection target |
| `port` | integer | No |  |
| `database` | string | Yes |  |
| `connect_timeout` | string | No | Connection settings |
| `sslmode` | string | No | disable, allow, prefer, require, verify-ca, verify-full |
| `sslkey` | string | No | path to client key |
| `sslcert` | string | No | path to client cert |
| `sslrootcert` | string | No | path to root CA cert |
| `sslpassword` | string | No | password for encrypted key |
| `statement_cache_capacity` | integer | No | 0 to disable, default 512 |
| `description_cache_capacity` | integer | No | 0 to disable, default 512 |
| `pool_max_conns` | integer | Yes | Pool settings |
| `pool_min_idle_conns` | integer | No |  |
| `pool_max_conn_lifetime` | string | No | duration |
| `pool_max_conn_lifetime_jitter` | string | No | duration |
| `pool_max_conn_idle_time` | string | No | duration |
| `pool_health_check_period` | string | No | duration |
| `default_startup_parameters` | object | No | Startup parameters sent to backend on connection |


### JsonTLSConfig

JsonTLSConfig configures TLS for incoming client connections.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sslmode` | string | No | SSLMode controls whether TLS is required, preferred, or disabled.
Valid values: "disable", "allow", "prefer", "require" |
| `cert_path` | string | No | CertPath is the path to the TLS certificate file (PEM format). |
| `cert_private_key_path` | string | No | CertPrivateKeyPath is the path to the TLS private key file (PEM format). |
| `generate_cert` | boolean | No | GenerateCert, when true, generates a self-signed certificate at startup.
If CertPath and CertPrivateKeyPath are also set, the generated cert
will be written to those paths (if they don't already exist). |


### SecretRef

SecretRef identifies a secret value from one of several sources.
Exactly one of AwsSecretArn, InsecureValue, or EnvVar must be set.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `aws_secret_arn` | string | No | AwsSecretArn is the ARN of an AWS Secrets Manager secret.
Key must also be set to extract a specific field from the JSON secret. |
| `key` | string | No |  |
| `insecure_value` | string | No | InsecureValue is a plaintext secret value. Use only for development. |
| `env_var` | string | No | EnvVar is the name of an environment variable containing the secret. |




## Enumeration Types


### AuthMethod

AuthMethod represents the authentication method to use for client connections.

| Value | Description |
|-------|-------------|
| `md5_password` | AuthMethodMD5 uses MD5-hashed password authentication.
TLS is strongly recommended but not required. |
| `plaintext` | AuthMethodPlaintext uses cleartext password authentication.
Requires TLS to be enabled. |
| `scram-sha-256` | AuthMethodSCRAMSHA256 uses SCRAM-SHA-256 authentication.
This is the default and most secure option.
When TLS is available, SCRAM-SHA-256-PLUS with channel binding will be offered. |
| `scram-sha-256-plus` | AuthMethodSCRAMSHA256Plus uses SCRAM-SHA-256 with channel binding.
Requires TLS. This is not directly configurable; use "scram-sha-256"
and PLUS will be offered automatically when TLS is available. |


### SSLMode

SSLMode represents the SSL mode for incoming client connections.
These mirror PostgreSQL's sslmode settings but apply to the proxy as a server.

| Value | Description |
|-------|-------------|
| `allow` | SSLModeAllow accepts both SSL and non-SSL connections. |
| `disable` | SSLModeDisable disables SSL entirely. |
| `prefer` | SSLModePrefer prefers SSL but accepts non-SSL connections. |
| `require` | SSLModeRequire requires SSL for all connections. |




## Secret References

Secrets (usernames and passwords) can be loaded from multiple sources.
Use exactly one of the following in a `SecretRef`:

### Environment Variable

```json
{
  "username": {"env_var": "PG_USER"},
  "password": {"env_var": "PG_PASSWORD"}
}
```

### AWS Secrets Manager

```json
{
  "username": {"aws_secret_arn": "arn:aws:secretsmanager:...", "key": "username"},
  "password": {"aws_secret_arn": "arn:aws:secretsmanager:...", "key": "password"}
}
```

### Insecure Value (Development Only)

```json
{
  "username": {"insecure_value": "postgres"},
  "password": {"insecure_value": "secret"}
}
```

## TLS Configuration

By default, pglink generates a self-signed certificate in memory and accepts
both TLS and non-TLS connections.

### TLS Modes

| Mode | Description |
|------|-------------|
| `disable` | TLS disabled, only non-TLS connections accepted |
| `allow` | Both TLS and non-TLS connections accepted |
| `prefer` | TLS preferred, non-TLS accepted (default behavior) |
| `require` | TLS required for all connections |

### Using Your Own Certificate

```json
{
  "tls": {
    "sslmode": "require",
    "cert_path": "/path/to/server.crt",
    "cert_private_key_path": "/path/to/server.key"
  }
}
```

### Generating a Certificate on Startup

```json
{
  "tls": {
    "sslmode": "prefer",
    "generate_cert": true,
    "cert_path": "pglink.crt",
    "cert_private_key_path": "pglink.key"
  }
}
```

This will generate a self-signed certificate and write it to the specified paths
if they don't already exist.

## Authentication Methods

| Method | Description |
|--------|-------------|
| `scram-sha-256` | Default. Most secure option. Channel binding (PLUS) offered when TLS available. |
| `md5_password` | MD5-hashed password. TLS strongly recommended. |
| `plaintext` | Cleartext password. Requires TLS to be enabled. |

## License

MIT

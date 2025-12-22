# pglink

```
{{.Banner}}
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
{{.CLIExample}}
```

### Command Line Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
{{- range .Flags}}
| `-{{.Name}}` | {{.Type}} | {{if .Default}}`{{.Default}}`{{else}}*required*{{end}} | {{.Description}} |
{{- end}}

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

{{range .Config.Types}}
### {{.Name}}

{{.Description}}

| Field | Type | Required | Description |
|-------|------|----------|-------------|
{{- range .Fields}}
| `{{.Name}}` | {{jsonType .Type}} | {{if .Required}}Yes{{else}}No{{end}} | {{.Description}}{{if .Default}} Default: `{{.Default}}`{{end}} |
{{- end}}

{{end}}

{{if .Config.Enums}}
## Enumeration Types

{{range .Config.Enums}}
### {{.Name}}

{{.Description}}

| Value | Description |
|-------|-------------|
{{- range $value, $desc := .Values}}
| `{{$value}}` | {{$desc}} |
{{- end}}

{{end}}
{{end}}

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

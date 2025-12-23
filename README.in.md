# pglink

```
{{.Banner}}
```

PostgreSQL wire protocol proxy with transaction-based connection pooling. Supports SCRAM-SHA-256 authentication with TLS.

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

{{range .Config.Types}}
### {{.DisplayName}}

{{.Description}}

| Field | Type | Required | Description |
|-------|------|----------|-------------|
{{- range .Fields}}
| `{{.Name}}` | {{.JSONType}} | {{if .Required}}Yes{{else}}No{{end}} | {{oneline .Description}}{{if .Default}} Default: `{{.Default}}`{{end}} |
{{- end}}

{{end}}
{{range .Config.Enums}}
### {{.Name}}

{{.Description}}

| Value | Description |
|-------|-------------|
{{- range .Values}}
| `{{.Value}}` | {{oneline .Description}} |
{{- end}}

{{end}}
## License

MIT

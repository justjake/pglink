package config

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"iter"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DatabaseConfig configures a single database that clients can connect to.
type DatabaseConfig struct {
	// Database is the database name clients use to connect.
	// This is set from the map key in the parent Config, not from JSON.
	Database string `json:"-"`

	// Users is the list of user credentials that can connect to this database.
	// Each user can authenticate with their own username and password.
	Users []UserConfig `json:"users"`

	// Backend configures the PostgreSQL server to proxy connections to.
	Backend BackendConfig `json:"backend"`

	// PoolAcquireTimeout specifies the max time to wait for a connection from the backend connection pool.
	// Defaults to 1 second.
	PoolAcquireTimeoutMilliseconds *int `json:"pool_acquire_timeout_milliseconds,omitempty"`

	// TrackExtraParameters is a list of additional PostgreSQL startup parameters
	// to track and forward to the backend. By default, only standard parameters
	// like client_encoding and application_name are tracked.
	TrackExtraParameters []string `json:"track_extra_parameters,omitzero"`

	// MessageBufferBytes is the byte capacity for the ring buffer used to
	// proxy messages between client and server.
	// Must be a power of 2: 4KiB, 8KiB, 16KiB, 32KiB, 64KiB, 128KiB, 256KiB, 512KiB, 1MiB.
	// Defaults to "16KiB".
	MessageBufferBytes ByteSize `json:"message_buffer_bytes,omitzero"`
}

// Validate checks that the database configuration is valid.
// It verifies pool constraints, backend config, and that all usernames are unique.
func (c *DatabaseConfig) Validate(ctx context.Context, secrets *SecretCache) error {
	var errs []error

	if c.PoolAcquireTimeoutMilliseconds != nil && *c.PoolAcquireTimeoutMilliseconds <= 0 {
		errs = append(errs, fmt.Errorf("pool_acquire_timeout_milliseconds must be > 0"))
	}

	if c.MessageBufferBytes != 0 {
		n := c.MessageBufferBytes.Int64()
		if n <= 0 || (n&(n-1)) != 0 {
			errs = append(errs, fmt.Errorf("message_buffer_bytes must be a power of 2 (e.g., 16KiB, 64KiB, 256KiB), got %s", c.MessageBufferBytes))
		}
	}

	if c.Backend.PoolMaxConns <= 0 {
		errs = append(errs, fmt.Errorf("backend.pool_max_conns is required and must be > 0"))
	}

	if c.Backend.PoolMinIdleConns != nil && *c.Backend.PoolMinIdleConns > 0 {
		minRequired := int32(len(c.Users)) * *c.Backend.PoolMinIdleConns
		if minRequired > c.Backend.PoolMaxConns {
			errs = append(errs, fmt.Errorf(
				"backend.pool_min_idle_conns (%d) * len(users) (%d) = %d exceeds backend.pool_max_conns (%d)",
				*c.Backend.PoolMinIdleConns, len(c.Users), minRequired, c.Backend.PoolMaxConns))
		}
	}

	// Validate backend configuration
	if err := c.Backend.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("backend: %w", err))
	}

	// Check for duplicate usernames
	seenUsernames := make(map[string]int) // username -> first index seen
	for i, user := range c.Users {
		username, err := secrets.Get(ctx, user.Username)
		if err != nil {
			// Secret resolution errors are reported elsewhere; skip duplicate check for this user
			continue
		}
		if firstIdx, exists := seenUsernames[username]; exists {
			errs = append(errs, fmt.Errorf("users[%d].username: duplicate username %q (first seen at users[%d])", i, username, firstIdx))
		} else {
			seenUsernames[username] = i
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

func (c *DatabaseConfig) PoolAcquireTimeout() time.Duration {
	if c.PoolAcquireTimeoutMilliseconds != nil {
		return time.Duration(*c.PoolAcquireTimeoutMilliseconds) * time.Millisecond
	}
	return 1 * time.Second
}

// GetMessageBufferBytes returns the ring buffer size in bytes.
// Defaults to 16KiB if not configured.
func (c *DatabaseConfig) GetMessageBufferBytes() int64 {
	if c.MessageBufferBytes == 0 {
		return 16 * 1024 // 16KiB default
	}
	return c.MessageBufferBytes.Int64()
}

// UserConfig configures authentication credentials for a user.
type UserConfig struct {
	// Username is the username for this user, loaded from a secret source.
	Username SecretRef `json:"username"`

	// Password is the password for this user, loaded from a secret source.
	Password SecretRef `json:"password"`
}

// String returns a string representation of the user config.
// Format: "insecure:<value>", "env:<var>", or "aws:<arn>?key=<key>"
func (u UserConfig) String() string {
	ref := u.Username
	switch {
	case ref.InsecureValue != "":
		return "insecure:" + ref.InsecureValue
	case ref.EnvVar != "":
		return "env:" + ref.EnvVar
	case ref.AwsSecretArn != "":
		return "aws:" + ref.AwsSecretArn + "?key=" + ref.Key
	default:
		return "<invalid>"
	}
}

// BackendConfig configures the backend PostgreSQL server to proxy to.
type BackendConfig struct {
	// Host is the hostname or IP address of the backend PostgreSQL server.
	Host string `json:"host"`

	// Port is the port number of the backend PostgreSQL server. Defaults to 5432.
	Port *uint16 `json:"port,omitempty"`

	// Database is the name of the database on the backend server.
	Database string `json:"database"`

	// ConnectTimeout is the connection timeout in seconds (e.g., "5").
	ConnectTimeout *string `json:"connect_timeout,omitempty"`

	// SSLMode controls SSL/TLS for backend connections.
	// Values: disable, allow, prefer, require, verify-ca, verify-full
	SSLMode *string `json:"sslmode,omitempty"`

	// SSLKey is the path to the client private key file for SSL authentication.
	SSLKey *string `json:"sslkey,omitempty"`

	// SSLCert is the path to the client certificate file for SSL authentication.
	SSLCert *string `json:"sslcert,omitempty"`

	// SSLRootCert is the path to the root CA certificate file for SSL verification.
	SSLRootCert *string `json:"sslrootcert,omitempty"`

	// SSLPassword is the password for the encrypted client private key.
	SSLPassword *string `json:"sslpassword,omitempty"`

	// StatementCacheCapacity is the prepared statement cache size.
	// Set to 0 to disable caching. Defaults to 512.
	StatementCacheCapacity *int32 `json:"statement_cache_capacity,omitempty"`

	// DescriptionCacheCapacity is the statement description cache size.
	// Set to 0 to disable caching. Defaults to 512.
	DescriptionCacheCapacity *int32 `json:"description_cache_capacity,omitempty"`

	// PreparedStatementCacheSize is the maximum number of prepared statements
	// to cache per database for statement reuse across backend connections.
	// This is pglink's own cache, separate from pgx's statement cache.
	// Set to 0 to disable caching. Defaults to 1000.
	PreparedStatementCacheSize *int32 `json:"prepared_statement_cache_size,omitempty"`

	// PoolMaxConns is the maximum number of connections in the pool.
	// This is required and must be greater than 0.
	PoolMaxConns int32 `json:"pool_max_conns"`

	// PoolMinIdleConns is the minimum number of idle connections to maintain.
	// The total across all users must not exceed PoolMaxConns.
	PoolMinIdleConns *int32 `json:"pool_min_idle_conns,omitempty"`

	// PoolMaxConnLifetime is the maximum lifetime of a connection (e.g., "1h").
	// Connections older than this are closed and replaced.
	PoolMaxConnLifetime *string `json:"pool_max_conn_lifetime,omitempty"`

	// PoolMaxConnLifetimeJitter adds randomness to connection lifetimes (e.g., "5m").
	// Prevents all connections from expiring simultaneously.
	PoolMaxConnLifetimeJitter *string `json:"pool_max_conn_lifetime_jitter,omitempty"`

	// PoolMaxConnIdleTime is the maximum time a connection can be idle (e.g., "30m").
	// Idle connections older than this are closed.
	PoolMaxConnIdleTime *string `json:"pool_max_conn_idle_time,omitempty"`

	// PoolHealthCheckPeriod is the interval between health checks (e.g., "1m").
	// Unhealthy connections are closed and replaced.
	PoolHealthCheckPeriod *string `json:"pool_health_check_period,omitempty"`

	// DefaultStartupParameters are PostgreSQL parameters sent when connecting.
	// These override client-provided parameters for keys present here.
	DefaultStartupParameters PgStartupParameters `json:"default_startup_parameters,omitempty"`
}

func (c *BackendConfig) Addr() string {
	port := "5432"
	if c.Port != nil {
		port = strconv.Itoa(int(*c.Port))
	}
	return net.JoinHostPort(c.Host, port)
}

// GetPreparedStatementCacheSize returns the prepared statement cache size,
// or the default of 1000 if not configured. Returns 0 to disable caching.
func (c *BackendConfig) GetPreparedStatementCacheSize() int {
	if c.PreparedStatementCacheSize == nil {
		return 1000 // Default
	}
	return int(*c.PreparedStatementCacheSize)
}

// PoolConfigString builds a libpq-style connection string in key=value format.
// This includes all configured parameters except user credentials.
func (c *BackendConfig) PoolConfigString() string {
	var parts []string

	// Helper to add a parameter if non-nil
	addStr := func(key string, val *string) {
		if val != nil {
			parts = append(parts, fmt.Sprintf("%s=%s", key, escapeConnStringValue(*val)))
		}
	}
	addInt32 := func(key string, val *int32) {
		if val != nil {
			parts = append(parts, fmt.Sprintf("%s=%d", key, *val))
		}
	}
	addUint16 := func(key string, val *uint16) {
		if val != nil {
			parts = append(parts, fmt.Sprintf("%s=%d", key, *val))
		}
	}

	// Connection target
	parts = append(parts, fmt.Sprintf("host=%s", escapeConnStringValue(c.Host)))
	addUint16("port", c.Port)
	parts = append(parts, fmt.Sprintf("database=%s", escapeConnStringValue(c.Database)))

	// Connection settings
	addStr("connect_timeout", c.ConnectTimeout)
	addStr("sslmode", c.SSLMode)
	addStr("sslkey", c.SSLKey)
	addStr("sslcert", c.SSLCert)
	addStr("sslrootcert", c.SSLRootCert)
	addStr("sslpassword", c.SSLPassword)
	addInt32("statement_cache_capacity", c.StatementCacheCapacity)
	addInt32("description_cache_capacity", c.DescriptionCacheCapacity)

	// Pool settings
	parts = append(parts, fmt.Sprintf("pool_max_conns=%d", c.PoolMaxConns))
	// Note: pool_min_idle_conns is set directly in PoolConfig(), not via connection string
	addStr("pool_max_conn_lifetime", c.PoolMaxConnLifetime)
	addStr("pool_max_conn_lifetime_jitter", c.PoolMaxConnLifetimeJitter)
	addStr("pool_max_conn_idle_time", c.PoolMaxConnIdleTime)
	addStr("pool_health_check_period", c.PoolHealthCheckPeriod)

	return strings.Join(parts, " ")
}

// Validate checks that the backend configuration is valid by attempting
// to parse the pool configuration.
func (c *BackendConfig) Validate() error {
	_, err := c.PoolConfig()
	return err
}

// PoolConfig parses the connection string and returns a pgxpool.Config.
// User credentials should be set on the returned config before use.
func (c *BackendConfig) PoolConfig() (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(c.PoolConfigString())
	if err != nil {
		return nil, fmt.Errorf("failed to parse pool config: %w", err)
	}

	// Set MinIdleConns (not supported via connection string)
	if c.PoolMinIdleConns != nil {
		cfg.MinIdleConns = *c.PoolMinIdleConns
	}

	// Apply default startup parameters
	for key, value := range c.DefaultStartupParameters.All() {
		cfg.ConnConfig.RuntimeParams[key] = value
	}

	return cfg, nil
}

// escapeConnStringValue escapes a value for use in a libpq connection string.
// Values containing spaces, backslashes, or single quotes need quoting.
func escapeConnStringValue(s string) string {
	if s == "" {
		return "''"
	}
	needsQuoting := strings.ContainsAny(s, " \\'")
	if !needsQuoting {
		return s
	}
	// Escape backslashes and single quotes, then wrap in single quotes
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	return "'" + s + "'"
}

// PgStartupParameters is a map of PostgreSQL startup parameters
// that preserves insertion order (i.e., the order from the JSON file).
type PgStartupParameters struct {
	keys   []string
	values map[string]string
}

// All returns an iterator over parameters in insertion order.
func (p *PgStartupParameters) All() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for _, k := range p.keys {
			if !yield(k, p.values[k]) {
				return
			}
		}
	}
}

// UnmarshalJSON parses a JSON object, preserving key order from the file.
func (p *PgStartupParameters) UnmarshalJSON(data []byte) error {
	p.keys = nil
	p.values = make(map[string]string)

	dec := jsontext.NewDecoder(strings.NewReader(string(data)))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return err
	}

	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		key := keyTok.String()

		valTok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		val := valTok.String()

		p.keys = append(p.keys, key)
		p.values[key] = val
	}
	return nil
}

// MarshalJSON serializes parameters in insertion order.
func (p PgStartupParameters) MarshalJSON() ([]byte, error) {
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range p.keys {
		if i > 0 {
			b.WriteByte(',')
		}
		keyBytes, _ := json.Marshal(k)
		valBytes, _ := json.Marshal(p.values[k])
		b.Write(keyBytes)
		b.WriteByte(':')
		b.Write(valBytes)
	}
	b.WriteByte('}')
	return []byte(b.String()), nil
}

// ListenAddr is a network address suitable for net.Listen.
// It normalizes JSON input formats like "5432", ":5432", or "127.0.0.1:5432"
// into the "host:port" format expected by Go's net package.
type ListenAddr string

// UnmarshalJSON parses a listen address string and normalizes it.
func (l *ListenAddr) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	*l = ListenAddr(normalizeListenAddr(s))
	return nil
}

// String returns the normalized address string.
func (l ListenAddr) String() string {
	return string(l)
}

// normalizeListenAddr converts various address formats to "host:port".
// Accepts: "5432", ":5432", "127.0.0.1:5432"
func normalizeListenAddr(s string) string {
	if !strings.Contains(s, ":") {
		// Just a port number like "5432"
		return ":" + s
	}
	return s
}

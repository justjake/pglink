package config

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DatabaseConfig configures a single database that clients can connect to.
type DatabaseConfig struct {
	// Database is the database name clients use to connect.
	// This is set from the map key in the parent Config, not from JSON.
	Database string `json:"-"`

	Users                []UserConfig  `json:"users"`
	Backend              BackendConfig `json:"backend"`
	TrackExtraParameters []string      `json:"track_extra_parameters,omitzero"`
}

// Validate checks that the database configuration is valid.
func (c *DatabaseConfig) Validate() error {
	var errs []error

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

	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

// UserConfig configures authentication credentials for a user.
type UserConfig struct {
	Username SecretRef `json:"username"`
	Password SecretRef `json:"password"`
}

// BackendConfig configures the backend PostgreSQL server to proxy to.
type BackendConfig struct {
	// Connection target
	Host     string  `json:"host"`
	Port     *uint16 `json:"port,omitempty"`
	Database string  `json:"database"`

	// Connection settings
	ConnectTimeout           *string `json:"connect_timeout,omitempty"`            // seconds, e.g. "5"
	SSLMode                  *string `json:"sslmode,omitempty"`                    // disable, allow, prefer, require, verify-ca, verify-full
	SSLKey                   *string `json:"sslkey,omitempty"`                     // path to client key
	SSLCert                  *string `json:"sslcert,omitempty"`                    // path to client cert
	SSLRootCert              *string `json:"sslrootcert,omitempty"`                // path to root CA cert
	SSLPassword              *string `json:"sslpassword,omitempty"`                // password for encrypted key
	StatementCacheCapacity   *int32  `json:"statement_cache_capacity,omitempty"`   // 0 to disable, default 512
	DescriptionCacheCapacity *int32  `json:"description_cache_capacity,omitempty"` // 0 to disable, default 512

	// Pool settings
	PoolMaxConns              int32   `json:"pool_max_conns"`
	PoolMinIdleConns          *int32  `json:"pool_min_idle_conns,omitempty"`
	PoolMaxConnLifetime       *string `json:"pool_max_conn_lifetime,omitempty"`        // duration
	PoolMaxConnLifetimeJitter *string `json:"pool_max_conn_lifetime_jitter,omitempty"` // duration
	PoolMaxConnIdleTime       *string `json:"pool_max_conn_idle_time,omitempty"`       // duration
	PoolHealthCheckPeriod     *string `json:"pool_health_check_period,omitempty"`      // duration

	// Startup parameters sent to backend on connection
	DefaultStartupParameters PgStartupParameters `json:"default_startup_parameters,omitempty"`
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

// Package config handles interpreting the pglink.json config file.
package config

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
)

// AuthMethod represents the authentication method to use for client connections.
type AuthMethod string

const (
	// AuthMethodPlaintext uses cleartext password authentication.
	// Requires TLS to be enabled.
	AuthMethodPlaintext AuthMethod = "plaintext"
	// AuthMethodMD5 uses MD5-hashed password authentication.
	// TLS is strongly recommended but not required.
	AuthMethodMD5 AuthMethod = "md5_password"
	// AuthMethodSCRAMSHA256 uses SCRAM-SHA-256 authentication.
	// This is the default and most secure option.
	// When TLS is available, SCRAM-SHA-256-PLUS with channel binding will be offered.
	AuthMethodSCRAMSHA256 AuthMethod = "scram-sha-256"
	// AuthMethodSCRAMSHA256Plus uses SCRAM-SHA-256 with channel binding.
	// Requires TLS. This is not directly configurable; use "scram-sha-256"
	// and PLUS will be offered automatically when TLS is available.
	AuthMethodSCRAMSHA256Plus AuthMethod = "scram-sha-256-plus"
)

// Valid returns true if the auth method is a recognized user-configurable value.
func (m AuthMethod) Valid() bool {
	switch m {
	case AuthMethodPlaintext, AuthMethodMD5, AuthMethodSCRAMSHA256, "":
		return true
	default:
		return false
	}
}

// DefaultListenAddr is the default listen address if none is specified.
const DefaultListenAddr = ":16432"

// Config holds the pglink configuration.
type Config struct {
	// Listen is the network address to listen on.
	// Examples: "5432", ":5432", "127.0.0.1:5432", "0.0.0.0:5432"
	// Defaults to ":16432" if not specified.
	Listen ListenAddr `json:"listen,omitzero"`

	// TLS configures TLS for incoming client connections.
	// If not specified, a self-signed certificate is generated in memory.
	TLS *JsonTLSConfig `json:"tls,omitzero"`

	// Databases maps database names to their configurations.
	// Clients connect by specifying a database name; the proxy routes
	// the connection to the corresponding backend.
	Databases map[string]*DatabaseConfig `json:"databases"`

	// AuthMethod specifies the authentication method for client connections.
	// Valid values: "plaintext", "md5_password", "scram-sha-256"
	// Defaults to "scram-sha-256" if not specified.
	AuthMethod AuthMethod `json:"auth_method,omitzero"`

	// SCRAMIterations is the number of iterations for SCRAM-SHA-256 authentication.
	// Higher values provide better protection against brute-force attacks but
	// make authentication slower. Defaults to 4096 (PostgreSQL default).
	SCRAMIterations *int32 `json:"scram_iterations,omitzero"`

	// MaxClientConnections is the maximum number of concurrent client connections
	// the proxy will accept. New connections beyond this limit are immediately
	// rejected with an error. Defaults to 1000.
	MaxClientConnections *int32 `json:"max_client_connections,omitzero"`

	// OpenTelemetry configures distributed tracing via OpenTelemetry.
	// If nil, tracing is disabled.
	OpenTelemetry *OpenTelemetryConfig `json:"opentelemetry,omitzero"`

	// Prometheus configures Prometheus metrics export.
	// If nil, metrics are disabled. Presence of this config enables metrics.
	Prometheus *PrometheusConfig `json:"prometheus,omitzero"`

	// FlightRecorder configures the runtime/trace flight recorder.
	// If nil, flight recording is disabled.
	FlightRecorder *FlightRecorderConfig `json:"flight_recorder,omitzero"`

	// filePath is the path to the config file on disk (not serialized).
	// Used for resolving relative paths in the config.
	filePath string
}

// GetMaxClientConnections returns the maximum client connections setting,
// defaulting to 1000 if not configured.
func (c *Config) GetMaxClientConnections() int32 {
	if c.MaxClientConnections == nil || *c.MaxClientConnections == 0 {
		return 1000
	}
	return *c.MaxClientConnections
}

// GetAuthMethod returns the configured auth method, defaulting to SCRAM-SHA-256.
func (c *Config) GetAuthMethod() AuthMethod {
	if c.AuthMethod == "" {
		return AuthMethodSCRAMSHA256
	}
	return c.AuthMethod
}

// GetSCRAMIterations returns the SCRAM iteration count, defaulting to 4096 (PostgreSQL default).
func (c *Config) GetSCRAMIterations() int {
	if c.SCRAMIterations == nil || *c.SCRAMIterations == 0 {
		return 4096
	}
	return int(*c.SCRAMIterations)
}

// GetListenAddr returns the listen address, defaulting to DefaultListenAddr (":16432").
func (c *Config) GetListenAddr() ListenAddr {
	if c.Listen == "" {
		return ListenAddr(DefaultListenAddr)
	}
	return c.Listen
}

// SetListenAddr sets the listen address, overriding any configured value.
func (c *Config) SetListenAddr(addr string) {
	c.Listen = ListenAddr(normalizeListenAddr(addr))
}

// FilePath returns the path to the config file on disk.
func (c *Config) FilePath() string {
	return c.filePath
}

// Dir returns the directory containing the config file.
func (c *Config) Dir() string {
	if c.filePath == "" {
		return "."
	}
	return filepath.Dir(c.filePath)
}

// FileRelativePath resolves a path relative to the config file's directory.
// If name is already absolute, it is returned as-is.
func (c *Config) FileRelativePath(name string) string {
	if filepath.IsAbs(name) {
		return name
	}
	return filepath.Join(c.Dir(), name)
}

// ParseConfig parses a JSON configuration string and returns a Config.
func ParseConfig(jsonStr string) (*Config, error) {
	var cfg Config
	if err := json.Unmarshal([]byte(jsonStr), &cfg); err != nil {
		return nil, err
	}

	// Set Database field from map keys
	for name, db := range cfg.Databases {
		db.Database = name
	}

	return &cfg, nil
}

// ReadConfigFile reads and parses a configuration file from the given path.
func ReadConfigFile(path string) (*Config, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, err
	}

	cfg, err := ParseConfig(string(data))
	if err != nil {
		return nil, err
	}

	cfg.filePath = absPath
	return cfg, nil
}

// Secrets returns an iterator over all secret references in the config.
// Each secret is yielded with a description of where it appears in the config.
func (c *Config) Secrets() iter.Seq2[string, SecretRef] {
	return func(yield func(string, SecretRef) bool) {
		for name, db := range c.Databases {
			for j, user := range db.Users {
				if !yield(fmt.Sprintf("databases[%s].users[%d].username", name, j), user.Username) {
					return
				}
				if !yield(fmt.Sprintf("databases[%s].users[%d].password", name, j), user.Password) {
					return
				}
			}
		}
	}
}

// TLSConfig returns the TLS configuration for incoming client connections.
// Returns a TLSResult with nil Config if TLS is explicitly disabled.
// If TLS is not configured at all, returns a default TLS config with an
// in-memory generated self-signed certificate.
// The fsys parameter is used to read certificate files; paths are relative to fsys root.
// The caller should call Validate() before calling TLSConfig().
func (c *Config) TLSConfig(fsys fs.FS) (TLSResult, error) {
	if c.TLS == nil {
		// No TLS config specified - use default with in-memory generated cert
		return defaultTLSConfig()
	}
	return c.TLS.NewTLS(fsys, c.FileRelativePath)
}

// TLSEnabled returns true if TLS is enabled (either explicitly or by default).
func (c *Config) TLSEnabled() bool {
	if c.TLS == nil {
		return true // Default is TLS enabled with generated cert
	}
	return c.TLS.Enabled()
}

// TLSRequired returns true if TLS is required for all connections.
func (c *Config) TLSRequired() bool {
	if c.TLS == nil {
		return false // Default allows non-TLS connections
	}
	return c.TLS.Required()
}

// Validate verifies the configuration is valid:
// - Auth method is valid and compatible with TLS settings
// - TLS config is valid
// - All backend configs produce valid pool configs
// - All secrets are accessible
// The fsys parameter is used to check if referenced files exist.
// It does not stop at the first error; all errors are accumulated and returned together.
// Warnings are logged via the provided logger.
func (c *Config) Validate(ctx context.Context, fsys fs.FS, secrets *SecretCache, logger *slog.Logger) error {
	var errs []error

	// Validate auth method
	if !c.AuthMethod.Valid() {
		errs = append(errs, fmt.Errorf("auth_method: invalid value %q, must be one of: plaintext, md5_password, scram-sha-256", c.AuthMethod))
	}

	// Check auth method vs TLS requirements
	tlsEnabled := c.TLSEnabled()
	authMethod := c.GetAuthMethod()

	if authMethod == AuthMethodPlaintext && !tlsEnabled {
		errs = append(errs, errors.New("auth_method \"plaintext\" requires TLS to be enabled"))
	}

	if authMethod == AuthMethodMD5 && !tlsEnabled && logger != nil {
		logger.Warn("auth_method \"md5_password\" without TLS is insecure; consider enabling TLS")
	}

	// Validate TLS config (only if explicitly configured)
	if c.TLS != nil {
		if err := c.TLS.Validate(fsys); err != nil {
			errs = append(errs, fmt.Errorf("tls: %w", err))
		}
	}

	for name, db := range c.Databases {
		if err := db.Validate(ctx, secrets); err != nil {
			errs = append(errs, fmt.Errorf("databases[%s]: %w", name, err))
		}
	}

	for path, ref := range c.Secrets() {
		if _, err := secrets.Get(ctx, ref); err != nil {
			errs = append(errs, errors.Join(errors.New(path), err))
		}
	}

	// Validate OpenTelemetry config if present
	if c.OpenTelemetry != nil {
		if err := c.OpenTelemetry.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("opentelemetry: %w", err))
		}
	}

	// Validate Prometheus config if present
	if c.Prometheus != nil {
		if err := c.Prometheus.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("prometheus: %w", err))
		}
	}

	// Validate FlightRecorder config if present
	if c.FlightRecorder != nil {
		if err := c.FlightRecorder.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("flight_recorder: %w", err))
		}
	}

	return errors.Join(errs...)
}

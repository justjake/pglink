// Package config handles interpreting the pglink.json config file.
package config

import (
	"context"
	"crypto/tls"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
)

// Config holds the pglink configuration.
type Config struct {
	Listen  []ListenAddr              `json:"listen"`
	TLS     JsonTLSConfig             `json:"tls,omitzero"`
	Databases map[string]*DatabaseConfig `json:"databases"`

	// filePath is the path to the config file on disk (not serialized).
	// Used for resolving relative paths in the config.
	filePath string
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
// Returns nil, nil if TLS is disabled.
// The fsys parameter is used to read certificate files; paths are relative to fsys root.
// The caller should call Validate() before calling TLSConfig().
func (c *Config) TLSConfig(fsys fs.FS) (*tls.Config, error) {
	return c.TLS.NewTLS(fsys, c.FileRelativePath)
}

// Validate verifies the configuration is valid:
// - TLS config is valid
// - All backend configs produce valid pool configs
// - All secrets are accessible
// The fsys parameter is used to check if referenced files exist.
// It does not stop at the first error; all errors are accumulated and returned together.
func (c *Config) Validate(ctx context.Context, fsys fs.FS, secrets *SecretCache) error {
	var errs []error

	if err := c.TLS.Validate(fsys); err != nil {
		errs = append(errs, fmt.Errorf("tls: %w", err))
	}

	for name, db := range c.Databases {
		if err := db.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("databases[%s]: %w", name, err))
		}
		if _, err := db.Backend.PoolConfig(); err != nil {
			errs = append(errs, fmt.Errorf("databases[%s].backend: %w", name, err))
		}
	}

	for path, ref := range c.Secrets() {
		if _, err := secrets.Get(ctx, ref); err != nil {
			errs = append(errs, errors.Join(errors.New(path), err))
		}
	}

	return errors.Join(errs...)
}

// Package config handles interpreting the pglink.json config file.
package config

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"iter"
	"os"
)

// Config holds the pglink configuration.
type Config struct {
	Listen  []ListenAddr   `json:"listen"`
	Servers []ServerConfig `json:"servers"`
}

// ParseConfig parses a JSON configuration string and returns a Config.
func ParseConfig(jsonStr string) (*Config, error) {
	var cfg Config
	if err := json.Unmarshal([]byte(jsonStr), &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ReadConfigFile reads and parses a configuration file from the given path.
func ReadConfigFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseConfig(string(data))
}

// Secrets returns an iterator over all secret references in the config.
// Each secret is yielded with a description of where it appears in the config.
func (c *Config) Secrets() iter.Seq2[string, SecretRef] {
	return func(yield func(string, SecretRef) bool) {
		for i, server := range c.Servers {
			for j, user := range server.Users {
				if !yield(fmt.Sprintf("servers[%d].users[%d].username", i, j), user.Username) {
					return
				}
				if !yield(fmt.Sprintf("servers[%d].users[%d].password", i, j), user.Password) {
					return
				}
			}
		}
	}
}

// Validate verifies the configuration is valid:
// - All backend configs produce valid pool configs
// - All secrets are accessible
// It does not stop at the first error; all errors are accumulated and returned together.
func (c *Config) Validate(ctx context.Context, secrets *SecretCache) error {
	var errs []error

	for i, server := range c.Servers {
		if _, err := server.Backend.PoolConfig(); err != nil {
			errs = append(errs, fmt.Errorf("servers[%d].backend: %w", i, err))
		}
	}

	for path, ref := range c.Secrets() {
		if _, err := secrets.Get(ctx, ref); err != nil {
			errs = append(errs, errors.Join(errors.New(path), err))
		}
	}

	return errors.Join(errs...)
}

// Package config handles interpreting the pglink.json config file.
package config

import (
	"encoding/json/v2"
	"os"
	"strings"
)

// Config holds the pglink configuration.
type Config struct {
	Servers []ServerConfig `json:"servers"`
}

// ServerConfig configures a single server instance.
type ServerConfig struct {
	Listen []ListenAddr `json:"listen"`
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

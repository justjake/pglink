// Package config handles interpreting the pglink.json config file.
package config

import (
	"encoding/json/v2"
	"os"
)

// Config holds the pglink configuration.
type Config struct {
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

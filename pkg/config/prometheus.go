package config

import (
	"errors"
	"fmt"
	"strings"
)

// PrometheusConfig configures Prometheus metrics export.
// If this config is present in the config file, Prometheus metrics are enabled.
type PrometheusConfig struct {
	// Listen is the address to listen on for the metrics HTTP server.
	// Format: "host:port" or ":port"
	// Default: ":9090"
	Listen string `json:"listen,omitzero"`

	// Path is the HTTP path for the metrics endpoint.
	// Default: "/metrics"
	Path string `json:"path,omitzero"`
}

// GetListen returns the listen address, defaulting to ":9090".
func (c *PrometheusConfig) GetListen() string {
	if c.Listen == "" {
		return ":9090"
	}
	return c.Listen
}

// GetPath returns the metrics path, defaulting to "/metrics".
func (c *PrometheusConfig) GetPath() string {
	if c.Path == "" {
		return "/metrics"
	}
	return c.Path
}

// Validate validates the Prometheus configuration.
func (c *PrometheusConfig) Validate() error {
	var errs []error

	// Validate listen address format
	listen := c.GetListen()
	if !strings.Contains(listen, ":") {
		errs = append(errs, fmt.Errorf("listen address %q must contain a port (e.g., ':9090' or '0.0.0.0:9090')", listen))
	}

	// Validate path starts with /
	path := c.GetPath()
	if !strings.HasPrefix(path, "/") {
		errs = append(errs, fmt.Errorf("path %q must start with '/'", path))
	}

	return errors.Join(errs...)
}

// ParsePrometheusListen parses a CLI listen argument in "host:port/path" format
// and returns a PrometheusConfig. If path is not specified, defaults to "/metrics".
func ParsePrometheusListen(listen string) *PrometheusConfig {
	if listen == "" {
		return nil
	}

	// Split on first / to separate address from path
	// Format: ":9090/metrics" or "0.0.0.0:9090/metrics" or ":9090"
	parts := strings.SplitN(listen, "/", 2)
	addr := parts[0]
	path := "/metrics"
	if len(parts) > 1 {
		path = "/" + parts[1]
	}

	return &PrometheusConfig{
		Listen: addr,
		Path:   path,
	}
}

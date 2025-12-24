package config

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
)

// DefaultTraceparentSQLRegex is the default regex for extracting traceparent from SQL comments.
// Matches: /*traceparent='00-866e8698eac009878483cfd029f62af9-bb14062982c63db3-01'*/
const DefaultTraceparentSQLRegex = `/\*traceparent='([^']+)'\*/`

// DefaultApplicationNameSQLRegex matches application_name in SQL comments or SET statements.
// Matches: /* application_name=checkout-service */ or SET application_name = 'batch-processor'
const DefaultApplicationNameSQLRegex = `(?:/\*\s*application_name\s*=\s*([^\s*]+)\s*\*/|SET\s+(?:LOCAL\s+)?application_name\s*=\s*'([^']+)')`

// OpenTelemetryConfig configures OpenTelemetry distributed tracing.
type OpenTelemetryConfig struct {
	// Enabled enables OpenTelemetry tracing. Default: false.
	Enabled bool `json:"enabled,omitzero"`

	// ServiceName is the service name to use in traces. Default: "pglink".
	ServiceName string `json:"service_name,omitzero"`

	// OTLPEndpoint is the OTLP collector endpoint.
	// If not set, the OTEL_EXPORTER_OTLP_ENDPOINT environment variable is used.
	OTLPEndpoint string `json:"otlp_endpoint,omitzero"`

	// OTLPProtocol is the OTLP protocol to use: "grpc" or "http". Default: "grpc".
	OTLPProtocol string `json:"otlp_protocol,omitzero"`

	// SamplingRate is the sampling rate from 0.0 to 1.0. Default: 1.0 (sample all).
	SamplingRate *float64 `json:"sampling_rate,omitzero"`

	// IncludeQueryText includes SQL query text in spans. Default: false.
	// Warning: This may expose sensitive data in traces.
	IncludeQueryText bool `json:"include_query_text,omitzero"`

	// TraceparentStartupParameter is the name of the startup parameter used to
	// pass W3C trace context from clients. Default: "traceparent".
	TraceparentStartupParameter string `json:"traceparent_startup_parameter,omitzero"`

	// TraceparentSQLRegex controls extraction of W3C trace context from SQL comments.
	// - true: use default regex /*traceparent='([^']+)'*/
	// - false: disable SQL comment extraction
	// - string: custom regex with capture group for traceparent value
	// This field is REQUIRED when OpenTelemetry is enabled.
	TraceparentSQLRegex BoolOrString `json:"traceparent_sql_regex"`

	// ApplicationNameSQLRegex extracts per-query application_name from SQL.
	// - true: use default regex matching comments and SET statements
	// - false: disable (default)
	// - string: custom regex with capture group for application_name value
	ApplicationNameSQLRegex BoolOrString `json:"application_name_sql_regex,omitzero"`

	// Compiled regexes (not serialized)
	traceparentRegex *regexp.Regexp
	appNameRegex     *regexp.Regexp
}

// BoolOrString is a type that can be unmarshaled from either a boolean or string JSON value.
type BoolOrString struct {
	Bool   bool
	String string
	IsSet  bool
}

func (b BoolOrString) MarshalJSON() ([]byte, error) {
	if !b.IsSet {
		return []byte("null"), nil
	}
	if b.String != "" {
		return json.Marshal(b.String)
	}
	return json.Marshal(b.Bool)
}

func (b *BoolOrString) UnmarshalJSON(data []byte) error {
	// Try string first
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		b.String = s
		b.IsSet = true
		return nil
	}

	// Try bool
	var bv bool
	if err := json.Unmarshal(data, &bv); err == nil {
		b.Bool = bv
		b.IsSet = true
		return nil
	}

	return fmt.Errorf("expected bool or string, got %s", string(data))
}

// GetServiceName returns the service name, defaulting to "pglink".
func (c *OpenTelemetryConfig) GetServiceName() string {
	if c.ServiceName == "" {
		return "pglink"
	}
	return c.ServiceName
}

// GetOTLPProtocol returns the OTLP protocol, defaulting to "grpc".
func (c *OpenTelemetryConfig) GetOTLPProtocol() string {
	if c.OTLPProtocol == "" {
		return "grpc"
	}
	return c.OTLPProtocol
}

// GetSamplingRate returns the sampling rate, defaulting to 1.0.
func (c *OpenTelemetryConfig) GetSamplingRate() float64 {
	if c.SamplingRate == nil {
		return 1.0
	}
	return *c.SamplingRate
}

// GetTraceparentStartupParameter returns the startup parameter name for traceparent.
func (c *OpenTelemetryConfig) GetTraceparentStartupParameter() string {
	if c.TraceparentStartupParameter == "" {
		return "traceparent"
	}
	return c.TraceparentStartupParameter
}

// GetTraceparentRegex returns the compiled regex for extracting traceparent from SQL.
// Returns nil if traceparent_sql_regex is false or not configured.
func (c *OpenTelemetryConfig) GetTraceparentRegex() *regexp.Regexp {
	return c.traceparentRegex
}

// GetApplicationNameRegex returns the compiled regex for extracting application_name from SQL.
// Returns nil if application_name_sql_regex is false or not configured.
func (c *OpenTelemetryConfig) GetApplicationNameRegex() *regexp.Regexp {
	return c.appNameRegex
}

// Validate validates the OpenTelemetry configuration.
func (c *OpenTelemetryConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	var errs []error

	// Validate OTLP protocol
	protocol := c.GetOTLPProtocol()
	if protocol != "grpc" && protocol != "http" {
		errs = append(errs, fmt.Errorf("otlp_protocol must be \"grpc\" or \"http\", got %q", protocol))
	}

	// Validate sampling rate
	rate := c.GetSamplingRate()
	if rate < 0.0 || rate > 1.0 {
		errs = append(errs, fmt.Errorf("sampling_rate must be between 0.0 and 1.0, got %f", rate))
	}

	// traceparent_sql_regex is REQUIRED when enabled
	if !c.TraceparentSQLRegex.IsSet {
		errs = append(errs, errors.New("traceparent_sql_regex is required when OpenTelemetry is enabled"))
	} else {
		// Compile the regex
		regex, err := c.compileRegex(c.TraceparentSQLRegex, DefaultTraceparentSQLRegex)
		if err != nil {
			errs = append(errs, fmt.Errorf("traceparent_sql_regex: %w", err))
		} else {
			c.traceparentRegex = regex
		}
	}

	// Compile application_name_sql_regex if set
	if c.ApplicationNameSQLRegex.IsSet && (c.ApplicationNameSQLRegex.Bool || c.ApplicationNameSQLRegex.String != "") {
		regex, err := c.compileRegex(c.ApplicationNameSQLRegex, DefaultApplicationNameSQLRegex)
		if err != nil {
			errs = append(errs, fmt.Errorf("application_name_sql_regex: %w", err))
		} else {
			c.appNameRegex = regex
		}
	}

	return errors.Join(errs...)
}

// compileRegex compiles a BoolOrString into a regex, using the default if Bool is true.
// Returns nil if Bool is false and String is empty.
func (c *OpenTelemetryConfig) compileRegex(bos BoolOrString, defaultPattern string) (*regexp.Regexp, error) {
	if !bos.IsSet {
		return nil, nil
	}

	if bos.String != "" {
		return regexp.Compile(bos.String)
	}

	if bos.Bool {
		return regexp.Compile(defaultPattern)
	}

	return nil, nil
}

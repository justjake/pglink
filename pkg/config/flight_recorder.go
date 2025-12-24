package config

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"time"
)

// Duration is a time.Duration that can be unmarshaled from a JSON string like "10s", "1m", etc.
type Duration time.Duration

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		// Try parsing as number (seconds)
		var n float64
		if err := json.Unmarshal(data, &n); err != nil {
			return fmt.Errorf("expected duration string or number, got %s", string(data))
		}
		*d = Duration(time.Duration(n * float64(time.Second)))
		return nil
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// FlightRecorderConfig configures the runtime/trace flight recorder.
// The flight recorder continuously records execution traces in a ring buffer,
// allowing snapshots to be captured on demand for post-mortem analysis.
//
// The presence of this config enables the flight recorder. To disable,
// remove the flight_recorder key from the config entirely.
type FlightRecorderConfig struct {
	// MinAge is the minimum duration of trace data to retain in the ring buffer.
	// The flight recorder will keep at least this much recent trace data available.
	// Default: "10s". For production debugging, set to 2x the expected problem duration.
	MinAge Duration `json:"min_age,omitzero"`

	// MaxBytes is the maximum memory (in bytes) for the trace buffer.
	// This bounds memory usage regardless of MinAge setting.
	// Expect 2-10 MB/s of trace data for busy services.
	// Default: 10485760 (10 MiB).
	MaxBytes int64 `json:"max_bytes,omitzero"`

	// OutputDir is the directory where trace snapshots are written.
	// Required.
	OutputDir string `json:"output_dir"`

	// PeriodicInterval enables periodic snapshot capture at the specified interval.
	// Set to 0 or omit to disable periodic snapshots.
	// Example: "5m" captures a snapshot every 5 minutes.
	PeriodicInterval Duration `json:"periodic_interval,omitzero"`

	// Triggers configures automatic snapshot triggers.
	// If nil, only manual triggers (signal, HTTP) and periodic (if configured) are available.
	Triggers *FlightRecorderTriggers `json:"triggers,omitzero"`
}

// FlightRecorderTriggers configures automatic snapshot triggers.
type FlightRecorderTriggers struct {
	// OnSlowQueryMs captures a snapshot when a query exceeds this duration in milliseconds.
	// Set to 0 to disable. Default: 0 (disabled).
	OnSlowQueryMs int `json:"on_slow_query_ms,omitzero"`

	// OnError captures a snapshot when a protocol error occurs.
	// Default: false.
	OnError bool `json:"on_error,omitzero"`

	// OnSignal captures a snapshot when SIGUSR1 is received.
	// Default: true.
	OnSignal *bool `json:"on_signal,omitzero"`

	// Cooldown is the minimum time between automatic trigger captures.
	// This prevents flooding with snapshots during sustained issues.
	// Does not affect manual triggers (signal, HTTP).
	// Default: "60s".
	Cooldown Duration `json:"cooldown,omitzero"`
}

// GetMinAge returns the minimum age setting, defaulting to 10 seconds.
func (c *FlightRecorderConfig) GetMinAge() time.Duration {
	if c.MinAge == 0 {
		return 10 * time.Second
	}
	return c.MinAge.Duration()
}

// GetMaxBytes returns the max bytes setting, defaulting to 10 MiB.
func (c *FlightRecorderConfig) GetMaxBytes() int64 {
	if c.MaxBytes == 0 {
		return 10 * 1024 * 1024 // 10 MiB
	}
	return c.MaxBytes
}

// GetTriggers returns the triggers config with defaults applied.
func (c *FlightRecorderConfig) GetTriggers() FlightRecorderTriggers {
	if c.Triggers == nil {
		return FlightRecorderTriggers{
			OnSignal: boolPtr(true),
		}
	}
	triggers := *c.Triggers
	if triggers.OnSignal == nil {
		triggers.OnSignal = boolPtr(true)
	}
	return triggers
}

// GetOnSignal returns whether SIGUSR1 triggers snapshots, defaulting to true.
func (t FlightRecorderTriggers) GetOnSignal() bool {
	if t.OnSignal == nil {
		return true
	}
	return *t.OnSignal
}

// GetCooldown returns the cooldown duration, defaulting to 60 seconds.
func (t FlightRecorderTriggers) GetCooldown() time.Duration {
	if t.Cooldown == 0 {
		return 60 * time.Second
	}
	return t.Cooldown.Duration()
}

// Validate validates the flight recorder configuration.
func (c *FlightRecorderConfig) Validate() error {
	var errs []error

	// OutputDir is required
	if c.OutputDir == "" {
		errs = append(errs, errors.New("output_dir is required"))
	} else {
		// Check if directory exists or can be created
		if info, err := os.Stat(c.OutputDir); err != nil {
			if os.IsNotExist(err) {
				// Try to create it
				if err := os.MkdirAll(c.OutputDir, 0755); err != nil {
					errs = append(errs, fmt.Errorf("output_dir %q does not exist and cannot be created: %w", c.OutputDir, err))
				}
			} else {
				errs = append(errs, fmt.Errorf("output_dir %q: %w", c.OutputDir, err))
			}
		} else if !info.IsDir() {
			errs = append(errs, fmt.Errorf("output_dir %q is not a directory", c.OutputDir))
		}
	}

	// Validate MinAge
	if c.MinAge < 0 {
		errs = append(errs, errors.New("min_age must be non-negative"))
	}

	// Validate MaxBytes
	if c.MaxBytes < 0 {
		errs = append(errs, errors.New("max_bytes must be non-negative"))
	}

	// Validate PeriodicInterval
	if c.PeriodicInterval < 0 {
		errs = append(errs, errors.New("periodic_interval must be non-negative"))
	}

	// Validate triggers
	if c.Triggers != nil {
		if c.Triggers.OnSlowQueryMs < 0 {
			errs = append(errs, errors.New("triggers.on_slow_query_ms must be non-negative"))
		}
		if c.Triggers.Cooldown < 0 {
			errs = append(errs, errors.New("triggers.cooldown must be non-negative"))
		}
	}

	return errors.Join(errs...)
}

func boolPtr(b bool) *bool {
	return &b
}

// GetPeriodicInterval returns the periodic interval, or 0 if disabled.
func (c *FlightRecorderConfig) GetPeriodicInterval() time.Duration {
	return c.PeriodicInterval.Duration()
}

// ParseFlightRecorderDir creates a FlightRecorderConfig from a CLI directory argument.
// This creates a flight recorder config with default settings, saving snapshots to the given directory.
func ParseFlightRecorderDir(dir string) *FlightRecorderConfig {
	if dir == "" {
		return nil
	}
	return &FlightRecorderConfig{
		OutputDir: dir,
	}
}

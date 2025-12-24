package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/justjake/pglink/pkg/config"
)

// FlightRecorderService manages the runtime/trace flight recorder.
// It provides continuous execution tracing with on-demand snapshot capture.
type FlightRecorderService struct {
	fr     *trace.FlightRecorder
	config *config.FlightRecorderConfig
	logger *slog.Logger

	mu            sync.Mutex
	lastSnapshot  time.Time
	snapshotCount int64

	// Channel to signal shutdown to signal handler goroutine
	done chan struct{}
}

// FlightRecorderStatus represents the current status of the flight recorder.
type FlightRecorderStatus struct {
	Enabled         bool      `json:"enabled"`
	Running         bool      `json:"running"`
	OutputDir       string    `json:"output_dir"`
	MinAge          string    `json:"min_age"`
	MaxBytes        int64     `json:"max_bytes"`
	LastSnapshot    time.Time `json:"last_snapshot,omitempty"`
	SnapshotCount   int64     `json:"snapshot_count"`
	TriggerCooldown string    `json:"trigger_cooldown"`
}

// NewFlightRecorderService creates a new flight recorder service.
// Returns nil if the config is nil or flight recording is not enabled.
func NewFlightRecorderService(cfg *config.FlightRecorderConfig, logger *slog.Logger) (*FlightRecorderService, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, nil
	}

	fr := trace.NewFlightRecorder(trace.FlightRecorderConfig{
		MinAge:   cfg.GetMinAge(),
		MaxBytes: uint64(cfg.GetMaxBytes()),
	})

	return &FlightRecorderService{
		fr:     fr,
		config: cfg,
		logger: logger,
		done:   make(chan struct{}),
	}, nil
}

// Start begins recording trace data.
func (s *FlightRecorderService) Start() error {
	if s == nil || s.fr == nil {
		return nil
	}
	if err := s.fr.Start(); err != nil {
		return fmt.Errorf("failed to start flight recorder: %w", err)
	}
	s.logger.Info("flight recorder started",
		"output_dir", s.config.OutputDir,
		"min_age", s.config.GetMinAge(),
		"max_bytes", s.config.GetMaxBytes(),
	)
	return nil
}

// Stop ends recording trace data.
func (s *FlightRecorderService) Stop() {
	if s == nil || s.fr == nil {
		return
	}
	close(s.done)
	s.fr.Stop()
	s.logger.Info("flight recorder stopped",
		"snapshot_count", atomic.LoadInt64(&s.snapshotCount),
	)
}

// Enabled returns true if the flight recorder is active.
func (s *FlightRecorderService) Enabled() bool {
	return s != nil && s.fr != nil && s.fr.Enabled()
}

// TakeSnapshot captures the current trace buffer and writes it to a file.
// The reason parameter is included in the filename for identification.
// Returns the path to the written file, or an error.
func (s *FlightRecorderService) TakeSnapshot(reason string) (string, error) {
	if s == nil || s.fr == nil {
		return "", fmt.Errorf("flight recorder not enabled")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Generate filename with timestamp and reason
	timestamp := time.Now().Format("2006-01-02T15-04-05")
	filename := fmt.Sprintf("snapshot-%s-%s.trace", timestamp, sanitizeFilename(reason))
	path := filepath.Join(s.config.OutputDir, filename)

	// Create output file
	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("failed to create snapshot file: %w", err)
	}

	// Write snapshot
	if _, err := s.fr.WriteTo(f); err != nil {
		_ = f.Close()
		_ = os.Remove(path) // Clean up partial file
		return "", fmt.Errorf("failed to write snapshot: %w", err)
	}

	if err := f.Close(); err != nil {
		return "", fmt.Errorf("failed to close snapshot file: %w", err)
	}

	s.lastSnapshot = time.Now()
	atomic.AddInt64(&s.snapshotCount, 1)

	s.logger.Info("flight recorder snapshot captured",
		"path", path,
		"reason", reason,
	)

	return path, nil
}

// TryTakeSnapshot attempts to capture a snapshot respecting the cooldown period.
// Returns the path and true if a snapshot was taken, or empty string and false if
// the cooldown period hasn't elapsed since the last automatic snapshot.
// This is used for automatic triggers; manual triggers bypass cooldown.
func (s *FlightRecorderService) TryTakeSnapshot(reason string) (string, bool) {
	if s == nil || s.fr == nil {
		return "", false
	}

	s.mu.Lock()
	cooldown := s.config.GetTriggers().GetCooldown()
	if time.Since(s.lastSnapshot) < cooldown {
		s.mu.Unlock()
		return "", false
	}
	s.mu.Unlock()

	path, err := s.TakeSnapshot(reason)
	if err != nil {
		s.logger.Error("failed to take automatic snapshot",
			"reason", reason,
			"error", err,
		)
		return "", false
	}
	return path, true
}

// WriteSnapshotTo writes the current trace buffer to the given writer.
// This is used for the HTTP endpoint to stream the snapshot directly.
func (s *FlightRecorderService) WriteSnapshotTo(w io.Writer) error {
	if s == nil || s.fr == nil {
		return fmt.Errorf("flight recorder not enabled")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.fr.WriteTo(w); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	s.lastSnapshot = time.Now()
	atomic.AddInt64(&s.snapshotCount, 1)

	return nil
}

// OnSlowQuery is called when a query exceeds the slow query threshold.
// It captures a snapshot if the trigger is enabled and cooldown has elapsed.
func (s *FlightRecorderService) OnSlowQuery(duration time.Duration) {
	if s == nil {
		return
	}
	triggers := s.config.GetTriggers()
	if triggers.OnSlowQueryMs <= 0 {
		return
	}
	threshold := time.Duration(triggers.OnSlowQueryMs) * time.Millisecond
	if duration < threshold {
		return
	}

	reason := fmt.Sprintf("slow-query-%dms", duration.Milliseconds())
	if path, ok := s.TryTakeSnapshot(reason); ok {
		s.logger.Warn("slow query triggered snapshot",
			"duration", duration,
			"threshold", threshold,
			"path", path,
		)
	}
}

// OnError is called when a protocol error occurs.
// It captures a snapshot if the trigger is enabled and cooldown has elapsed.
func (s *FlightRecorderService) OnError(err error) {
	if s == nil {
		return
	}
	triggers := s.config.GetTriggers()
	if !triggers.OnError {
		return
	}

	if path, ok := s.TryTakeSnapshot("error"); ok {
		s.logger.Warn("error triggered snapshot",
			"error", err,
			"path", path,
		)
	}
}

// SetupSignalHandler sets up a handler for SIGUSR1 to trigger snapshots.
// The handler runs in a separate goroutine and stops when Stop() is called.
func (s *FlightRecorderService) SetupSignalHandler(ctx context.Context) {
	if s == nil {
		return
	}
	triggers := s.config.GetTriggers()
	if !triggers.GetOnSignal() {
		return
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGUSR1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				signal.Stop(sigCh)
				return
			case <-s.done:
				signal.Stop(sigCh)
				return
			case <-sigCh:
				// Signal handler bypasses cooldown
				path, err := s.TakeSnapshot("signal")
				if err != nil {
					s.logger.Error("failed to take signal-triggered snapshot",
						"error", err,
					)
				} else {
					s.logger.Info("signal triggered snapshot",
						"path", path,
					)
				}
			}
		}
	}()

	s.logger.Info("flight recorder signal handler registered",
		"signal", "SIGUSR1",
	)
}

// Status returns the current status of the flight recorder.
func (s *FlightRecorderService) Status() FlightRecorderStatus {
	if s == nil {
		return FlightRecorderStatus{Enabled: false}
	}

	s.mu.Lock()
	lastSnapshot := s.lastSnapshot
	s.mu.Unlock()

	return FlightRecorderStatus{
		Enabled:         true,
		Running:         s.fr.Enabled(),
		OutputDir:       s.config.OutputDir,
		MinAge:          s.config.GetMinAge().String(),
		MaxBytes:        s.config.GetMaxBytes(),
		LastSnapshot:    lastSnapshot,
		SnapshotCount:   atomic.LoadInt64(&s.snapshotCount),
		TriggerCooldown: s.config.GetTriggers().GetCooldown().String(),
	}
}

// RegisterHTTPHandlers registers the flight recorder HTTP endpoints on the given mux.
// Endpoints:
//   - GET /debug/flight-recorder/snapshot - Download a trace snapshot
//   - GET /debug/flight-recorder/status - Get flight recorder status as JSON
func (s *FlightRecorderService) RegisterHTTPHandlers(mux *http.ServeMux) {
	if s == nil {
		return
	}

	mux.HandleFunc("GET /debug/flight-recorder/snapshot", s.handleSnapshot)
	mux.HandleFunc("GET /debug/flight-recorder/status", s.handleStatus)

	s.logger.Info("flight recorder HTTP endpoints registered",
		"snapshot", "/debug/flight-recorder/snapshot",
		"status", "/debug/flight-recorder/status",
	)
}

func (s *FlightRecorderService) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if !s.Enabled() {
		http.Error(w, "flight recorder not running", http.StatusServiceUnavailable)
		return
	}

	// Set headers for trace file download
	timestamp := time.Now().Format("2006-01-02T15-04-05")
	filename := fmt.Sprintf("pglink-snapshot-%s.trace", timestamp)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))

	if err := s.WriteSnapshotTo(w); err != nil {
		s.logger.Error("failed to write HTTP snapshot",
			"error", err,
		)
		// Can't send error to client since we may have started writing
	}
}

func (s *FlightRecorderService) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := s.Status()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		s.logger.Error("failed to encode status response",
			"error", err,
		)
	}
}

// sanitizeFilename removes or replaces characters that might be problematic in filenames.
func sanitizeFilename(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			result = append(result, c)
		} else if c == ' ' || c == '/' || c == '\\' {
			result = append(result, '-')
		}
		// Skip other characters
	}
	if len(result) == 0 {
		return "unknown"
	}
	return string(result)
}

package observability

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/justjake/pglink/pkg/config"
)

// MetricsServer serves Prometheus metrics over HTTP.
type MetricsServer struct {
	server *http.Server
	logger *slog.Logger
}

// NewMetricsServer creates a new MetricsServer from the given configuration.
// Returns nil if config is nil (metrics disabled).
func NewMetricsServer(cfg *config.PrometheusConfig, logger *slog.Logger) *MetricsServer {
	if cfg == nil {
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle(cfg.GetPath(), promhttp.Handler())

	return &MetricsServer{
		server: &http.Server{
			Addr:    cfg.GetListen(),
			Handler: mux,
		},
		logger: logger,
	}
}

// Start starts the metrics server in a goroutine.
// Returns immediately. Use Shutdown to stop the server.
func (s *MetricsServer) Start() error {
	if s == nil {
		return nil
	}

	go func() {
		s.logger.Info("starting metrics server", "addr", s.server.Addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("metrics server error", "error", err)
		}
	}()

	return nil
}

// Shutdown gracefully shuts down the metrics server.
func (s *MetricsServer) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

// Addr returns the address the server is listening on.
func (s *MetricsServer) Addr() string {
	if s == nil || s.server == nil {
		return ""
	}
	return s.server.Addr
}

// Enabled returns true if the metrics server is configured.
func (s *MetricsServer) Enabled() bool {
	return s != nil && s.server != nil
}

// String returns a string representation for logging.
func (s *MetricsServer) String() string {
	if s == nil {
		return "MetricsServer(disabled)"
	}
	return fmt.Sprintf("MetricsServer(addr=%s)", s.server.Addr)
}

// Package observability provides OpenTelemetry tracing, metrics, and logging for pglink.
package observability

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"

	"github.com/justjake/pglink/pkg/config"
)

// LogProvider wraps the OpenTelemetry SDK LoggerProvider with pglink-specific setup.
type LogProvider struct {
	provider *sdklog.LoggerProvider
	handler  slog.Handler
}

// NewLogProvider creates a new LogProvider from the given configuration.
// Returns nil if logging is not enabled or config is nil.
func NewLogProvider(ctx context.Context, cfg *config.OpenTelemetryConfig) (*LogProvider, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, nil
	}

	// Check if logs are configured
	if cfg.Logs == nil || !cfg.Logs.Enabled {
		return nil, nil
	}

	// Determine endpoint
	endpoint := cfg.Logs.Endpoint
	if endpoint == "" {
		endpoint = cfg.OTLPEndpoint
	}
	if endpoint == "" {
		return nil, fmt.Errorf("no OTLP endpoint configured for logs")
	}

	// Create OTLP HTTP log exporter
	// Loki expects HTTP OTLP at /otlp/v1/logs
	exporter, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpoint(endpoint),
		otlploghttp.WithURLPath("/otlp/v1/logs"),
		otlploghttp.WithInsecure(), // TODO: make configurable
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP log exporter: %w", err)
	}

	// Create resource with service name
	// Note: We create our own resource rather than merging with resource.Default()
	// to avoid schema URL conflicts between semconv versions
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.GetServiceName()),
	)

	// Create logger provider
	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
		sdklog.WithResource(res),
	)

	// Set as global provider
	global.SetLoggerProvider(provider)

	// Create slog handler that bridges to OTEL
	handler := otelslog.NewHandler(cfg.GetServiceName())

	return &LogProvider{
		provider: provider,
		handler:  handler,
	}, nil
}

// Handler returns an slog.Handler that sends logs to OTEL.
// Use this to create an slog.Logger: slog.New(logProvider.Handler())
func (lp *LogProvider) Handler() slog.Handler {
	if lp == nil {
		return nil
	}
	return lp.handler
}

// Shutdown gracefully shuts down the log provider, flushing any pending logs.
func (lp *LogProvider) Shutdown(ctx context.Context) error {
	if lp == nil || lp.provider == nil {
		return nil
	}
	return lp.provider.Shutdown(ctx)
}

// Enabled returns true if OTEL logging is enabled.
func (lp *LogProvider) Enabled() bool {
	return lp != nil && lp.provider != nil
}

// LevelFilterHandler wraps an slog.Handler and filters out log records below the specified level.
// This is useful because some handlers (like OTEL BatchProcessor) always return true for Enabled(),
// which causes unnecessary overhead when debug logging is disabled.
type LevelFilterHandler struct {
	handler slog.Handler
	level   slog.Leveler
}

// NewLevelFilterHandler creates a new LevelFilterHandler that filters out log records below the specified level.
func NewLevelFilterHandler(handler slog.Handler, level slog.Leveler) *LevelFilterHandler {
	return &LevelFilterHandler{
		handler: handler,
		level:   level,
	}
}

func (h *LevelFilterHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.level.Level() && h.handler.Enabled(ctx, level)
}

func (h *LevelFilterHandler) Handle(ctx context.Context, r slog.Record) error {
	return h.handler.Handle(ctx, r)
}

func (h *LevelFilterHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &LevelFilterHandler{
		handler: h.handler.WithAttrs(attrs),
		level:   h.level,
	}
}

func (h *LevelFilterHandler) WithGroup(name string) slog.Handler {
	return &LevelFilterHandler{
		handler: h.handler.WithGroup(name),
		level:   h.level,
	}
}

// MultiHandler creates an slog.Handler that writes to multiple handlers.
// Useful for writing to both OTEL and stdout.
func MultiHandler(handlers ...slog.Handler) slog.Handler {
	// Filter out nil handlers
	active := make([]slog.Handler, 0, len(handlers))
	for _, h := range handlers {
		if h != nil {
			active = append(active, h)
		}
	}
	if len(active) == 0 {
		return nil
	}
	if len(active) == 1 {
		return active[0]
	}
	return &multiHandler{handlers: active}
}

type multiHandler struct {
	handlers []slog.Handler
}

func (m *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range m.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (m *multiHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, h := range m.handlers {
		if h.Enabled(ctx, r.Level) {
			if err := h.Handle(ctx, r.Clone()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		handlers[i] = h.WithAttrs(attrs)
	}
	return &multiHandler{handlers: handlers}
}

func (m *multiHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		handlers[i] = h.WithGroup(name)
	}
	return &multiHandler{handlers: handlers}
}

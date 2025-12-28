package observability

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"

	"github.com/justjake/pglink/pkg/config"
)

// MetricsPusher handles push-based metrics export via OTLP HTTP.
type MetricsPusher struct {
	provider *metric.MeterProvider
	config   *config.PrometheusConfig
}

// NewMetricsPusher creates a new MetricsPusher that exports metrics via OTLP HTTP.
// The endpoint should be the Prometheus OTLP endpoint (e.g., "localhost:19090").
// Prometheus uses HTTP OTLP receiver at /api/v1/otlp/v1/metrics
func NewMetricsPusher(ctx context.Context, cfg *config.PrometheusConfig, otelCfg *config.OpenTelemetryConfig) (*MetricsPusher, error) {
	if cfg == nil || cfg.Push == nil {
		return nil, nil
	}

	endpoint := cfg.Push.Endpoint
	if endpoint == "" {
		return nil, fmt.Errorf("push endpoint is required")
	}

	// Create OTLP HTTP metric exporter
	// Prometheus OTLP receiver expects HTTP at /api/v1/otlp/v1/metrics
	exporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpoint(endpoint),
		otlpmetrichttp.WithURLPath("/api/v1/otlp/v1/metrics"),
		otlpmetrichttp.WithInsecure(), // TODO: make configurable
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP metric exporter: %w", err)
	}

	// Get service name from OTEL config or default
	serviceName := "pglink"
	if otelCfg != nil {
		serviceName = otelCfg.GetServiceName()
	}

	// Create resource with service name
	// Note: We create our own resource rather than merging with resource.Default()
	// to avoid schema URL conflicts between semconv versions
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
	)

	// Get push interval
	pushInterval := 10 * time.Second
	if cfg.Push.PushInterval > 0 {
		pushInterval = time.Duration(cfg.Push.PushInterval)
	}

	// Create meter provider with periodic reader
	provider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(exporter,
			metric.WithInterval(pushInterval),
		)),
	)

	// Set as global meter provider
	otel.SetMeterProvider(provider)

	return &MetricsPusher{
		provider: provider,
		config:   cfg,
	}, nil
}

// Shutdown gracefully shuts down the metrics pusher, flushing any pending metrics.
func (mp *MetricsPusher) Shutdown(ctx context.Context) error {
	if mp == nil || mp.provider == nil {
		return nil
	}
	return mp.provider.Shutdown(ctx)
}

// Enabled returns true if metrics pushing is enabled.
func (mp *MetricsPusher) Enabled() bool {
	return mp != nil && mp.provider != nil
}

// MeterProvider returns the underlying MeterProvider.
func (mp *MetricsPusher) MeterProvider() *metric.MeterProvider {
	if mp == nil {
		return nil
	}
	return mp.provider
}

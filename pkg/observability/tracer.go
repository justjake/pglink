// Package observability provides OpenTelemetry tracing and Prometheus metrics for pglink.
package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/justjake/pglink/pkg/config"
)

// TracerProvider wraps the OpenTelemetry SDK TracerProvider with pglink-specific setup.
type TracerProvider struct {
	provider *sdktrace.TracerProvider
	config   *config.OpenTelemetryConfig
}

// NewTracerProvider creates a new TracerProvider from the given configuration.
// Returns nil if tracing is not enabled or config is nil.
func NewTracerProvider(ctx context.Context, cfg *config.OpenTelemetryConfig) (*TracerProvider, error) {
	if cfg == nil || !cfg.Enabled {
		return nil, nil
	}

	// Create exporter based on protocol
	var exporter sdktrace.SpanExporter
	var err error

	opts := []otlptracegrpc.Option{}
	httpOpts := []otlptracehttp.Option{}

	// Set endpoint if configured
	if cfg.OTLPEndpoint != "" {
		opts = append(opts, otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint))
		httpOpts = append(httpOpts, otlptracehttp.WithEndpoint(cfg.OTLPEndpoint))
	}

	switch cfg.GetOTLPProtocol() {
	case "grpc":
		exporter, err = otlptracegrpc.New(ctx, opts...)
	case "http":
		exporter, err = otlptracehttp.New(ctx, httpOpts...)
	default:
		return nil, fmt.Errorf("unsupported OTLP protocol: %s", cfg.GetOTLPProtocol())
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	// Build resource attributes
	attrs := []attribute.KeyValue{
		semconv.ServiceName(cfg.GetServiceName()),
		semconv.ServiceVersion("0.1.0"), // TODO: inject version
	}

	// Add extra attributes from config
	for k, v := range cfg.ExtraAttributes {
		attrs = append(attrs, attribute.String(k, v))
	}

	// Create resource with service name and extra attributes
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, attrs...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create sampler based on sampling rate
	var sampler sdktrace.Sampler
	rate := cfg.GetSamplingRate()
	if rate >= 1.0 {
		sampler = sdktrace.AlwaysSample()
	} else if rate <= 0.0 {
		sampler = sdktrace.NeverSample()
	} else {
		sampler = sdktrace.TraceIDRatioBased(rate)
	}

	// Create tracer provider
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	// Set as global provider
	otel.SetTracerProvider(provider)

	// Set up propagator for W3C trace context
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &TracerProvider{
		provider: provider,
		config:   cfg,
	}, nil
}

// Tracer returns a tracer with the given name.
func (tp *TracerProvider) Tracer(name string) trace.Tracer {
	if tp == nil || tp.provider == nil {
		return otel.Tracer(name) // Returns a no-op tracer
	}
	return tp.provider.Tracer(name)
}

// Shutdown gracefully shuts down the tracer provider.
func (tp *TracerProvider) Shutdown(ctx context.Context) error {
	if tp == nil || tp.provider == nil {
		return nil
	}
	return tp.provider.Shutdown(ctx)
}

// Config returns the OpenTelemetry configuration.
func (tp *TracerProvider) Config() *config.OpenTelemetryConfig {
	if tp == nil {
		return nil
	}
	return tp.config
}

// Enabled returns true if tracing is enabled.
func (tp *TracerProvider) Enabled() bool {
	return tp != nil && tp.provider != nil
}

// Common span attribute keys used throughout pglink.
const (
	AttrDBUser           = "db.user"
	AttrDBName           = "db.name"
	AttrDBStatement      = "db.statement"
	AttrDBOperation      = "db.operation"
	AttrApplicationName  = "application_name"
	AttrApplicationNameQ = "application_name.query"
	AttrQueryType        = "pglink.query_type"
	AttrRowCount         = "pglink.row_count"
	AttrByteCount        = "pglink.byte_count"
	AttrStatementName    = "pglink.statement_name"
	AttrPortalName       = "pglink.portal_name"
)

// SessionAttributes returns common attributes for a session.
func SessionAttributes(user, database, appName string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String(AttrDBUser, user),
		attribute.String(AttrDBName, database),
	}
	if appName != "" {
		attrs = append(attrs, attribute.String(AttrApplicationName, appName))
	}
	return attrs
}

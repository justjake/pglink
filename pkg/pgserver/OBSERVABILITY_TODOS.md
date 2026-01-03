# Observability TODOs for pkg/pgserver

## Goal
Add instrumentation hooks that support both **metrics** and **distributed tracing** without hardcoding any specific backend (Prometheus, OTel, Jaeger, etc.).

## Design Principles
1. **Unopinionated**: No dependency on specific observability backends
2. **Supports both metrics AND tracing**: Start/End pattern allows span creation
3. **Zero overhead when disabled**: Nil/noop default
4. **Context propagation**: Start hooks return context (for span attachment)
5. **Familiar pattern**: Matches `pgx.QueryTracer` in vendor

## Recommended Approach: Start/End Hooks (like pgx)

**Why Start/End pairs?**
- **Metrics only** could use consolidated `OnX(info)` hooks
- **Tracing** requires Start hook to create span and return context with span attached, then End hook to finish it

```go
// pgx pattern for reference
TraceQueryStart(ctx, conn, data) context.Context  // Returns ctx with span
TraceQueryEnd(ctx, conn, data)                    // Finishes span from ctx
```

## Interface Design

```go
// pkg/pgserver/tracer.go

// ServerTracer provides hooks for instrumenting server operations.
// Supports both metrics collection and distributed tracing.
//
// Start methods may return a modified context (e.g., with a span attached).
// End methods receive that context to complete the operation.
//
// Implementations can embed NoopServerTracer and override only needed methods.
type ServerTracer interface {
    // Connection lifecycle
    TraceConnStart(ctx context.Context, info TraceConnStartInfo) context.Context
    TraceConnEnd(ctx context.Context, info TraceConnEndInfo)

    // TLS handshake (only called if TLS is negotiated)
    TraceTLSStart(ctx context.Context, info TraceTLSStartInfo) context.Context
    TraceTLSEnd(ctx context.Context, info TraceTLSEndInfo)

    // Authentication
    TraceAuthStart(ctx context.Context, info TraceAuthStartInfo) context.Context
    TraceAuthEnd(ctx context.Context, info TraceAuthEndInfo)

    // Startup (post-auth, pre-handler)
    TraceStartupStart(ctx context.Context, info TraceStartupStartInfo) context.Context
    TraceStartupEnd(ctx context.Context, info TraceStartupEndInfo)

    // Cancel requests (separate TCP connections)
    TraceCancelStart(ctx context.Context, info TraceCancelStartInfo) context.Context
    TraceCancelEnd(ctx context.Context, info TraceCancelEndInfo)
}

// Start info structs - data available at operation start

type TraceConnStartInfo struct {
    RemoteAddr net.Addr
    LocalAddr  net.Addr
}

type TraceTLSStartInfo struct {
    RemoteAddr net.Addr
}

type TraceAuthStartInfo struct {
    RemoteAddr net.Addr
    User       string
    Database   string
}

type TraceStartupStartInfo struct {
    RemoteAddr net.Addr
    User       string
    Database   string
}

type TraceCancelStartInfo struct {
    RemoteAddr net.Addr
    ProcessID  uint32
    SecretKey  uint32
}

// End info structs - includes outcome data

type TraceConnEndInfo struct {
    Error error  // nil if clean close
}

type TraceTLSEndInfo struct {
    Error   error
    Version uint16  // TLS version negotiated (0 if failed)
}

type TraceAuthEndInfo struct {
    Error error
}

type TraceStartupEndInfo struct {
    Error error
}

type TraceCancelEndInfo struct {
    Error error
}

// NoopServerTracer is the default implementation.
type NoopServerTracer struct{}

func (NoopServerTracer) TraceConnStart(ctx context.Context, _ TraceConnStartInfo) context.Context { return ctx }
func (NoopServerTracer) TraceConnEnd(context.Context, TraceConnEndInfo) {}
func (NoopServerTracer) TraceTLSStart(ctx context.Context, _ TraceTLSStartInfo) context.Context { return ctx }
func (NoopServerTracer) TraceTLSEnd(context.Context, TraceTLSEndInfo) {}
func (NoopServerTracer) TraceAuthStart(ctx context.Context, _ TraceAuthStartInfo) context.Context { return ctx }
func (NoopServerTracer) TraceAuthEnd(context.Context, TraceAuthEndInfo) {}
func (NoopServerTracer) TraceStartupStart(ctx context.Context, _ TraceStartupStartInfo) context.Context { return ctx }
func (NoopServerTracer) TraceStartupEnd(context.Context, TraceStartupEndInfo) {}
func (NoopServerTracer) TraceCancelStart(ctx context.Context, _ TraceCancelStartInfo) context.Context { return ctx }
func (NoopServerTracer) TraceCancelEnd(context.Context, TraceCancelEndInfo) {}
```

## ServerConfig Integration

```go
type ServerConfig struct {
    // ... existing fields ...

    // Tracer provides hooks for instrumenting server operations.
    // Supports both metrics and distributed tracing.
    // If nil, a no-op implementation is used.
    Tracer ServerTracer
}
```

## Instrumentation Points in server.go

| Phase | Start Hook | End Hook | Notes |
|-------|------------|----------|-------|
| Connection | `TraceConnStart` (after Accept) | `TraceConnEnd` (in defer) | Spans entire connection lifetime |
| TLS | `TraceTLSStart` (before handshake) | `TraceTLSEnd` (after) | Only if TLS negotiated |
| Auth | `TraceAuthStart` (before AuthHandler) | `TraceAuthEnd` (after) | Includes user/db info |
| Startup | `TraceStartupStart` (before StartupHandler) | `TraceStartupEnd` (after) | Post-auth setup |
| Cancel | `TraceCancelStart` (on cancel conn) | `TraceCancelEnd` (after handler) | Separate connections |

## Usage Examples

### Metrics-only Implementation (Prometheus)

```go
type PrometheusTracer struct {
    pgserver.NoopServerTracer
    connTotal     prometheus.Counter
    activeConns   prometheus.Gauge
    connDuration  prometheus.Histogram
    connStartTime sync.Map  // ctx -> time.Time
}

func (t *PrometheusTracer) TraceConnStart(ctx context.Context, info pgserver.TraceConnStartInfo) context.Context {
    t.connTotal.Inc()
    t.activeConns.Inc()
    t.connStartTime.Store(ctx, time.Now())
    return ctx
}

func (t *PrometheusTracer) TraceConnEnd(ctx context.Context, info pgserver.TraceConnEndInfo) {
    t.activeConns.Dec()
    if start, ok := t.connStartTime.LoadAndDelete(ctx); ok {
        t.connDuration.Observe(time.Since(start.(time.Time)).Seconds())
    }
}
```

### Tracing Implementation (OTel)

```go
type OTelTracer struct {
    pgserver.NoopServerTracer
    tracer trace.Tracer
}

func (t *OTelTracer) TraceConnStart(ctx context.Context, info pgserver.TraceConnStartInfo) context.Context {
    ctx, _ = t.tracer.Start(ctx, "pgserver.connection",
        trace.WithAttributes(
            attribute.String("net.peer.addr", info.RemoteAddr.String()),
        ),
    )
    return ctx
}

func (t *OTelTracer) TraceConnEnd(ctx context.Context, info pgserver.TraceConnEndInfo) {
    span := trace.SpanFromContext(ctx)
    if info.Error != nil {
        span.RecordError(info.Error)
        span.SetStatus(codes.Error, info.Error.Error())
    }
    span.End()
}

func (t *OTelTracer) TraceAuthStart(ctx context.Context, info pgserver.TraceAuthStartInfo) context.Context {
    ctx, _ = t.tracer.Start(ctx, "pgserver.auth",
        trace.WithAttributes(
            attribute.String("db.user", info.User),
            attribute.String("db.name", info.Database),
        ),
    )
    return ctx
}

func (t *OTelTracer) TraceAuthEnd(ctx context.Context, info pgserver.TraceAuthEndInfo) {
    span := trace.SpanFromContext(ctx)
    if info.Error != nil {
        span.RecordError(info.Error)
    }
    span.End()
}
```

### Combined Metrics + Tracing

```go
type CompositeTracer struct {
    tracers []pgserver.ServerTracer
}

func (c *CompositeTracer) TraceConnStart(ctx context.Context, info pgserver.TraceConnStartInfo) context.Context {
    for _, t := range c.tracers {
        ctx = t.TraceConnStart(ctx, info)
    }
    return ctx
}
// ... etc for each method
```

## Implementation Notes

1. Start hooks are called *before* the operation, End hooks *after*
2. Context returned from Start is passed to End (and subsequent operations)
3. The connection-level context flows through all child operations
4. Hook calls should be non-blocking (user's responsibility)
5. Consider adding `time.Time` fields in EndInfo structs for convenience

## Metrics Worth Tracking

**Counters**:
- Connections accepted/closed
- TLS handshakes (success/failure)
- Auth attempts (success/failure by method)
- Startup completions (success/failure)
- Cancel requests

**Histograms** (user calculates from Start/End timing):
- Connection lifetime
- TLS handshake duration
- Auth duration
- Startup duration

**Gauges**:
- Active connections (increment on Start, decrement on End)

## Files to Create/Modify

| File | Changes |
|------|---------|
| `pkg/pgserver/tracer.go` | New file: interface, info structs, noop impl |
| `pkg/pgserver/server.go` | Add Tracer to ServerConfig, call hooks at instrumentation points |

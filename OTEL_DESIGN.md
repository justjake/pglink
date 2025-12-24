# OpenTelemetry Observability and Prometheus Metrics Design

## Overview

Add distributed tracing via OpenTelemetry and operational metrics via Prometheus to pglink. Leverage pgx's built-in tracer interfaces via the `otelpgx` library.

## Configuration

### Config Keys
- `"opentelemetry"` - OpenTelemetry tracing configuration
- `"prometheus"` - Prometheus metrics configuration

### Example Configuration

```json
{
  "listen": ":16432",
  "opentelemetry": {
    "enabled": true,
    "service_name": "pglink-prod",
    "otlp_endpoint": "otel-collector:4317",
    "otlp_protocol": "grpc",
    "sampling_rate": 0.1,
    "include_query_text": false,
    "traceparent_startup_parameter": "traceparent",
    "traceparent_sql_regex": true
  },
  "prometheus": {
    "listen": ":9090",
    "path": "/metrics"
  },
  "databases": { ... }
}
```

### OpenTelemetry Config Fields
| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | false | Enable tracing |
| `service_name` | string | "pglink" | Service name in traces |
| `otlp_endpoint` | string | env var | OTLP collector endpoint |
| `otlp_protocol` | string | "grpc" | "grpc" or "http" |
| `sampling_rate` | float | 1.0 | Sampling rate 0.0-1.0 |
| `include_query_text` | bool | false | Include SQL in spans (security risk) |
| `traceparent_startup_parameter` | string | "traceparent" | Startup parameter name for W3C trace context |
| `traceparent_sql_regex` | bool/string | **REQUIRED** | Trace context extraction from SQL comments (see below) |
| `application_name_sql_regex` | bool/string | false | Per-query application_name extraction from SQL (see below) |

#### traceparent_sql_regex (required)

This field controls extraction of W3C trace context from SQL comments. **Must be explicitly set.**

- `true` - Enable with default regex: `/*traceparent='([^']+)'*/`
- `false` - Disable SQL comment extraction
- `"custom regex"` - Custom regex with capture group for traceparent value

Example SQL with trace context:
```sql
/*traceparent='00-866e8698eac009878483cfd029f62af9-bb14062982c63db3-01'*/ SELECT * FROM users
```

#### application_name_sql_regex (optional)

Extract per-query `application_name` from SQL to include in spans. Useful for identifying which part of an application issued a query.

- `true` - Enable with default regex that matches both:
  - Comments: `/\*\s*application_name\s*=\s*([^\s*]+)\s*\*/`
  - SET statements: `SET\s+(?:LOCAL\s+)?application_name\s*=\s*'([^']+)'`
- `false` - Disable (default)
- `"custom regex"` - Custom regex with capture group for application_name value

Examples:
```sql
/* application_name=checkout-service */ SELECT * FROM orders
SET application_name = 'batch-processor'; SELECT * FROM jobs
SET LOCAL application_name = 'migration-v2'; ALTER TABLE ...
```

#### Span Attributes

All spans include:
- `db.user` - from session startup
- `db.name` - database name from session startup
- `application_name` - from session startup parameter (always included)
- `application_name.query` - per-query override if `application_name_sql_regex` matched (optional)

### Prometheus Config Fields

If the `prometheus` section is present, metrics are enabled. No `enabled` field needed.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `listen` | string | ":9090" | HTTP server address (host:port/path format) |
| `path` | string | "/metrics" | Metrics endpoint path |

**CLI Override:** `-prometheus-listen=":9090/metrics"` enables Prometheus and overrides config.

**Environment Variable:** `PGLINK_PROMETHEUS_LISTEN=":9090/metrics"`

## Distributed Tracing Approach

PostgreSQL wire protocol doesn't natively support trace context propagation. Implementation:

1. **Session-level tracing**: Each client session gets a root span
2. **Optional startup parameter extraction**: If client sends `traceparent` startup parameter, extract and use as parent context (enables full distributed tracing for modified clients)

### Span Hierarchy
```
pglink.session (root, per client connection)
├── pglink.backend.acquire (pool acquisition)
├── pglink.query.simple (simple query)
│   └── pgx spans (via otelpgx)
└── pglink.query.extended (extended query)
    └── pgx spans (via otelpgx)
```

## Prometheus Metrics

### Counters
- `pglink_client_connections_total{database, user}` - Total connections
- `pglink_queries_total{database, user, query_type, status}` - Total queries
- `pglink_backend_acquire_total{database, status}` - Pool acquisitions
- `pglink_errors_total{type}` - Errors by type

### Gauges
- `pglink_client_connections_active{database, user}` - Active connections
- `pglink_backend_pool_connections_total{database}` - Pool size
- `pglink_backend_pool_connections_idle{database}` - Idle connections

### Histograms
- `pglink_query_duration_seconds{database, user, query_type}` - Query latency
- `pglink_backend_acquire_duration_seconds{database}` - Pool acquire time

## Implementation

### New Files

1. **`pkg/config/opentelemetry.go`**
   - `OpenTelemetryConfig` struct with fields above
   - `Validate()` method
   - Getter methods with defaults

2. **`pkg/config/prometheus.go`**
   - `PrometheusConfig` struct with fields above
   - `Validate()` method
   - Getter methods with defaults

3. **`pkg/observability/tracer.go`**
   - `TracerProvider` struct wrapping OTEL SDK
   - `NewTracerProvider(cfg)` - initialize exporter, sampler, resource
   - `Shutdown()` for graceful cleanup

4. **`pkg/observability/metrics.go`**
   - Prometheus metric definitions (counters, gauges, histograms)
   - Global vars using `promauto`

5. **`pkg/observability/metrics_server.go`**
   - `MetricsServer` struct
   - HTTP server exposing `/metrics` via `promhttp.Handler()`

6. **`pkg/pgwire/flow.go`**
   - `Flow` interface
   - `FlowRecognizer` interface
   - `FlowType` enum (SimpleQuery, Prepare, Execute, CopyIn, CopyOut)
   - `FlowData` struct

7. **`pkg/pgwire/flow_simple_query.go`**
   - `SimpleQueryRecognizer` - detects `Query` client message
   - `SimpleQueryFlow` - tracks until `ReadyForQuery`

8. **`pkg/pgwire/flow_extended_query.go`**
   - `ExtendedQueryRecognizer` - detects `Parse` client message
   - `PrepareFlow` - tracks Parse+Describe+Sync
   - `ExecuteFlow` - tracks Bind+Execute+Sync

9. **`pkg/pgwire/flow_copy.go`**
   - `CopyRecognizer` - detects `CopyInResponse`/`CopyOutResponse` server messages
   - `CopyInFlow`, `CopyOutFlow` - track until `ReadyForQuery`

10. **`pkg/pgwire/flow_test.go`**
    - Unit tests for flow recognizers with mock message sequences

### Modified Files

1. **`pkg/config/config.go`**
   - Add `OpenTelemetry *OpenTelemetryConfig` field
   - Add `Prometheus *PrometheusConfig` field
   - Update `Validate()` to validate both if non-nil

2. **`cmd/pglink/main.go`**
   - Add `-prometheus-listen` flag (overrides config, enables if set)
   - Initialize `TracerProvider` if opentelemetry section present
   - Start `MetricsServer` if prometheus config present or CLI flag set
   - Pass tracer to `frontend.NewService()`
   - Graceful shutdown of both

3. **`pkg/frontend/service.go`**
   - Accept tracer in `NewService()`
   - Start goroutine to collect pool metrics periodically
   - Pass tracer to sessions

4. **`pkg/frontend/session.go`**
   - Add `tracer` field to Session
   - Create tracing recognizers with callbacks via `setupFlowTracing()`
   - Extract trace context from startup params (if present)
   - Create session span in `Run()`
   - Define `TracingSimpleQueryRecognizer`, `TracingExtendedQueryRecognizer`, `TracingCopyRecognizer`

5. **`pkg/pgwire/protocol_state.go`**
   - Add `activeFlows []Flow` field
   - Add `recognizers []FlowRecognizer` field
   - Add `AddRecognizer()` method
   - Add `processFlows()` called from message handlers

6. **`pkg/backend/database.go`**
   - Accept tracer provider in constructor
   - Set `cfg.ConnConfig.Tracer` to `otelpgx.NewTracer()` in `poolConfigForUser()`

7. **`go.mod`**
   - Add OTEL dependencies
   - Add prometheus/client_golang
   - Add github.com/exaring/otelpgx

### Dependencies to Add

```
go.opentelemetry.io/otel v1.24.0
go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.24.0
go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.24.0
go.opentelemetry.io/otel/sdk v1.24.0
go.opentelemetry.io/otel/trace v1.24.0
github.com/exaring/otelpgx v0.6.0
github.com/prometheus/client_golang v1.19.0
```

## Implementation Order

1. **Config** - Add OpenTelemetryConfig and PrometheusConfig structs
2. **pgwire flows** - Add Flow/FlowRecognizer interfaces and implementations
3. **pgwire integration** - Integrate flow tracking into ProtocolState
4. **Observability package** - Create tracer provider and metrics definitions
5. **Backend integration** - Add otelpgx tracer to pgxpool config (ConnectTracer)
6. **Frontend integration** - Subscribe to flow callbacks, create spans, record metrics
7. **Main integration** - Wire up initialization and shutdown
8. **Testing** - Unit tests for flows, integration tests with OTEL collector and Prometheus

## Flow Recognition Architecture (pkg/pgwire)

Queries are multi-message flows, not single request-response pairs. We need state machine tracking to recognize when flows start and end.

### Design Principles
- **pkg/pgwire** recognizes flows and captures data (SQL, command tag, errors)
- **pkg/pgwire** is unaware of tracing - just reports "what is happening"
- **frontend.Session** subscribes to flow events and invokes tracers

### Flow Interface

```go
// pkg/pgwire/flow.go

// Flow tracks a multi-message protocol flow (query, prepare, copy, etc.)
type Flow interface {
    // UpdateHandlers returns handlers to update flow state from messages.
    // Handler returns false when flow is complete.
    UpdateHandlers(state *ProtocolState) MessageHandlers[bool]

    // Close is called when flow ends (complete or timeout/eviction).
    // The flow implementation handles its own cleanup and callbacks.
    Close()
}

// FlowRecognizer detects when new flows start
type FlowRecognizer interface {
    // StartHandlers returns handlers that detect flow starts.
    // Returns non-nil Flow when a new flow begins.
    // The recognizer is responsible for passing any callbacks (like onClose)
    // to the flows it creates.
    StartHandlers(state *ProtocolState) MessageHandlers[Flow]
}
```

**Key insight:** The recognizer passes `onClose` callbacks to flows it creates. This means `frontend.Session` creates recognizers that encapsulate all tracing/metrics logic. `pkg/pgwire` stays generic and unaware of observability.

### Message Flow Definitions

All flows end on `ReadyForQuery` from server.

| Flow Type | Client Start | Server End | Data Captured |
|-----------|--------------|------------|---------------|
| SimpleQuery | `Query` | `ReadyForQuery` | SQL, CommandTag, Error |
| Prepare | `Parse` [+ `Sync`] + `Describe` + `Sync` | `ReadyForQuery` | SQL, StatementName, ParameterOIDs |
| Execute | `Bind` + `Execute` + `Sync` | `ReadyForQuery` | StatementName, CommandTag, Error |
| ExecParams | `Parse` [+ `Sync`] + `Bind` [+ `Describe`] + `Execute` + `Sync` | `ReadyForQuery` | SQL, CommandTag, Error |
| CopyIn | `Query` (COPY FROM) → `CopyInResponse` | `ReadyForQuery` | SQL, CommandTag, Error |
| CopyOut | `Query` (COPY TO) → `CopyOutResponse` | `ReadyForQuery` | SQL, CommandTag, Error |

**Note:** Non-pgx clients may structure extended query flows differently. `[+ Sync]` indicates optional sync points that some clients send after Parse or Describe. The flow recognizer must handle these variations.

### Flow Recognizers to Implement

```go
// pkg/pgwire/flow_recognizers.go

// SimpleQueryRecognizer detects Query messages
type SimpleQueryRecognizer struct{}

func (r *SimpleQueryRecognizer) StartHandlers(state *ProtocolState) MessageHandlers[Flow] {
    return MessageHandlers[Flow]{
        ClientSimpleQuery: func(msg *pgproto3.Query) Flow {
            return &SimpleQueryFlow{
                sql:       msg.String,
                startTime: time.Now(),
            }
        },
    }
}

// ExtendedQueryRecognizer detects Parse messages (start of extended query)
type ExtendedQueryRecognizer struct{}

// CopyRecognizer detects CopyInResponse/CopyOutResponse from server
type CopyRecognizer struct{}
```

### SimpleQueryFlow Example

```go
// Created by a recognizer in frontend.Session with tracing callback injected
type SimpleQueryFlow struct {
    sql        string
    startTime  time.Time
    endTime    time.Time
    commandTag pgconn.CommandTag
    err        error
    onClose    func(*SimpleQueryFlow) // injected by recognizer
}

func (f *SimpleQueryFlow) UpdateHandlers(state *ProtocolState) MessageHandlers[bool] {
    return MessageHandlers[bool]{
        ServerCommandComplete: func(msg *pgproto3.CommandComplete) bool {
            f.commandTag = pgconn.CommandTag(msg.CommandTag)
            return true // continue
        },
        ServerErrorResponse: func(msg *pgproto3.ErrorResponse) bool {
            f.err = pgconn.ErrorResponseToPgError(msg)
            return true // continue until ReadyForQuery
        },
        ServerReadyForQuery: func(msg *pgproto3.ReadyForQuery) bool {
            f.endTime = time.Now()
            return false // FLOW COMPLETE
        },
    }
}

func (f *SimpleQueryFlow) Close() {
    if f.onClose != nil {
        f.onClose(f) // pass self - callback has access to all captured data
    }
}
```

### Integration with ProtocolState

```go
// pkg/pgwire/protocol_state.go

type ProtocolState struct {
    // ... existing fields

    // Active flows being tracked
    activeFlows []Flow
    recognizers []FlowRecognizer
}

func (s *ProtocolState) AddRecognizer(r FlowRecognizer) {
    s.recognizers = append(s.recognizers, r)
}

// Called for each message to update active flows and detect new ones
func (s *ProtocolState) processFlows(msg any) {
    // Check if any recognizer detects a new flow
    for _, r := range s.recognizers {
        handlers := r.StartHandlers(s)
        if flow := handlers.Handle(msg); flow != nil {
            s.activeFlows = append(s.activeFlows, flow)
        }
    }

    // Update active flows, remove completed ones
    remaining := s.activeFlows[:0]
    for _, flow := range s.activeFlows {
        handlers := flow.UpdateHandlers(s)
        if handlers.Handle(msg) { // returns true to continue
            remaining = append(remaining, flow)
        } else {
            flow.Close() // flow handles its own callbacks
        }
    }
    s.activeFlows = remaining
}
```

## Tracing Integration (frontend.Session)

Session creates recognizers with tracing callbacks injected:

```go
// frontend/session.go

func (s *Session) setupFlowTracing() {
    // Create recognizers that encapsulate tracing logic
    s.state.AddRecognizer(&TracingSimpleQueryRecognizer{
        tracer:           s.tracer,
        sqlRegex:         s.cfg.TraceparentSQLRegex(),
        includeQueryText: s.cfg.IncludeQueryText(),
        metrics:          s.metrics,
    })
    s.state.AddRecognizer(&TracingExtendedQueryRecognizer{...})
    s.state.AddRecognizer(&TracingCopyRecognizer{...})
}

// TracingSimpleQueryRecognizer creates flows with tracing callbacks
type TracingSimpleQueryRecognizer struct {
    session              *Session // for startup params like application_name
    tracer               trace.Tracer
    traceparentRegex     *regexp.Regexp
    appNameRegex         *regexp.Regexp
    includeQueryText     bool
    metrics              *Metrics
}

func (r *TracingSimpleQueryRecognizer) StartHandlers(state *ProtocolState) MessageHandlers[Flow] {
    return MessageHandlers[Flow]{
        ClientSimpleQuery: func(msg *pgproto3.Query) Flow {
            // Extract trace context from SQL if configured
            ctx := r.extractTraceContext(msg.String)

            // Start span with session-level attributes
            ctx, span := r.tracer.Start(ctx, "pglink.query.simple",
                trace.WithAttributes(
                    attribute.String("db.user", r.session.User()),
                    attribute.String("db.name", r.session.Database()),
                    attribute.String("application_name", r.session.ApplicationName()),
                ),
            )

            // Extract per-query application_name if configured
            queryAppName := r.extractAppName(msg.String)

            return &SimpleQueryFlow{
                sql:       msg.String,
                startTime: time.Now(),
                onClose: func(f *SimpleQueryFlow) {
                    // Add query-specific attributes
                    if r.includeQueryText {
                        span.SetAttributes(attribute.String("db.statement", f.sql))
                    }
                    span.SetAttributes(attribute.String("db.operation", f.commandTag.String()))
                    if queryAppName != "" {
                        span.SetAttributes(attribute.String("application_name.query", queryAppName))
                    }
                    if f.err != nil {
                        span.RecordError(f.err)
                        span.SetStatus(codes.Error, f.err.Error())
                    }
                    span.End()

                    // Record metrics
                    r.metrics.RecordQuery(f)
                },
            }
        },
    }
}
```

## pgx Tracer on pgxpool

Still set tracer on pgxpool config to capture:
- `ConnectTracer` spans (connection establishment)
- Any automatic health check queries

```go
// pkg/backend/database.go
cfg.ConnConfig.Tracer = otelpgx.NewTracer(...)
```

### Trace Context Extraction

Two methods for extracting W3C trace context, both configurable:

#### 1. Startup Parameter Extraction
Check startup parameters using configured parameter name:
```go
paramName := cfg.GetTraceparentStartupParameter() // default: "traceparent"
if traceparent, ok := startupParams[paramName]; ok {
    ctx = otel.GetTextMapPropagator().Extract(ctx, ...)
}
```

#### 2. SQL Comment Extraction (via traceparent_sql_regex)
Extract from SQL comments when flow starts:
```go
// If traceparent_sql_regex is true, use default:
//   /*traceparent='([^']+)'*/
// If string, compile as custom regex
if match := cfg.TraceparentSQLRegex().FindStringSubmatch(sql); len(match) > 1 {
    ctx = otel.GetTextMapPropagator().Extract(ctx, ...)
}
```

This is extracted in FlowData and passed to Session's flow callback.

### Metric Recording Points
- Session start/end: connection counters
- `acquireBackend()`: acquire duration, acquire total
- `runSimpleQueryWithBackend()`/`runExtendedQueryWithBackend()`: query duration, query total
- Error handlers: error counters
- Periodic goroutine: pool stats gauges

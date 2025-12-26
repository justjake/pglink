package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for pglink.
type Metrics struct {
	// Counters
	ClientConnectionsTotal *prometheus.CounterVec
	QueriesTotal           *prometheus.CounterVec
	BackendAcquireTotal    *prometheus.CounterVec
	ErrorsTotal            *prometheus.CounterVec

	// Prepared statement cache counters
	PreparedStatementCacheHitsTotal      *prometheus.CounterVec
	PreparedStatementCacheMissesTotal    *prometheus.CounterVec
	PreparedStatementRecreationsTotal    *prometheus.CounterVec
	PreparedStatementParseSkippedTotal   *prometheus.CounterVec
	PreparedStatementCacheEvictionsTotal *prometheus.CounterVec

	// Gauges
	ClientConnectionsActive     *prometheus.GaugeVec
	BackendPoolConnectionsTotal *prometheus.GaugeVec
	BackendPoolConnectionsIdle  *prometheus.GaugeVec
	PreparedStatementCacheSize  *prometheus.GaugeVec

	// Histograms
	QueryDuration          *prometheus.HistogramVec
	BackendAcquireDuration *prometheus.HistogramVec
}

// DefaultMetrics creates a new Metrics instance with all metrics registered.
func DefaultMetrics() *Metrics {
	return &Metrics{
		// Counters
		ClientConnectionsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "pglink_client_connections_total",
				Help: "Total number of client connections",
			},
			[]string{"database", "user"},
		),
		QueriesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "pglink_queries_total",
				Help: "Total number of queries executed",
			},
			[]string{"database", "user", "query_type", "status"},
		),
		BackendAcquireTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "pglink_backend_acquire_total",
				Help: "Total number of backend connection acquisitions",
			},
			[]string{"database", "status"},
		),
		ErrorsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "pglink_errors_total",
				Help: "Total number of errors by type",
			},
			[]string{"type"},
		),

		// Prepared statement cache counters
		PreparedStatementCacheHitsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "pglink_prepared_statement_cache_hits_total",
				Help: "Total number of prepared statement cache hits (statement found in cache)",
			},
			[]string{"database"},
		),
		PreparedStatementCacheMissesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "pglink_prepared_statement_cache_misses_total",
				Help: "Total number of prepared statement cache misses (statement not found in cache)",
			},
			[]string{"database"},
		),
		PreparedStatementRecreationsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "pglink_prepared_statement_recreations_total",
				Help: "Total number of prepared statements re-created on backend (using cached query)",
			},
			[]string{"database"},
		),
		PreparedStatementParseSkippedTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "pglink_prepared_statement_parse_skipped_total",
				Help: "Total number of Parse messages skipped (statement already exists on backend)",
			},
			[]string{"database"},
		),
		PreparedStatementCacheEvictionsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "pglink_prepared_statement_cache_evictions_total",
				Help: "Total number of prepared statements evicted from cache due to LRU",
			},
			[]string{"database"},
		),

		// Gauges
		ClientConnectionsActive: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pglink_client_connections_active",
				Help: "Number of active client connections",
			},
			[]string{"database", "user"},
		),
		BackendPoolConnectionsTotal: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pglink_backend_pool_connections_total",
				Help: "Total connections in the backend pool",
			},
			[]string{"database"},
		),
		BackendPoolConnectionsIdle: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pglink_backend_pool_connections_idle",
				Help: "Idle connections in the backend pool",
			},
			[]string{"database"},
		),
		PreparedStatementCacheSize: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "pglink_prepared_statement_cache_size",
				Help: "Current number of prepared statements in the cache",
			},
			[]string{"database"},
		),

		// Histograms
		QueryDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "pglink_query_duration_seconds",
				Help:    "Query execution duration in seconds",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~32s
			},
			[]string{"database", "user", "query_type"},
		),
		BackendAcquireDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "pglink_backend_acquire_duration_seconds",
				Help:    "Time to acquire a backend connection in seconds",
				Buckets: prometheus.ExponentialBuckets(0.0001, 2, 15), // 0.1ms to ~3.2s
			},
			[]string{"database"},
		),
	}
}

// RecordClientConnection increments the connection counter and gauge.
func (m *Metrics) RecordClientConnection(database, user string) {
	if m == nil {
		return
	}
	m.ClientConnectionsTotal.WithLabelValues(database, user).Inc()
	m.ClientConnectionsActive.WithLabelValues(database, user).Inc()
}

// RecordClientDisconnect decrements the active connections gauge.
func (m *Metrics) RecordClientDisconnect(database, user string) {
	if m == nil {
		return
	}
	m.ClientConnectionsActive.WithLabelValues(database, user).Dec()
}

// RecordQuery records a query execution.
func (m *Metrics) RecordQuery(database, user, queryType string, durationSeconds float64, success bool) {
	if m == nil {
		return
	}
	status := "success"
	if !success {
		status = "error"
	}
	m.QueriesTotal.WithLabelValues(database, user, queryType, status).Inc()
	m.QueryDuration.WithLabelValues(database, user, queryType).Observe(durationSeconds)
}

// RecordBackendAcquire records a backend connection acquisition.
func (m *Metrics) RecordBackendAcquire(database string, durationSeconds float64, success bool) {
	if m == nil {
		return
	}
	status := "success"
	if !success {
		status = "error"
	}
	m.BackendAcquireTotal.WithLabelValues(database, status).Inc()
	m.BackendAcquireDuration.WithLabelValues(database).Observe(durationSeconds)
}

// RecordError records an error.
func (m *Metrics) RecordError(errorType string) {
	if m == nil {
		return
	}
	m.ErrorsTotal.WithLabelValues(errorType).Inc()
}

// UpdatePoolStats updates the backend pool stats gauges.
func (m *Metrics) UpdatePoolStats(database string, total, idle int) {
	if m == nil {
		return
	}
	m.BackendPoolConnectionsTotal.WithLabelValues(database).Set(float64(total))
	m.BackendPoolConnectionsIdle.WithLabelValues(database).Set(float64(idle))
}

// RecordPreparedStatementCacheHit records a cache hit for prepared statement lookup.
func (m *Metrics) RecordPreparedStatementCacheHit(database string) {
	if m == nil {
		return
	}
	m.PreparedStatementCacheHitsTotal.WithLabelValues(database).Inc()
}

// RecordPreparedStatementCacheMiss records a cache miss for prepared statement lookup.
func (m *Metrics) RecordPreparedStatementCacheMiss(database string) {
	if m == nil {
		return
	}
	m.PreparedStatementCacheMissesTotal.WithLabelValues(database).Inc()
}

// RecordPreparedStatementRecreation records when a statement is re-created on a backend.
func (m *Metrics) RecordPreparedStatementRecreation(database string) {
	if m == nil {
		return
	}
	m.PreparedStatementRecreationsTotal.WithLabelValues(database).Inc()
}

// RecordPreparedStatementParseSkipped records when a Parse is skipped because
// the statement already exists on the backend.
func (m *Metrics) RecordPreparedStatementParseSkipped(database string) {
	if m == nil {
		return
	}
	m.PreparedStatementParseSkippedTotal.WithLabelValues(database).Inc()
}

// RecordPreparedStatementCacheEviction records when a statement is evicted from the cache.
func (m *Metrics) RecordPreparedStatementCacheEviction(database string) {
	if m == nil {
		return
	}
	m.PreparedStatementCacheEvictionsTotal.WithLabelValues(database).Inc()
}

// UpdatePreparedStatementCacheSize updates the cache size gauge.
func (m *Metrics) UpdatePreparedStatementCacheSize(database string, size int) {
	if m == nil {
		return
	}
	m.PreparedStatementCacheSize.WithLabelValues(database).Set(float64(size))
}

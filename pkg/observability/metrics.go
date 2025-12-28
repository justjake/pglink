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
	TimeoutsTotal          *prometheus.CounterVec

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

	// Ring buffer metrics
	RingBufferMessagesTotal   *prometheus.CounterVec
	RingBufferBytesTotal      *prometheus.CounterVec
	RingBufferUtilization     *prometheus.GaugeVec
	RingBufferSessionsByState *prometheus.GaugeVec

	// =========================================================================
	// pgbouncer_exporter-compatible metrics
	// These metrics mirror those exposed by pgbouncer_exporter for compatibility
	// with existing monitoring dashboards.
	// =========================================================================

	// SHOW VERSION equivalent
	// pgbouncer_exporter: pgbouncer_version_info
	VersionInfo *prometheus.GaugeVec

	// Scrape status
	// pgbouncer_exporter: pgbouncer_up
	Up prometheus.Gauge

	// -------------------------------------------------------------------------
	// SHOW LISTS equivalents (11 metrics, 5 stubs)
	// -------------------------------------------------------------------------

	// pgbouncer_exporter: pgbouncer_databases
	// pgbouncer: Count of entries in database list
	Databases prometheus.Gauge

	// pgbouncer_exporter: pgbouncer_users
	// pgbouncer: Count of entries in user list
	Users prometheus.Gauge

	// pgbouncer_exporter: pgbouncer_pools
	// pgbouncer: Count of entries in pool list
	Pools prometheus.Gauge

	// pgbouncer_exporter: pgbouncer_free_clients
	// STUB: Go doesn't pre-allocate connection slots. Always 0.
	FreeClients prometheus.Gauge

	// pgbouncer_exporter: pgbouncer_used_clients
	// pgbouncer: Count of active client connections
	UsedClients prometheus.Gauge

	// pgbouncer_exporter: pgbouncer_login_clients
	// pgbouncer: Clients in authentication/login phase
	LoginClients prometheus.Gauge

	// pgbouncer_exporter: pgbouncer_free_servers
	// STUB: Go doesn't pre-allocate connection slots. Always 0.
	FreeServers prometheus.Gauge

	// pgbouncer_exporter: pgbouncer_used_servers
	// pgbouncer: Count of server connections in use
	UsedServers prometheus.Gauge

	// pgbouncer_exporter: pgbouncer_cached_dns_names
	// STUB: Uses Go stdlib DNS. Always 0.
	CachedDNSNames prometheus.Gauge

	// pgbouncer_exporter: pgbouncer_cached_dns_zones
	// STUB: Uses Go stdlib DNS. Always 0.
	CachedDNSZones prometheus.Gauge

	// pgbouncer_exporter: pgbouncer_in_flight_dns_queries
	// STUB: Uses Go stdlib DNS. Always 0.
	InFlightDNSQueries prometheus.Gauge

	// -------------------------------------------------------------------------
	// SHOW CONFIG equivalents (2 metrics)
	// -------------------------------------------------------------------------

	// pgbouncer_exporter: pgbouncer_config_max_client_connections
	// pgbouncer: Value of max_client_conn setting
	ConfigMaxClientConnections prometheus.Gauge

	// pgbouncer_exporter: pgbouncer_config_max_user_connections
	// pgbouncer: Value of max_user_connections setting
	ConfigMaxUserConnections prometheus.Gauge

	// -------------------------------------------------------------------------
	// SHOW DATABASES equivalents (6 metrics, 3 stubs)
	// Labels: database, backend_host, backend_port, pool_mode
	// -------------------------------------------------------------------------

	// pgbouncer_exporter: pgbouncer_databases_pool_size
	// pgbouncer: pool_size config value
	DatabasesPoolSize *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_databases_reserve_pool
	// STUB: No reserve pool concept. Always 0.
	DatabasesReservePool *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_databases_max_connections
	// pgbouncer: max_connections config value
	DatabasesMaxConnections *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_databases_current_connections
	// pgbouncer: Current count of server connections
	DatabasesCurrentConnections *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_databases_paused
	// STUB: Pause not implemented. Always 0.
	DatabasesPaused *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_databases_disabled
	// STUB: Disable not implemented. Always 0.
	DatabasesDisabled *prometheus.GaugeVec

	// -------------------------------------------------------------------------
	// SHOW STATS equivalents (12 counter metrics)
	// Labels: database
	// -------------------------------------------------------------------------

	// pgbouncer_exporter: pgbouncer_stats_queries_pooled_total
	// pgbouncer: query_count in PgStats, incremented in client.c:1597
	StatsQueriesPooledTotal *prometheus.CounterVec

	// pgbouncer_exporter: pgbouncer_stats_queries_duration_seconds_total
	// pgbouncer: query_time in microseconds, accumulated in server.c:584-586
	StatsQueriesDurationSecondsTotal *prometheus.CounterVec

	// pgbouncer_exporter: pgbouncer_stats_received_bytes_total
	// pgbouncer: client_bytes, packet len from client in client.c:1585,1612
	StatsReceivedBytesTotal *prometheus.CounterVec

	// pgbouncer_exporter: pgbouncer_stats_sent_bytes_total
	// pgbouncer: server_bytes, packet len to client in server.c:552
	StatsSentBytesTotal *prometheus.CounterVec

	// pgbouncer_exporter: pgbouncer_stats_queries_total
	// pgbouncer: requests field, same as query_count
	StatsQueriesTotal *prometheus.CounterVec

	// pgbouncer_exporter: pgbouncer_stats_client_wait_seconds_total
	// pgbouncer: wait_time in microseconds, in objects.c:837
	StatsClientWaitSecondsTotal *prometheus.CounterVec

	// pgbouncer_exporter: pgbouncer_stats_sql_transactions_pooled_total
	// pgbouncer: xact_count in client.c:1600, includes both explicit and implicit txns
	StatsSQLTransactionsPooledTotal *prometheus.CounterVec

	// pgbouncer_exporter: pgbouncer_stats_server_in_transaction_seconds_total
	// pgbouncer: xact_time in microseconds, in server.c:596-600
	StatsServerInTransactionSecondsTotal *prometheus.CounterVec

	// pgbouncer_exporter: pgbouncer_stats_client_parses_total
	// pgbouncer: ps_client_parse_count in prepare.c:333
	StatsClientParsesTotal *prometheus.CounterVec

	// pgbouncer_exporter: pgbouncer_stats_server_parses_total
	// pgbouncer: ps_server_parse_count in prepare.c:377
	StatsServerParsesTotal *prometheus.CounterVec

	// pgbouncer_exporter: pgbouncer_stats_binds_total
	// pgbouncer: ps_bind_count in prepare.c:521
	StatsBindsTotal *prometheus.CounterVec

	// pgbouncer_exporter: pgbouncer_stats_server_assignments_total
	// pgbouncer: server_assignment_count in objects.c:966
	StatsServerAssignmentsTotal *prometheus.CounterVec

	// -------------------------------------------------------------------------
	// SHOW POOLS equivalents (12 gauge metrics, 6 stubs)
	// Labels: database, user
	// -------------------------------------------------------------------------

	// pgbouncer_exporter: pgbouncer_pools_client_active_connections
	// pgbouncer: cl_active, clients linked to server
	PoolsClientActiveConnections *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_pools_client_active_cancel_connections
	// STUB: Different cancel model. Always 0.
	PoolsClientActiveCancelConnections *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_pools_client_waiting_connections
	// pgbouncer: cl_waiting, clients waiting for server
	PoolsClientWaitingConnections *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_pools_client_waiting_cancel_connections
	// STUB: Different cancel model. Always 0.
	PoolsClientWaitingCancelConnections *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_pools_server_active_connections
	// pgbouncer: sv_active, server connections linked to client
	PoolsServerActiveConnections *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_pools_server_active_cancel_connections
	// STUB: Different cancel model. Always 0.
	PoolsServerActiveCancelConnections *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_pools_server_being_canceled_connections
	// STUB: Different cancel model. Always 0.
	PoolsServerBeingCanceledConnections *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_pools_server_idle_connections
	// pgbouncer: sv_idle, idle server connections
	PoolsServerIdleConnections *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_pools_server_used_connections
	// STUB: pgxpool health check differs. Always 0.
	PoolsServerUsedConnections *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_pools_server_testing_connections
	// STUB: pgxpool health check differs. Always 0.
	PoolsServerTestingConnections *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_pools_server_login_connections
	// pgbouncer: sv_login, servers connecting to PostgreSQL
	PoolsServerLoginConnections *prometheus.GaugeVec

	// pgbouncer_exporter: pgbouncer_pools_client_maxwait_seconds
	// pgbouncer: maxwait, oldest unserved client age
	PoolsClientMaxWaitSeconds *prometheus.GaugeVec
}

// DefaultMetrics creates a new Metrics instance with all metrics registered using "pglink" prefix.
func DefaultMetrics() *Metrics {
	return NewMetrics("pglink")
}

// NewMetrics creates a new Metrics instance with all metrics registered using the given prefix.
// The prefix should be either "pglink" (default) or "pgbouncer" (for pgbouncer_exporter compatibility).
func NewMetrics(prefix string) *Metrics {
	return &Metrics{
		// Counters
		ClientConnectionsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_client_connections_total",
				Help: "Total number of client connections",
			},
			[]string{"database", "user"},
		),
		QueriesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_queries_total",
				Help: "Total number of queries executed",
			},
			[]string{"database", "user", "query_type", "status"},
		),
		BackendAcquireTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_backend_acquire_total",
				Help: "Total number of backend connection acquisitions",
			},
			[]string{"database", "status"},
		),
		ErrorsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_errors_total",
				Help: "Total number of errors by type",
			},
			[]string{"type"},
		),
		TimeoutsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "pglink_timeouts_total",
				Help: "Total number of timeouts triggered",
			},
			[]string{"database", "timeout_type", "outcome"},
			// timeout_type: "query", "idle_transaction", "transaction"
			// outcome: "canceled" (cancel worked), "terminated" (had to disconnect)
		),

		// Prepared statement cache counters
		PreparedStatementCacheHitsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_prepared_statement_cache_hits_total",
				Help: "Total number of prepared statement cache hits (statement found in cache)",
			},
			[]string{"database"},
		),
		PreparedStatementCacheMissesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_prepared_statement_cache_misses_total",
				Help: "Total number of prepared statement cache misses (statement not found in cache)",
			},
			[]string{"database"},
		),
		PreparedStatementRecreationsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_prepared_statement_recreations_total",
				Help: "Total number of prepared statements re-created on backend (using cached query)",
			},
			[]string{"database"},
		),
		PreparedStatementParseSkippedTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_prepared_statement_parse_skipped_total",
				Help: "Total number of Parse messages skipped (statement already exists on backend)",
			},
			[]string{"database"},
		),
		PreparedStatementCacheEvictionsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_prepared_statement_cache_evictions_total",
				Help: "Total number of prepared statements evicted from cache due to LRU",
			},
			[]string{"database"},
		),

		// Gauges
		ClientConnectionsActive: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_client_connections_active",
				Help: "Number of active client connections",
			},
			[]string{"database", "user"},
		),
		BackendPoolConnectionsTotal: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_backend_pool_connections_total",
				Help: "Total connections in the backend pool",
			},
			[]string{"database"},
		),
		BackendPoolConnectionsIdle: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_backend_pool_connections_idle",
				Help: "Idle connections in the backend pool",
			},
			[]string{"database"},
		),
		PreparedStatementCacheSize: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_prepared_statement_cache_size",
				Help: "Current number of prepared statements in the cache",
			},
			[]string{"database"},
		),

		// Histograms
		QueryDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    prefix + "_query_duration_seconds",
				Help:    "Query execution duration in seconds",
				Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~32s
			},
			[]string{"database", "user", "query_type"},
		),
		BackendAcquireDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    prefix + "_backend_acquire_duration_seconds",
				Help:    "Time to acquire a backend connection in seconds",
				Buckets: prometheus.ExponentialBuckets(0.0001, 2, 15), // 0.1ms to ~3.2s
			},
			[]string{"database"},
		),

		// Ring buffer metrics
		RingBufferMessagesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_ringbuffer_messages_total",
				Help: "Total messages processed by ring buffers",
			},
			[]string{"direction"}, // "frontend" or "backend"
		),
		RingBufferBytesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_ringbuffer_bytes_total",
				Help: "Total bytes processed by ring buffers",
			},
			[]string{"direction"},
		),
		RingBufferUtilization: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_ringbuffer_utilization_ratio",
				Help: "Current ring buffer utilization (0-1)",
			},
			[]string{"direction"},
		),
		RingBufferSessionsByState: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_ringbuffer_sessions_by_state",
				Help: "Number of sessions in each ring buffer state",
			},
			[]string{"direction", "state"},
		),

		// =====================================================================
		// pgbouncer_exporter-compatible metrics
		// =====================================================================

		// SHOW VERSION equivalent
		VersionInfo: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_version_info",
				Help: "The pglink version info (pgbouncer_exporter: pgbouncer_version_info)",
			},
			[]string{"version"},
		),

		// Scrape status
		Up: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: prefix + "_up",
				Help: "The pglink scrape succeeded (pgbouncer_exporter: pgbouncer_up)",
			},
		),

		// -----------------------------------------------------------------
		// SHOW LISTS equivalents
		// -----------------------------------------------------------------
		Databases: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: prefix + "_databases",
				Help: "Count of databases (pgbouncer_exporter: pgbouncer_databases)",
			},
		),
		Users: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: prefix + "_users",
				Help: "Count of users (pgbouncer_exporter: pgbouncer_users)",
			},
		),
		Pools: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: prefix + "_pools",
				Help: "Count of pools (pgbouncer_exporter: pgbouncer_pools)",
			},
		),
		FreeClients: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: prefix + "_free_clients",
				Help: "STUB: Go doesn't pre-allocate. Always 0 (pgbouncer_exporter: pgbouncer_free_clients)",
			},
		),
		UsedClients: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: prefix + "_used_clients",
				Help: "Count of used clients (pgbouncer_exporter: pgbouncer_used_clients)",
			},
		),
		LoginClients: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: prefix + "_login_clients",
				Help: "Count of clients in login state (pgbouncer_exporter: pgbouncer_login_clients)",
			},
		),
		FreeServers: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: prefix + "_free_servers",
				Help: "STUB: Go doesn't pre-allocate. Always 0 (pgbouncer_exporter: pgbouncer_free_servers)",
			},
		),
		UsedServers: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: prefix + "_used_servers",
				Help: "Count of used servers (pgbouncer_exporter: pgbouncer_used_servers)",
			},
		),
		CachedDNSNames: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: prefix + "_cached_dns_names",
				Help: "STUB: Uses Go stdlib DNS. Always 0 (pgbouncer_exporter: pgbouncer_cached_dns_names)",
			},
		),
		CachedDNSZones: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: prefix + "_cached_dns_zones",
				Help: "STUB: Uses Go stdlib DNS. Always 0 (pgbouncer_exporter: pgbouncer_cached_dns_zones)",
			},
		),
		InFlightDNSQueries: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: prefix + "_in_flight_dns_queries",
				Help: "STUB: Uses Go stdlib DNS. Always 0 (pgbouncer_exporter: pgbouncer_in_flight_dns_queries)",
			},
		),

		// -----------------------------------------------------------------
		// SHOW CONFIG equivalents
		// -----------------------------------------------------------------
		ConfigMaxClientConnections: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: prefix + "_config_max_client_connections",
				Help: "Config max client connections (pgbouncer_exporter: pgbouncer_config_max_client_connections)",
			},
		),
		ConfigMaxUserConnections: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: prefix + "_config_max_user_connections",
				Help: "Config max user connections (pgbouncer_exporter: pgbouncer_config_max_user_connections)",
			},
		),

		// -----------------------------------------------------------------
		// SHOW DATABASES equivalents
		// -----------------------------------------------------------------
		DatabasesPoolSize: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_databases_pool_size",
				Help: "Maximum number of server connections (pgbouncer_exporter: pgbouncer_databases_pool_size)",
			},
			[]string{"database", "backend_host", "backend_port", "pool_mode"},
		),
		DatabasesReservePool: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_databases_reserve_pool",
				Help: "STUB: No reserve pool concept. Always 0 (pgbouncer_exporter: pgbouncer_databases_reserve_pool)",
			},
			[]string{"database", "backend_host", "backend_port", "pool_mode"},
		),
		DatabasesMaxConnections: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_databases_max_connections",
				Help: "Maximum number of allowed connections (pgbouncer_exporter: pgbouncer_databases_max_connections)",
			},
			[]string{"database", "backend_host", "backend_port", "pool_mode"},
		),
		DatabasesCurrentConnections: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_databases_current_connections",
				Help: "Current number of connections (pgbouncer_exporter: pgbouncer_databases_current_connections)",
			},
			[]string{"database", "backend_host", "backend_port", "pool_mode"},
		),
		DatabasesPaused: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_databases_paused",
				Help: "STUB: Pause not implemented. Always 0 (pgbouncer_exporter: pgbouncer_databases_paused)",
			},
			[]string{"database", "backend_host", "backend_port", "pool_mode"},
		),
		DatabasesDisabled: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_databases_disabled",
				Help: "STUB: Disable not implemented. Always 0 (pgbouncer_exporter: pgbouncer_databases_disabled)",
			},
			[]string{"database", "backend_host", "backend_port", "pool_mode"},
		),

		// -----------------------------------------------------------------
		// SHOW STATS equivalents
		// -----------------------------------------------------------------
		StatsQueriesPooledTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_stats_queries_pooled_total",
				Help: "Total SQL queries pooled (pgbouncer: query_count, pgbouncer_exporter: pgbouncer_stats_queries_pooled_total)",
			},
			[]string{"database"},
		),
		StatsQueriesDurationSecondsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_stats_queries_duration_seconds_total",
				Help: "Total seconds spent executing queries (pgbouncer: query_time, pgbouncer_exporter: pgbouncer_stats_queries_duration_seconds_total)",
			},
			[]string{"database"},
		),
		StatsReceivedBytesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_stats_received_bytes_total",
				Help: "Total bytes received from clients (pgbouncer: client_bytes, pgbouncer_exporter: pgbouncer_stats_received_bytes_total)",
			},
			[]string{"database"},
		),
		StatsSentBytesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_stats_sent_bytes_total",
				Help: "Total bytes sent to clients (pgbouncer: server_bytes, pgbouncer_exporter: pgbouncer_stats_sent_bytes_total)",
			},
			[]string{"database"},
		),
		StatsQueriesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_stats_queries_total",
				Help: "Total SQL requests pooled (pgbouncer: requests, pgbouncer_exporter: pgbouncer_stats_queries_total)",
			},
			[]string{"database"},
		),
		StatsClientWaitSecondsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_stats_client_wait_seconds_total",
				Help: "Total seconds clients waited for server (pgbouncer: wait_time, pgbouncer_exporter: pgbouncer_stats_client_wait_seconds_total)",
			},
			[]string{"database"},
		),
		StatsSQLTransactionsPooledTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_stats_sql_transactions_pooled_total",
				Help: "Total SQL transactions pooled, includes implicit (pgbouncer: xact_count, pgbouncer_exporter: pgbouncer_stats_sql_transactions_pooled_total)",
			},
			[]string{"database"},
		),
		StatsServerInTransactionSecondsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_stats_server_in_transaction_seconds_total",
				Help: "Total seconds in transaction (pgbouncer: xact_time, pgbouncer_exporter: pgbouncer_stats_server_in_transaction_seconds_total)",
			},
			[]string{"database"},
		),
		StatsClientParsesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_stats_client_parses_total",
				Help: "Total Parse messages from clients (pgbouncer: ps_client_parse_count, pgbouncer_exporter: pgbouncer_stats_client_parses_total)",
			},
			[]string{"database"},
		),
		StatsServerParsesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_stats_server_parses_total",
				Help: "Total Parse messages sent to server (pgbouncer: ps_server_parse_count, pgbouncer_exporter: pgbouncer_stats_server_parses_total)",
			},
			[]string{"database"},
		),
		StatsBindsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_stats_binds_total",
				Help: "Total Bind messages (pgbouncer: ps_bind_count, pgbouncer_exporter: pgbouncer_stats_binds_total)",
			},
			[]string{"database"},
		),
		StatsServerAssignmentsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: prefix + "_stats_server_assignments_total",
				Help: "Total server assignments (pgbouncer: server_assignment_count, pgbouncer_exporter: pgbouncer_stats_server_assignments_total)",
			},
			[]string{"database"},
		),

		// -----------------------------------------------------------------
		// SHOW POOLS equivalents
		// -----------------------------------------------------------------
		PoolsClientActiveConnections: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_pools_client_active_connections",
				Help: "Client connections linked to server (pgbouncer: cl_active, pgbouncer_exporter: pgbouncer_pools_client_active_connections)",
			},
			[]string{"database", "user"},
		),
		PoolsClientActiveCancelConnections: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_pools_client_active_cancel_connections",
				Help: "STUB: Different cancel model. Always 0 (pgbouncer_exporter: pgbouncer_pools_client_active_cancel_connections)",
			},
			[]string{"database", "user"},
		),
		PoolsClientWaitingConnections: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_pools_client_waiting_connections",
				Help: "Client connections waiting for server (pgbouncer: cl_waiting, pgbouncer_exporter: pgbouncer_pools_client_waiting_connections)",
			},
			[]string{"database", "user"},
		),
		PoolsClientWaitingCancelConnections: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_pools_client_waiting_cancel_connections",
				Help: "STUB: Different cancel model. Always 0 (pgbouncer_exporter: pgbouncer_pools_client_waiting_cancel_connections)",
			},
			[]string{"database", "user"},
		),
		PoolsServerActiveConnections: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_pools_server_active_connections",
				Help: "Server connections linked to client (pgbouncer: sv_active, pgbouncer_exporter: pgbouncer_pools_server_active_connections)",
			},
			[]string{"database", "user"},
		),
		PoolsServerActiveCancelConnections: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_pools_server_active_cancel_connections",
				Help: "STUB: Different cancel model. Always 0 (pgbouncer_exporter: pgbouncer_pools_server_active_cancel_connections)",
			},
			[]string{"database", "user"},
		),
		PoolsServerBeingCanceledConnections: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_pools_server_being_canceled_connections",
				Help: "STUB: Different cancel model. Always 0 (pgbouncer_exporter: pgbouncer_pools_server_being_canceled_connections)",
			},
			[]string{"database", "user"},
		),
		PoolsServerIdleConnections: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_pools_server_idle_connections",
				Help: "Server connections idle (pgbouncer: sv_idle, pgbouncer_exporter: pgbouncer_pools_server_idle_connections)",
			},
			[]string{"database", "user"},
		),
		PoolsServerUsedConnections: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_pools_server_used_connections",
				Help: "STUB: pgxpool health check differs. Always 0 (pgbouncer_exporter: pgbouncer_pools_server_used_connections)",
			},
			[]string{"database", "user"},
		),
		PoolsServerTestingConnections: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_pools_server_testing_connections",
				Help: "STUB: pgxpool health check differs. Always 0 (pgbouncer_exporter: pgbouncer_pools_server_testing_connections)",
			},
			[]string{"database", "user"},
		),
		PoolsServerLoginConnections: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_pools_server_login_connections",
				Help: "Server connections logging in (pgbouncer: sv_login, pgbouncer_exporter: pgbouncer_pools_server_login_connections)",
			},
			[]string{"database", "user"},
		),
		PoolsClientMaxWaitSeconds: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: prefix + "_pools_client_maxwait_seconds",
				Help: "Age of oldest waiting client (pgbouncer: maxwait, pgbouncer_exporter: pgbouncer_pools_client_maxwait_seconds)",
			},
			[]string{"database", "user"},
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

// AddRingBufferMessages adds to the total messages counter.
func (m *Metrics) AddRingBufferMessages(direction string, count int64) {
	if m == nil {
		return
	}
	m.RingBufferMessagesTotal.WithLabelValues(direction).Add(float64(count))
}

// AddRingBufferBytes adds to the total bytes counter.
func (m *Metrics) AddRingBufferBytes(direction string, count int64) {
	if m == nil {
		return
	}
	m.RingBufferBytesTotal.WithLabelValues(direction).Add(float64(count))
}

// SetRingBufferUtilization sets the utilization gauge.
func (m *Metrics) SetRingBufferUtilization(direction string, ratio float64) {
	if m == nil {
		return
	}
	m.RingBufferUtilization.WithLabelValues(direction).Set(ratio)
}

// SetRingBufferSessionsByState sets the count of sessions in a given state.
func (m *Metrics) SetRingBufferSessionsByState(direction, state string, count int) {
	if m == nil {
		return
	}
	m.RingBufferSessionsByState.WithLabelValues(direction, state).Set(float64(count))
}

// =============================================================================
// pgbouncer_exporter-compatible metric helper methods
// =============================================================================

// InitVersionInfo sets the version info gauge. Call once at startup.
// pgbouncer_exporter: pgbouncer_version_info
func (m *Metrics) InitVersionInfo(version string) {
	if m == nil {
		return
	}
	m.VersionInfo.WithLabelValues(version).Set(1)
	m.Up.Set(1)
}

// SetDatabaseCount sets the count of configured databases.
// pgbouncer_exporter: pgbouncer_databases (from SHOW LISTS)
func (m *Metrics) SetDatabaseCount(count int) {
	if m == nil {
		return
	}
	m.Databases.Set(float64(count))
}

// SetUserCount sets the count of configured users.
// pgbouncer_exporter: pgbouncer_users (from SHOW LISTS)
func (m *Metrics) SetUserCount(count int) {
	if m == nil {
		return
	}
	m.Users.Set(float64(count))
}

// SetPoolCount sets the count of active pools.
// pgbouncer_exporter: pgbouncer_pools (from SHOW LISTS)
func (m *Metrics) SetPoolCount(count int) {
	if m == nil {
		return
	}
	m.Pools.Set(float64(count))
}

// SetUsedClients sets the count of used client connections.
// pgbouncer_exporter: pgbouncer_used_clients (from SHOW LISTS)
func (m *Metrics) SetUsedClients(count int) {
	if m == nil {
		return
	}
	m.UsedClients.Set(float64(count))
}

// SetLoginClients sets the count of clients in login phase.
// pgbouncer_exporter: pgbouncer_login_clients (from SHOW LISTS)
func (m *Metrics) SetLoginClients(count int) {
	if m == nil {
		return
	}
	m.LoginClients.Set(float64(count))
}

// SetUsedServers sets the count of used server connections.
// pgbouncer_exporter: pgbouncer_used_servers (from SHOW LISTS)
func (m *Metrics) SetUsedServers(count int) {
	if m == nil {
		return
	}
	m.UsedServers.Set(float64(count))
}

// SetConfigMaxClientConnections sets the max client connections config value.
// pgbouncer_exporter: pgbouncer_config_max_client_connections (from SHOW CONFIG)
func (m *Metrics) SetConfigMaxClientConnections(max int) {
	if m == nil {
		return
	}
	m.ConfigMaxClientConnections.Set(float64(max))
}

// SetConfigMaxUserConnections sets the max user connections config value.
// pgbouncer_exporter: pgbouncer_config_max_user_connections (from SHOW CONFIG)
func (m *Metrics) SetConfigMaxUserConnections(max int) {
	if m == nil {
		return
	}
	m.ConfigMaxUserConnections.Set(float64(max))
}

// SetDatabaseConfig sets database configuration metrics.
// pgbouncer_exporter: pgbouncer_databases_* (from SHOW DATABASES)
func (m *Metrics) SetDatabaseConfig(database, backendHost, backendPort, poolMode string, poolSize, maxConns, currentConns int) {
	if m == nil {
		return
	}
	m.DatabasesPoolSize.WithLabelValues(database, backendHost, backendPort, poolMode).Set(float64(poolSize))
	m.DatabasesMaxConnections.WithLabelValues(database, backendHost, backendPort, poolMode).Set(float64(maxConns))
	m.DatabasesCurrentConnections.WithLabelValues(database, backendHost, backendPort, poolMode).Set(float64(currentConns))
	// Stubs - always 0
	m.DatabasesReservePool.WithLabelValues(database, backendHost, backendPort, poolMode).Set(0)
	m.DatabasesPaused.WithLabelValues(database, backendHost, backendPort, poolMode).Set(0)
	m.DatabasesDisabled.WithLabelValues(database, backendHost, backendPort, poolMode).Set(0)
}

// -----------------------------------------------------------------------------
// SHOW STATS recording methods
// -----------------------------------------------------------------------------

// RecordQueryPooled increments the query count for a database.
// pgbouncer_exporter: pgbouncer_stats_queries_pooled_total
// pgbouncer: query_count, incremented in client.c:1597
func (m *Metrics) RecordQueryPooled(database string) {
	if m == nil {
		return
	}
	m.StatsQueriesPooledTotal.WithLabelValues(database).Inc()
	m.StatsQueriesTotal.WithLabelValues(database).Inc()
}

// AddQueryDuration adds to the total query duration for a database.
// pgbouncer_exporter: pgbouncer_stats_queries_duration_seconds_total
// pgbouncer: query_time, accumulated in server.c:584-586
func (m *Metrics) AddQueryDuration(database string, seconds float64) {
	if m == nil {
		return
	}
	m.StatsQueriesDurationSecondsTotal.WithLabelValues(database).Add(seconds)
}

// AddReceivedBytes adds to the total bytes received from clients.
// pgbouncer_exporter: pgbouncer_stats_received_bytes_total
// pgbouncer: client_bytes, packet len in client.c:1585,1612
func (m *Metrics) AddReceivedBytes(database string, bytes int64) {
	if m == nil {
		return
	}
	m.StatsReceivedBytesTotal.WithLabelValues(database).Add(float64(bytes))
}

// AddSentBytes adds to the total bytes sent to clients.
// pgbouncer_exporter: pgbouncer_stats_sent_bytes_total
// pgbouncer: server_bytes, packet len in server.c:552
func (m *Metrics) AddSentBytes(database string, bytes int64) {
	if m == nil {
		return
	}
	m.StatsSentBytesTotal.WithLabelValues(database).Add(float64(bytes))
}

// AddClientWaitTime adds to the total client wait time for a database.
// pgbouncer_exporter: pgbouncer_stats_client_wait_seconds_total
// pgbouncer: wait_time, in objects.c:837
func (m *Metrics) AddClientWaitTime(database string, seconds float64) {
	if m == nil {
		return
	}
	m.StatsClientWaitSecondsTotal.WithLabelValues(database).Add(seconds)
}

// RecordTransactionPooled increments the transaction count.
// pgbouncer_exporter: pgbouncer_stats_sql_transactions_pooled_total
// pgbouncer: xact_count, includes both explicit and implicit transactions
func (m *Metrics) RecordTransactionPooled(database string) {
	if m == nil {
		return
	}
	m.StatsSQLTransactionsPooledTotal.WithLabelValues(database).Inc()
}

// AddTransactionDuration adds to the total transaction duration.
// pgbouncer_exporter: pgbouncer_stats_server_in_transaction_seconds_total
// pgbouncer: xact_time, in server.c:596-600
func (m *Metrics) AddTransactionDuration(database string, seconds float64) {
	if m == nil {
		return
	}
	m.StatsServerInTransactionSecondsTotal.WithLabelValues(database).Add(seconds)
}

// RecordClientParse increments the client Parse count.
// pgbouncer_exporter: pgbouncer_stats_client_parses_total
// pgbouncer: ps_client_parse_count in prepare.c:333
func (m *Metrics) RecordClientParse(database string) {
	if m == nil {
		return
	}
	m.StatsClientParsesTotal.WithLabelValues(database).Inc()
}

// RecordServerParse increments the server Parse count.
// pgbouncer_exporter: pgbouncer_stats_server_parses_total
// pgbouncer: ps_server_parse_count in prepare.c:377
func (m *Metrics) RecordServerParse(database string) {
	if m == nil {
		return
	}
	m.StatsServerParsesTotal.WithLabelValues(database).Inc()
}

// RecordBind increments the Bind message count.
// pgbouncer_exporter: pgbouncer_stats_binds_total
// pgbouncer: ps_bind_count in prepare.c:521
func (m *Metrics) RecordBind(database string) {
	if m == nil {
		return
	}
	m.StatsBindsTotal.WithLabelValues(database).Inc()
}

// RecordServerAssignment increments the server assignment count.
// pgbouncer_exporter: pgbouncer_stats_server_assignments_total
// pgbouncer: server_assignment_count in objects.c:966
func (m *Metrics) RecordServerAssignment(database string) {
	if m == nil {
		return
	}
	m.StatsServerAssignmentsTotal.WithLabelValues(database).Inc()
}

// -----------------------------------------------------------------------------
// SHOW POOLS recording methods
// -----------------------------------------------------------------------------

// SetPoolClientActive sets the count of active client connections for a pool.
// pgbouncer_exporter: pgbouncer_pools_client_active_connections
// pgbouncer: cl_active
func (m *Metrics) SetPoolClientActive(database, user string, count int) {
	if m == nil {
		return
	}
	m.PoolsClientActiveConnections.WithLabelValues(database, user).Set(float64(count))
}

// SetPoolClientWaiting sets the count of waiting client connections for a pool.
// pgbouncer_exporter: pgbouncer_pools_client_waiting_connections
// pgbouncer: cl_waiting
func (m *Metrics) SetPoolClientWaiting(database, user string, count int) {
	if m == nil {
		return
	}
	m.PoolsClientWaitingConnections.WithLabelValues(database, user).Set(float64(count))
}

// SetPoolServerActive sets the count of active server connections for a pool.
// pgbouncer_exporter: pgbouncer_pools_server_active_connections
// pgbouncer: sv_active
func (m *Metrics) SetPoolServerActive(database, user string, count int) {
	if m == nil {
		return
	}
	m.PoolsServerActiveConnections.WithLabelValues(database, user).Set(float64(count))
}

// SetPoolServerIdle sets the count of idle server connections for a pool.
// pgbouncer_exporter: pgbouncer_pools_server_idle_connections
// pgbouncer: sv_idle
func (m *Metrics) SetPoolServerIdle(database, user string, count int) {
	if m == nil {
		return
	}
	m.PoolsServerIdleConnections.WithLabelValues(database, user).Set(float64(count))
}

// SetPoolServerLogin sets the count of server connections in login state.
// pgbouncer_exporter: pgbouncer_pools_server_login_connections
// pgbouncer: sv_login
func (m *Metrics) SetPoolServerLogin(database, user string, count int) {
	if m == nil {
		return
	}
	m.PoolsServerLoginConnections.WithLabelValues(database, user).Set(float64(count))
}

// SetPoolClientMaxWait sets the max wait time for clients in a pool.
// pgbouncer_exporter: pgbouncer_pools_client_maxwait_seconds
// pgbouncer: maxwait
func (m *Metrics) SetPoolClientMaxWait(database, user string, seconds float64) {
	if m == nil {
		return
	}
	m.PoolsClientMaxWaitSeconds.WithLabelValues(database, user).Set(seconds)
}

// InitPoolStubs initializes stub metrics for a pool to 0.
// Call this when creating a new pool to ensure all metrics exist.
func (m *Metrics) InitPoolStubs(database, user string) {
	if m == nil {
		return
	}
	// Cancel-related stubs
	m.PoolsClientActiveCancelConnections.WithLabelValues(database, user).Set(0)
	m.PoolsClientWaitingCancelConnections.WithLabelValues(database, user).Set(0)
	m.PoolsServerActiveCancelConnections.WithLabelValues(database, user).Set(0)
	m.PoolsServerBeingCanceledConnections.WithLabelValues(database, user).Set(0)
	// Health check stubs
	m.PoolsServerUsedConnections.WithLabelValues(database, user).Set(0)
	m.PoolsServerTestingConnections.WithLabelValues(database, user).Set(0)
}

// RecordTimeout records a timeout event.
// timeoutType: "query", "idle_transaction", "transaction"
// outcome: "canceled" (cancel worked), "terminated" (had to disconnect)
func (m *Metrics) RecordTimeout(database, timeoutType, outcome string) {
	if m == nil {
		return
	}
	m.TimeoutsTotal.WithLabelValues(database, timeoutType, outcome).Inc()
}

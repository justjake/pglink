package frontend

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/justjake/pglink/pkg/backend"
	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/observability"
	"github.com/justjake/pglink/pkg/pgserver"
	"github.com/justjake/pglink/pkg/pgwire"
)

// Service handles incoming client connections.
type Service struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger

	config    *config.Config
	secrets   *config.SecretCache
	tlsConfig *tls.Config

	// pgserver handles listener, TLS, auth, and cancel requests
	server *pgserver.Server

	listener  net.Listener
	databases map[*config.DatabaseConfig]*backend.Database

	// Observability
	tracingEnabled bool                   // Whether OTEL tracing is enabled
	metrics        *observability.Metrics // May be nil if metrics disabled

	// Connection tracking
	activeConns atomic.Int32

	// Session management
	sessionsMu sync.Mutex
	sessions   map[*Session]struct{}
	sessionsWg sync.WaitGroup
	nextPID    atomic.Uint32

	// Cancel registry: maps proxy PID to session for query cancellation.
	// When a cancel request arrives, we look up the session by PID,
	// validate the secret key, and forward the cancel to the backend.
	cancelRegistry   map[uint32]*Session
	cancelRegistryMu sync.RWMutex
}

// NewService creates a new frontend Service with the given configuration.
// The caller should validate the config before calling this function.
// The fsys parameter should be rooted at the config file's directory for resolving relative paths.
// If tracingEnabled is true, OTEL tracing will be used (assumes global provider is configured).
// The metrics parameter is optional; pass nil to disable metrics.
func NewService(ctx context.Context, cfg *config.Config, fsys fs.FS, secrets *config.SecretCache, logger *slog.Logger, tracingEnabled bool, metrics *observability.Metrics) (*Service, error) {
	tlsResult, err := cfg.TLSConfig(fsys)
	if err != nil {
		return nil, fmt.Errorf("failed to create TLS config: %w", err)
	}
	for _, path := range tlsResult.WrittenFiles {
		logger.Warn("wrote generated TLS certificate", "path", path)
	}

	innerCtx, cancel := context.WithCancel(ctx)

	return &Service{
		ctx:            innerCtx,
		cancel:         cancel,
		logger:         logger,
		config:         cfg,
		secrets:        secrets,
		tlsConfig:      tlsResult.Config,
		databases:      make(map[*config.DatabaseConfig]*backend.Database),
		sessions:       make(map[*Session]struct{}),
		cancelRegistry: make(map[uint32]*Session),
		tracingEnabled: tracingEnabled,
		metrics:        metrics,
	}, nil
}

// Listen starts the service and listens for incoming connections on the
// configured address. Returns an error if the listener fails to start.
// When the service's context is cancelled, all sessions are cancelled and
// the method waits for them to close cleanly before returning.
func (s *Service) Listen() error {
	// Set up all databases
	for name, dbConfig := range s.config.Databases {
		db, err := backend.NewDatabase(s.ctx, dbConfig, s.secrets, s.logger, s.tracingEnabled)
		if err != nil {
			return fmt.Errorf("failed to create database %s: %w", name, err)
		}
		defer db.Close()
		s.logger.Info("created backend", "name", name, "config", dbConfig)
		s.databases[dbConfig] = db
	}

	// Create pgserver.Server
	addr := s.config.GetListenAddr()
	server, err := pgserver.NewServer(pgserver.ServerConfig{
		Addr:           addr.String(),
		TLSConfig:      s.tlsConfig,
		TLSOptional:    !s.config.TLSRequired(),
		BaseContext:    func(net.Listener) context.Context { return s.ctx },
		ConnContext:    s.connContext, // Connection limiting
		AuthHandler:    s.makeAuthHandler(),
		StartupHandler: s.startupHandler,
		Handler:        s.connHandler,
		CancelHandler:  pgserver.DefaultCancelHandler,
		Logger:         s.logger,
	})
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}
	s.server = server

	// Set up listener
	ln, err := net.Listen("tcp", addr.String())
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	s.listener = ln
	s.logger.Info("listening", "addr", addr.String())

	// Start periodic pool stats collection for metrics
	s.startPoolStatsCollection(s.ctx)

	// Start a goroutine to close listener and shutdown server when context is cancelled
	go func() {
		<-s.ctx.Done()
		_ = s.server.Close()
		s.cancelAllSessions()
	}()

	// Run the server (blocks until server is closed or error)
	serverErr := s.server.Serve(ln)

	// Wait for all sessions to finish
	s.sessionsWg.Wait()

	// Return context error if that's why we stopped, otherwise return server error
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	if errors.Is(serverErr, pgserver.ErrServerClosed) {
		return nil
	}
	return serverErr
}

func (s *Service) allocPID() uint32 {
	return s.nextPID.Add(1)
}

// cancelAllSessions cancels all active sessions and closes their connections.
// Closing the connection ensures that any blocked reads return immediately.
func (s *Service) cancelAllSessions() {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	for sess := range s.sessions {
		sess.cancel()
		// Close the connection to unblock any readers waiting on I/O
		if sess.conn != nil {
			_ = sess.conn.Close()
		}
	}
}

// Shutdown cancels the service's context, triggering graceful shutdown of all sessions.
func (s *Service) Shutdown() {
	s.cancel()
}

// ActiveConnections returns the current number of active client connections.
func (s *Service) ActiveConnections() int32 {
	return s.activeConns.Load()
}

// registerForCancel adds a session to the cancel registry so it can receive
// cancel requests. Called after the session has been assigned a PID.
func (s *Service) registerForCancel(sess *Session) {
	s.cancelRegistryMu.Lock()
	defer s.cancelRegistryMu.Unlock()
	s.cancelRegistry[sess.state.PID] = sess
}

// unregisterForCancel removes a session from the cancel registry.
// Called when the session is closing.
func (s *Service) unregisterForCancel(sess *Session) {
	s.cancelRegistryMu.Lock()
	defer s.cancelRegistryMu.Unlock()
	delete(s.cancelRegistry, sess.state.PID)
}

// DumpRingBufferStats logs ring buffer statistics for all active sessions.
// This is called when SIGUSR1 is received, before taking a flight recorder snapshot.
func (s *Service) DumpRingBufferStats() {
	s.logger.Info("ring buffer stats dump (SIGUSR1)")

	s.sessionsMu.Lock()
	sessions := make([]*Session, 0, len(s.sessions))
	for sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.sessionsMu.Unlock()

	for _, sess := range sessions {
		sess.LogRingBufferStats(s.logger)
	}
}

// SetupFlightRecorderCallback registers the ring buffer dump callback with the flight recorder.
// This should be called after creating the flight recorder service.
func (s *Service) SetupFlightRecorderCallback(fr *observability.FlightRecorderService) {
	if fr == nil {
		return
	}
	fr.SetSignalCallback(s.DumpRingBufferStats)
}

// collectPoolStats updates pool metrics from all databases.
// This is called periodically to update gauge metrics for pool state.
func (s *Service) collectPoolStats() {
	if s.metrics == nil {
		return
	}

	poolCount := 0
	for dbConfig, db := range s.databases {
		dbName := dbConfig.Database
		stats := db.Stats()

		// Get backend port (default 5432)
		backendPort := 5432
		if dbConfig.Backend.Port != nil {
			backendPort = int(*dbConfig.Backend.Port)
		}

		// Per-database stats
		// Note: pglink uses transaction pooling mode
		s.metrics.SetDatabaseConfig(
			dbName,
			dbConfig.Backend.Host,
			fmt.Sprint(backendPort),
			"transaction",         // pglink uses transaction pooling
			int(stats.MaxConns),   // pool_size
			int(stats.MaxConns),   // max_connections
			int(stats.TotalConns), // current_connections
		)

		// Per-pool (database/user) stats
		for userName, poolStats := range stats.Pools {
			poolCount++
			// Server connections
			s.metrics.SetPoolServerActive(dbName, userName, int(poolStats.AcquiredConns))
			s.metrics.SetPoolServerIdle(dbName, userName, int(poolStats.IdleConns))
			s.metrics.SetPoolServerLogin(dbName, userName, 0) // pgxpool doesn't expose this

			// Initialize pool stub metrics
			s.metrics.InitPoolStubs(dbName, userName)
		}
	}

	s.metrics.SetPoolCount(poolCount)

	// Global counts
	s.metrics.SetUsedClients(int(s.activeConns.Load()))
	s.metrics.SetLoginClients(0) // Would need to track auth state
}

// startPoolStatsCollection starts a goroutine that periodically collects pool stats.
func (s *Service) startPoolStatsCollection(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		// Collect once immediately
		s.collectPoolStats()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.collectPoolStats()
			}
		}
	}()
}

// handleCancelRequest processes a cancel request from a client.
// It looks up the target session by PID, validates the secret key,
// and forwards the cancel to the backend if valid.
// Returns nil if the cancel was processed (whether or not it succeeded),
// or an error if the cancel request itself was malformed.
func (s *Service) handleCancelRequest(req *pgproto3.CancelRequest) error {
	s.cancelRegistryMu.RLock()
	sess := s.cancelRegistry[req.ProcessID]
	s.cancelRegistryMu.RUnlock()

	if sess == nil {
		// No session found with this PID - silently ignore.
		// This is expected if the session has already ended.
		s.logger.Debug("cancel request for unknown frontend PID", "pid", req.ProcessID)
		return nil
	}

	if sess.state.SecretCancelKey != req.SecretKey {
		s.logger.Debug("cancel request with invalid secret", "pid", req.ProcessID)
		return nil
	}
	if err := sess.cancelBackendQuery(); err != nil {
		s.logger.Debug("failed to cancel backend query", "pid", req.ProcessID, "error", err)
	} else {
		s.logger.Info("cancelled query", "pid", req.ProcessID)
	}
	return nil
}

// ============================================================================
// pgserver handlers for Phase 3 migration
// ============================================================================

// authConnData stores authentication-related data that needs to be passed
// from the authorizer to the handler via the connection context.
type authConnData struct {
	dbConfig   *config.DatabaseConfig
	userConfig *config.UserConfig
	database   *backend.Database
}

type authConnDataKey struct{}

// passwordAuthorizer implements pgserver.PasswordAuthorizer.
// It looks up the database and user config and returns credentials.
func (s *Service) passwordAuthorizer(ctx context.Context, conn *pgserver.UnauthorizedConn) (pgwire.UserSecretData, error) {
	params := pgwire.ParameterStatuses(conn.StartupMessage.Parameters)
	databaseName := params.Database()
	userName := params.User()

	// Look up database config
	dbConfig, ok := s.config.Databases[databaseName]
	if !ok {
		return pgwire.UserSecretData{}, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidCatalogName,
			fmt.Sprintf("database \"%s\" does not exist", databaseName), nil)
	}

	database := s.databases[dbConfig]
	if database == nil {
		return pgwire.UserSecretData{}, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidCatalogName,
			fmt.Sprintf("database \"%s\" not initialized", databaseName), nil)
	}

	// Find matching user config
	var userConfig *config.UserConfig
	for i := range dbConfig.Users {
		user := &dbConfig.Users[i]
		resolvedUsername, err := s.secrets.Get(ctx, user.Username)
		if err != nil {
			continue
		}
		if resolvedUsername == userName {
			userConfig = user
			break
		}
	}
	if userConfig == nil {
		return pgwire.UserSecretData{}, pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidAuthorizationSpecification,
			fmt.Sprintf("user \"%s\" does not exist", userName), nil)
	}

	// Store config data for later use by handler
	conn.SetExtraData(authConnDataKey{}, &authConnData{
		dbConfig:   dbConfig,
		userConfig: userConfig,
		database:   database,
	})

	// Get credentials
	username, err := s.secrets.Get(ctx, userConfig.Username)
	if err != nil {
		return pgwire.UserSecretData{}, fmt.Errorf("failed to resolve username: %w", err)
	}
	password, err := s.secrets.Get(ctx, userConfig.Password)
	if err != nil {
		return pgwire.UserSecretData{}, fmt.Errorf("failed to resolve password: %w", err)
	}

	return pgwire.NewUserSecretData(username, password), nil
}

// makeAuthHandler creates the appropriate auth handler based on config.
func (s *Service) makeAuthHandler() pgserver.AuthHandler {
	authenticator := &pgserver.PasswordAuthenticator{
		PasswordAuthorizer:  s.passwordAuthorizer,
		SCRAMIterationCount: s.config.GetSCRAMIterations(),
	}

	authMethod := s.config.GetAuthMethod()
	switch authMethod {
	case config.AuthMethodPlaintext:
		return authenticator.CleartextPassword
	case config.AuthMethodMD5:
		return authenticator.MD5Password
	case config.AuthMethodSCRAMSHA256, config.AuthMethodSCRAMSHA256Plus:
		return authenticator.SASL
	default:
		// Default to SCRAM-SHA-256
		return authenticator.SASL
	}
}

// startupHandler implements pgserver.StartupHandler.
// It assigns PID, secret key, and startup parameters.
func (s *Service) startupHandler(ctx context.Context, conn *pgserver.AuthorizedConn) (*pgserver.ClientConn, error) {
	// Get config data saved by authorizer
	authData, ok := conn.GetExtraData(authConnDataKey{}).(*authConnData)
	if !ok || authData == nil {
		return nil, fmt.Errorf("missing auth data")
	}

	// Allocate PID and generate secret key
	pid := pgwire.ProcessID(s.allocPID())
	var secretKeyBytes [4]byte
	if _, err := rand.Read(secretKeyBytes[:]); err != nil {
		return nil, fmt.Errorf("failed to generate secret key: %w", err)
	}
	secretKey := pgwire.SecretKey(uint32(secretKeyBytes[0])<<24 | uint32(secretKeyBytes[1])<<16 |
		uint32(secretKeyBytes[2])<<8 | uint32(secretKeyBytes[3]))

	// Build startup parameters
	params := make(pgwire.ParameterStatuses)
	// Start with default server parameters
	params["server_version"] = "17.0"
	params["server_encoding"] = "UTF8"
	params["client_encoding"] = "UTF8"
	params["DateStyle"] = "ISO, MDY"
	params["TimeZone"] = "UTC"
	params["integer_datetimes"] = "on"
	params["standard_conforming_strings"] = "on"
	params["is_superuser"] = "off"
	params["session_authorization"] = conn.User

	// Add parameters from backend config
	for key, value := range authData.dbConfig.Backend.DefaultStartupParameters.All() {
		params[key] = value
	}

	// Copy client-provided parameters that the server should echo back
	clientParams := pgwire.ParameterStatuses(conn.StartupMessage.Parameters)
	if clientEncoding := clientParams["client_encoding"]; clientEncoding != "" {
		params["client_encoding"] = clientEncoding
	}
	if app := clientParams["application_name"]; app != "" {
		params["application_name"] = app
	}

	return &pgserver.ClientConn{
		FrontendConn:      conn.FrontendConn,
		User:              conn.User,
		Database:          conn.Database,
		ProcessID:         pid,
		SecretKey:         secretKey,
		StartupParameters: params,
		ExtraData:         authData,
	}, nil
}

// connHandler implements pgserver.ConnHandler.
// It runs the proxy loop using pgproxy.Session.
func (s *Service) connHandler(ctx context.Context, conn *pgserver.ClientConn) error {
	// Get config data
	authData, ok := conn.ExtraData.(*authConnData)
	if !ok || authData == nil {
		return fmt.Errorf("missing auth data")
	}

	// Track active connections
	s.activeConns.Add(1)
	defer s.activeConns.Add(-1)

	// Send startup messages BEFORE creating pgproxy.Session
	// (pgproxy.Session will take ownership of the connection and start a ring buffer)
	conn.Frontend.Send(&pgproto3.BackendKeyData{
		ProcessID: uint32(conn.ProcessID),
		SecretKey: uint32(conn.SecretKey),
	})
	for key, value := range conn.StartupParameters {
		conn.Frontend.Send(&pgproto3.ParameterStatus{
			Name:  key,
			Value: value,
		})
	}
	conn.Frontend.Send(&pgproto3.ReadyForQuery{TxStatus: byte(pgwire.TxIdle)})
	if err := conn.Frontend.Flush(); err != nil {
		return fmt.Errorf("failed to send startup messages: %w", err)
	}

	// Run the pgproxy-based proxy loop
	return s.runProxyLoop(ctx, conn, authData)
}

// connContext implements connection limiting via pgserver.ConnContext.
func (s *Service) connContext(ctx context.Context, conn net.Conn) (context.Context, error) {
	maxConns := s.config.GetMaxClientConnections()
	currentConns := s.activeConns.Load()
	if currentConns >= maxConns {
		return nil, fmt.Errorf("too many connections (current: %d, max: %d)", currentConns, maxConns)
	}
	return ctx, nil
}

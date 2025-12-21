package frontend

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/justjake/pglink/pkg/backend"
	"github.com/justjake/pglink/pkg/config"
)

// Session represents a client's session with the Service.
type Session struct {
	ctx    context.Context
	cancel context.CancelFunc

	service   *Service
	conn      net.Conn
	logger    *slog.Logger
	tlsConfig *tls.Config
	secrets   *config.SecretCache
	config    *config.Config

	// Protocol handling - created fresh after TLS upgrade if needed
	frontend *pgproto3.Backend

	// Session state set during startup
	startupParameters map[string]string
	databaseName      string
	userName          string
	dbConfig          *config.DatabaseConfig
	userConfig        *config.UserConfig
	tlsState          *tls.ConnectionState

	// Backend connection (set after authentication)
	backendConn *backend.PooledConn
}

// Context returns the session's context, which is canceled when Close is called.
func (s *Session) Context() context.Context {
	return s.ctx
}

// Close cancels the session's context and releases associated resources.
func (s *Session) Close() {
	s.cancel()
	if s.conn != nil {
		_ = s.conn.Close()
	}
	if s.backendConn != nil {
		s.backendConn.Release()
	}
}

// Run handles the full lifecycle of a client session.
// It processes the PostgreSQL protocol, authenticates the client,
// establishes a backend connection, and proxies messages.
func (s *Session) Run() {
	defer s.Close()

	// Create pgproto3 backend for protocol handling
	s.frontend = pgproto3.NewBackend(s.conn, s.conn)
	s.enableTracing()

	// Handle TLS and startup
	if err := s.handleStartup(); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
			s.logger.Error("startup failed", "error", err)
		}
		return
	}

	s.logger = s.logger.With("user", s.userName, "database", s.databaseName)

	// Authenticate the client
	if err := s.authenticate(); err != nil {
		s.logger.Error("authentication failed", "error", err)
		return
	}

	s.logger.Info("client authenticated")

	// TODO: Establish backend connection and proxy messages
	// For now, send a "not implemented" error
	s.sendError("FATAL", pgerrcode.FeatureNotSupported, "proxy functionality not yet implemented")
}

// handleStartup processes the initial connection: TLS negotiation and startup message.
func (s *Session) handleStartup() error {
	// Use ReceiveStartupMessage which handles the special startup format
	startupMsg, err := s.frontend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("failed to read startup message: %w", err)
	}

	// Handle SSL request if present
	if _, ok := startupMsg.(*pgproto3.SSLRequest); ok {
		if err := s.handleSSLRequest(); err != nil {
			return fmt.Errorf("SSL negotiation failed: %w", err)
		}
		// After TLS upgrade, read the actual startup message
		startupMsg, err = s.frontend.ReceiveStartupMessage()
		if err != nil {
			return fmt.Errorf("failed to read startup message after TLS: %w", err)
		}
	}

	// Handle GSS encryption request (not supported)
	if _, ok := startupMsg.(*pgproto3.GSSEncRequest); ok {
		// Send 'N' to decline GSS encryption
		if _, err := s.conn.Write([]byte{'N'}); err != nil {
			return fmt.Errorf("failed to decline GSS encryption: %w", err)
		}
		// Read the actual startup message
		startupMsg, err = s.frontend.ReceiveStartupMessage()
		if err != nil {
			return fmt.Errorf("failed to read startup message after GSS decline: %w", err)
		}
	}

	// Reject non-SSL connections if SSL is required
	if s.config.TLSRequired() && s.tlsState == nil {
		s.sendError("FATAL", pgerrcode.ProtocolViolation, "SSL/TLS required")
		return errors.New("SSL/TLS required but client did not request SSL")
	}

	// Process startup message
	startup, ok := startupMsg.(*pgproto3.StartupMessage)
	if !ok {
		return fmt.Errorf("expected StartupMessage, got %T", startupMsg)
	}

	// Extract and validate startup parameters
	s.startupParameters = startup.Parameters
	s.userName = startup.Parameters["user"]
	s.databaseName = startup.Parameters["database"]

	if s.userName == "" {
		s.sendError("FATAL", pgerrcode.InvalidAuthorizationSpecification, "no user specified")
		return errors.New("no user specified in startup message")
	}

	if s.databaseName == "" {
		// Default to username if no database specified (PostgreSQL behavior)
		s.databaseName = s.userName
	}

	// Look up database config
	dbConfig, ok := s.config.Databases[s.databaseName]
	if !ok {
		s.sendError("FATAL", pgerrcode.InvalidCatalogName, fmt.Sprintf("database \"%s\" does not exist", s.databaseName))
		return fmt.Errorf("unknown database: %s", s.databaseName)
	}
	s.dbConfig = dbConfig

	// Find matching user config
	userConfig, err := s.findUserConfig(dbConfig)
	if err != nil {
		s.sendError("FATAL", pgerrcode.InvalidAuthorizationSpecification, fmt.Sprintf("user \"%s\" does not exist", s.userName))
		return err
	}
	s.userConfig = userConfig

	return nil
}

// handleSSLRequest handles the SSL/TLS negotiation.
func (s *Session) handleSSLRequest() error {
	if s.tlsConfig == nil {
		// TLS not configured, decline
		if _, err := s.conn.Write([]byte{'N'}); err != nil {
			return err
		}
		return nil
	}

	// Accept TLS
	if _, err := s.conn.Write([]byte{'S'}); err != nil {
		return err
	}

	// Upgrade connection to TLS
	tlsConn := tls.Server(s.conn, s.tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return fmt.Errorf("TLS handshake failed: %w", err)
	}

	s.conn = tlsConn
	state := tlsConn.ConnectionState()
	s.tlsState = &state

	// Recreate the pgproto3 backend with the TLS connection
	s.frontend = pgproto3.NewBackend(s.conn, s.conn)
	s.enableTracing()

	return nil
}

// findUserConfig finds the user configuration matching the session's username.
func (s *Session) findUserConfig(dbConfig *config.DatabaseConfig) (*config.UserConfig, error) {
	for i := range dbConfig.Users {
		user := &dbConfig.Users[i]
		// Resolve username from secret
		username, err := s.secrets.Get(s.ctx, user.Username)
		if err != nil {
			continue // Skip users we can't resolve
		}
		if username == s.userName {
			return user, nil
		}
	}
	return nil, fmt.Errorf("user not found: %s", s.userName)
}

// authenticate performs client authentication.
func (s *Session) authenticate() error {
	// Get credentials for verification
	username, err := s.secrets.Get(s.ctx, s.userConfig.Username)
	if err != nil {
		return fmt.Errorf("failed to get username: %w", err)
	}
	password, err := s.secrets.Get(s.ctx, s.userConfig.Password)
	if err != nil {
		return fmt.Errorf("failed to get password: %w", err)
	}

	creds := NewUserSecretData(username, password)

	// Create and run auth session
	authSession, err := NewAuthSession(s.frontend, creds, s.config.GetAuthMethod(), s.tlsState, s.config.GetSCRAMIterations())
	if err != nil {
		return fmt.Errorf("failed to create auth session: %w", err)
	}

	if err := authSession.Run(); err != nil {
		return err
	}

	return nil
}

// sendError sends an error response to the client.
func (s *Session) sendError(severity, code, message string) {
	s.frontend.Send(&pgproto3.ErrorResponse{
		Severity: severity,
		Code:     code,
		Message:  message,
	})
}

// enableTracing enables pgproto3 protocol tracing if debug logging is enabled.
func (s *Session) enableTracing() {
	if s.logger.Enabled(s.ctx, slog.LevelDebug) {
		s.frontend.Trace(&slogTraceWriter{session: s}, pgproto3.TracerOptions{
			SuppressTimestamps: true,
		})
	}
}

// slogTraceWriter implements io.Writer to convert pgproto3 trace output to slog debug calls.
// It references the Session directly so it picks up logger metadata updates.
type slogTraceWriter struct {
	session *Session
	buf     bytes.Buffer
}

// Write implements io.Writer. It buffers input and logs complete lines.
func (w *slogTraceWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	w.buf.Write(p)

	// Process complete lines
	for {
		line, err := w.buf.ReadBytes('\n')
		if err != nil {
			// No complete line yet, put the partial data back
			w.buf.Write(line)
			break
		}
		// Trim the newline and log
		line = bytes.TrimSuffix(line, []byte("\n"))
		if len(line) > 0 {
			w.session.logger.Debug("pgproto3", "trace", string(line))
		}
	}

	return n, nil
}

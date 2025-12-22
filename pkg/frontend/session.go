package frontend

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math/rand/v2"
	"net"
	"runtime"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/justjake/pglink/pkg/backend"
	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/pgwire"
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

	// The client.
	frontend Frontend

	// Client's configuration.
	// Populated during startup.
	startupParameters map[string]string
	databaseName      string
	userName          string
	dbConfig          *config.DatabaseConfig
	userConfig        *config.UserConfig
	tlsState          *tls.ConnectionState
	trackedParameters []string

	// Client's view of the server's session state.
	state pgwire.ProtocolState

	// nil until a backend connection is acquired at least once
	// handles forwarding NOTIFY messages to the appropriate client.

	// Established client connection.
	frontendNextMsg    chan pgproto3.FrontendMessage
	frontendDisposeMsg chan pgproto3.FrontendMessage

	readErrors chan error

	// The server.
	// nil until a backend connection is acquired to run a transaction.
	// nil once the backend connection is released.
	backend             *backend.PooledConn
	backendNextMsg      chan pgproto3.BackendMessage
	backendDisposeMsg   chan pgproto3.BackendMessage
	backendConnectedCtx context.Context
}

// Context returns the session's context, which is canceled when Close is called.
func (s *Session) Context() context.Context {
	return s.ctx
}

// Select over reading from frontend, reading from backend, or receiving a context cancellation.
func (s *Session) ReceiveAny() (pgproto3.Message, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case err := <-s.readErrors:
		return nil, err
	case msg := <-s.frontendNextMsg:
		return msg, nil
	case msg := <-s.backendNextMsg:
		return msg, nil
	}
}

// Close cancels the session's context and releases associated resources.
func (s *Session) Close() {
	s.cancel()
	if s.frontendNextMsg != nil {
		close(s.frontendNextMsg)
	}
	if s.frontendDisposeMsg != nil {
		close(s.frontendDisposeMsg)
	}
	if s.backendNextMsg != nil {
		close(s.backendNextMsg)
	}
	if s.backendDisposeMsg != nil {
		close(s.backendDisposeMsg)
	}
	if s.conn != nil {
		closeErr := s.conn.Close()
		if closeErr != nil {
			s.logger.Error("error closing client", "error", closeErr)
		}
	}
	if s.backend != nil {
		s.backend.Release()
	}
}

// Run handles the full lifecycle of a client session.
// It processes the PostgreSQL protocol, authenticates the client,
// establishes a backend connection, and proxies messages.
func (s *Session) Run() {
	defer s.Close()

	// Create pgproto3 backend for protocol handling
	s.frontend = Frontend{ctx: s.ctx, Backend: pgproto3.NewBackend(s.conn, s.conn)}
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

	/*
		https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-START-UP
		After having received AuthenticationOk, the frontend must wait for further
		messages from the server. In this phase a backend process is being started,
		and the frontend is just an interested bystander. It is still possible for
		the startup attempt to fail (ErrorResponse) or the server to decline support
		for the requested minor protocol version (NegotiateProtocolVersion), but in
		the normal case the backend will send some ParameterStatus messages,
		BackendKeyData, and finally ReadyForQuery.

		During this phase the backend will attempt to apply any additional run-time
		parameter settings that were given in the startup message. If successful,
		these values become session defaults. An error causes ErrorResponse and
		exit.

		The possible messages from the backend in this phase are:

		BackendKeyData
		This message provides secret-key data that the frontend must save if it
		wants to be able to issue cancel requests later. The frontend should not
		respond to this message, but should continue listening for a ReadyForQuery
		message.

		The PostgreSQL server will always send this message, but some third party
		backend implementations of the protocol that don't support query
		cancellation are known not to.

		ParameterStatus
		This message informs the frontend about the current (initial) setting of
		backend parameters, such as client_encoding or DateStyle. The frontend can
		ignore this message, or record the settings for its future use; see Section
		54.2.7 for more details. The frontend should not respond to this message,
		but should continue listening for a ReadyForQuery message.

		ReadyForQuery
		Start-up is completed. The frontend can now issue commands.

		ErrorResponse
		Start-up failed. The connection is closed after sending this message.

		NoticeResponse
		A warning message has been issued. The frontend should display the message
		but continue listening for ReadyForQuery or ErrorResponse.
	*/
	s.initSessionProcessState()
	s.sendInitialParameterStatuses()
	s.sendBackendKeyData()
	// The ReadyForQuery message is the same one that the backend will issue after
	// each command cycle. Depending on the coding needs of the frontend, it is
	// reasonable to consider ReadyForQuery as starting a command cycle, or to
	// consider ReadyForQuery as ending the start-up phase and each subsequent
	// command cycle.
	s.sendReadyForQuery()

	go func() {
		defer s.logger.Debug("frontend reader routine exited")
		for {
			msg, err := s.frontend.Receive()
			if s.ctx.Err() != nil {
				return
			}
			if err != nil {
				s.readErrors <- err
				return
			}
			s.frontendNextMsg <- msg

			// Messages are ony valid until the next call to Receive() So we need to
			// block the reader routine until the session is done with the message.
			select {
			case <-s.ctx.Done():
				return
			case returned := <-s.frontendDisposeMsg:
				if returned != msg {
					panic(fmt.Errorf("session did not release message in order for reuse: expectedc %T %p != actual %T %p", msg, msg, returned, returned))
				}
			}
		}
	}()

	for {
		// Initial state: no backend connection, so only receive from frontend.
		anyMsg, err := s.ReceiveAny()
		if err != nil {
			s.sendError(pgwire.ErrorFatal, pgerrcode.ProtocolViolation, fmt.Sprintf("error receiving client message: %v", err))
			return
		}

		msg, ok := anyMsg.(pgproto3.FrontendMessage)
		if !ok {
			panic(fmt.Errorf("expected frontend message, got %T", anyMsg))
		}

		s.logger.Debug("recv client message", "type", fmt.Sprintf("%T", msg))

		if pgwire.IsTerminateConnMessage(msg) {
			// Messages that close the connection.
			s.logger.Info("client terminated connection")
			return
		} else if pgwire.IsCancelMessage(msg) {
			s.sendError(pgwire.ErrorFatal, pgerrcode.ProtocolViolation, fmt.Sprintf("cancel request received on normal connection: %T", msg))
			return
		} else if pgwire.IsStartupModeMessage(msg) {
			s.sendError(pgwire.ErrorFatal, pgerrcode.ProtocolViolation, fmt.Sprintf("startup completed already: %T", msg))
			return
		} else if pgwire.IsCopyModeMessage(msg) {
			s.sendError(pgwire.ErrorFatal, pgerrcode.ProtocolViolation, fmt.Sprintf("idle client not in copy mode: %T", msg))
			return
		} else if pgwire.IsSimpleQueryModeMessage(msg) || pgwire.IsExtendedQueryModeMessage(msg) {
			// Enter state where we have a backend connection.
			if err := s.runWithBackend(msg); err != nil {
				return err
			}
		} else {
			s.sendError(pgwire.ErrorFatal, pgerrcode.ProtocolViolation, fmt.Sprintf("unknown client message: %T", msg))
			return
		}

		// We have returned from having a backend to being idle.
	}
}

func (s *Session) runWithBackend(firstMsg pgproto3.FrontendMessage) error {
	oldLogger := s.logger
	if err := s.acquireBackend(); err != nil {
		s.sendError(pgwire.ErrorFatal, pgerrcode.CannotConnectNow, fmt.Sprintf("failed to acquire backend: %v", err))
		return err
	}
	defer s.releaseBackend()
	s.logger = s.logger.With("backend", s.backend.Name())
	defer func() { s.logger = oldLogger }()

	msg := firstMsg

	for {
		if pgwire.IsSimpleQueryModeMessage(msg) {
			if err := s.runSimpleQuery(msg); err != nil {
				return err
			}
		} else if pgwire.IsExtendedQueryModeMessage(msg) {
			clientMsg
		}

		msg, err := s.ReceiveAny()
		if err != nil {
			s.sendError(pgwire.ErrorFatal, pgerrcode.ProtocolViolation, fmt.Sprintf("error receiving message: %v", err))
			return err
		}
	}
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
	s.frontend = Frontend{ctx: s.ctx, Backend: pgproto3.NewBackend(s.conn, s.conn)}
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
	authSession, err := NewAuthSession(&s.frontend, creds, s.config.GetAuthMethod(), s.tlsState, s.config.GetSCRAMIterations())
	if err != nil {
		return fmt.Errorf("failed to create auth session: %w", err)
	}

	if err := authSession.Run(); err != nil {
		return err
	}

	return nil
}

func (s *Session) initSessionProcessState() {
	s.state.PID = s.service.allocPID()
	s.logger = s.logger.With("pid", s.state.PID)
	s.state.SecretCancelKey = rand.Uint32()
	s.state.ParameterStatuses = maps.Collect(s.dbConfig.Backend.DefaultStartupParameters.All())
	maps.Copy(s.state.ParameterStatuses, s.startupParameters)
	s.state.TxStatus = pgwire.TxIdle
	if len(s.dbConfig.TrackExtraParameters) > 0 {
		s.trackedParameters = append(s.trackedParameters, pgwire.BaseTrackedParameters...)
		s.trackedParameters = append(s.trackedParameters, s.dbConfig.TrackExtraParameters...)
	} else {
		s.trackedParameters = pgwire.BaseTrackedParameters
	}
	// TODO: channels?
}

func (s *Session) sendReadyForQuery() {
	s.sendBackendParameterStatusChanges()
	s.frontend.Send(&pgproto3.ReadyForQuery{TxStatus: byte(s.state.TxStatus)})
}

func (s *Session) sendBackendKeyData() {
	s.frontend.Send(&pgproto3.BackendKeyData{
		ProcessID: s.state.PID,
		SecretKey: s.state.SecretCancelKey,
	})
}

func (s *Session) sendInitialParameterStatuses() {
	for key, value := range s.state.ParameterStatuses {
		s.frontend.Send(&pgproto3.ParameterStatus{
			Name:  key,
			Value: value,
		})
	}
}

func (s *Session) sendBackendParameterStatusChanges() {
	if s.backend == nil {
		return
	}
	diff := s.state.ParameterStatuses.DiffToTip(s.backend.ParameterStatuses(s.trackedParameters))
	if diff == nil {
		return
	}

	for key, value := range diff {
		str := ""
		if value == nil {
			delete(s.state.ParameterStatuses, key)
		} else {
			str = *value
			s.state.ParameterStatuses[key] = str
		}
		s.frontend.Send(&pgproto3.ParameterStatus{
			Name:  key,
			Value: str,
		})
	}
}

// sendError sends an error response to the client.
func (s *Session) sendError(severity pgwire.Severity, code string, message string) {
	_, file, line, _ := runtime.Caller(1)

	// TODO: i hope we aren't disclosing secrets in this log message
	s.logger.Warn("sent error to client", "severity", severity, "code", code, "message", message, "file", file, "line", line)

	// This we definitely want to do
	s.frontend.Send(&pgproto3.ErrorResponse{
		Severity: string(severity),
		Code:     code,
		Message:  message,
		File:     file,
		Line:     int32(line),
		Hint:     "pglink proxy error",
	})
	err := s.frontend.Flush()
	if err != nil {
		s.logger.Error("error flushing to client", "error", err)
	}
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

type Frontend struct {
	*pgproto3.Backend
	ctx context.Context
}

func (f *Frontend) Receive() (pgwire.FrontendMessage, error) {
	if err := f.ctx.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled: %w", err)
	}
	msg, err := f.Backend.Receive()
	if err != nil {
		return nil, err
	}
	if err := f.ctx.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled: %w", err)
	}

	if m, ok := pgwire.ToFrontendMessage(msg); ok {
		return m, nil
	}

	return nil, fmt.Errorf("unknown frontend message: %T", msg)
}

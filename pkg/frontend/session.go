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

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/justjake/pglink/pkg/backend"
	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/params"
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
	state backend.ServerSession

	// The server.
	// nil until a backend connection is acquired to run a transaction.
	backend *backend.PooledConn
}

// Context returns the session's context, which is canceled when Close is called.
func (s *Session) Context() context.Context {
	return s.ctx
}

// Select over reading from frontend, reading from backend, or receiving a context cancellation.
func (s *Session) ReceiveFrontendOrBackend() (pgproto3.FrontendMessage, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case msg := <-s.frontend.Receive():
		return msg, nil
	case msg := <-s.backend.Receive():
		return msg, nil
	}
}

// Close cancels the session's context and releases associated resources.
func (s *Session) Close() {
	s.cancel()
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

	for {
		// Initial state: no backend connection, so only receive from frontend.
		msg, err := s.frontend.Receive()
		if err != nil {
			s.logger.Error("error receiving client message", "error", err)
		}
		s.logger.Debug("recv client message", "type", fmt.Sprintf("%T", msg))

		if IsTerminateConnMessage(msg) {
			// Messages that close the connection.
			s.logger.Info("client terminated connection")
			return
		} else if IsCancelMessage(msg) {
			s.sendError(backend.ErrorFatal, pgerrcode.ProtocolViolation, fmt.Sprintf("cancel request received on normal connection: %T", msg))
			return
		} else if IsStartupModeMessage(msg) {
			s.sendError(backend.ErrorFatal, pgerrcode.ProtocolViolation, fmt.Sprintf("startup completed already: %T", msg))
			return
		} else if IsCopyModeMessage(msg) {
			s.sendError(backend.ErrorFatal, pgerrcode.ProtocolViolation, fmt.Sprintf("idle client not in copy mode: %T", msg))
			return
		} else if IsSimpleQueryModeMessage(msg) || IsExtendedQueryModeMessage(msg) {
			// Messages that acquire a backend connection (happy path).
			if err := s.handleMessageWithBackend(msg); err != nil {
				return err
			}
		} else {
			s.sendError(backend.ErrorFatal, pgerrcode.ProtocolViolation, fmt.Sprintf("unknown client message: %T", msg))
			return
		}

		// Handle client messages while there's no backend connection.
		// The happy path is the start of a new command cycle.

		switch msg := msg.(type) {
		case *pgproto3.Terminate:
			s.logger.Info("client terminated connection")
			return
		// Simple Query flow:
		case *pgproto3.Query:
			// Simple query.
			// Destroys unnamed prepared statement & portal.

		// Extended Query flow:
		case *pgproto3.Parse:
			// Extended Query 1: parse text into a prepared statement.
		case *pgproto3.Bind:
			// Extended Query 2: Bind parameters to a prepared statement.
		case *pgproto3.Execute:
			// Extended Query 3: Execute a prepared statement, requesting N or all rows.
			// May need to be called again if server replies PortalSuspended.
			// Execute phase is always terminated by the appearance of exactly one of
			// these messages:
			// - PortalSuspended: execute ended before completion, call Execute again.
			// - CommandComplete: success
			// - ErrorResponse: failure
			// - EmptyQueryResponse: the portal was created from an empty query string
		case *pgproto3.Sync:
			// Extended Query 4: Command pipeline complete.
			//
			// Causes the backend to close the current transaction if it's not inside a
			// BEGIN/COMMIT transaction block (“close” meaning to commit if no error, or
			// roll back if error).
			// then, a ReadyForQuery response is issued.
			//
			// The purpose of Sync is to provide a resynchronization point for error
			// recovery. When an error is detected while processing any extended-query
			// message, the backend issues ErrorResponse, then reads and discards
			// messages until a Sync is reached, then issues ReadyForQuery and returns
			// to normal message processing. (But note that no skipping occurs if an
			// error is detected while processing Sync — this ensures that there is
			// one and only one ReadyForQuery sent for each Sync.)

		// In addition to these fundamental, required operations, there are several
		// optional operations that can be used with extended-query protocol.
		case *pgproto3.Describe:
			// Extended Query tool: Describe prepared statement or portal.
			//
			// The Describe message (portal variant) specifies the name of an existing
			// portal (or an empty string for the unnamed portal). The response is a
			// RowDescription message describing the rows that will be returned by
			// executing the portal; or a NoData message if the portal does not
			// contain a query that will return rows; or ErrorResponse if there is no
			// such portal.
			//
			// The Describe message (statement variant) specifies the name of an
			// existing prepared statement (or an empty string for the unnamed
			// prepared statement). The response is a ParameterDescription message
			// describing the parameters needed by the statement, followed by a
			// RowDescription message describing the rows that will be returned when
			// the statement is eventually executed (or a NoData message if the
			// statement will not return rows). ErrorResponse is issued if there is no
			// such prepared statement. Note that since Bind has not yet been issued,
			// the formats to be used for returned columns are not yet known to the
			// backend; the format code fields in the RowDescription message will be
			// zeroes in this case.
		case *pgproto3.Close:
			// Close prepared statement/portal.
			// Note that closing a prepared statement implicitly closes any open
			// portals that were constructed from that statement.
		case *pgproto3.Flush:
			// The Flush message does not cause any specific output to be generated,
			// but forces the backend to deliver any data pending in its output
			// buffers. A Flush must be sent after any extended-query command except
			// Sync, if the frontend wishes to examine the results of that command
			// before issuing more commands. Without Flush, messages returned by the
			// backend will be combined into the minimum possible number of packets to
			// minimize network overhead.

		case *pgproto3.FunctionCall:
			// Call a function; seems to work like a simple query? Or maybe it works with both modes?

		case *pgproto3.CopyData:
		case *pgproto3.CopyDone:
		case *pgproto3.CopyFail:
			// state error: we cannot be copying without a backend.
			return

		case *pgproto3.GSSEncRequest:
		case *pgproto3.GSSResponse:
		case *pgproto3.PasswordMessage:
		case *pgproto3.SASLInitialResponse:
		case *pgproto3.SASLResponse:
		case *pgproto3.SSLRequest:
		case *pgproto3.StartupMessage:
			// state error: startup completed already.
			s.sendError(backend.ErrorFatal, pgerrcode.ProtocolViolation, fmt.Sprintf("startup completed already: %T", msg))
			return
		case *pgproto3.CancelRequest:
			return
		default:
			s.sendError(backend.ErrorFatal, pgerrcode.ProtocolViolation, fmt.Sprintf("unknown client message: %T", msg))
			return
		}
	}

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
	s.state.TxStatus = backend.TxIdle
	if len(s.dbConfig.TrackExtraParameters) > 0 {
		s.trackedParameters = append(s.trackedParameters, params.BaseTrackedParameters...)
		s.trackedParameters = append(s.trackedParameters, s.dbConfig.TrackExtraParameters...)
	} else {
		s.trackedParameters = params.BaseTrackedParameters
	}
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
func (s *Session) sendError(severity backend.Severity, code string, message string) {
	s.frontend.Send(&pgproto3.ErrorResponse{
		Severity: string(severity),
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

type Frontend struct {
	*pgproto3.Backend
	ctx context.Context
}

func (f *Frontend) Receive() (pgproto3.FrontendMessage, error) {
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

	return msg, nil
}

func IsStartupModeMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.GSSEncRequest:
	case *pgproto3.GSSResponse:
	case *pgproto3.PasswordMessage:
	case *pgproto3.SASLInitialResponse:
	case *pgproto3.SASLResponse:
	case *pgproto3.SSLRequest:
	case *pgproto3.StartupMessage:
		return true
	default:
		return false
	}
}

func IsSimpleQueryModeMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.Query:
	case *pgproto3.FunctionCall:
		return true
	}
	return false
}

func IsExtendedQueryModeMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.Parse:
	case *pgproto3.Bind:
	case *pgproto3.Execute:
	case *pgproto3.Sync:
	case *pgproto3.Describe:
	case *pgproto3.Close:
	case *pgproto3.Flush:
		return true
	}
	return false
}

func IsCopyModeMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.CopyData:
	case *pgproto3.CopyDone:
	case *pgproto3.CopyFail:
		return true
	}
	return false
}

func IsCancelMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.CancelRequest:
		return true
	}
	return false
}

func IsTerminateConnMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.Terminate:
		return true
	}
	return false
}

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
	"regexp"
	"sync/atomic"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/justjake/pglink/pkg/backend"
	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/observability"
	"github.com/justjake/pglink/pkg/pgwire"
)

// errCancelRequest is a sentinel error returned from handleStartup when the
// connection was a cancel request. This is not a real error - the cancel was
// handled successfully and the connection should be closed.
var errCancelRequest = errors.New("cancel request handled")

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
	database          *backend.Database

	// Client's view of the server's session state.
	state pgwire.ProtocolState

	// nil until a backend connection is acquired at least once
	// handles forwarding NOTIFY messages to the appropriate client.

	// The server.
	// nil until a backend connection is acquired to run a transaction.
	// nil once the backend connection is released.
	backend *backend.PooledBackend

	// TODO: do we actually want this, or is it Claude papering over bugs?
	TODO_backendAcquisitionID uint64 // Incremented each time we acquire a backend

	// Backend key data for query cancellation.
	// These are captured from the backend's BackendKeyData message when we
	// first acquire a backend connection. We use them to forward cancel
	// requests to the correct backend.
	backendPID       uint32
	backendSecretKey uint32

	// Map from client statement name to server statement name.
	// Used to ensure consistent naming within a backend acquisition.
	statementNameMap map[string]string

	// Message stored until the next call to Recv.
	lastRecv pgwire.Message

	// Observability
	tracingEnabled bool                   // Whether OTEL tracing is enabled
	metrics        *observability.Metrics // May be nil if metrics disabled
	sessionSpan    trace.Span             // Root span for this session (nil if tracing disabled)

	// For CopyRecognizer: track the last SQL query to associate COPY with its query
	lastSQL string
}

// Select over reading from frontend, reading from backend, or receiving a context cancellation.
func (s *Session) RecvFrontend() (pgwire.ClientMessage, error) {
	s.freeLastRecvAndContinueSender()
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case msg := <-s.frontend.Reader().ReadingChan():
		s.lastRecv = msg.Value
		s.state.UpdateForFrontentMessage(msg.Value.Client())
		s.state.ProcessFlows(msg.Value)
		return msg.Value, msg.Error
	}
}

func (s *Session) RecvAny() (pgwire.Message, error) {
	s.freeLastRecvAndContinueSender()
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case msg := <-s.frontend.Reader().ReadingChan():
		s.lastRecv = msg.Value
		s.state.UpdateForFrontentMessage(msg.Value.Client())
		s.state.ProcessFlows(msg.Value)
		return msg.Value, msg.Error
	case msg := <-s.backend.ReadingChan():
		s.lastRecv = msg.Value
		// We never want to rewrite a server message, since they never contain
		// information like portal or statement names.
		s.state.UpdateForServerMessage(msg.Value)
		s.state.ProcessFlows(msg.Value)
		return msg.Value, msg.Error
	}
}

func (s *Session) freeLastRecvAndContinueSender() {
	if s.lastRecv == nil {
		return
	}

	if _, ok := s.lastRecv.(pgwire.ServerMessage); ok {
		if s.backend != nil {
			s.backend.Continue()
		}
	} else {
		s.frontend.Reader().Continue()
	}

	s.lastRecv = nil
}

// Close cancels the session's context and releases associated resources.
func (s *Session) Close() {
	// Unregister from cancel registry if we were registered.
	// Check if we have a PID (meaning initSessionProcessState was called).
	if s.state.PID != 0 {
		s.service.unregisterForCancel(s)
	}

	if flushError := s.flush(); flushError != nil {
		s.logger.Error("session close: error flushing to client", "error", flushError)
	}

	s.cancel()

	if s.conn != nil {
		closeErr := s.conn.Close()
		if closeErr != nil {
			s.logger.Error("session close: error closing client connection", "error", closeErr)
		}
	}

	s.releaseBackend()

	// Close any active flows
	s.state.CloseAllFlows()

	// End session span
	if s.sessionSpan != nil {
		s.sessionSpan.End()
	}

	// Record client disconnection metric
	if s.metrics != nil && s.databaseName != "" && s.userName != "" {
		s.metrics.RecordClientDisconnect(s.databaseName, s.userName)
	}

	if s.logger.Enabled(s.ctx, slog.LevelDebug) {
		s.logger.Debug("session closed", "state", s.state)
	} else {
		s.logger.Info("session closed")
	}
}

// Run handles the full lifecycle of a client session.
// It processes the PostgreSQL protocol, authenticates the client,
// establishes a backend connection, and proxies messages.
func (s *Session) Run() {
	defer s.Close()

	// Create pgproto3 backend for protocol handling
	s.frontend = Frontend{ctx: s.ctx, Backend: pgproto3.NewBackend(s.conn, s.conn), conn: s.conn}
	s.enableTracing()

	// Handle TLS and startup
	if err := s.handleStartup(); err != nil {
		// Cancel requests are one-shot connections that don't establish a session.
		// They return errCancelRequest which is not a real error.
		if errors.Is(err, errCancelRequest) {
			return
		}
		if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
			s.logger.Error("startup failed", "error", err)
		} else {
			s.logger.Debug("startup cancelled", "error", err)
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

	// Set up observability after we know the user/database
	s.startSessionSpan()
	s.setupFlowRecognizers()
	if s.metrics != nil {
		s.metrics.RecordClientConnection(s.databaseName, s.userName)
	}

	for key, value := range s.state.ParameterStatuses {
		s.frontend.Send(&pgproto3.ParameterStatus{
			Name:  key,
			Value: value,
		})
	}
	s.frontend.Send(&pgproto3.BackendKeyData{
		ProcessID: s.state.PID,
		SecretKey: s.state.SecretCancelKey,
	})
	// The ReadyForQuery message is the same one that the backend will issue after
	// each command cycle. Depending on the coding needs of the frontend, it is
	// reasonable to consider ReadyForQuery as starting a command cycle, or to
	// consider ReadyForQuery as ending the start-up phase and each subsequent
	// command cycle.
	s.frontend.Send(&pgproto3.ReadyForQuery{TxStatus: byte(s.state.TxStatus)})

	if err := s.flush(); err != nil {
		s.sendError(err)
		return
	}

	// Idle client state.
	// When true, transition to backend connected state to handle the query.
	// When false, close the client connection.
	idleClientState := pgwire.ClientMessageHandlers[bool]{
		SimpleQuery: func(msg pgwire.ClientSimpleQuery) (bool, error) {
			return true, nil
		},
		ExtendedQuery: func(msg pgwire.ClientExtendedQuery) (bool, error) {
			return true, nil
		},

		TerminateConn: func(msg pgwire.ClientTerminateConn) (bool, error) {
			s.logger.Info("client terminated connection")
			return false, nil
		},

		// These messages don't make any sense in the idle state.
		Cancel: func(msg pgwire.ClientCancel) (bool, error) {
			return false, pgwire.NewProtocolViolation(fmt.Errorf("cancel request received on normal connection"), msg)
		},
		Copy: func(msg pgwire.ClientCopy) (bool, error) {
			return false, pgwire.NewProtocolViolation(fmt.Errorf("idle client not in copy mode"), msg)
		},
		Startup: func(msg pgwire.ClientStartup) (bool, error) {
			return false, pgwire.NewProtocolViolation(fmt.Errorf("startup completed already"), msg)
		},
	}

	for {
		// Initial state: no backend connection, so only receive from frontend.
		msg, err := s.RecvFrontend()
		if err != nil {
			s.sendError(pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.ConnectionException, "error receiving client message", err))
			return
		}

		if transitionToBackend, err := idleClientState.Handle(msg); err != nil || !transitionToBackend {
			if err != nil {
				s.sendError(err)
			}
			return
		}

		if err := s.runWithBackend(msg); err != nil {
			s.sendError(err)
			return
		}

		// We have returned from having a backend to being idle.
		// Wait for next client message that needs a backend.
	}
}

func (s *Session) acquireBackend() (*slog.Logger, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.dbConfig.PoolAcquireTimeout())
	defer cancel()

	be, err := s.database.Acquire(ctx, *s.userConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire backend: %w", err)
	}

	s.backend = be
	logger := s.logger.With("backend", be.Name())
	s.TODO_backendAcquisitionID++ // Increment to ensure unique statement names per acquisition

	// Capture the backend's key data for query cancellation.
	// These credentials are used to forward cancel requests to the backend.
	pgConn := s.backend.PgConn()
	s.backendPID = pgConn.PID()
	s.backendSecretKey = pgConn.SecretKey()

	return logger, nil
}

func (s *Session) releaseBackend() {
	if s.backend == nil {
		return
	}

	s.backend.Release()
	s.backend = nil

	//// TODO: why is this comment here?
	//
	// NOTE: We intentionally do NOT clear statementNameMap here.
	// In transaction pooling mode with QueryExecModeDescribeExec, the client
	// does Parse+Describe+Sync in one round trip, then Bind+Execute+Sync in
	// another. The prepared statement exists on the backend between these
	// round trips, and we need to use the same name in both.
	// Name collisions with other sessions are avoided by using unique names
	// (PID + global counter).
}

// cancelBackendQuery sends a cancel request to the backend to cancel
// the currently running query. This is called when the proxy receives
// a cancel request from the client.
//
// The cancel request is sent on a new TCP connection to the backend,
// as per the PostgreSQL protocol. The connection is closed immediately
// after sending the request.
func (s *Session) cancelBackendQuery() error {
	// Check if we have backend credentials
	if s.backendPID == 0 {
		return fmt.Errorf("no backend connection to cancel")
	}

	// Build the backend address
	addr := s.dbConfig.Backend.Addr()

	// Connect to the backend
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to backend for cancel: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			s.logger.Error("failed to close backend connection for cancel", "error", err)
		}
	}()

	// Build and send the CancelRequest message
	cancelReq := &pgproto3.CancelRequest{
		ProcessID: s.backendPID,
		SecretKey: s.backendSecretKey,
	}
	buf, err := cancelReq.Encode(nil)
	if err != nil {
		return fmt.Errorf("failed to encode cancel request: %w", err)
	}

	if _, err := conn.Write(buf); err != nil {
		return fmt.Errorf("failed to send cancel request: %w", err)
	}

	s.logger.Debug("sent cancel request to backend",
		"backendPID", s.backendPID,
		"addr", addr)

	return nil
}

func (s *Session) runWithBackend(firstMsg pgwire.ClientMessage) error {
	logger, err := s.acquireBackend()
	if err != nil {
		s.logger.Error("failed to acquire backend", "error", err)
		pgErr := pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.CannotConnectNow, "failed to acquire backend", err)
		pgErr.Detail = fmt.Sprintf("while handling message %T", firstMsg)
		return pgErr
	}
	defer s.releaseBackend()

	oldLogger := s.logger
	s.logger = logger
	defer func() { s.logger = oldLogger }()

	backendAcquiredState := pgwire.MessageHandlers[bool]{
		Client: pgwire.ClientMessageHandlers[bool]{
			SimpleQuery:   s.runSimpleQueryWithBackend,
			ExtendedQuery: s.runExtendedQueryWithBackend,
			Copy: func(msg pgwire.ClientCopy) (bool, error) {
				// Forward copy data to backend regardless of our CopyMode state.
				// The client may have pipelined CopyData before we received
				// CopyInResponse from backend. Let the backend validate the protocol.
				if err := s.backend.Send(msg.Client()); err != nil {
					return false, err
				}
				return true, nil
			},

			TerminateConn: func(msg pgwire.ClientTerminateConn) (bool, error) {
				s.logger.Info("client terminated connection")
				return false, nil
			},

			Cancel: func(msg pgwire.ClientCancel) (bool, error) {
				return false, pgwire.NewProtocolViolation(fmt.Errorf("cancel request on normal connection"), msg)
			},
			Startup: func(msg pgwire.ClientStartup) (bool, error) {
				return false, pgwire.NewProtocolViolation(fmt.Errorf("startup completed already"), msg)
			},
		},
		Server: pgwire.ServerMessageHandlers[bool]{
			Async:         s.handleServerAsync,
			Copy:          s.handleServerCopy,
			ExtendedQuery: s.handleServerExtendedQuery,
			Response:      s.handleServerResponse,
			Startup: func(msg pgwire.ServerStartup) (bool, error) {
				err := pgwire.NewProtocolViolation(fmt.Errorf("backend sent startup message to active client session"), msg)
				s.backend.MarkForDestroy(err)
				return false, err
			},
		},
	}

	var msg pgwire.Message = firstMsg
	for {
		continueWithBackend, err := backendAcquiredState.Handle(msg)

		// Ensure we've sent any pending messages
		if flushErr := s.flush(); flushErr != nil {
			return errors.Join(err, flushErr)
		}

		if !continueWithBackend || err != nil {
			if err != nil {
				return err
			}
			return nil
		}

		// Read next message from client or backend,
		// and continue the loop to handle it.
		msg, err = s.RecvAny()
		if err != nil {
			return err
		}
	}
}

func (s *Session) runSimpleQueryWithBackend(msg pgwire.ClientSimpleQuery) (bool, error) {
	if err := s.backend.Send(msg.Client()); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Session) runExtendedQueryWithBackend(msg pgwire.ClientExtendedQuery) (bool, error) {
	extendedQueryRewriter := pgwire.ClientExtendedQueryHandlers[pgproto3.FrontendMessage]{
		Bind: func(msg pgwire.ClientExtendedQueryBind) (pgproto3.FrontendMessage, error) {
			parsed := msg.Parse()
			return &pgproto3.Bind{
				PreparedStatement:    s.clientToServerPreparedStatementName(parsed.PreparedStatement),
				DestinationPortal:    s.clientToServerPortalName(parsed.DestinationPortal),
				ParameterFormatCodes: parsed.ParameterFormatCodes,
				Parameters:           parsed.Parameters,
				ResultFormatCodes:    parsed.ResultFormatCodes,
			}, nil
		},
		Parse: func(msg pgwire.ClientExtendedQueryParse) (pgproto3.FrontendMessage, error) {
			parsed := msg.Parse()
			return &pgproto3.Parse{
				Name:          s.clientToServerPreparedStatementName(parsed.Name),
				Query:         parsed.Query,
				ParameterOIDs: parsed.ParameterOIDs,
			}, nil
		},
		Execute: func(msg pgwire.ClientExtendedQueryExecute) (pgproto3.FrontendMessage, error) {
			parsed := msg.Parse()
			return &pgproto3.Execute{
				Portal:  s.clientToServerPortalName(parsed.Portal),
				MaxRows: parsed.MaxRows,
			}, nil
		},
		Describe: func(msg pgwire.ClientExtendedQueryDescribe) (pgproto3.FrontendMessage, error) {
			parsed := msg.Parse()
			return &pgproto3.Describe{
				ObjectType: parsed.ObjectType,
				Name:       s.clientToServerObjectName(parsed.ObjectType, parsed.Name),
			}, nil
		},
		Close: func(msg pgwire.ClientExtendedQueryClose) (pgproto3.FrontendMessage, error) {
			parsed := msg.Parse()
			return &pgproto3.Close{
				ObjectType: parsed.ObjectType,
				Name:       s.clientToServerObjectName(parsed.ObjectType, parsed.Name),
			}, nil
		},
		Sync: func(msg pgwire.ClientExtendedQuerySync) (pgproto3.FrontendMessage, error) {
			return msg.Parse(), nil
		},
		Flush: func(msg pgwire.ClientExtendedQueryFlush) (pgproto3.FrontendMessage, error) {
			return msg.Parse(), nil
		},
	}

	rewritten, err := extendedQueryRewriter.Handle(msg)
	if err != nil {
		return false, err
	}

	if err := s.backend.Send(rewritten); err != nil {
		return false, err
	}

	return true, nil
}

func (s *Session) handleServerResponse(msg pgwire.ServerResponse) (bool, error) {
	continueWithBackend := true
	if _, ok := msg.(pgwire.ServerResponseReadyForQuery); ok {
		// We may have missed a status update message, double check.
		s.sendBackendParameterStatusChanges()
		if !s.state.InTxOrQuery() {
			s.logger.Info("transaction ended, releasing backend")
			continueWithBackend = false
		}
	}

	// Use fast path for DataRow, CommandComplete, etc.
	// ReadyForQuery and ErrorResponse will use slow path (not fast-forwardable)
	s.forwardServerMessage(msg)
	return continueWithBackend, nil
}

func (s *Session) handleServerExtendedQuery(msg pgwire.ServerExtendedQuery) (bool, error) {
	s.forwardServerMessage(msg)
	return true, nil
}

func (s *Session) handleServerAsync(msg pgwire.ServerAsync) (bool, error) {
	s.forwardServerMessage(msg)
	return true, nil
}

func (s *Session) handleServerCopy(msg pgwire.ServerCopy) (bool, error) {
	s.forwardServerMessage(msg)
	return true, nil
}

// forwardServerMessage sends a server message to the client.
// For fast-forwardable message types, it uses raw byte forwarding to avoid
// re-encoding overhead. For other messages, it uses the standard Send path.
func (s *Session) forwardServerMessage(msg pgwire.ServerMessage) {
	// Check if we have raw bytes available (from RawReader)
	raw := msg.Raw()
	if !raw.IsZero() && raw.IsFastForwardable() {
		// Fast path: forward raw bytes directly
		if err := s.frontend.ForwardRaw(raw); err != nil {
			// Fall back to slow path on error
			s.frontend.Send(msg.Server())
		}
		return
	}

	// Check if this message type is fast-forwardable
	// Even without raw bytes, we can encode and forward to bypass pgproto3 buffering
	serverMsg := msg.Server()
	raw = pgwire.EncodeBackendMessage(serverMsg)
	if !raw.IsZero() && raw.IsFastForwardable() {
		if err := s.frontend.ForwardRaw(raw); err != nil {
			// Fall back to slow path on error
			s.frontend.Send(serverMsg)
		}
		return
	}

	// Slow path: use standard Send
	s.frontend.Send(serverMsg)
}

func (s *Session) clientToServerObjectName(objectType byte, name string) string {
	if objectType == pgwire.ObjectTypePreparedStatement {
		return s.clientToServerPreparedStatementName(name)
	} else {
		return s.clientToServerPortalName(name)
	}
}

var globalStatementCounter atomic.Uint64

func (s *Session) clientToServerPreparedStatementName(name string) string {
	// The unnamed prepared statement ("") is special in PostgreSQL - it's
	// automatically deallocated when a new Parse is done. Pass it through
	// unchanged to let the backend handle it naturally.
	if name == "" {
		return ""
	}

	// For named statements, use unique names to avoid collisions when
	// backend connections are shared across sessions.
	if s.statementNameMap == nil {
		s.statementNameMap = make(map[string]string)
	}
	if serverName, ok := s.statementNameMap[name]; ok {
		return serverName
	}

	// Create a new unique server name using global counter
	counter := globalStatementCounter.Add(1)
	serverName := fmt.Sprintf("pgwire_%d_%d_%s", s.state.PID, counter, name)
	s.statementNameMap[name] = serverName
	return serverName
}

func (s *Session) clientToServerPortalName(name string) string {
	// The unnamed portal ("") is special in PostgreSQL - it's automatically
	// destroyed when a new Bind is done. Pass it through unchanged.
	if name == "" {
		return ""
	}

	// For named portals, use unique names to avoid collisions when
	// backend connections are shared across sessions.
	// Portals are per-transaction so we use the acquisition ID.
	return fmt.Sprintf("pgwire_%d_%d_%s", s.state.PID, s.TODO_backendAcquisitionID, name)
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

	// Handle cancel request - these are one-shot connections that don't
	// establish a session. The client sends a CancelRequest with the PID
	// and secret key of the session to cancel, and then closes the connection.
	if cancelReq, ok := startupMsg.(*pgproto3.CancelRequest); ok {
		// Forward the cancel request to the target session via the service
		if err := s.service.handleCancelRequest(cancelReq); err != nil {
			s.logger.Error("failed to handle cancel request", "error", err)
		}
		// Cancel connections don't send a response - just close
		return errCancelRequest
	}

	// Reject non-SSL connections if SSL is required
	if s.config.TLSRequired() && s.tlsState == nil {
		err := pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.ProtocolViolation, "SSL/TLS required", nil)
		s.sendError(err)
		return err
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
		err := pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidAuthorizationSpecification, "no user specified", nil)
		s.sendError(err)
		return errors.New("no user specified in startup message")
	}

	if s.databaseName == "" {
		// Default to username if no database specified (PostgreSQL behavior)
		s.databaseName = s.userName
	}

	// Look up database config
	dbConfig, ok := s.config.Databases[s.databaseName]
	if !ok {
		err := pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidCatalogName, fmt.Sprintf("database \"%s\" does not exist", s.databaseName), nil)
		s.sendError(err)
		return err
	}
	s.dbConfig = dbConfig
	s.database = s.service.databases[dbConfig]

	// Find matching user config
	userConfig, err := s.findUserConfig(dbConfig)
	if err != nil {
		err := pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InvalidAuthorizationSpecification, fmt.Sprintf("user \"%s\" does not exist", s.userName), nil)
		s.sendError(err)
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
	s.frontend = Frontend{ctx: s.ctx, Backend: pgproto3.NewBackend(s.conn, s.conn), conn: s.conn}
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
	s.state.Statements = pgwire.NamedObjectState[bool]{
		Alive:         make(map[string]bool),
		PendingCreate: make(map[string]bool),
		PendingClose:  make(map[string]bool),
	}
	s.state.Portals = pgwire.NamedObjectState[bool]{
		Alive:         make(map[string]bool),
		PendingCreate: make(map[string]bool),
		PendingClose:  make(map[string]bool),
	}

	// Register for cancel requests so this session can be found by PID.
	s.service.registerForCancel(s)
}

func (s *Session) sendBackendParameterStatusChanges() {
	if s.backend == nil {
		return
	}
	diff := s.backend.ParameterStatusChanges(s.backend.TrackedParameters(), s.state.ParameterStatuses)
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

			s.frontend.Send(&pgproto3.ParameterStatus{
				Name:  key,
				Value: str,
			})
		}
	}
}

// sendError sends an error response to the client.
func (s *Session) sendError(err error) {
	var pgErr *pgwire.Err
	if !errors.As(err, &pgErr) {
		pgErr = pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.InternalError, "unexpected error", err)
	}
	s.logger.Error("session error",
		"severity", pgErr.Severity,
		"code", pgErr.Code,
		"message", pgErr.Message,
		"file", pgErr.File,
		"line", pgErr.Line,
	)

	s.frontend.Send(pgErr)
	flushErr := s.frontend.Flush()
	if flushErr != nil {
		s.logger.Error("session error: error flushing to client", "error", err)
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

func (s *Session) flush() error {
	var errs []error
	if flushErr := s.frontend.Flush(); flushErr != nil {
		errs = append(errs, flushErr)
	}
	if s.backend != nil {
		flushErr := s.backend.Flush()
		if flushErr != nil {
			errs = append(errs, flushErr)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("error flushing: %w", errors.Join(errs...))
	}
	return nil
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

// setupFlowRecognizers configures flow recognizers with tracing and metrics callbacks.
// This should be called after authentication when we know the database/user.
func (s *Session) setupFlowRecognizers() {
	// Get OpenTelemetry config for regex patterns
	var traceparentRegex, appNameRegex *regexp.Regexp
	if s.config.OpenTelemetry != nil {
		traceparentRegex = s.config.OpenTelemetry.GetTraceparentRegex()
		appNameRegex = s.config.OpenTelemetry.GetApplicationNameRegex()
	}

	// Get the tracer if tracing is enabled
	var tracer trace.Tracer
	if s.tracingEnabled {
		tracer = otel.Tracer("github.com/justjake/pglink/pkg/frontend")
	}

	// Helper to get application name from startup parameters
	appName := s.startupParameters["application_name"]

	// SimpleQuery recognizer
	s.state.AddRecognizer(&pgwire.SimpleQueryRecognizer{
		OnStart: func(msg pgwire.ClientSimpleQueryQuery) func(*pgwire.SimpleQueryFlow) {
			parsed := msg.Parse()
			s.lastSQL = parsed.String // Track for CopyRecognizer

			// Start span if tracing enabled
			var span trace.Span
			if tracer != nil {
				ctx := s.ctx
				// Extract trace context from SQL if configured
				if traceparentRegex != nil {
					ctx = s.extractTraceContextFromSQL(ctx, parsed.String, traceparentRegex)
				}
				_, span = tracer.Start(ctx, "pglink.query.simple",
					trace.WithAttributes(observability.SessionAttributes(s.userName, s.databaseName, appName)...),
				)
				// Extract per-query application_name if configured
				if appNameRegex != nil {
					if queryAppName := extractRegexMatch(parsed.String, appNameRegex); queryAppName != "" {
						span.SetAttributes(attribute.String(observability.AttrApplicationNameQ, queryAppName))
					}
				}
			}

			return func(f *pgwire.SimpleQueryFlow) {
				// Record metrics
				if s.metrics != nil {
					duration := f.EndTime.Sub(f.StartTime).Seconds()
					s.metrics.RecordQuery(s.databaseName, s.userName, pgwire.FlowTypeSimpleQuery.String(), duration, f.Err == nil)
				}

				// End span with attributes
				if span != nil {
					span.SetAttributes(
						attribute.String(observability.AttrQueryType, pgwire.FlowTypeSimpleQuery.String()),
						attribute.Int64(observability.AttrRowCount, f.RowCount),
					)
					if s.config.OpenTelemetry != nil && s.config.OpenTelemetry.IncludeQueryText {
						span.SetAttributes(attribute.String(observability.AttrDBStatement, f.SQL))
					}
					if f.CommandTag.String() != "" {
						span.SetAttributes(attribute.String(observability.AttrDBOperation, f.CommandTag.String()))
					}
					if f.Err != nil {
						span.RecordError(fmt.Errorf("%s: %s", f.Err.Code, f.Err.Message))
						span.SetStatus(codes.Error, f.Err.Message)
					}
					span.End()
				}
			}
		},
	})

	// ExtendedQuery recognizer
	s.state.AddRecognizer(&pgwire.ExtendedQueryRecognizer{
		OnStart: func(msg pgwire.ClientExtendedQueryParse) func(*pgwire.ExtendedQueryFlow) {
			parsed := msg.Parse()
			s.lastSQL = parsed.Query // Track for CopyRecognizer

			// Start span if tracing enabled
			var span trace.Span
			if tracer != nil {
				ctx := s.ctx
				// Extract trace context from SQL if configured
				if traceparentRegex != nil {
					ctx = s.extractTraceContextFromSQL(ctx, parsed.Query, traceparentRegex)
				}
				_, span = tracer.Start(ctx, "pglink.query.extended",
					trace.WithAttributes(observability.SessionAttributes(s.userName, s.databaseName, appName)...),
				)
				// Extract per-query application_name if configured
				if appNameRegex != nil {
					if queryAppName := extractRegexMatch(parsed.Query, appNameRegex); queryAppName != "" {
						span.SetAttributes(attribute.String(observability.AttrApplicationNameQ, queryAppName))
					}
				}
				if parsed.Name != "" {
					span.SetAttributes(attribute.String(observability.AttrStatementName, parsed.Name))
				}
			}

			return func(f *pgwire.ExtendedQueryFlow) {
				// Record metrics
				if s.metrics != nil {
					duration := f.EndTime.Sub(f.StartTime).Seconds()
					s.metrics.RecordQuery(s.databaseName, s.userName, f.Type.String(), duration, f.Err == nil)
				}

				// End span with attributes
				if span != nil {
					span.SetAttributes(
						attribute.String(observability.AttrQueryType, f.Type.String()),
						attribute.Int64(observability.AttrRowCount, f.RowCount),
					)
					if s.config.OpenTelemetry != nil && s.config.OpenTelemetry.IncludeQueryText && f.SQL != "" {
						span.SetAttributes(attribute.String(observability.AttrDBStatement, f.SQL))
					}
					if f.CommandTag.String() != "" {
						span.SetAttributes(attribute.String(observability.AttrDBOperation, f.CommandTag.String()))
					}
					if f.PortalName != "" {
						span.SetAttributes(attribute.String(observability.AttrPortalName, f.PortalName))
					}
					if f.Err != nil {
						span.RecordError(fmt.Errorf("%s: %s", f.Err.Code, f.Err.Message))
						span.SetStatus(codes.Error, f.Err.Message)
					}
					span.End()
				}
			}
		},
	})

	// Copy recognizer
	s.state.AddRecognizer(&pgwire.CopyRecognizer{
		GetLastSQL: func() string {
			return s.lastSQL
		},
		OnStart: func(flowType pgwire.FlowType, sql string) func(*pgwire.CopyFlow) {
			// Start span if tracing enabled
			var span trace.Span
			if tracer != nil {
				ctx := s.ctx
				// Extract trace context from SQL if configured
				if traceparentRegex != nil && sql != "" {
					ctx = s.extractTraceContextFromSQL(ctx, sql, traceparentRegex)
				}
				_, span = tracer.Start(ctx, "pglink.copy",
					trace.WithAttributes(observability.SessionAttributes(s.userName, s.databaseName, appName)...),
				)
			}

			return func(f *pgwire.CopyFlow) {
				// Record metrics
				if s.metrics != nil {
					duration := f.EndTime.Sub(f.StartTime).Seconds()
					s.metrics.RecordQuery(s.databaseName, s.userName, f.Type.String(), duration, f.Err == nil)
				}

				// End span with attributes
				if span != nil {
					span.SetAttributes(
						attribute.String(observability.AttrQueryType, f.Type.String()),
						attribute.Int64(observability.AttrByteCount, f.ByteCount),
					)
					if s.config.OpenTelemetry != nil && s.config.OpenTelemetry.IncludeQueryText && f.SQL != "" {
						span.SetAttributes(attribute.String(observability.AttrDBStatement, f.SQL))
					}
					if f.CommandTag.String() != "" {
						span.SetAttributes(attribute.String(observability.AttrDBOperation, f.CommandTag.String()))
					}
					if f.Err != nil {
						span.RecordError(fmt.Errorf("%s: %s", f.Err.Code, f.Err.Message))
						span.SetStatus(codes.Error, f.Err.Message)
					}
					span.End()
				}
			}
		},
	})
}

// extractTraceContextFromSQL extracts W3C trace context from SQL using the given regex.
func (s *Session) extractTraceContextFromSQL(ctx context.Context, sql string, regex *regexp.Regexp) context.Context {
	traceparent := extractRegexMatch(sql, regex)
	if traceparent == "" {
		return ctx
	}

	// Use the global propagator to extract trace context
	carrier := traceparentCarrier{traceparent: traceparent}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// extractRegexMatch extracts the first capture group from sql using the regex.
func extractRegexMatch(sql string, regex *regexp.Regexp) string {
	if regex == nil {
		return ""
	}
	match := regex.FindStringSubmatch(sql)
	if len(match) > 1 {
		return match[1]
	}
	return ""
}

// traceparentCarrier implements propagation.TextMapCarrier for extracting traceparent.
type traceparentCarrier struct {
	traceparent string
}

func (c traceparentCarrier) Get(key string) string {
	if key == "traceparent" {
		return c.traceparent
	}
	return ""
}

func (c traceparentCarrier) Set(key, value string) {
	// No-op, this is read-only
}

func (c traceparentCarrier) Keys() []string {
	return []string{"traceparent"}
}

// startSessionSpan starts the root span for this session.
// Should be called after authentication when we know the user/database.
func (s *Session) startSessionSpan() {
	if !s.tracingEnabled {
		return
	}

	tracer := otel.Tracer("github.com/justjake/pglink/pkg/frontend")

	// Check for traceparent in startup parameters
	ctx := s.ctx
	if s.config.OpenTelemetry != nil {
		paramName := s.config.OpenTelemetry.GetTraceparentStartupParameter()
		if traceparent, ok := s.startupParameters[paramName]; ok && traceparent != "" {
			carrier := traceparentCarrier{traceparent: traceparent}
			ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
		}
	}

	appName := s.startupParameters["application_name"]
	ctx, s.sessionSpan = tracer.Start(ctx, "pglink.session",
		trace.WithAttributes(observability.SessionAttributes(s.userName, s.databaseName, appName)...),
		trace.WithAttributes(attribute.Int("pglink.pid", int(s.state.PID))),
	)
	// Update context so child spans use the session span as parent
	s.ctx = ctx
}

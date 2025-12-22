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
	"sync/atomic"
	"time"

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
	database          *backend.Database

	// Client's view of the server's session state.
	state pgwire.ProtocolState

	// nil until a backend connection is acquired at least once
	// handles forwarding NOTIFY messages to the appropriate client.

	// The server.
	// nil until a backend connection is acquired to run a transaction.
	// nil once the backend connection is released.
	backend              *backend.PooledConn
	backendReadingChan   chan backend.ReadResult[pgwire.ServerMessage]
	backendCtx           context.Context
	cancelBackendCtx     context.CancelFunc
	backendAcquisitionID uint64 // Incremented each time we acquire a backend

	// Map from client statement name to server statement name.
	// Used to ensure consistent naming within a backend acquisition.
	statementNameMap map[string]string

	// Message stored until the next call to Recv.
	lastRecv pgwire.Message
}

// Context returns the session's context, which is canceled when Close is called.
func (s *Session) Context() context.Context {
	return s.ctx
}

// Select over reading from frontend, reading from backend, or receiving a context cancellation.
func (s *Session) RecvFrontend() (pgwire.ClientMessage, error) {
	s.logger.Debug("RecvFrontend: freeing lastRecv", "lastRecv", fmt.Sprintf("%T", s.lastRecv))
	s.freeLastRecvAndContinueSender()
	s.logger.Debug("RecvFrontend: waiting for frontend message")
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case msg := <-s.frontend.Reader().ReadingChan():
		s.logger.Debug("RecvFrontend: received", "msg", fmt.Sprintf("%T", msg.Value))
		s.lastRecv = msg.Value
		return msg.Value, msg.Error
	}
}

func (s *Session) RecvAny() (pgwire.Message, error) {
	s.freeLastRecvAndContinueSender()
	s.logger.Debug("RecvAny: waiting")
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case msg := <-s.frontend.Reader().ReadingChan():
		s.lastRecv = msg.Value
		s.logger.Debug("RecvAny: received from frontend", "msg", fmt.Sprintf("%T", msg.Value))
		return msg.Value, msg.Error
	case msg := <-s.backendReadingChan:
		s.lastRecv = msg.Value
		s.logger.Debug("RecvAny: received from backend", "msg", fmt.Sprintf("%T", msg.Value))
		return msg.Value, msg.Error
	}
}

func (s *Session) freeLastRecvAndContinueSender() {
	if s.lastRecv == nil {
		s.logger.Debug("freeLastRecvAndContinueSender: lastRecv is nil, no-op")
		return
	}

	if _, ok := s.lastRecv.(pgwire.ServerMessage); ok {
		if s.backend != nil {
			s.logger.Debug("freeLastRecvAndContinueSender: calling backend.Continue()")
			s.backend.Continue()
		} else {
			s.logger.Debug("freeLastRecvAndContinueSender: lastRecv is ServerMessage but backend is nil, skipping Continue()")
		}
	} else {
		s.logger.Debug("freeLastRecvAndContinueSender: calling frontend.Reader().Continue()")
		s.frontend.Reader().Continue()
	}

	s.lastRecv = nil
}

// Close cancels the session's context and releases associated resources.
func (s *Session) Close() {
	if flushError := s.flush(); flushError != nil {
		s.logger.Error("session close: error flushing to client", "error", flushError)
	}

	s.cancel()

	s.releaseBackend()

	if s.conn != nil {
		closeErr := s.conn.Close()
		if closeErr != nil {
			s.logger.Error("session close: error closing client connection", "error", closeErr)
		}
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
		s.logger.Debug("recv client message", "type", fmt.Sprintf("%T", msg))

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
	ctx, cancel := context.WithTimeout(s.ctx, time.Second)
	defer cancel()
	be, err := s.database.Acquire(ctx, *s.userConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire backend: %w", err)
	}
	s.backend = be
	s.backendAcquisitionID++ // Increment to ensure unique statement names per acquisition
	s.backendReadingChan = make(chan backend.ReadResult[pgwire.ServerMessage])
	s.backendCtx, s.cancelBackendCtx = context.WithCancel(s.ctx)
	logger := s.logger.With("backend", s.backend.Name())

	// Debug: check if context is already done
	logger.Debug("acquireBackend: created backendCtx", "ctxPtr", fmt.Sprintf("%p", s.backendCtx), "cancelPtr", fmt.Sprintf("%p", s.cancelBackendCtx))
	if s.backendCtx.Err() != nil {
		logger.Error("acquireBackend: backendCtx is already done!", "err", s.backendCtx.Err())
	}
	if s.ctx.Err() != nil {
		logger.Error("acquireBackend: session ctx is done!", "err", s.ctx.Err())
	}

	// Get the reading channel once before starting the loop to avoid
	// race conditions when the backend is released.
	// Also capture the context locally to avoid race with releaseBackend setting it to nil.
	backendCh := s.backend.ReadingChan()
	backendCtx := s.backendCtx
	backendReadingChan := s.backendReadingChan
	logger.Debug("acquireBackend: captured backendCtx", "ctxPtr", fmt.Sprintf("%p", backendCtx))
	go func() {
		defer logger.Debug("backend reader goroutine exited")
		// Debug: check if context is already done at start
		logger.Debug("backend reader: goroutine starting", "ctxPtr", fmt.Sprintf("%p", backendCtx), "ctxErr", backendCtx.Err())
		if backendCtx.Err() != nil {
			logger.Error("backend reader: context already done at start!", "err", backendCtx.Err(), "ctxPtr", fmt.Sprintf("%p", backendCtx))
			return
		}
		for {
			logger.Debug("backend reader: waiting for message from backend")
			select {
			case <-backendCtx.Done():
				logger.Debug("backend reader: context done")
				return
			case msg, ok := <-backendCh:
				if !ok {
					// Channel closed
					logger.Debug("backend reader: channel closed")
					return
				}
				logger.Debug("backend reader: received message, forwarding", "msg", fmt.Sprintf("%T", msg.Value))
				select {
				case <-backendCtx.Done():
					logger.Debug("backend reader: context done while forwarding")
					return
				case backendReadingChan <- msg:
					logger.Debug("backend reader: forwarded message")
					// Continue() is called by freeLastRecvAndContinueSender when
					// the message is processed
				}
			}
		}
	}()

	return logger, nil
}

func (s *Session) releaseBackend() {
	if s.backend == nil {
		s.logger.Debug("releaseBackend: backend is nil, nothing to release")
		return
	}

	s.logger.Debug("releaseBackend: releasing", "ctxPtr", fmt.Sprintf("%p", s.backendCtx), "cancelPtr", fmt.Sprintf("%p", s.cancelBackendCtx))

	// Cancel the backend context FIRST to stop the reader goroutine,
	// before releasing the backend connection (which sets reader to nil)
	s.cancelBackendCtx()
	s.logger.Debug("releaseBackend: canceled context")
	s.backendCtx = nil
	s.cancelBackendCtx = nil

	// Now it's safe to release the backend and close the channel
	s.backend.Release()
	s.backend = nil

	close(s.backendReadingChan)
	s.backendReadingChan = nil

	// Clear statement name map so new acquisition gets fresh names.
	// The backend we just released may be used by another session,
	// and we don't want statement name collisions.
	s.statementNameMap = nil
}

func (s *Session) runWithBackend(firstMsg pgwire.ClientMessage) error {
	logger, err := s.acquireBackend()
	if err != nil {
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
				if s.state.CopyMode == pgwire.CopyNone {
					return false, pgwire.NewProtocolViolation(fmt.Errorf("copy request without active copy session"), msg)
				}
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
				s.backend.Kill(s.ctx, err)
				return false, err
			},
		},
	}

	var msg pgwire.Message = firstMsg
	for {
		s.logger.Debug("runWithBackend: handling message", "msgType", fmt.Sprintf("%T", msg))
		continueWithBackend, err := backendAcquiredState.Handle(msg)
		s.logger.Debug("runWithBackend: handler returned", "continueWithBackend", continueWithBackend, "err", err)
		if !continueWithBackend || err != nil {
			// Transition out to idle client state.
			// Ensure we flush any pending messages (like ReadyForQuery) before returning.
			if flushErr := s.flush(); flushErr != nil {
				s.logger.Error("error flushing before releasing backend", "error", flushErr)
			}
			if err != nil {
				return err
			}
			return nil
		}

		// Ensure we've sent any pending messages
		if err := s.flush(); err != nil {
			return err
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
	s.logger.Debug("runSimpleQueryWithBackend: sending to backend")
	if err := s.backend.Send(msg.Client()); err != nil {
		return false, err
	}
	s.logger.Debug("runSimpleQueryWithBackend: sent")
	return true, nil
}

func (s *Session) runExtendedQueryWithBackend(msg pgwire.ClientExtendedQuery) (bool, error) {
	s.logger.Debug("runExtendedQueryWithBackend: start", "msgType", fmt.Sprintf("%T", msg))
	extendedQueryRewriter := pgwire.ClientExtendedQueryHandlers[pgproto3.FrontendMessage]{
		Bind: func(msg pgwire.ClientExtendedQueryBind) (pgproto3.FrontendMessage, error) {
			return &pgproto3.Bind{
				PreparedStatement:    s.clientToServerPreparedStatementName(msg.T.PreparedStatement),
				DestinationPortal:    s.clientToServerPortalName(msg.T.DestinationPortal),
				ParameterFormatCodes: msg.T.ParameterFormatCodes,
				Parameters:           msg.T.Parameters,
				ResultFormatCodes:    msg.T.ResultFormatCodes,
			}, nil
		},
		Parse: func(msg pgwire.ClientExtendedQueryParse) (pgproto3.FrontendMessage, error) {
			return &pgproto3.Parse{
				Name:          s.clientToServerPreparedStatementName(msg.T.Name),
				Query:         msg.T.Query,
				ParameterOIDs: msg.T.ParameterOIDs,
			}, nil
		},
		Execute: func(msg pgwire.ClientExtendedQueryExecute) (pgproto3.FrontendMessage, error) {
			return &pgproto3.Execute{
				Portal:  s.clientToServerPortalName(msg.T.Portal),
				MaxRows: msg.T.MaxRows,
			}, nil
		},
		Describe: func(msg pgwire.ClientExtendedQueryDescribe) (pgproto3.FrontendMessage, error) {
			return &pgproto3.Describe{
				ObjectType: msg.T.ObjectType,
				Name:       s.clientToServerObjectName(msg.T.ObjectType, msg.T.Name),
			}, nil
		},
		Close: func(msg pgwire.ClientExtendedQueryClose) (pgproto3.FrontendMessage, error) {
			return &pgproto3.Close{
				ObjectType: msg.T.ObjectType,
				Name:       s.clientToServerObjectName(msg.T.ObjectType, msg.T.Name),
			}, nil
		},
		Sync: func(msg pgwire.ClientExtendedQuerySync) (pgproto3.FrontendMessage, error) {
			return msg.T, nil
		},
		Flush: func(msg pgwire.ClientExtendedQueryFlush) (pgproto3.FrontendMessage, error) {
			return msg.T, nil
		},
	}

	s.logger.Debug("runExtendedQueryWithBackend: calling Handle")
	rewritten, err := extendedQueryRewriter.Handle(msg)
	s.logger.Debug("runExtendedQueryWithBackend: Handle returned", "rewritten", fmt.Sprintf("%T", rewritten), "err", err)
	if err != nil {
		return false, err
	}
	s.logger.Debug("runExtendedQueryWithBackend: sending to backend")
	if err := s.backend.Send(rewritten); err != nil {
		return false, err
	}

	s.logger.Debug("runExtendedQueryWithBackend: updating state")
	s.state.UpdateForExtendedQueryMessage(msg)
	s.logger.Debug("runExtendedQueryWithBackend: done")
	return true, nil
}

func (s *Session) handleServerResponse(msg pgwire.ServerResponse) (bool, error) {
	continueWithBackend := true
	s.state.UpdateForServerResponseMessage(msg)
	if _, ok := msg.(pgwire.ServerResponseReadyForQuery); ok {
		// We may have missed a status update message, double check.
		s.sendBackendParameterStatusChanges()
		// Once the transaction ends, we're done with the backend.
		// TODO: should we also release the backend if s.state.TxStatus == error ?
		if s.state.TxStatus == pgwire.TxIdle {
			continueWithBackend = false
			s.logger.Info("transaction ended, releasing backend")
		}
	}

	s.frontend.Send(msg.Server())
	return continueWithBackend, nil
}

func (s *Session) handleServerExtendedQuery(msg pgwire.ServerExtendedQuery) (bool, error) {
	s.state.UpdateForServerExtendedQueryMessage(msg)
	s.frontend.Send(msg.Server())
	return true, nil
}

func (s *Session) handleServerAsync(msg pgwire.ServerAsync) (bool, error) {
	s.frontend.Send(msg.Server())
	return true, nil
}

func (s *Session) handleServerCopy(msg pgwire.ServerCopy) (bool, error) {
	s.state.UpdateForServerCopyMessage(msg)
	s.frontend.Send(msg.Server())
	return true, nil
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
	// Check if we already have a mapping for this client name
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
	// Portals are per-transaction and don't need global uniqueness like statements.
	// Just use the PID and acquisition ID as prefix.
	return fmt.Sprintf("pgwire_%d_%d_%s", s.state.PID, s.backendAcquisitionID, name)
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
	s.state.PreparedStatements = pgwire.NamedObjectState[bool]{
		Alive:         make(map[string]bool),
		PendingCreate: make(map[string]bool),
		PendingClose:  make(map[string]bool),
	}
	s.state.Portals = pgwire.NamedObjectState[bool]{
		Alive:         make(map[string]bool),
		PendingCreate: make(map[string]bool),
		PendingClose:  make(map[string]bool),
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

type Frontend struct {
	*pgproto3.Backend
	ctx    context.Context
	reader *backend.ChanReader[pgwire.ClientMessage]
}

func (f *Frontend) Receive() (pgwire.ClientMessage, error) {
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

	if m, ok := pgwire.ToClientMessage(msg); ok {
		return m, nil
	}

	return nil, fmt.Errorf("unknown frontend message: %T", msg)
}

func (f *Frontend) receiveBackgroundThread() (*pgwire.ClientMessage, error) {
	msg, err := f.Receive()
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	return &msg, nil
}

func (f *Frontend) Reader() *backend.ChanReader[pgwire.ClientMessage] {
	if f.reader == nil {
		f.reader = backend.NewChanReader(f.receiveBackgroundThread)
	}
	return f.reader
}

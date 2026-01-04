package frontend

// proxy_loop.go implements the main proxy loop using pgproxy.Session.
// This replaces the runMainLoop/runWithBackend implementation.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/justjake/pglink/pkg/backend"
	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/observability"
	"github.com/justjake/pglink/pkg/pgproxy"
	"github.com/justjake/pglink/pkg/pgserver"
	"github.com/justjake/pglink/pkg/pgwire"
)

// errTerminateConn signals that the client sent a Terminate message.
var errTerminateConn = errors.New("client terminating connection")

// proxyState holds state for a proxy session using pgproxy.Session.
// This replaces the Session struct for the pgproxy-based implementation.
type proxyState struct {
	// Service and config
	service    *Service
	dbConfig   *config.DatabaseConfig
	userConfig *config.UserConfig
	database   *backend.Database

	// Client info
	clientConn   *pgserver.ClientConn
	databaseName string
	userName     string
	logger       *slog.Logger

	// pgproxy session
	session *pgproxy.Session

	// Current backend (if acquired)
	pooledBackend *backend.PooledBackend

	// Statement name rewriting
	statementNameMap         map[string]string
	serverStatementQueryHash map[string]uint64

	// Protocol state tracking
	// TODO: Replace with pgproxy.MessageTrackers in Phase 5
	txStatus          pgwire.TxStatus
	extendedQueryMode bool
	pendingRequests   []pendingRequest
	parameterStatuses map[string]string
	copyMode          pgwire.CopyMode

	// Observability
	tracingEnabled bool
	metrics        *observability.Metrics
	sessionSpan    trace.Span
	lastSQL        string

	// Timeouts
	queryStartTime        time.Time
	txStartTime           time.Time
	lastReadyForQueryTime time.Time
}

// pendingRequest tracks an outstanding request to the backend.
type pendingRequest struct {
	requestType   pgwire.MsgType
	action        pgwire.ResponseAction
	statementName string
	query         string
	queryHash     uint64
}

// Global counter for unique statement names
var proxyStatementSeqCounter atomic.Uint64

// runProxyLoop runs the main proxy loop using pgproxy.Session.
// This is called from Service.connHandler after startup is complete.
func (s *Service) runProxyLoop(ctx context.Context, conn *pgserver.ClientConn, authData *authConnData) (err error) {
	// Create proxy state
	state := &proxyState{
		service:                  s,
		dbConfig:                 authData.dbConfig,
		userConfig:               authData.userConfig,
		database:                 authData.database,
		clientConn:               conn,
		databaseName:             conn.Database,
		userName:                 conn.User,
		logger:                   s.logger.With("client", conn.Conn.RemoteAddr().String(), "user", conn.User, "database", conn.Database, "pid", conn.ProcessID),
		statementNameMap:         make(map[string]string),
		serverStatementQueryHash: make(map[string]uint64),
		txStatus:                 pgwire.TxIdle,
		parameterStatuses:        make(map[string]string),
		tracingEnabled:           s.tracingEnabled,
		metrics:                  s.metrics,
	}

	// Set the cancel handler on the ClientConn so pgserver can route cancel requests to us.
	// CancelRequest opens a NEW TCP connection to send the cancel, so it's safe to call
	// from this separate goroutine without synchronization with the main loop.
	conn.CancelHandler = func(ctx context.Context, clientConn *pgserver.ClientConn, cancelConn *pgserver.CancelConn) error {
		if state.pooledBackend == nil {
			state.logger.Debug("cancel request but no backend connected")
			return nil
		}
		cancelCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := state.pooledBackend.PgConn().CancelRequest(cancelCtx); err != nil {
			state.logger.Warn("failed to send cancel request to backend", "error", err)
			return err
		}
		state.logger.Info("sent cancel request to backend")
		return nil
	}

	// Copy startup parameters
	for k, v := range conn.StartupParameters {
		state.parameterStatuses[k] = v
	}

	// Create ClientFrontend wrapper
	frontend := NewClientFrontend(conn)

	// Create pgproxy.Session
	// TODO: investigate why using `ctx` here causes i/o timeout on both client and backend
	// connections at the same millisecond. For now, use context.Background() to isolate
	// the session from any deadlines on the incoming context.
	session, err := pgproxy.NewSession(context.Background(), pgproxy.SessionConfig{
		Frontend: frontend,
		AcquireBackend: func(ctx context.Context) (pgproxy.Backend, error) {
			return state.acquireBackend(ctx)
		},
		HealthCheck: func(ctx context.Context) error {
			return state.checkTimeouts(ctx)
		},
		HealthCheckPeriod: computeHealthCheckPeriod(authData.dbConfig),
		Logger:            state.logger,
		RingBufferConfig:  pgwire.RingBufferConfigForSize(authData.dbConfig.GetMessageBufferBytes()),
	})
	if err != nil {
		return fmt.Errorf("failed to create proxy session: %w", err)
	}
	state.session = session
	defer func() {
		if closeErr := session.Close(ctx); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				// Log the close error since we're already returning a different error
				state.logger.Debug("session close error", "error", closeErr)
			}
		}
	}()

	// Start session span for tracing
	if state.tracingEnabled {
		_, state.sessionSpan = otel.Tracer("pglink").Start(ctx, "pglink.session",
			trace.WithAttributes(
				attribute.String(observability.AttrDBUser, state.userName),
				attribute.String(observability.AttrDBName, state.databaseName),
				attribute.Int64("pglink.pid", int64(conn.ProcessID)),
			),
		)
		defer state.sessionSpan.End()
	}

	// Record metrics
	if state.metrics != nil {
		state.metrics.RecordClientConnection(state.databaseName, state.userName)
		defer state.metrics.RecordClientDisconnect(state.databaseName, state.userName)
	}

	// Run the proxy loop
	return state.runLoop(ctx)
}

// runLoop is the main proxy message loop.
func (ps *proxyState) runLoop(ctx context.Context) error {
	// TODO: investigate why using `ctx` in Stream/Dispatch causes i/o timeout.
	// For now, use context.Background() to avoid any deadline interference.
	// The session will still terminate properly via the session's Close() method.
	bgCtx := context.Background()
	for pos, err := range ps.session.Stream(bgCtx) {
		if err != nil {
			return ps.handleStreamError(err)
		}

		action, err := ps.handlePos(bgCtx, pos)
		if err != nil {
			return err
		}

		if err := pos.Dispatch(bgCtx, action); err != nil {
			return err
		}
	}
	return nil
}

// handleStreamError converts stream errors to appropriate responses.
func (ps *proxyState) handleStreamError(err error) error {
	if errors.Is(err, context.Canceled) {
		return err
	}
	return err
}

// handlePos dispatches to the appropriate handler based on message source.
func (ps *proxyState) handlePos(ctx context.Context, pos pgproxy.Pos) (pgproxy.Action, error) {
	if pos.FromClient() {
		return ps.handleClientMessage(ctx, pos)
	}
	return ps.handleServerMessage(ctx, pos)
}

// ============================================================================
// Client Message Handlers
// ============================================================================

func (ps *proxyState) handleClientMessage(ctx context.Context, pos pgproxy.Pos) (pgproxy.Action, error) {
	msg := pos.ClientMsg()

	switch m := msg.(type) {
	// Simple query
	case *pgwire.ClientQuery:
		return ps.handleClientQuery(ctx, pos, m)

	// Extended query
	case *pgwire.ClientParse:
		return ps.handleClientParse(ctx, pos, m)
	case *pgwire.ClientBind:
		return ps.handleClientBind(ctx, pos, m)
	case *pgwire.ClientDescribe:
		return ps.handleClientDescribe(ctx, pos, m)
	case *pgwire.ClientExecute:
		return ps.handleClientExecute(ctx, pos, m)
	case *pgwire.ClientClose:
		return ps.handleClientClose(ctx, pos, m)
	case *pgwire.ClientSync:
		return ps.handleClientSync(ctx, pos, m)
	case *pgwire.ClientFlush:
		return pgproxy.Forward(msg), nil

	// Copy
	case *pgwire.ClientCopyData:
		return pgproxy.Forward(msg), nil
	case *pgwire.ClientCopyDone:
		return pgproxy.Forward(msg), nil
	case *pgwire.ClientCopyFail:
		return pgproxy.Forward(msg), nil

	// Terminate
	case *pgwire.ClientTerminate:
		return pgproxy.TerminateClient(m, errTerminateConn), nil

	// Invalid messages
	case *pgwire.ClientCancelRequest:
		err := pgwire.NewProtocolViolation(fmt.Errorf("cancel request on normal connection"), m)
		return pgproxy.TerminateBoth(m, err), nil

	default:
		// Check for startup messages using interface assertion
		if _, ok := msg.(pgwire.ClientStartup); ok {
			err := pgwire.NewProtocolViolation(fmt.Errorf("startup completed already"), msg)
			return pgproxy.TerminateBoth(msg, err), nil
		}
		// Unknown message - forward anyway
		return pgproxy.Forward(msg), nil
	}
}

func (ps *proxyState) handleClientQuery(ctx context.Context, pos pgproxy.Pos, msg *pgwire.ClientQuery) (pgproxy.Action, error) {
	parsed := msg.Parse()
	ps.lastSQL = parsed.String
	ps.queryStartTime = time.Now()

	ps.pushRequest(pendingRequest{
		requestType: pgwire.MsgClientQuery,
		action:      pgwire.ActionForward,
		query:       parsed.String,
		queryHash:   pgwire.HashQuery(parsed.String),
	})

	return pgproxy.Forward(msg), nil
}

func (ps *proxyState) handleClientParse(ctx context.Context, pos pgproxy.Pos, msg *pgwire.ClientParse) (pgproxy.Action, error) {
	parsed := msg.Parse()
	serverStmt := ps.clientToServerStatementName(parsed.Name)
	queryHash := pgwire.HashQuery(parsed.Query)

	// Track query hash for potential re-creation
	ps.serverStatementQueryHash[serverStmt] = queryHash
	ps.lastSQL = parsed.Query
	ps.extendedQueryMode = true

	ps.pushRequest(pendingRequest{
		requestType:   pgwire.MsgClientParse,
		action:        pgwire.ActionForward,
		statementName: parsed.Name,
		query:         parsed.Query,
		queryHash:     queryHash,
	})

	// Check if we can skip the Parse (statement already exists and cached)
	// We need to acquire the backend first to check HasStatement
	if serverStmt != "" {
		// Acquire backend if not already acquired
		if ps.pooledBackend == nil {
			if _, err := ps.acquireBackend(ctx); err != nil {
				return nil, fmt.Errorf("acquire backend for parse skip check: %w", err)
			}
		}
		if ps.pooledBackend.HasStatement(serverStmt) {
			if _, inCache := ps.database.StatementCache().Get(queryHash); inCache {
				// Skip Parse, mark for fake ParseComplete
				ps.pendingRequests[len(ps.pendingRequests)-1].action = pgwire.ActionSkip
				if ps.metrics != nil {
					ps.metrics.RecordPreparedStatementCacheHit(ps.databaseName)
				}
				return pgproxy.Skip(msg), nil
			}
		}
	}

	// Rewrite statement name if different
	if serverStmt != parsed.Name {
		rewritten := &pgproto3.Parse{
			Name:          serverStmt,
			Query:         parsed.Query,
			ParameterOIDs: parsed.ParameterOIDs,
		}
		rewrittenMsg := pgwire.Client(rewritten)
		// Update backend state to track PendingCreate for the statement (named statements only)
		if serverStmt != "" && ps.pooledBackend != nil {
			ps.pooledBackend.UpdateState(rewrittenMsg)
		}
		return pgproxy.Rewrite(msg, rewrittenMsg), nil
	}

	// Update backend state to track PendingCreate for the statement (named statements only)
	if serverStmt != "" && ps.pooledBackend != nil {
		ps.pooledBackend.UpdateState(msg)
	}
	return pgproxy.Forward(msg), nil
}

func (ps *proxyState) handleClientBind(ctx context.Context, pos pgproxy.Pos, msg *pgwire.ClientBind) (pgproxy.Action, error) {
	parsed := msg.Parse()
	serverStmt := ps.clientToServerStatementName(parsed.PreparedStatement)
	serverPortal := parsed.DestinationPortal // Portals don't need rewriting

	ps.extendedQueryMode = true
	ps.pushRequest(pendingRequest{
		requestType:   pgwire.MsgClientBind,
		action:        pgwire.ActionForward,
		statementName: parsed.PreparedStatement,
	})

	// Check if statement exists on this backend
	statementExistsOnBackend := ps.pooledBackend != nil && ps.pooledBackend.HasStatement(serverStmt)

	if !statementExistsOnBackend && serverStmt != "" {
		// Try to re-create statement from cache
		queryHash, hasHash := ps.serverStatementQueryHash[serverStmt]
		if hasHash {
			cachedStmt, inCache := ps.database.StatementCache().Get(queryHash)
			if inCache {
				pos.Logger().Debug("re-creating statement on backend",
					"clientName", parsed.PreparedStatement,
					"serverName", serverStmt,
					"queryHash", queryHash)

				if ps.metrics != nil {
					ps.metrics.RecordPreparedStatementCacheHit(ps.databaseName)
					ps.metrics.RecordPreparedStatementRecreation(ps.databaseName)
				}

				// Record span event
				if ps.tracingEnabled {
					_, span := otel.Tracer("pglink").Start(ctx, "pglink.stmt.recreate",
						trace.WithAttributes(
							attribute.String(observability.AttrStatementName, parsed.PreparedStatement),
							attribute.Int64("query_hash", int64(queryHash)),
						),
					)
					span.End()
				}

				// Send Parse before Bind
				parseMsg := pgwire.Client(&pgproto3.Parse{
					Name:          serverStmt,
					Query:         cachedStmt.Query,
					ParameterOIDs: cachedStmt.ParameterOIDs,
				})
				if err := ps.session.QueueSend(ctx, parseMsg); err != nil {
					return nil, fmt.Errorf("failed to send Parse for statement recreation: %w", err)
				}

				// Push a request for the ParseComplete we'll receive
				ps.pushRequest(pendingRequest{
					requestType:   pgwire.MsgClientParse,
					action:        pgwire.ActionSkip, // Don't forward ParseComplete to client
					statementName: parsed.PreparedStatement,
					query:         cachedStmt.Query,
					queryHash:     queryHash,
				})
			}
		}
	}

	// Rewrite statement/portal names if different
	if serverStmt != parsed.PreparedStatement || serverPortal != parsed.DestinationPortal {
		rewritten := &pgproto3.Bind{
			DestinationPortal:    serverPortal,
			PreparedStatement:    serverStmt,
			ParameterFormatCodes: parsed.ParameterFormatCodes,
			Parameters:           parsed.Parameters,
			ResultFormatCodes:    parsed.ResultFormatCodes,
		}
		return pgproxy.Rewrite(msg, pgwire.Client(rewritten)), nil
	}

	return pgproxy.Forward(msg), nil
}

func (ps *proxyState) handleClientDescribe(ctx context.Context, pos pgproxy.Pos, msg *pgwire.ClientDescribe) (pgproxy.Action, error) {
	parsed := msg.Parse()
	ps.extendedQueryMode = true

	ps.pushRequest(pendingRequest{
		requestType:   pgwire.MsgClientDescribe,
		action:        pgwire.ActionForward,
		statementName: parsed.Name,
	})

	var serverName string
	if parsed.ObjectType == 'S' {
		serverName = ps.clientToServerStatementName(parsed.Name)
	} else {
		serverName = parsed.Name
	}

	if serverName != parsed.Name {
		rewritten := &pgproto3.Describe{
			ObjectType: parsed.ObjectType,
			Name:       serverName,
		}
		return pgproxy.Rewrite(msg, pgwire.Client(rewritten)), nil
	}

	return pgproxy.Forward(msg), nil
}

func (ps *proxyState) handleClientExecute(ctx context.Context, pos pgproxy.Pos, msg *pgwire.ClientExecute) (pgproxy.Action, error) {
	ps.extendedQueryMode = true
	ps.queryStartTime = time.Now()

	ps.pushRequest(pendingRequest{
		requestType: pgwire.MsgClientExecute,
		action:      pgwire.ActionForward,
	})

	// Portal names don't need rewriting
	return pgproxy.Forward(msg), nil
}

func (ps *proxyState) handleClientClose(ctx context.Context, pos pgproxy.Pos, msg *pgwire.ClientClose) (pgproxy.Action, error) {
	parsed := msg.Parse()

	ps.pushRequest(pendingRequest{
		requestType:   pgwire.MsgClientClose,
		action:        pgwire.ActionForward,
		statementName: parsed.Name,
	})

	var serverName string
	if parsed.ObjectType == 'S' {
		serverName = ps.clientToServerStatementName(parsed.Name)
	} else {
		serverName = parsed.Name
	}

	if serverName != parsed.Name {
		rewritten := &pgproto3.Close{
			ObjectType: parsed.ObjectType,
			Name:       serverName,
		}
		return pgproxy.Rewrite(msg, pgwire.Client(rewritten)), nil
	}

	return pgproxy.Forward(msg), nil
}

func (ps *proxyState) handleClientSync(ctx context.Context, pos pgproxy.Pos, msg *pgwire.ClientSync) (pgproxy.Action, error) {
	ps.pushRequest(pendingRequest{
		requestType: pgwire.MsgClientSync,
		action:      pgwire.ActionForward,
	})
	return pgproxy.Forward(msg), nil
}

// ============================================================================
// Server Message Handlers
// ============================================================================

func (ps *proxyState) handleServerMessage(ctx context.Context, pos pgproxy.Pos) (pgproxy.Action, error) {
	msg := pos.ServerMsg()

	// Update backend state
	if ps.pooledBackend != nil {
		ps.pooledBackend.UpdateState(msg)
	}

	switch m := msg.(type) {
	case *pgwire.ServerReadyForQuery:
		return ps.handleServerReadyForQuery(ctx, pos, m)
	case *pgwire.ServerParseComplete:
		return ps.handleServerParseComplete(ctx, pos, m)
	case *pgwire.ServerBindComplete:
		return ps.handleServerBindComplete(ctx, pos, m)
	case *pgwire.ServerCloseComplete:
		return ps.handleServerCloseComplete(ctx, pos, m)
	case *pgwire.ServerParameterStatus:
		return ps.handleServerParameterStatus(ctx, pos, m)
	case *pgwire.ServerErrorResponse:
		return ps.handleServerError(ctx, pos, m)
	case *pgwire.ServerCopyInResponse:
		ps.copyMode = pgwire.CopyIn
		return pgproxy.Forward(msg), nil
	case *pgwire.ServerCopyOutResponse:
		ps.copyMode = pgwire.CopyOut
		return pgproxy.Forward(msg), nil
	case *pgwire.ServerCopyDone:
		ps.copyMode = pgwire.CopyNone
		return pgproxy.Forward(msg), nil
	default:
		return pgproxy.Forward(msg), nil
	}
}

func (ps *proxyState) handleServerReadyForQuery(ctx context.Context, pos pgproxy.Pos, msg *pgwire.ServerReadyForQuery) (pgproxy.Action, error) {
	parsed := msg.Parse()
	oldTxStatus := ps.txStatus
	ps.txStatus = pgwire.TxStatus(parsed.TxStatus)
	ps.extendedQueryMode = false
	ps.copyMode = pgwire.CopyNone

	// Track transaction start time
	if oldTxStatus == pgwire.TxIdle && ps.txStatus != pgwire.TxIdle {
		ps.txStartTime = time.Now()
	}

	// Track when we became idle (for idle transaction timeout)
	ps.lastReadyForQueryTime = time.Now()
	// Clear query start time since query is complete
	ps.queryStartTime = time.Time{}

	// Pop the corresponding request
	ps.popRequest()

	// Check if we should release the backend
	if ps.txStatus == pgwire.TxIdle && len(ps.pendingRequests) == 0 {
		if err := ps.session.ReleaseBackend(ctx); err != nil {
			pos.Logger().Warn("error releasing backend", "error", err)
		}
		ps.pooledBackend = nil
	}

	return pgproxy.Forward(msg), nil
}

func (ps *proxyState) handleServerParseComplete(ctx context.Context, pos pgproxy.Pos, msg *pgwire.ServerParseComplete) (pgproxy.Action, error) {
	// Check if we should skip this response
	if len(ps.pendingRequests) > 0 {
		req := ps.pendingRequests[0]
		if req.requestType == pgwire.MsgClientParse && req.action == pgwire.ActionSkip {
			ps.popRequest()
			// Send fake ParseComplete to client for skipped Parse
			fakeMsg := pgwire.Server(&pgproto3.ParseComplete{})
			if err := ps.session.QueueSend(ctx, fakeMsg); err != nil {
				return nil, err
			}
			return pgproxy.Skip(msg), nil
		}
	}

	// Cache the prepared statement if it has a name
	if len(ps.pendingRequests) > 0 {
		req := ps.pendingRequests[0]
		if req.query != "" && req.statementName != "" {
			ps.database.StatementCache().Put(&pgwire.PreparedStatement{
				Query:     req.query,
				QueryHash: req.queryHash,
			})
		}
	}

	ps.popRequest()
	return pgproxy.Forward(msg), nil
}

func (ps *proxyState) handleServerBindComplete(ctx context.Context, pos pgproxy.Pos, msg *pgwire.ServerBindComplete) (pgproxy.Action, error) {
	ps.popRequest()
	return pgproxy.Forward(msg), nil
}

func (ps *proxyState) handleServerCloseComplete(ctx context.Context, pos pgproxy.Pos, msg *pgwire.ServerCloseComplete) (pgproxy.Action, error) {
	ps.popRequest()
	return pgproxy.Forward(msg), nil
}

func (ps *proxyState) handleServerParameterStatus(ctx context.Context, pos pgproxy.Pos, msg *pgwire.ServerParameterStatus) (pgproxy.Action, error) {
	parsed := msg.Parse()
	ps.parameterStatuses[parsed.Name] = parsed.Value
	return pgproxy.Forward(msg), nil
}

func (ps *proxyState) handleServerError(ctx context.Context, pos pgproxy.Pos, msg *pgwire.ServerErrorResponse) (pgproxy.Action, error) {
	parsed := msg.Parse()

	// Pop requests on error
	ps.popRequest()

	// Check if this is a fatal error
	if parsed.Severity == "FATAL" {
		err := pgwire.NewErr(pgwire.ErrorFatal, parsed.Code, parsed.Message, errors.New(parsed.Detail))
		return pgproxy.TerminateBoth(msg, err), nil
	}

	return pgproxy.Forward(msg), nil
}

// ============================================================================
// Backend Acquisition
// ============================================================================

func (ps *proxyState) acquireBackend(ctx context.Context) (pgproxy.Backend, error) {
	// Return existing backend if already acquired.
	// This is important because Session.AcquireBackend may call this callback
	// during restoreVariables or message forwarding, and we must return the
	// same backend to avoid acquiring multiple connections from the pool.
	if ps.pooledBackend != nil {
		return ps.pooledBackend, nil
	}

	acquireCtx, cancel := context.WithTimeout(ctx, ps.dbConfig.PoolAcquireTimeout())
	// DON'T defer cancel() - call it explicitly BEFORE restoreVariables
	// to avoid pgconn setting deadline when context is cancelled

	be, err := ps.database.Acquire(acquireCtx, *ps.userConfig)
	if err != nil {
		cancel()
		return nil, err
	}
	ps.pooledBackend = be

	// Cancel the acquire context BEFORE any further operations.
	// This prevents any context watchers from interfering with later I/O.
	cancel()

	// Restore variables before returning.
	// Use context.Background() with no deadline to avoid any deadline interference.
	// The SET query should complete quickly. If it hangs, we have bigger problems.
	if err := ps.restoreVariables(context.Background()); err != nil {
		be.MarkForDestroy(err)
		be.Release()
		ps.pooledBackend = nil
		return nil, fmt.Errorf("failed to restore variables: %w", err)
	}

	// Note: Do NOT call SyncConn here. The old working code in session.go doesn't
	// call it - it just runs restoreVariables via pgconn.Exec(), cancels the context,
	// then starts the ring buffer. pgconn.Exec() internally stops the bgReader after
	// each query completes (see exitPotentialWriteReadDeadlock).

	// Note: Do NOT call be.StartRingBuffer() here.
	// The pgproxy.Session will create its own ring buffer for the backend.

	ps.logger = ps.logger.With("backend", be.String())
	return be, nil
}

func (ps *proxyState) restoreVariables(ctx context.Context) error {
	if ps.pooledBackend == nil {
		return nil
	}

	// Get parameter status changes
	tracked := ps.pooledBackend.TrackedParameters()
	if len(tracked) == 0 {
		return nil
	}

	// Use the proper API that filters out immutable parameters
	backendStatuses := ps.pooledBackend.ParameterStatuses()
	diff := backendStatuses.DiffToTip(tracked, pgwire.ParameterStatuses(ps.parameterStatuses))
	query := diff.BuildSetQuery()

	if query == "" {
		ps.pooledBackend.SyncParameterStatusesFromPgConn()
		return nil
	}

	ps.logger.Debug("restoring variables", "query", query)

	pgConn := ps.pooledBackend.PgConn()
	_, err := pgConn.Exec(ctx, query).ReadAll()
	if err != nil {
		return fmt.Errorf("failed to execute SET: %w", err)
	}

	// Sync parameter statuses from pgconn
	ps.pooledBackend.SyncParameterStatusesFromPgConn()

	return nil
}

// ============================================================================
// Timeout Handling
// ============================================================================

func (ps *proxyState) checkTimeouts(ctx context.Context) error {
	now := time.Now()

	// Query timeout
	queryTimeout := ps.dbConfig.QueryTimeout.Duration()
	if queryTimeout > 0 && !ps.queryStartTime.IsZero() {
		if now.Sub(ps.queryStartTime) > queryTimeout {
			return ps.handleQueryTimeout(ctx)
		}
	}

	// Idle transaction timeout - fires when idle IN a transaction for too long
	// Only check when NOT actively running a query (queryStartTime is zero when idle)
	idleTxTimeout := ps.dbConfig.IdleTransactionTimeout.Duration()
	queryIsActive := !ps.queryStartTime.IsZero()
	if idleTxTimeout > 0 && ps.txStatus != pgwire.TxIdle && !ps.lastReadyForQueryTime.IsZero() && !queryIsActive {
		if now.Sub(ps.lastReadyForQueryTime) > idleTxTimeout {
			return ps.handleIdleTransactionTimeout(ctx)
		}
	}

	// Transaction timeout - fires when total time in transaction exceeds limit
	txTimeout := ps.dbConfig.TransactionTimeout.Duration()
	if txTimeout > 0 && ps.txStatus != pgwire.TxIdle && !ps.txStartTime.IsZero() {
		if now.Sub(ps.txStartTime) > txTimeout {
			return ps.handleTransactionTimeout(ctx)
		}
	}

	return nil
}

func (ps *proxyState) handleQueryTimeout(ctx context.Context) error {
	ps.logger.Warn("query timeout exceeded", "elapsed", time.Since(ps.queryStartTime))

	if ps.pooledBackend != nil {
		// Try to cancel the query
		cancelCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := ps.pooledBackend.PgConn().CancelRequest(cancelCtx); err != nil {
			ps.logger.Warn("failed to cancel query", "error", err)
		}
	}

	return pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.QueryCanceled, "canceling statement due to query timeout", nil)
}

func (ps *proxyState) handleIdleTransactionTimeout(ctx context.Context) error {
	ps.logger.Warn("idle transaction timeout exceeded", "elapsed", time.Since(ps.lastReadyForQueryTime))
	return pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.IdleSessionTimeout, "terminating connection due to idle transaction timeout", nil)
}

func (ps *proxyState) handleTransactionTimeout(ctx context.Context) error {
	ps.logger.Warn("transaction timeout exceeded", "elapsed", time.Since(ps.txStartTime))
	return pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.QueryCanceled, "terminating connection due to transaction timeout", nil)
}

// ============================================================================
// Helpers
// ============================================================================

func (ps *proxyState) clientToServerStatementName(clientName string) string {
	if clientName == "" {
		return ""
	}

	if serverName, ok := ps.statementNameMap[clientName]; ok {
		return serverName
	}

	seq := proxyStatementSeqCounter.Add(1)
	serverName := fmt.Sprintf("pglink_stmt_%d_%s", seq, clientName)
	ps.statementNameMap[clientName] = serverName
	return serverName
}

func (ps *proxyState) pushRequest(req pendingRequest) {
	ps.pendingRequests = append(ps.pendingRequests, req)
}

func (ps *proxyState) popRequest() {
	if len(ps.pendingRequests) > 0 {
		ps.pendingRequests = ps.pendingRequests[1:]
	}
}

// computeHealthCheckPeriod returns the health check period based on configured timeouts.
// It returns the minimum of all configured timeouts divided by 4, with bounds between
// 100ms and 1s. This ensures timeouts are checked frequently enough to be accurate.
func computeHealthCheckPeriod(cfg *config.DatabaseConfig) time.Duration {
	const (
		minPeriod     = 100 * time.Millisecond
		maxPeriod     = time.Second
		periodDivisor = 4
	)

	minTimeout := time.Duration(0)

	if d := cfg.QueryTimeout.Duration(); d > 0 {
		if minTimeout == 0 || d < minTimeout {
			minTimeout = d
		}
	}
	if d := cfg.IdleTransactionTimeout.Duration(); d > 0 {
		if minTimeout == 0 || d < minTimeout {
			minTimeout = d
		}
	}
	if d := cfg.TransactionTimeout.Duration(); d > 0 {
		if minTimeout == 0 || d < minTimeout {
			minTimeout = d
		}
	}

	// No timeouts configured
	if minTimeout == 0 {
		return maxPeriod
	}

	period := minTimeout / periodDivisor
	if period < minPeriod {
		period = minPeriod
	}
	if period > maxPeriod {
		period = maxPeriod
	}
	return period
}

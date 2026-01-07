package pgproxy

// package pgproxy

// import (
// 	"context"
// 	"errors"
// 	"fmt"
// 	"io"
// 	"iter"
// 	"log/slog"
// 	"net"
// 	"sync"
// 	"time"

// 	"github.com/justjake/pglink/pkg/pgwire"
// )

// // ringBufferRuntime implements proxying on top of [pgwire.RingBuffer].
// // The session runs on the "main" goroutine, with helper reader goroutines for each connection.
// type ringBufferRuntime struct {
// 	*Session
// 	ringCfg pgwire.RingBufferConfig

// 	clientConn       Conn
// 	clientNetConn    net.Conn
// 	clientRingBuffer *pgwire.RingBuffer
// 	clientCursor     *pgwire.Cursor
// 	clientPos        pos

// 	backendConn       Conn
// 	backendNetConn    net.Conn
// 	backendRingBuffer *pgwire.RingBuffer
// 	backendCursor     *pgwire.Cursor
// 	backendPos        pos

// 	healthCheckTicker *time.Ticker
// 	nextMsgWaitCtx    context.Context
// 	cancelWaitCtx     func()

// 	isStreaming bool

// 	ringCtx       context.Context
// 	cancelRingCtx func()

// 	closeOnce sync.Once

// 	// todo: remove
// 	msgParser flyweightParser
// 	pos       pos
// }

// var _ Runtime = (*ringBufferRuntime)(nil)

// func NewRingBufferRuntime(ctx context.Context, session *Session) (Runtime, error) {
// 	panic("not implemented")
// 	// return newRingBufferRuntime(ctx, session, pgwire.DefaultRingBufferConfig())
// }

// func newRingBufferRuntime(ctx context.Context, session *Session, cfg pgwire.RingBufferConfig) (*ringBufferRuntime, error) {
// 	ringCtx, cancelRingCtx := context.WithCancel(ctx)
// 	runtime := &ringBufferRuntime{
// 		Session:       session,
// 		ringCfg:       cfg,
// 		ringCtx:       ringCtx,
// 		cancelRingCtx: cancelRingCtx,
// 	}
// 	return runtime, nil
// }

// // StartConn implements [Runtime].
// func (s *ringBufferRuntime) StartConn(ctx context.Context, role ProxyRole, conn Conn) error {
// 	connPtr, netConnPtr, ringPtr, cursorPtr, _ := s.conns(role)
// 	if *connPtr != nil || *netConnPtr != nil || *ringPtr != nil || *cursorPtr != nil {
// 		return fmt.Errorf("%s connection already started: %v %v %v %v", role, *connPtr, *netConnPtr, *ringPtr, *cursorPtr)
// 	}

// 	netConn, err := conn.AcquireNetConn(ctx)
// 	if err != nil {
// 		return fmt.Errorf("failed to acquire net conn: %w", err)
// 	}

// 	logger := s.Logger().WithGroup("runtime").With("role", role, "conn", conn)
// 	ring := pgwire.NewRingBuffer(s.ringCfg)
// 	if logger.Enabled(ctx, slog.LevelDebug) {
// 		ring.SetDebugLog(func(msg string, args ...any) {
// 			logger.Debug(msg, args...)
// 		})
// 	}
// 	ring.SetConn(netConn)
// 	ring.StartNetConnReader(ctx, netConn)

// 	var cursor *pgwire.Cursor
// 	if role == RoleServer {
// 		cursor = pgwire.NewServerCursor(ring)
// 	} else {
// 		cursor = pgwire.NewClientCursor(ring)
// 	}

// 	*connPtr = conn
// 	*netConnPtr = netConn
// 	*ringPtr = ring
// 	*cursorPtr = cursor

// 	return nil
// }

// // StopConn implements [Runtime].
// func (s *ringBufferRuntime) StopConn(ctx context.Context, role ProxyRole) error {
// 	connPtr, netConnPtr, ringPtr, cursorPtr, posPtr := s.conns(role)
// 	if *connPtr == nil || *ringPtr == nil || *cursorPtr == nil {
// 		return fmt.Errorf("%s connection not started: %v %v %v", role, *connPtr, *ringPtr, *cursorPtr)
// 	}

// 	if err := (*ringPtr).StopNetConnReader(); err != nil {
// 		return fmt.Errorf("failed to stop ring buffer reader: %w", err)
// 	}

// 	if err := (*connPtr).ReleaseNetConn(); err != nil {
// 		return fmt.Errorf("failed to release net conn: %w", err)
// 	}

// 	posPtr.reset(role, nil, nil, nil, nil)
// 	*connPtr = nil
// 	*netConnPtr = nil
// 	*ringPtr = nil
// 	*cursorPtr = nil

// 	return nil
// }

// // WriteConn implements [Runtime].
// func (s *ringBufferRuntime) WriteConn(ctx context.Context, role ProxyRole, queued *pgwire.WriteQueue) error {
// 	_, netConnPtr, _, _, _ := s.conns(role)
// 	netConn := *netConnPtr

// 	if netConn == nil {
// 		return fmt.Errorf("net conn not started: %v", netConn)
// 	}

// 	if deadline, ok := ctx.Deadline(); ok {
// 		if err := netConn.SetWriteDeadline(deadline); err != nil {
// 			return fmt.Errorf("failed to set write deadline: %w", err)
// 		}
// 		defer netConn.SetWriteDeadline(time.Time{})
// 	}

// 	_, err := queued.WriteTo(netConn)
// 	if err != nil {
// 		return fmt.Errorf("failed to write to net conn: %w", err)
// 	}

// 	return nil
// }

// // Close flushes pending writes then closes the session. It releases the
// // acquired backend, and stops all concurrent reads so the client and backend
// // can be re-used.
// func (s *ringBufferRuntime) Stop(ctx context.Context) error {
// 	if s.cancelRingCtx != nil {
// 		s.cancelRingCtx()
// 	}

// 	if s.cancelWaitCtx != nil {
// 		s.cancelWaitCtx()
// 		s.cancelWaitCtx = nil
// 	} else {
// 		s.SetWaitCtx(ctx)
// 		s.cancelWaitCtx()
// 		s.cancelWaitCtx = nil
// 	}

// 	return nil
// }

// func (s *ringBufferRuntime) getCursor(from ProxyRole) *pgwire.Cursor {
// 	if from == RoleServer {
// 		return s.backendCursor
// 	} else {
// 		return s.clientCursor
// 	}
// }

// func (s *ringBufferRuntime) healthCheckChan() <-chan time.Time {
// 	if s.cfg.HealthCheck == nil {
// 		// reading from a nil channel blocks forever
// 		return nil
// 	}
// 	if s.healthCheckTicker == nil {
// 		s.healthCheckTicker = time.NewTicker(s.HealthCheckPeriod())
// 	}
// 	return s.healthCheckTicker.C
// }

// // SetWaitCtx sets the context used for the next stream iteration.
// // This method is called with the context provided to [Session.Next] or [Session.Stream].
// func (s *ringBufferRuntime) SetWaitCtx(ctx context.Context) {
// 	ctx, cancel := context.WithCancel(ctx)
// 	if s.cancelWaitCtx != nil {
// 		s.cancelWaitCtx()
// 	}
// 	s.cancelWaitCtx = cancel
// 	s.nextMsgWaitCtx = ctx
// }

// func (s *ringBufferRuntime) waitCtx() context.Context {
// 	if s.nextMsgWaitCtx == nil {
// 		return context.Background()
// 	}
// 	return s.nextMsgWaitCtx
// }

// // Stream returns [iter.Seq2] of [Pos] and [error] for reading messages from the session.
// // When `ctx` is cancelled, the iterator will exit return (nil, ctx.Err()) then end.
// // See [Session.Next] for the "pull" form, which allows per-iteration cancellation.
// //
// // Stream will panic if re-used or called while already streaming.
// func (s *ringBufferRuntime) Stream(ctx context.Context) iter.Seq2[Pos, error] {
// 	used := false
// 	return func(yield func(Pos, error) bool) {
// 		if s.isStreaming {
// 			panic("already streaming")
// 		}

// 		if used {
// 			panic("proxy stream iterator cannot be re-used")
// 		} else {
// 			used = true
// 		}

// 		s.isStreaming = true
// 		defer func() {
// 			s.isStreaming = false
// 		}()

// 		s.SetWaitCtx(ctx)

// 		for pos, err := range s.yieldMsgs {
// 			if !yield(pos, err) {
// 				return
// 			}
// 		}
// 	}
// }

// func (s *ringBufferRuntime) Run(ctx context.Context) error {
// 	// Stream iterator handles cleanup.
// 	for pos, err := range s.Stream(ctx) {
// 		if s.ringCtx.Err() != nil {
// 			return s.ringCtx.Err()
// 		}

// 		if err := s.HandlePos(ctx, pos, err); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// func (s *ringBufferRuntime) yieldMsgs(yield func(*pos, error) bool) {
// 	var flushErr error

// 	defer func() {
// 		if flushErr != nil {
// 			s.logger.Error("unhandled flush error", "err", flushErr)
// 		}
// 	}()

// 	for batchFrom, batchErr := range s.yieldBatches {
// 		if batchErr != nil {
// 			yield(nil, batchErr)
// 			return
// 		}

// 		cursorIter := s.iterCursor(batchFrom, &flushErr)

// 		for pos, err := range cursorIter {
// 			if !yield(pos, err) {
// 				return
// 			}
// 		}

// 		if flushErr != nil {
// 			yield(nil, flushErr)
// 			flushErr = nil
// 			return
// 		}
// 	}
// }

// func (s *ringBufferRuntime) yieldBatches(yield func(ProxyRole, error) bool) {
// 	lastHealthCheckTime := time.Now()

// 	for {
// 		if s.waitCtx().Err() != nil {
// 			yield(RoleProxy, s.waitCtx().Err())
// 			return
// 		}

// 		if s.cfg.HealthCheck != nil {
// 			if time.Since(lastHealthCheckTime) >= s.HealthCheckPeriod() {
// 				lastHealthCheckTime = time.Now()
// 				if err := s.cfg.HealthCheck(s.waitCtx()); err != nil {
// 					yield(RoleProxy, err)
// 					return
// 				}
// 			}
// 		}

// 		gotFrontend := false
// 		gotBackend := false
// 		gotFrontend, errF := s.cursor(RoleClient).TryNextBatch()
// 		if errF != nil {
// 			yield(RoleClient, s.classifyIOError(RoleClient, errF))
// 			return
// 		}

// 		if backendCursor := s.cursor(RoleServer); backendCursor != nil {
// 			var err error
// 			gotBackend, err = backendCursor.TryNextBatch()
// 			if err != nil {
// 				yield(RoleServer, s.classifyIOError(RoleServer, err))
// 				return
// 			}
// 		}

// 		// Always handle backend first: backend responds to frontend.
// 		// Handle responses before sending new requests from client.
// 		if gotBackend {
// 			if !yield(RoleServer, nil) {
// 				return
// 			}
// 		}

// 		if gotFrontend {
// 			if !yield(RoleClient, nil) {
// 				return
// 			}
// 		}

// 		// If no messages were available synchronously, wait for either side to be ready.
// 		if !gotFrontend && !gotBackend {
// 			select {
// 			case <-s.waitCtx().Done():
// 			case <-s.frontendReadyChan():
// 			case <-s.backendReadyChan():
// 			case <-s.frontendDoneChan():
// 				yield(RoleClient, s.classifyIOError(RoleClient, s.clientCursor.Err()))
// 				return
// 			case <-s.backendDoneChan():
// 				yield(RoleServer, s.classifyIOError(RoleServer, s.backendCursor.Err()))
// 				return
// 			case <-s.healthCheckChan():
// 				// Continue to run health check.
// 			}
// 		}
// 	}
// }

// func (s *ringBufferRuntime) iterCursor(from ProxyRole, flushErr *error) iter.Seq2[*pos, error] {
// 	return func(yield func(*pos, error) bool) {
// 		cursor := s.getCursor(from)
// 		if cursor == nil {
// 			return
// 		}

// 		defer func() {
// 			*flushErr = s.Flush(s.waitCtx())
// 		}()

// 		// if cursor changes (like backend release/re-acquire), drop the loop (and messages)
// 		for s.getCursor(from) == cursor && cursor.NextMsg() {
// 			if !s.yieldSinglePos(yield, from) {
// 				return
// 			}
// 		}

// 		if s.getCursor(from) != cursor {
// 			s.logger.Debug("cursor changed, dropping loop", "from", from, "old", cursor, "new", s.getCursor(from))
// 		}
// 	}
// }

// func (s *ringBufferRuntime) yieldSinglePos(yield func(*pos, error) bool, from ProxyRole) (loop bool) {
// 	// todo: remove uglyness
// 	pos := &s.pos
// 	parser := &s.msgParser
// 	parser.Prepare(from)
// 	defer parser.Release()
// 	pos.reset(from, &s.cursor(from).RingMsg, s.Session, s.waitCtx(), parser)
// 	defer pos.reset(RoleProxy, nil, nil, nil, nil)
// 	return yield(pos, nil)
// }

// func (s *ringBufferRuntime) cursor(role ProxyRole) *pgwire.Cursor {
// 	if role == RoleServer {
// 		return s.backendCursor
// 	} else {
// 		return s.clientCursor
// 	}
// }

// func (s *ringBufferRuntime) conns(role ProxyRole) (*Conn, *net.Conn, **pgwire.RingBuffer, **pgwire.Cursor, *pos) {
// 	if role == RoleServer {
// 		return &s.backendConn, &s.backendNetConn, &s.backendRingBuffer, &s.backendCursor, &s.backendPos
// 	} else {
// 		return &s.clientConn, &s.clientNetConn, &s.clientRingBuffer, &s.clientCursor, &s.clientPos
// 	}
// }

// func (s *ringBufferRuntime) frontendReadyChan() <-chan struct{} {
// 	if s.clientCursor == nil {
// 		return nil
// 	}
// 	return s.clientCursor.Ready()
// }

// func (s *ringBufferRuntime) frontendDoneChan() <-chan struct{} {
// 	if s.clientCursor == nil {
// 		return nil
// 	}
// 	return s.clientCursor.Done()
// }

// func (s *ringBufferRuntime) backendReadyChan() <-chan struct{} {
// 	if s.backendCursor == nil {
// 		return nil
// 	}
// 	return s.backendCursor.Ready()
// }

// func (s *ringBufferRuntime) backendDoneChan() <-chan struct{} {
// 	if s.backendCursor == nil {
// 		return nil
// 	}
// 	return s.backendCursor.Done()
// }

// func dest(msg pgwire.Message) ProxyRole {
// 	if _, ok := msg.(pgwire.ClientMessage); ok {
// 		return RoleServer
// 	} else {
// 		return RoleClient
// 	}
// }

// // classifyIOError classifies an IO error from the given side (RoleClient or RoleServer).
// // For EOF errors, it checks ring buffer state to determine if termination was clean or torn.
// // Returns nil for clean terminations, or ErrTornClientConnection/ErrTornBackendConnection.
// // Non-EOF errors are returned unchanged.
// //
// // TODO: move to Session?
// func (s *ringBufferRuntime) classifyIOError(from ProxyRole, err error) error {
// 	if !errors.Is(err, io.EOF) {
// 		return err // Not EOF - return as-is
// 	}

// 	// Get state for this side
// 	var ring *pgwire.RingBuffer
// 	var terminatedByProxy bool
// 	var terminateReceived bool
// 	var tornErr error

// 	if from == RoleClient {
// 		ring = s.clientRingBuffer
// 		terminatedByProxy = s.clientTerminatedByProxy
// 		terminateReceived = s.ClientTerminateTracker().Active()
// 		tornErr = ErrTornClientConnection
// 	} else {
// 		ring = s.backendRingBuffer
// 		terminatedByProxy = s.backendTerminatedByProxy
// 		terminateReceived = false // Backend doesn't send Terminate
// 		tornErr = ErrTornBackendConnection
// 	}

// 	if ring == nil {
// 		return nil
// 	}

// 	stats := ring.Stats()
// 	isTorn := stats.TotalBytes > stats.PublishedBytes
// 	unparsedBytes := stats.TotalBytes - stats.PublishedBytes
// 	logger := s.Logger().With("role", from, "unparsed_bytes", unparsedBytes)

// 	if isTorn {
// 		if terminatedByProxy {
// 			if from == RoleClient {
// 				// Client torn + proxy closed → WARN
// 				logger.Warn("torn connection (proxy-initiated close)")
// 			} else {
// 				// Backend torn + proxy closed → ERROR (possible proxy bug)
// 				logger.Error("torn connection (proxy-initiated close, possible bug)")
// 			}
// 		} else {
// 			// Unknown why torn → ERROR
// 			logger.Error("torn connection")
// 		}
// 		return tornErr
// 	}

// 	// Clean message boundary
// 	if terminatedByProxy {
// 		return nil // Proxy initiated, clean - no log
// 	}
// 	if terminateReceived {
// 		return nil // Client sent Terminate - cleanest case
// 	}

// 	// Closed without Terminate at message boundary - unusual
// 	if from == RoleClient {
// 		logger.Warn("connection closed without Terminate message")
// 	} else {
// 		logger.Warn("connection closed unexpectedly")
// 	}
// 	return nil
// }

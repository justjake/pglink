package frontend

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/justjake/pglink/pkg/backend"
	"github.com/justjake/pglink/pkg/config"
)

// Service handles incoming client connections.
type Service struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger

	config    *config.Config
	secrets   *config.SecretCache
	tlsConfig *tls.Config

	listener  net.Listener
	databases map[*config.DatabaseConfig]*backend.Database

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
func NewService(ctx context.Context, cfg *config.Config, fsys fs.FS, secrets *config.SecretCache, logger *slog.Logger) (*Service, error) {
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
	}, nil
}

// Listen starts the service and listens for incoming connections on the
// configured address. Returns an error if the listener fails to start.
// When the service's context is cancelled, all sessions are cancelled and
// the method waits for them to close cleanly before returning.
func (s *Service) Listen() error {
	// Set up all databases
	for name, dbConfig := range s.config.Databases {
		db, err := backend.NewDatabase(s.ctx, dbConfig, s.secrets, s.logger)
		if err != nil {
			return fmt.Errorf("failed to create database %s: %w", name, err)
		}
		defer db.Close()
		s.logger.Info("created backend", "name", name, "config", dbConfig)
		s.databases[dbConfig] = db
	}

	// Set up listener
	addr := s.config.GetListenAddr()
	ln, err := net.Listen("tcp", addr.String())
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	s.listener = ln
	s.logger.Info("listening", "addr", addr.String())

	// Start a goroutine to close listener and cancel sessions when context is cancelled
	go func() {
		<-s.ctx.Done()
		_ = s.listener.Close()
		s.cancelAllSessions()
	}()

	// Run accept loop (blocks until listener is closed or error)
	acceptErr := s.acceptLoop(ln)

	// Wait for all sessions to finish
	s.sessionsWg.Wait()

	// Return context error if that's why we stopped, otherwise return accept error
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	return acceptErr
}

// acceptLoop accepts connections on the given listener until it is closed.
func (s *Service) acceptLoop(ln net.Listener) error {
	maxConns := s.config.GetMaxClientConnections()

	for {
		conn, err := ln.Accept()
		if err != nil {
			// Check if we're shutting down
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			// Log but continue on transient errors
			s.logger.Error("accept error", "error", err)
			continue
		}

		// Check connection limit before processing
		currentConns := s.activeConns.Load()
		if currentConns >= maxConns {
			s.rejectConnection(conn, fmt.Sprintf("too many connections (current: %d, max: %d)", currentConns, maxConns))
			continue
		}

		// Atomically increment and check again to handle race
		newCount := s.activeConns.Add(1)
		if newCount > maxConns {
			// We went over, decrement and reject
			s.activeConns.Add(-1)
			s.rejectConnection(conn, fmt.Sprintf("too many connections (current: %d, max: %d)", newCount, maxConns))
			continue
		}

		// Create and start session
		session := s.newSession(conn)
		s.registerSession(session)

		s.sessionsWg.Add(1)
		go func() {
			defer s.sessionsWg.Done()
			defer s.activeConns.Add(-1)
			defer s.unregisterSession(session)
			session.Run()
		}()
	}
}

// rejectConnection sends an error to the client and closes the connection.
func (s *Service) rejectConnection(conn net.Conn, reason string) {
	defer func() { _ = conn.Close() }()

	// We need to handle the initial message to know if we should respond
	// For simplicity, send an error response. The client should handle it.
	// PostgreSQL error response format
	errResp := &pgproto3.ErrorResponse{
		Severity: "FATAL",
		Code:     pgerrcode.TooManyConnections,
		Message:  reason,
	}
	buf, _ := errResp.Encode(nil)
	_, _ = conn.Write(buf)

	s.logger.Info("rejected connection",
		"client", conn.RemoteAddr().String(),
		"reason", reason,
	)
}

// newSession creates a new Session for the given connection.
func (s *Service) newSession(conn net.Conn) *Session {
	ctx, cancel := context.WithCancel(s.ctx)
	return &Session{
		ctx:       ctx,
		cancel:    cancel,
		service:   s,
		conn:      conn,
		logger:    s.logger.With("client", conn.RemoteAddr().String()),
		tlsConfig: s.tlsConfig,
		secrets:   s.secrets,
		config:    s.config,
	}
}

func (s *Service) allocPID() uint32 {
	return s.nextPID.Add(1)
}

// registerSession adds a session to the active sessions map.
func (s *Service) registerSession(sess *Session) {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	s.sessions[sess] = struct{}{}
}

// unregisterSession removes a session from the active sessions map.
func (s *Service) unregisterSession(sess *Session) {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	delete(s.sessions, sess)
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

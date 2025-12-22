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

	listeners []net.Listener
	databases map[*config.DatabaseConfig]*backend.Database

	// Connection tracking
	activeConns atomic.Int32

	// Session management
	sessionsMu sync.Mutex
	sessions   map[*Session]struct{}
	sessionsWg sync.WaitGroup
	nextPID    atomic.Uint32
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
		ctx:       innerCtx,
		cancel:    cancel,
		logger:    logger,
		config:    cfg,
		secrets:   secrets,
		tlsConfig: tlsResult.Config,
		listeners: make([]net.Listener, 0, len(cfg.Listen)),
		databases: make(map[*config.DatabaseConfig]*backend.Database),
		sessions:  make(map[*Session]struct{}),
	}, nil
}

// Listen starts the service and listens for incoming connections on all
// configured addresses. Returns an error if any listener fails to start.
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

	// Set up all listeners
	for _, addr := range s.config.Listen {
		ln, err := net.Listen("tcp", addr.String())
		if err != nil {
			// Close any listeners we already opened
			for _, l := range s.listeners {
				_ = l.Close()
			}
			return fmt.Errorf("failed to listen on %s: %w", addr, err)
		}
		s.listeners = append(s.listeners, ln)
		s.logger.Info("listening", "addr", addr.String())
	}

	// All listeners created successfully, start accept loops
	var listenerWg sync.WaitGroup
	errCh := make(chan error, len(s.listeners))

	for i, ln := range s.listeners {
		listenerWg.Add(1)
		go func(ln net.Listener) {
			defer listenerWg.Done()
			if err := s.acceptLoop(i, ln); err != nil {
				errCh <- err
			}
		}(ln)
	}

	// Start a goroutine to close listeners and cancel sessions when context is cancelled
	go func() {
		<-s.ctx.Done()
		// Close all listeners to stop accept loops
		for _, ln := range s.listeners {
			_ = ln.Close()
		}
		// Cancel all active sessions
		s.cancelAllSessions()
	}()

	// Wait for context cancellation or first error
	var firstErr error
	select {
	case <-s.ctx.Done():
		firstErr = s.ctx.Err()
	case err := <-errCh:
		firstErr = err
		// Cancel context to trigger shutdown
		s.cancel()
	}

	// Wait for all listener goroutines to finish
	listenerWg.Wait()

	// Wait for all sessions to finish
	s.sessionsWg.Wait()

	return firstErr
}

// acceptLoop accepts connections on the given listener until it is closed.
func (s *Service) acceptLoop(idx int, ln net.Listener) error {
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

// cancelAllSessions cancels all active sessions.
func (s *Service) cancelAllSessions() {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	for sess := range s.sessions {
		sess.cancel()
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

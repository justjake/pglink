// Command mitm-proxy is a simple PostgreSQL man-in-the-middle proxy for testing pkg/pgserver.
// It captures client credentials via cleartext password auth and validates them by connecting to a backend.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/pgproxy"
	"github.com/justjake/pglink/pkg/pgserver"
	"github.com/justjake/pglink/pkg/pgwire"
)

func main() {
	backendURI := flag.String("backend", "", "PostgreSQL backend URI (e.g., postgres://localhost:5432/mydb)")
	listenAddr := flag.String("addr", ":15432", "listen address")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	flag.Parse()

	if *backendURI == "" {
		fmt.Fprintln(os.Stderr, "error: -backend is required")
		flag.Usage()
		os.Exit(1)
	}

	// Set up logger
	var slogLevel slog.Level
	switch strings.ToLower(*logLevel) {
	case "debug":
		slogLevel = slog.LevelDebug
	case "warn", "warning":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slogLevel}))
	slog.SetDefault(logger)

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info("received shutdown signal", "signal", sig)
		cancel()
	}()

	proxy := &MitmProxy{
		BackendURI: *backendURI,
		Logger:     logger,
		backends:   &sync.Map{},
	}

	server, err := pgserver.NewServer(pgserver.ServerConfig{
		Addr:           *listenAddr,
		AuthHandler:    proxy.AuthHandler,
		StartupHandler: proxy.StartupHandler,
		Handler:        proxy.Handler,
		Logger:         logger,
	})
	if err != nil {
		logger.Error("failed to create server", "error", err)
		os.Exit(1)
	}

	logger.Info("starting mitm-proxy", "addr", *listenAddr, "backend", *backendURI)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, pgserver.ErrServerClosed) {
		logger.Error("server error", "error", err)
		os.Exit(1)
	}
}

// MitmProxy captures client credentials and proxies to a backend.
type MitmProxy struct {
	BackendURI string
	Logger     *slog.Logger
	// backends maps frontend connection address to *pgconn.PgConn
	backends *sync.Map
}

// AuthHandler implements cleartext password auth and validates credentials against the backend.
func (p *MitmProxy) AuthHandler(ctx context.Context, conn *pgserver.UnauthorizedConn) (*pgserver.AuthorizedConn, error) {
	// Request cleartext password
	if err := conn.Frontend.SetAuthType(pgproto3.AuthTypeCleartextPassword); err != nil {
		return nil, fmt.Errorf("failed to set auth type: %w", err)
	}
	if err := conn.Send(ctx, &pgproto3.AuthenticationCleartextPassword{}); err != nil {
		return nil, err
	}

	// Receive password
	msg, err := pgwire.Expect[*pgwire.ClientPasswordMessage](conn.Receive(ctx))
	if err != nil {
		return nil, err
	}
	password := msg.Parse().Password

	// Extract user and database from startup params
	params := pgwire.ParameterStatuses(conn.StartupMessage.Parameters)
	user := params.User()
	database := params.Database()
	if database == "" {
		database = user
	}

	p.Logger.Debug("captured credentials", "user", user, "database", database)

	// Connect to backend with captured credentials
	backendConnString := buildBackendConnString(p.BackendURI, user, password, database)
	backend, err := pgconn.Connect(ctx, backendConnString)
	if err != nil {
		p.Logger.Warn("backend auth failed", "user", user, "error", err)
		return nil, pgwire.NewErr(pgwire.ErrorFatal, "28P01", "password authentication failed", err)
	}

	p.Logger.Info("backend connection established",
		"user", user,
		"database", database,
		"pid", backend.PID(),
		"secret", backend.SecretKey())

	// Store backend connection keyed by frontend connection address
	connKey := conn.Conn.RemoteAddr().String()
	p.backends.Store(connKey, backend)

	// Send auth success
	if err := conn.Send(ctx, &pgproto3.AuthenticationOk{}); err != nil {
		backend.Close(ctx)
		p.backends.Delete(connKey)
		return nil, err
	}

	return &pgserver.AuthorizedConn{
		FrontendConn:   conn.FrontendConn,
		User:           user,
		Database:       database,
		StartupMessage: conn.StartupMessage,
	}, nil
}

// StartupHandler sets up ProcessID/SecretKey from the backend.
func (p *MitmProxy) StartupHandler(ctx context.Context, conn *pgserver.AuthorizedConn) (*pgserver.ClientConn, error) {
	connKey := conn.Conn.RemoteAddr().String()
	backendVal, ok := p.backends.Load(connKey)
	if !ok {
		return nil, fmt.Errorf("backend connection not found for %s", connKey)
	}
	backend := backendVal.(*pgconn.PgConn)

	// Build startup parameters from backend's reported values
	startupParams := make(pgwire.ParameterStatuses)
	for _, key := range []string{
		"server_version", "server_encoding", "client_encoding",
		"TimeZone", "DateStyle", "integer_datetimes",
	} {
		if val := backend.ParameterStatus(key); val != "" {
			startupParams[key] = val
		}
	}

	// Use backend's ProcessID and SecretKey
	return &pgserver.ClientConn{
		FrontendConn:      conn.FrontendConn,
		User:              conn.User,
		Database:          conn.Database,
		ProcessID:         pgwire.ProcessID(backend.PID()),
		SecretKey:         pgwire.SecretKey(backend.SecretKey()),
		StartupParameters: startupParams,
	}, nil
}

// Handler proxies messages between frontend and backend.
func (p *MitmProxy) Handler(ctx context.Context, conn *pgserver.ClientConn) error {
	connKey := conn.Conn.RemoteAddr().String()
	backendVal, ok := p.backends.LoadAndDelete(connKey)
	if !ok {
		return fmt.Errorf("backend connection not found for %s", connKey)
	}
	backend := backendVal.(*pgconn.PgConn)
	defer backend.Close(ctx)

	p.Logger.Info("session started",
		"user", conn.User,
		"database", conn.Database,
		"pid", conn.ProcessID,
		"backend", backend.Conn().RemoteAddr())

	// Create pgproxy session
	frontendAdapter := &PgxFrontend{ClientConn: conn}
	backendAdapter := NewPgxBackend(backend)

	session, err := pgproxy.NewSession(ctx, pgproxy.SessionConfig{
		Frontend: frontendAdapter,
		AcquireBackend: func(ctx context.Context) (pgproxy.Backend, error) {
			return backendAdapter, nil
		},
		Logger: p.Logger,
	})
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	defer session.Close(ctx)

	// Proxy loop
	for pos, err := range session.Stream(ctx) {
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		// Log message
		if p.Logger.Enabled(ctx, slog.LevelDebug) {
			p.Logger.Debug("proxy message",
				"from", pos.From(),
				"type", pos.MessageType())
		}

		// Check for terminate from client
		if pos.FromClient() {
			if _, ok := pos.ClientMsg().(*pgwire.ClientTerminate); ok {
				if err := pos.Skip(); err != nil {
					return err
				}
				return nil
			}
		}

		// Forward message to destination
		if err := pos.Forward(ctx); err != nil {
			return fmt.Errorf("forward: %w", err)
		}
	}

	return nil
}

// buildBackendConnString builds a connection string for the backend.
// It takes the base URI (which provides the host/port) and overlays client credentials.
func buildBackendConnString(baseURI, user, password, database string) string {
	// Check if it's a URI format (postgres://...)
	if strings.HasPrefix(baseURI, "postgres://") || strings.HasPrefix(baseURI, "postgresql://") {
		// Parse and rebuild with new credentials
		// Simple approach: just use keyword format which pgx handles well
		// Extract host from URI
		uri := baseURI
		uri = strings.TrimPrefix(uri, "postgres://")
		uri = strings.TrimPrefix(uri, "postgresql://")

		// Find host:port (after @ if present, before / or ?)
		hostPart := uri
		if idx := strings.Index(hostPart, "@"); idx != -1 {
			hostPart = hostPart[idx+1:]
		}
		if idx := strings.Index(hostPart, "/"); idx != -1 {
			hostPart = hostPart[:idx]
		}
		if idx := strings.Index(hostPart, "?"); idx != -1 {
			hostPart = hostPart[:idx]
		}

		// Extract host and port
		host := hostPart
		port := "5432"
		if idx := strings.LastIndex(hostPart, ":"); idx != -1 {
			host = hostPart[:idx]
			port = hostPart[idx+1:]
		}

		// Build keyword format
		connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable",
			host, port, user, database)
		if password != "" {
			connStr += fmt.Sprintf(" password=%s", password)
		}
		return connStr
	}

	// Keyword format: just append/override
	connStr := baseURI
	if !strings.Contains(connStr, "user=") {
		connStr += fmt.Sprintf(" user=%s", user)
	}
	if password != "" && !strings.Contains(connStr, "password=") {
		connStr += fmt.Sprintf(" password=%s", password)
	}
	if database != "" && !strings.Contains(connStr, "dbname=") {
		connStr += fmt.Sprintf(" dbname=%s", database)
	}

	return connStr
}

// PgxFrontend adapts pgserver.ClientConn to pgproxy.Frontend.
type PgxFrontend struct {
	*pgserver.ClientConn
	acquired bool
}

func (f *PgxFrontend) AcquireNetConn(ctx context.Context) (net.Conn, error) {
	if f.acquired {
		return nil, pgserver.ErrNetConnInUse
	}
	f.acquired = true
	return f.Conn, nil
}

func (f *PgxFrontend) ReleaseNetConn() error {
	if !f.acquired {
		return pgserver.ErrNetConnNotAcquired
	}
	f.acquired = false
	return nil
}

func (f *PgxFrontend) Terminate(ctx context.Context, err error) error {
	return f.Conn.Close()
}

func (f *PgxFrontend) MessageTrackers() []pgproxy.MessageTracker {
	return nil
}

func (f *PgxFrontend) String() string {
	return fmt.Sprintf("frontend[%s]", f.Conn.RemoteAddr())
}

// PgxBackend adapts pgconn.PgConn to pgproxy.Backend.
type PgxBackend struct {
	*pgconn.PgConn
	netConn  net.Conn
	acquired bool
}

func NewPgxBackend(conn *pgconn.PgConn) *PgxBackend {
	return &PgxBackend{PgConn: conn}
}

func (b *PgxBackend) AcquireNetConn(ctx context.Context) (net.Conn, error) {
	if b.acquired {
		return nil, pgserver.ErrNetConnInUse
	}
	conn := b.PgConn.Conn()
	if conn == nil {
		return nil, errors.New("backend connection is nil")
	}
	b.netConn = conn
	b.acquired = true
	return conn, nil
}

func (b *PgxBackend) ReleaseNetConn() error {
	if !b.acquired {
		return pgserver.ErrNetConnNotAcquired
	}
	b.acquired = false
	b.netConn = nil
	return nil
}

func (b *PgxBackend) Terminate(ctx context.Context, err error) error {
	return b.PgConn.Close(ctx)
}

func (b *PgxBackend) MessageTrackers() []pgproxy.MessageTracker {
	return nil
}

func (b *PgxBackend) Release() {
	b.PgConn.Close(context.Background())
}

func (b *PgxBackend) String() string {
	if b.netConn != nil {
		return fmt.Sprintf("backend[%s]", b.netConn.RemoteAddr())
	}
	return "backend[disconnected]"
}

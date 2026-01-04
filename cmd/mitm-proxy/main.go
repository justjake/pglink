// Command mitm-proxy is a simple PostgreSQL man-in-the-middle proxy for testing pkg/pgserver.
// It captures client credentials via cleartext password auth and validates them by connecting to a backend.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
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

	// Parse and validate backend URI
	backendCfg, err := parseBackendURI(*backendURI)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
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
		BackendCfg: backendCfg,
		Logger:     logger,
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
	BackendCfg *pgconn.Config
	Logger     *slog.Logger
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
	cfg := p.BackendCfg.Copy()
	cfg.User = user
	cfg.Password = password
	cfg.Database = database
	backend, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		p.Logger.Warn("backend auth failed", "user", user, "error", err)
		return nil, pgwire.NewErr(pgwire.ErrorFatal, "28P01", "password authentication failed", err)
	}

	p.Logger.Info("backend connection established",
		"user", user,
		"database", database,
		"pid", backend.PID(),
		"secret", backend.SecretKey())

	// SyncConn drains any buffered data before hijacking
	if err := backend.SyncConn(ctx); err != nil {
		backend.Close(ctx)
		return nil, fmt.Errorf("failed to sync backend connection: %w", err)
	}

	// Hijack the connection to take full ownership from pgconn.
	// This is necessary because pgproxy will read/write directly to the net.Conn.
	hijacked, err := backend.Hijack()
	if err != nil {
		backend.Close(ctx)
		return nil, fmt.Errorf("failed to hijack backend connection: %w", err)
	}

	// Send auth success
	if err := conn.Send(ctx, &pgproto3.AuthenticationOk{}); err != nil {
		hijacked.Conn.Close()
		return nil, err
	}

	return &pgserver.AuthorizedConn{
		FrontendConn:   conn.FrontendConn,
		User:           user,
		Database:       database,
		StartupMessage: conn.StartupMessage,
		ExtraData:      hijacked, // Pass backend connection to next handler
	}, nil
}

// StartupHandler sets up ProcessID/SecretKey from the backend.
func (p *MitmProxy) StartupHandler(ctx context.Context, conn *pgserver.AuthorizedConn) (*pgserver.ClientConn, error) {
	hijacked := conn.ExtraData.(*pgconn.HijackedConn)

	// Build startup parameters from backend's reported values
	startupParams := make(pgwire.ParameterStatuses)
	for key, val := range hijacked.ParameterStatuses {
		startupParams[key] = val
	}

	// Use backend's ProcessID and SecretKey
	return &pgserver.ClientConn{
		FrontendConn:      conn.FrontendConn,
		User:              conn.User,
		Database:          conn.Database,
		ProcessID:         pgwire.ProcessID(hijacked.PID),
		SecretKey:         pgwire.SecretKey(hijacked.SecretKey),
		StartupParameters: startupParams,
		ExtraData:         hijacked, // Pass backend connection to handler
	}, nil
}

// Handler proxies messages between frontend and backend.
func (p *MitmProxy) Handler(ctx context.Context, conn *pgserver.ClientConn) error {
	hijacked := conn.ExtraData.(*pgconn.HijackedConn)
	defer hijacked.Conn.Close()

	p.Logger.Info("session started",
		"user", conn.User,
		"database", conn.Database,
		"pid", conn.ProcessID,
		"backend", hijacked.Conn.RemoteAddr())

	// Create pgproxy session
	frontendAdapter := &PgxFrontend{ClientConn: conn}
	backendAdapter := NewHijackedBackend(hijacked, p.Logger)

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

		// Log messages at debug level
		if p.Logger.Enabled(ctx, slog.LevelDebug) {
			var parsed any
			if pos.FromClient() {
				parsed = pos.ClientMsg().ParseAny()
			} else {
				parsed = pos.ServerMsg().ParseAny()
			}
			p.Logger.Debug("MSG", "from", pos.From(), "type", pos.MessageType(), "msg", mustJSON(parsed))
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

// parseBackendURI parses and validates the backend URI.
// Returns a config with user cleared (credentials come from client).
// Errors if password is set in the URI.
func parseBackendURI(uri string) (*pgconn.Config, error) {
	cfg, err := pgconn.ParseConfig(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid backend URI: %w", err)
	}

	if cfg.Password != "" {
		return nil, fmt.Errorf("-backend must not contain password. Credentials come from the client")
	}

	// Clear user - will be set from client credentials
	cfg.User = ""

	return cfg, nil
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

// HijackedBackend adapts a hijacked pgconn connection to pgproxy.Backend.
type HijackedBackend struct {
	hijacked *pgconn.HijackedConn
	acquired bool
}

func NewHijackedBackend(hijacked *pgconn.HijackedConn, logger *slog.Logger) *HijackedBackend {
	// Wrap the connection to log all writes at debug level
	hijacked.Conn = &loggingConn{Conn: hijacked.Conn, label: "backend", logger: logger}
	return &HijackedBackend{hijacked: hijacked}
}

type loggingConn struct {
	net.Conn
	label  string
	logger *slog.Logger
}

func (c *loggingConn) Write(p []byte) (n int, err error) {
	if c.logger.Enabled(context.Background(), slog.LevelDebug) && len(p) > 0 {
		// Parse all message types in this write
		var msgs []string
		offset := 0
		for offset < len(p) {
			if offset+5 > len(p) {
				msgs = append(msgs, "[incomplete]")
				break
			}
			msgType := p[offset]
			msgLen := int(p[offset+1])<<24 | int(p[offset+2])<<16 | int(p[offset+3])<<8 | int(p[offset+4])
			msgs = append(msgs, fmt.Sprintf("%c(%d)", msgType, msgLen))
			offset += 1 + msgLen
		}
		c.logger.Debug("WRITE", "to", c.label, "bytes", len(p), "msgs", strings.Join(msgs, " "))
	}
	return c.Conn.Write(p)
}

func (b *HijackedBackend) AcquireNetConn(ctx context.Context) (net.Conn, error) {
	if b.acquired {
		return nil, pgserver.ErrNetConnInUse
	}
	if b.hijacked.Conn == nil {
		return nil, errors.New("backend connection is nil")
	}
	b.acquired = true
	return b.hijacked.Conn, nil
}

func (b *HijackedBackend) ReleaseNetConn() error {
	if !b.acquired {
		return pgserver.ErrNetConnNotAcquired
	}
	b.acquired = false
	return nil
}

func (b *HijackedBackend) Terminate(ctx context.Context, err error) error {
	return b.hijacked.Conn.Close()
}

func (b *HijackedBackend) MessageTrackers() []pgproxy.MessageTracker {
	return nil
}

func (b *HijackedBackend) Release() {
	b.hijacked.Conn.Close()
}

func (b *HijackedBackend) String() string {
	if b.hijacked.Conn != nil {
		return fmt.Sprintf("backend[%s]", b.hijacked.Conn.RemoteAddr())
	}
	return "backend[disconnected]"
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}


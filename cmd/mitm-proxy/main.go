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
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strings"

	"golang.org/x/term"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/pgproxy"
	"github.com/justjake/pglink/pkg/pgserver"
	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/lmittmann/tint"
)

func main() {
	backendURI := flag.String("backend", "", "PostgreSQL backend URI (e.g., postgres://localhost:5432/mydb)")
	listenAddr := flag.String("addr", ":15432", "listen address")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	pprofAddr := flag.String("pprof", "", "pprof HTTP server address (e.g., :6060)")
	enableStats := flag.Bool("stats", false, "enable pgwire stats collection (logged on shutdown)")
	useSplit := flag.Bool("split", false, "use 2-goroutine split I/O mode (experimental)")
	maxProcs := flag.Int("gomaxprocs", 0, "set GOMAXPROCS (0 = use default)")
	flag.Parse()

	if *maxProcs > 0 {
		runtime.GOMAXPROCS(*maxProcs)
	}

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
	handler := tint.NewHandler(os.Stdout, &tint.Options{
		Level:     slogLevel,
		AddSource: true,
		NoColor:   !term.IsTerminal(int(os.Stdout.Fd())),
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)

	// Enable stats collection when debug logging or -stats flag is enabled
	if slogLevel == slog.LevelDebug || *enableStats {
		pgwire.Stats.Enabled = true
	}

	// Start pprof server if enabled
	if *pprofAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		// Try to listen first to catch port conflicts early
		pprofListener, err := net.Listen("tcp", *pprofAddr)
		if err != nil {
			logger.Error("failed to start pprof server", "addr", *pprofAddr, "error", err)
			os.Exit(1)
		}
		logger.Info("starting pprof server", "addr", pprofListener.Addr())
		go func() {
			if err := http.Serve(pprofListener, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("pprof server failed", "error", err)
			}
		}()
	}

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Signal handling
	// sigChan := make(chan os.Signal, 1)
	// signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	// go func() {
	// 	sig := <-sigChan
	// 	logger.Info("received shutdown signal", "signal", sig)

	// 	// Log stats if enabled (at INFO level so it's visible without debug)
	// 	if pgwire.Stats.Enabled {
	// 		logger.Info("pgwire stats", "stats", pgwire.StatsSnapshot())
	// 	}

	// 	cancel()
	// }()

	proxy := &MitmProxy{
		BackendCfg: backendCfg,
		Logger:     logger,
		UseSplit:   *useSplit,
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
	UseSplit   bool // Use IOModeSplit (2-goroutine model)
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
	msg, err := pgwire.Expect[*pgproto3.PasswordMessage](conn.Receive(ctx))
	if err != nil {
		return nil, err
	}
	password := msg.Password

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
		return nil, errors.Join(fmt.Errorf("failed to sync backend connection: %w", err), backend.Close(ctx))
	}

	// Hijack the connection to take full ownership from pgconn.
	// This is necessary because pgproxy will read/write directly to the net.Conn.
	hijacked, err := backend.Hijack()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to hijack backend connection: %w", err), backend.Close(ctx))
	}

	// Send auth success
	if err := conn.Send(ctx, &pgproto3.AuthenticationOk{}); err != nil {
		return nil, errors.Join(err, hijacked.Conn.Close())
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
		CancelHandler:     p.CancelHandler,
		ExtraData:         hijacked, // Pass backend connection to handler
	}, nil
}

// Handler proxies messages between frontend and backend.
func (p *MitmProxy) Handler(ctx context.Context, conn *pgserver.ClientConn) (returnedErr error) {
	hijacked := conn.ExtraData.(*pgconn.HijackedConn)
	// NOTE: Don't close hijacked.Conn here - session.Close() calls ReleaseBackend()
	// which calls HijackedBackend.Release() to close the connection.

	p.Logger.Info("session started",
		"user", conn.User,
		"database", conn.Database,
		"pid", conn.ProcessID,
		"backend", hijacked.Conn.RemoteAddr(),
		"split_mode", p.UseSplit)

	// Create pgproxy session
	frontendAdapter := &PgxFrontend{ClientConn: conn}
	backendAdapter := NewHijackedBackend(hijacked, p.Logger)

	// Determine IO mode
	var newRuntime pgproxy.RuntimeFactory
	if p.UseSplit {
		newRuntime = pgproxy.NewGnetProxyRuntime
	} else {
		panic("not implemented")
		// newRuntime = pgproxy.NewRingBufferRuntime
	}

	session, err := pgproxy.NewSession(ctx, pgproxy.SessionConfig{
		Frontend:   frontendAdapter,
		NewRuntime: newRuntime,
		AcquireBackend: func(ctx context.Context) (pgproxy.Backend, error) {
			return backendAdapter, nil
		},
		Logger: p.Logger,
		Handler: func(ctx context.Context, session *pgproxy.Session, pos *pgproxy.Pos2, err error) error {
			if err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
					return io.EOF // Signal normal termination to Run()
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
					return io.EOF // Signal normal termination
				}
			}

			// Forward message to destination
			if err := pos.Forward(ctx); err != nil {
				return fmt.Errorf("forward: %w", err)
			}

			return nil
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}
	defer func() {
		returnedErr = errors.Join(returnedErr, session.Close(ctx))
	}()

	// Proxy using Run() which dispatches based on IOMode
	runErr := session.Run(ctx)

	// Don't return EOF as an error - it signals normal client termination
	if pgproxy.IsCleanTermination(runErr) {
		return nil
	}
	return runErr
}

func (p *MitmProxy) CancelHandler(ctx context.Context, conn *pgserver.ClientConn, cancel *pgserver.CancelConn) error {
	p.Logger.Error("cancel request received, exiting process", "user", conn.User, "database", conn.Database, "pid", conn.ProcessID)
	os.Exit(2)
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
	// Only wrap connection for debug logging if debug is enabled
	// if logger.Enabled(context.Background(), slog.LevelDebug) {
	// 	hijacked.Conn = &loggingConn{Conn: hijacked.Conn, label: "backend", logger: logger}
	// }
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
	if err := b.hijacked.Conn.Close(); err != nil {
		slog.Warn("failed to close hijacked backend connection on release", "error", err)
	}
}

func (b *HijackedBackend) OutstandingRequests() *pgproxy.OutstandingRequestQueue {
	// mitm-proxy doesn't use response handlers, so we return nil.
	// This is acceptable since we don't attach response handlers in the proxy loop.
	return nil
}

func (b *HijackedBackend) String() string {
	if b.hijacked.Conn != nil {
		return fmt.Sprintf("backend[%s]", b.hijacked.Conn.RemoteAddr())
	}
	return "backend[disconnected]"
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("<json error: %v>", err)
	}
	return string(b)
}

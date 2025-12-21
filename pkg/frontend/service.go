package frontend

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"sync"

	"github.com/rueian/pgbroker/backend"
	"github.com/rueian/pgbroker/proxy"

	"github.com/justjake/pglink/pkg/config"
)

// serviceListener holds a proxy server and its listener.
type serviceListener struct {
	server   *proxy.Server
	listener net.Listener
}

// Service handles incoming client connections.
type Service struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger

	config    *config.Config
	secrets   *config.SecretCache
	tlsConfig *tls.Config

	listeners map[config.ListenAddr]*serviceListener

	connInfoStore         backend.ConnInfoStore
	clientMessageHandlers *proxy.ClientMessageHandlers
	serverMessageHandlers *proxy.ServerMessageHandlers
}

// NewService creates a new frontend Service with the given configuration.
// It validates the config by fetching all referenced secrets.
// The fsys parameter should be rooted at the config file's directory for resolving relative paths.
func NewService(ctx context.Context, cfg *config.Config, fsys fs.FS, secrets *config.SecretCache, logger *slog.Logger) (*Service, error) {
	if err := cfg.Validate(ctx, fsys, secrets); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	tlsConfig, err := cfg.TLSConfig(fsys)
	if err != nil {
		return nil, fmt.Errorf("failed to create TLS config: %w", err)
	}

	innerCtx, cancel := context.WithCancel(ctx)

	clientHandlers := proxy.NewClientMessageHandlers()
	serverHandlers := proxy.NewServerMessageHandlers()
	connInfoStore := backend.NewInMemoryConnInfoStore()

	return &Service{
		ctx:                   innerCtx,
		cancel:                cancel,
		logger:                logger,
		config:                cfg,
		secrets:               secrets,
		tlsConfig:             tlsConfig,
		listeners:             make(map[config.ListenAddr]*serviceListener),
		connInfoStore:         connInfoStore,
		clientMessageHandlers: clientHandlers,
		serverMessageHandlers: serverHandlers,
	}, nil
}

// newServer creates a new proxy.Server with the service's handlers.
func (s *Service) newServer() *proxy.Server {
	return &proxy.Server{
		ConnInfoStore:         s.connInfoStore,
		ClientMessageHandlers: s.clientMessageHandlers,
		ServerMessageHandlers: s.serverMessageHandlers,
		TLSConfig:             s.tlsConfig,
		OnHandleConnError: func(err error, ctx *proxy.Ctx, conn net.Conn) {
			if err == io.EOF {
				return
			}

			client := conn.RemoteAddr().String()
			serverAddr := ""
			if ctx.ConnInfo.ServerAddress != nil {
				serverAddr = ctx.ConnInfo.ServerAddress.String()
			}
			user := ""
			database := ""
			if ctx.ConnInfo.StartupParameters != nil {
				user = ctx.ConnInfo.StartupParameters["user"]
				database = ctx.ConnInfo.StartupParameters["database"]
			}

			s.logger.Error("connection error",
				"client", client,
				"server", serverAddr,
				"user", user,
				"database", database,
				"error", err,
			)
		},
	}
}

// Listen starts the service and listens for incoming connections on all
// configured addresses. Returns an error if any listener fails to start.
// When the service's context is cancelled or an error occurs, all servers
// are shut down gracefully.
func (s *Service) Listen() error {
	// Set up all listeners first, each with its own server
	for _, addr := range s.config.Listen {
		ln, err := net.Listen("tcp", addr.String())
		if err != nil {
			// Close any listeners we already opened
			for _, sl := range s.listeners {
				_ = sl.listener.Close()
			}
			return fmt.Errorf("failed to listen on %s: %w", addr, err)
		}
		s.listeners[addr] = &serviceListener{
			server:   s.newServer(),
			listener: ln,
		}
		s.logger.Info("listening", "addr", addr.String())
	}

	// All listeners created successfully, start serving
	var wg sync.WaitGroup
	errCh := make(chan error, len(s.listeners))

	for addr, sl := range s.listeners {
		wg.Add(1)
		go func(addr config.ListenAddr, sl *serviceListener) {
			defer wg.Done()
			if err := sl.server.Serve(sl.listener); err != nil {
				errCh <- fmt.Errorf("server %s: %w", addr, err)
			}
		}(addr, sl)
	}

	// Start a goroutine to shutdown servers when context is cancelled
	go func() {
		<-s.ctx.Done()
		for _, sl := range s.listeners {
			sl.server.Shutdown()
		}
	}()

	// Wait for context cancellation or first error
	var firstErr error
	select {
	case <-s.ctx.Done():
		firstErr = s.ctx.Err()
	case err := <-errCh:
		firstErr = err
		// Cancel context to trigger shutdown of other servers
		s.cancel()
	}

	// Wait for all servers to finish
	wg.Wait()

	return firstErr
}

// Shutdown cancels the service's context, triggering graceful shutdown of all servers.
func (s *Service) Shutdown() {
	s.cancel()
}

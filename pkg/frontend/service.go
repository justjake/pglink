package frontend

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/rueian/pgbroker/backend"
	"github.com/rueian/pgbroker/proxy"

	"github.com/justjake/pglink/pkg/config"
)

// Service handles incoming client connections.
type Service struct {
	config  *config.Config
	secrets *config.SecretCache

	server                *proxy.Server
	connInfoStore         backend.ConnInfoStore
	clientMessageHandlers *proxy.ClientMessageHandlers
	serverMessageHandlers *proxy.ServerMessageHandlers
}

// NewService creates a new frontend Service with the given configuration.
// It validates the config by fetching all referenced secrets.
func NewService(ctx context.Context, cfg *config.Config, secrets *config.SecretCache) (*Service, error) {
	if err := cfg.Validate(ctx, secrets); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	clientHandlers := proxy.NewClientMessageHandlers()
	serverHandlers := proxy.NewServerMessageHandlers()
	connInfoStore := backend.NewInMemoryConnInfoStore()

	server := &proxy.Server{
		ConnInfoStore:         connInfoStore,
		ClientMessageHandlers: clientHandlers,
		ServerMessageHandlers: serverHandlers,
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

			log.Printf("connection error: client=%s server=%s user=%s db=%s err=%v", client, serverAddr, user, database, err)
		},
	}

	return &Service{
		config:                cfg,
		secrets:               secrets,
		server:                server,
		connInfoStore:         connInfoStore,
		clientMessageHandlers: clientHandlers,
		serverMessageHandlers: serverHandlers,
	}, nil
}

// Listen starts the service and listens for incoming connections on all
// configured addresses. Returns an error if any listener fails to start.
func (s *Service) Listen() error {
	listeners := make([]net.Listener, 0, len(s.config.Listen))

	// Set up all listeners first
	for _, addr := range s.config.Listen {
		ln, err := net.Listen("tcp", addr.String())
		if err != nil {
			// Close any listeners we already opened
			for _, l := range listeners {
				_ = l.Close()
			}
			return fmt.Errorf("failed to listen on %s: %w", addr, err)
		}
		listeners = append(listeners, ln)
	}

	// All listeners created successfully, start serving
	errCh := make(chan error, len(listeners))
	for _, ln := range listeners {
		go func(ln net.Listener) {
			errCh <- s.server.Serve(ln)
		}(ln)
	}

	// Return first error
	return <-errCh
}

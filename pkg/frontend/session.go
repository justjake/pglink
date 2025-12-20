package frontend

import (
	"context"
	"net"

	"github.com/justjake/pglink/pkg/backend"
	"github.com/justjake/pglink/pkg/config"
)

// Session represents a client's session with the Service.
type Session struct {
	ctx               context.Context
	cancel            context.CancelFunc
	Client            net.Conn
	Backend           *backend.Conn
	StartupParameters map[string]string
	DatabaseName      string
	User              config.UserConfig
}

// Context returns the session's context, which is canceled when Close is called.
func (s *Session) Context() context.Context {
	return s.ctx
}

// Close cancels the session's context and releases associated resources.
func (s *Session) Close() {
	s.cancel()
}

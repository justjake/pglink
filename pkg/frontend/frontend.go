// Package frontend handles communication with clients.
package frontend

import (
	_ "github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/rueian/pgbroker/proxy"

	"github.com/justjake/pglink/pkg/config"
)

// Service handles incoming client connections.
type Service struct {
	config *config.Config
}

// NewService creates a new frontend Service with the given configuration.
func NewService(cfg *config.Config) *Service {
	return &Service{config: cfg}
}

// Listen starts the service and listens for incoming connections.
func (s *Service) Listen() error {
	// TODO: implement listener
	return nil
}

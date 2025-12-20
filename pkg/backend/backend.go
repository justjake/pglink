// Package backend handles managing pool of backend server connections
// and communication with the server.
package backend

import (
	_ "github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/rueian/pgbroker/proxy"
)

// Conn represents a connection to a backend PostgreSQL server.
type Conn struct {
	// TODO: implement backend connection
}

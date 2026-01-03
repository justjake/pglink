package pgserver

import (
	"context"
	"math/rand/v2"

	"github.com/justjake/pglink/pkg/pgwire"
)

// DefaultStartupHandler randomly assigns a [pgwire.ProcessID] and [pgwire.SecretKey] to the connection,
// and applies pglink defaults for startup parameters.
//
// No effort is made to avoid re-using process IDs or secret keys.
func DefaultStartupHandler(ctx context.Context, conn *AuthorizedConn) (*ClientConn, error) {
	startupParameters, err := pgwire.ParseStartupParameters(conn.StartupMessage.Parameters, pgwire.DefaultParameterStatuses)
	if err != nil {
		return nil, err
	}

	processId := pgwire.ProcessID(rand.Uint32())
	secretKey := pgwire.SecretKey(rand.Uint32())

	return &ClientConn{
		User:              conn.User,
		Database:          conn.Database,
		ProcessID:         processId,
		SecretKey:         secretKey,
		StartupParameters: startupParameters,
	}, nil
}

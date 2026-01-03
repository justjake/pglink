package pgserver

import (
	"context"
	"fmt"

	"github.com/jackc/pgerrcode"
	"github.com/justjake/pglink/pkg/pgwire"
)

// DefaultCancelHandler handles cancellation requests by looking up the
// connection in the server's [ConnMap] and calling the connection's
// [ClientConn.CancelHandler] if set.
func DefaultCancelHandler(ctx context.Context, conn *CancelConn) error {
	server := CtxServer(ctx)
	if server == nil {
		panic("pgserver not found in context")
	}

	clientConn, ok := server.ConnMap.Get(CancelMessageConnKey(conn.CancelMessage))
	if !ok {
		return pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.ConnectionDoesNotExist, "connection not found", fmt.Errorf("ProcessId=%v SecretKey=<redacted>", conn.CancelMessage.ProcessID))
	}

	if clientConn.CancelHandler == nil {
		return pgwire.NewErr(pgwire.ErrorFatal, pgerrcode.FeatureNotSupported, "cancel handler not available", nil)
	}

	return clientConn.CancelHandler(ctx, clientConn, conn)
}

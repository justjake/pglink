package pgproxy

import (
	"context"
	"fmt"

	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/justjake/pglink/pkg/pure"
)

// StatementTracker tracks prepared statements on a backend connection.
// It tracks both "alive" statements (confirmed by ParseComplete) and
// "pending" statements (Parse sent but ParseComplete not yet received).
type StatementTracker struct {
	// Alive contains statements that have been confirmed by the server.
	Alive map[string]bool
	// PendingCreate contains statements that have been sent to the server
	// but not yet confirmed (Parse sent, ParseComplete not yet received).
	PendingCreate map[string]bool
	// PendingClose contains statements that are being closed
	// (Close sent, CloseComplete not yet received).
	PendingClose map[string]bool
}

var _ MessageTracker = (*StatementTracker)(nil)

// NewStatementTracker creates a new StatementTracker with initialized maps.
func NewStatementTracker() *StatementTracker {
	return &StatementTracker{
		Alive:         make(map[string]bool),
		PendingCreate: make(map[string]bool),
		PendingClose:  make(map[string]bool),
	}
}

// HasStatement returns true if the named prepared statement exists or is pending.
// This includes statements that are pending creation (Parse in-flight but
// ParseComplete not yet received).
func (t *StatementTracker) HasStatement(name string) bool {
	return t.Alive[name] || t.PendingCreate[name]
}

// TrackMessage implements MessageTracker.
func (t *StatementTracker) TrackMessage(ctx context.Context, msg pgwire.Message) (context.Context, error) {
	t.trackNow(msg)
	return ctx, nil
}

// TrackEffect implements MessageTracker.
func (t *StatementTracker) TrackEffect(msg pgwire.Message) pure.Effect {
	switch msg.(type) {
	case *pgwire.ClientParse, *pgwire.ClientClose, *pgwire.ClientQuery,
		*pgwire.ServerParseComplete, *pgwire.ServerCloseComplete:
		return pure.WithNameFunc(func() string {
			return fmt.Sprintf("StatementTracker.Track(%T)", msg)
		}, pure.Do(func() {
			t.trackNow(msg)
		}))
	}
	return nil
}

func (t *StatementTracker) trackNow(msg pgwire.Message) {
	switch msg := msg.(type) {
	case *pgwire.ClientParse:
		// Parse starts creating a statement
		parsed := msg.Parse()
		t.PendingCreate[parsed.Name] = true

	case *pgwire.ClientClose:
		// Close starts closing a statement or portal
		parsed := msg.Parse()
		if parsed.ObjectType == pgwire.ObjectTypePreparedStatement {
			t.PendingClose[parsed.Name] = true
		}

	case *pgwire.ClientQuery:
		// Simple query invalidates the unnamed statement
		delete(t.Alive, "")
		delete(t.PendingCreate, "")

	case *pgwire.ServerParseComplete:
		// ParseComplete confirms the statement was created
		// Move all pending creates to alive
		for name := range t.PendingCreate {
			t.Alive[name] = true
		}
		clear(t.PendingCreate)

	case *pgwire.ServerCloseComplete:
		// CloseComplete confirms statements were closed
		for name := range t.PendingClose {
			delete(t.Alive, name)
		}
		clear(t.PendingClose)
	}
}

package backend

import (
	"testing"

	"github.com/justjake/pglink/pkg/pgwire"
)

// TestHasStatement_ChecksPendingCreate verifies that HasStatement returns true
// for statements that are in PendingCreate (Parse sent, ParseComplete not yet received).
//
// This is critical for pipelined extended query protocol where Parse+Bind are
// sent in the same batch. When Bind runs, Parse is still in PendingCreate.
// Without this fix, HasStatement returned false and triggered unnecessary
// statement re-creation that failed because the statement wasn't in cache yet.
func TestHasStatement_ChecksPendingCreate(t *testing.T) {
	// Create a minimal session with state tracking
	session := &Session{
		State: pgwire.NewProtocolState(),
	}
	backend := &PooledBackend{
		session: session,
	}

	// Initially, no statements exist
	if backend.HasStatement("stmt1") {
		t.Error("HasStatement should return false for non-existent statement")
	}
	if backend.HasStatement("") {
		t.Error("HasStatement should return false for non-existent unnamed statement")
	}

	// Simulate Parse being sent (but ParseComplete not yet received)
	// This puts the statement in PendingCreate
	session.State.Statements.PendingCreate["stmt1"] = true

	// HasStatement should now return true because the statement is pending
	if !backend.HasStatement("stmt1") {
		t.Error("HasStatement should return true for statement in PendingCreate")
	}

	// Test with unnamed statement (empty string) - this is what pgbench uses
	session.State.Statements.PendingCreate[""] = true
	if !backend.HasStatement("") {
		t.Error("HasStatement should return true for unnamed statement in PendingCreate")
	}

	// After ParseComplete, statement moves from PendingCreate to Alive
	delete(session.State.Statements.PendingCreate, "stmt1")
	session.State.Statements.Alive["stmt1"] = true

	// HasStatement should still return true
	if !backend.HasStatement("stmt1") {
		t.Error("HasStatement should return true for statement in Alive")
	}

	// Remove from Alive - should now return false
	delete(session.State.Statements.Alive, "stmt1")
	if backend.HasStatement("stmt1") {
		t.Error("HasStatement should return false after statement removed from Alive")
	}
}

// TestHasStatement_PipelinedParseBindScenario tests the exact scenario that
// caused pgbench to fail with 0 TPS:
//
// pgbench sends: Parse (unnamed) + Bind (unnamed) + Execute + Sync
// When Bind is processed, Parse is in PendingCreate (ParseComplete not received).
// HasStatement must return true so we don't try to re-create the statement.
func TestHasStatement_PipelinedParseBindScenario(t *testing.T) {
	session := &Session{
		State: pgwire.NewProtocolState(),
	}
	backend := &PooledBackend{
		session: session,
	}

	// Step 1: Parse is sent for unnamed statement
	// This simulates UpdateState being called after Parse handler
	session.State.Statements.PendingCreate[""] = true

	// Step 2: Bind handler runs - it checks HasStatement
	// Before the fix, this returned false and caused re-creation to fail
	if !backend.HasStatement("") {
		t.Fatal("HasStatement must return true for unnamed statement in PendingCreate - this was the pgbench bug")
	}

	// Step 3: After ParseComplete is received, statement moves to Alive
	delete(session.State.Statements.PendingCreate, "")
	session.State.Statements.Alive[""] = true

	// HasStatement should still work
	if !backend.HasStatement("") {
		t.Error("HasStatement should return true for unnamed statement in Alive")
	}
}

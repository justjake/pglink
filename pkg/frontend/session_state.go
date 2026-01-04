package frontend

import (
	"fmt"
	"sync/atomic"

	"go.opentelemetry.io/otel/trace"

	"github.com/justjake/pglink/pkg/backend"
	"github.com/justjake/pglink/pkg/observability"
	"github.com/justjake/pglink/pkg/pgwire"
)

// statementSeqCounter is a global counter for generating unique statement names.
// See clientToServerPreparedStatementName.
var statementSeqCounter atomic.Uint64

// SessionState holds state for a client session that is managed separately from
// the pgproxy.Session proxy logic. This state is specific to the pglink application
// rather than general-purpose proxy functionality.
//
// This struct is designed to be stored in pgserver.ClientConn.ExtraData and
// accessed by handlers during the session lifecycle.
type SessionState struct {
	// Client-facing protocol state
	// TODO: This will be migrated to pgproxy.MessageTrackers in Phase 2
	ProtocolState pgwire.ProtocolState

	// Statement name rewriting state
	// Maps client statement names to server statement names.
	// Client statement names may be reused across backends, but each backend
	// needs its own unique name to avoid collisions.
	statementNameMap map[string]string

	// Maps server statement names to query hashes.
	// Used for prepared statement caching: when a Bind references a statement
	// that doesn't exist on the current backend, we use the hash to look up
	// the query and re-create the statement.
	serverStatementQueryHash map[string]uint64

	// Observability state
	TracingEnabled bool                   // Whether OTEL tracing is enabled
	Metrics        *observability.Metrics // May be nil if metrics disabled
	SessionSpan    trace.Span             // Root span for this session (nil if tracing disabled)

	// For CopyRecognizer: track the last SQL query to associate COPY with its query
	LastSQL string

	// Backend state - currently connected backend, if any
	// Note: Cancel handler should fetch this at cancel time rather than storing backend PID
	backend *backend.PooledBackend
}

// ClientToServerStatementName maps a client statement name to a unique server name.
// If no mapping exists, creates a new unique server name and stores the mapping.
//
// This ensures each client statement gets a unique server name, which is necessary
// when pooling backends because:
//  1. Different clients may use the same statement names for different queries
//  2. A backend may have prepared statements from previous sessions
//  3. Statement re-creation on Bind needs consistent naming
func (s *SessionState) ClientToServerStatementName(clientName string) string {
	if clientName == "" {
		// Empty name = unnamed prepared statement, which is connection-local
		// and doesn't need rewriting
		return ""
	}

	if s.statementNameMap == nil {
		s.statementNameMap = make(map[string]string)
	}

	if serverName, ok := s.statementNameMap[clientName]; ok {
		return serverName
	}

	// Generate unique server name: "pglink_stmt_<seq>_<clientName>"
	seq := statementSeqCounter.Add(1)
	serverName := fmt.Sprintf("pglink_stmt_%d_%s", seq, clientName)
	s.statementNameMap[clientName] = serverName
	return serverName
}

// TrackStatementQueryHash stores the query hash for a server statement name.
// This is called when a Parse message is forwarded to the backend.
func (s *SessionState) TrackStatementQueryHash(serverName string, queryHash uint64) {
	if serverName == "" {
		return
	}
	if s.serverStatementQueryHash == nil {
		s.serverStatementQueryHash = make(map[string]uint64)
	}
	s.serverStatementQueryHash[serverName] = queryHash
}

// GetStatementQueryHash retrieves the query hash for a server statement name.
// Returns the hash and whether it was found.
func (s *SessionState) GetStatementQueryHash(serverName string) (uint64, bool) {
	if s.serverStatementQueryHash == nil {
		return 0, false
	}
	hash, ok := s.serverStatementQueryHash[serverName]
	return hash, ok
}

// GetBackend returns the currently connected backend, or nil if not connected.
// The cancel handler uses this to get the backend for query cancellation.
func (s *SessionState) GetBackend() *backend.PooledBackend {
	return s.backend
}

// SetBackend sets the currently connected backend.
func (s *SessionState) SetBackend(be *backend.PooledBackend) {
	s.backend = be
}

// ClearBackend clears the backend reference (called on release).
func (s *SessionState) ClearBackend() {
	s.backend = nil
}

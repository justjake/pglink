package backend

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/pgwire"
)

const SessionExtraDataKey = "pgwire_session"

// Session with the backend.
// Each PgConn in our connection pool gets its own Session once we acquire it
// the first time.
type Session struct {
	DB                *Database
	Conn              *pgconn.PgConn
	User              config.UserConfig
	UserName          string
	State             pgwire.ProtocolState
	TrackedParameters []string

	logger *slog.Logger

	// Ring buffer for zero-copy message proxying
	ringBuffer *pgwire.RingBuffer
}

func GetSession(conn *pgconn.PgConn) *Session {
	custonData := conn.CustomData()
	if existingUntyped, ok := custonData[SessionExtraDataKey]; ok {
		return existingUntyped.(*Session)
	}
	return nil
}

func GetOrCreateSession(conn *pgconn.PgConn, db *Database, user config.UserConfig) (*Session, error) {
	if existing := GetSession(conn); existing != nil {
		if existing.DB == db && existing.User == user {
			return existing, nil
		} else {
			return nil, fmt.Errorf("backend session mismatch: existing (db %p, user %v) != new (db %p, user %v)", existing.DB, existing.User, db, user)
		}
	}

	tracked := pgwire.BaseTrackedParameters
	if len(db.config.TrackExtraParameters) > 0 {
		tracked = make([]string, 0, len(pgwire.BaseTrackedParameters)+len(db.config.TrackExtraParameters))
		tracked = append(tracked, pgwire.BaseTrackedParameters...)
		tracked = append(tracked, db.config.TrackExtraParameters...)
	}

	username := conn.ParameterStatus(pgwire.ParamUser)
	if username == "" {
		// TODO: this seems silly
		getNameCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		var err error
		username, err = db.secrets.Get(getNameCtx, user.Username)
		if err != nil {
			return nil, fmt.Errorf("failed to get username: %w", err)
		}
	}

	state := pgwire.NewProtocolState()
	state.PID = conn.PID()
	state.SecretCancelKey = conn.SecretKey()
	state.TxStatus = pgwire.TxStatus(conn.TxStatus())

	session := &Session{
		DB:                db,
		Conn:              conn,
		UserName:          username,
		User:              user,
		State:             state,
		TrackedParameters: tracked,
	}
	session.updateState()
	session.logger = db.logger.With("session", session.String())

	conn.CustomData()[SessionExtraDataKey] = session
	return session, nil
}

func (s *Session) String() string {
	return fmt.Sprintf("%s@%s?pid=%d", s.UserName, s.DB.Name(), s.Conn.PID())
}

func (s *Session) ParameterStatusChanges(keys []string, since pgwire.ParameterStatuses) pgwire.ParameterStatusDiff {
	return since.DiffToTip(s.updateParameterStatuses(keys))
}

// Acquire prepares the session for use but does NOT start the ring buffer.
//
// After acquiring, callers should:
// 1. Run any internal queries (like varcache SET statements) using PgConn() directly
// 2. Call StartRingBuffer() to begin reading from the connection
//
// This two-phase approach allows internal queries to be sent before the ring
// buffer starts consuming connection data.
//
// Note: We do NOT sync parameter statuses from pgconn here because:
//   - On first acquire, GetOrCreateSession() already initialized state from pgconn
//   - On subsequent acquires, Session.State is kept up-to-date via UpdateState()
//     during normal operation (zero-copy proxy traffic)
//   - pgconn doesn't know about changes made through the proxy, only our tracking does
//   - After internal queries, SyncParameterStatusesFromPgConn() is called explicitly
func (s *Session) Acquire() error {
	if s.ringBuffer != nil && s.ringBuffer.Running() {
		return fmt.Errorf("session already acquired")
	}
	// Don't sync ParameterStatuses from pgconn - our tracking is authoritative.
	// Only sync TxStatus which pgconn tracks correctly.
	s.State.TxStatus = pgwire.TxStatus(s.Conn.TxStatus())

	// Prepare ring buffer but don't start it yet
	if s.ringBuffer == nil {
		s.ringBuffer = pgwire.NewRingBuffer(pgwire.RingBufferConfigForSize(s.DB.config.GetMessageBufferBytes()))
	} else {
		s.ringBuffer = s.ringBuffer.NewWithSameBuffers()
	}
	// Note: StartNetConnReader is called separately via StartRingBuffer()

	return nil
}

// StartRingBuffer begins the ring buffer reader goroutine.
// Must be called after Acquire() and any internal queries are complete.
func (s *Session) StartRingBuffer() {
	if s.ringBuffer == nil {
		panic("StartRingBuffer called before Acquire")
	}
	if s.ringBuffer.Running() {
		return // Already running
	}
	s.ringBuffer.StartNetConnReader(context.Background(), s.Conn.Conn())
}

func (s *Session) Release() {
	// TODO: do some things to normalize state?
	// Run Sync, etc, before releasing? / releasing to the pool?

	if s.ringBuffer != nil && s.ringBuffer.Running() {
		if err := s.ringBuffer.StopNetConnReader(); err != nil {
			slog.Warn("backend session release: failed to stop ring buffer reader", "error", err)
		}
	}
}

// RingBuffer returns the ring buffer for zero-copy message proxying.
// The ring buffer is only available while the session is acquired.
func (s *Session) RingBuffer() *pgwire.RingBuffer {
	return s.ringBuffer
}

func (s *Session) WriteRange(r *pgwire.RingRange) (int64, error) {
	return io.Copy(s.Conn.Conn(), r.NewReader())
}

func (s *Session) WriteMsg(msg pgproto3.FrontendMessage) error {
	s.Conn.Frontend().Send(msg)
	return s.Conn.Frontend().Flush()
}

func (s *Session) Flush() error {
	return s.Conn.Frontend().Flush()
}

func (s *Session) updateParameterStatuses(keys []string) pgwire.ParameterStatuses {
	parameterStatuses := s.State.ParameterStatuses
	for _, key := range keys {
		value := s.Conn.ParameterStatus(key)
		if value == "" {
			delete(s.State.ParameterStatuses, key)
		} else {
			parameterStatuses[key] = value
		}
	}
	return parameterStatuses
}

// SyncParameterStatusesFromPgConn updates Session.State.ParameterStatuses from
// pgconn's internal parameter tracking. Call this after using pgconn.Exec()
// directly (e.g., for internal queries like varcache) to keep state in sync.
func (s *Session) SyncParameterStatusesFromPgConn() {
	s.updateParameterStatuses(s.TrackedParameters)
}

func (s *Session) updateState() {
	s.updateParameterStatuses(s.TrackedParameters)
	s.State.TxStatus = pgwire.TxStatus(s.Conn.TxStatus())
}

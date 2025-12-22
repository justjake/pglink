package backend

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/pgwire"
)

const SessionExtraDataKey = "pgwire_session"

func GetSession(conn *pgconn.PgConn) *Session {
	custonData := conn.CustomData()
	if existingUntyped, ok := custonData[SessionExtraDataKey]; ok {
		return existingUntyped.(*Session)
	}
	return nil
}

func GetOrCreateSession(conn *pgconn.PgConn, db *Database, user config.UserConfig) *Session {
	if existing := GetSession(conn); existing != nil {
		if existing.DB == db && existing.User == user {
			return existing
		} else {
			panic(fmt.Errorf("backend session mismatch: existing (db %p, user %v) != new (db %p, user %v)", existing.DB, existing.User, db, user))
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
			panic(fmt.Errorf("failed to get username: %w", err))
		}
	}

	session := &Session{
		DB:       db,
		Conn:     conn,
		UserName: username,
		User:     user,
		State: pgwire.ProtocolState{
			PID:               conn.PID(),
			SecretCancelKey:   conn.SecretKey(),
			TxStatus:          pgwire.TxStatus(conn.TxStatus()),
			ParameterStatuses: pgwire.ParameterStatuses{},
		},
		TrackedParameters: tracked,
	}
	session.reader = NewChanReader(session.readBackendMessage)
	session.updateState()
	session.logger = db.logger.With("backend", session.Name())

	conn.CustomData()[SessionExtraDataKey] = session
	return session
}

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

	reader *ChanReader[pgwire.ServerMessage]
	logger *slog.Logger
}

func (s *Session) Name() string {
	return fmt.Sprintf("%s@%s?pid=%d", s.UserName, s.DB.Name(), s.Conn.PID())
}

func (s *Session) ParameterStatusChanges(keys []string, since pgwire.ParameterStatuses) pgwire.ParameterStatusDiff {
	return since.DiffToTip(s.updateParameterStatuses(keys))
}

func (s *Session) Flush() error {
	return s.Conn.Frontend().Flush()
}

// We have error signature because it's likely we'll want one in the future.
func (s *Session) Send(msg pgproto3.FrontendMessage) error {
	s.Conn.Frontend().Send(msg)
	return nil
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

func (s *Session) updateState() {
	s.updateParameterStatuses(s.TrackedParameters)
	s.State.TxStatus = pgwire.TxStatus(s.Conn.TxStatus())
}

func (s *Session) readBackendMessage() (pgwire.ServerMessage, error) {
	msg, err := s.Conn.Frontend().Receive()
	if err != nil {
		return nil, err
	}
	if m, ok := pgwire.ToServerMessage(msg); ok {
		return m, nil
	}
	return nil, fmt.Errorf("unknown backend message: %T", msg)
}

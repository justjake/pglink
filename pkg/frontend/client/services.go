package client

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/pgwire"
)

type PreparedStatements interface {
	ForQuery(query *pgproto3.Parse) ServerQuery // not nil
	GetByClientName(name string) ServerQuery    // or nil

	AddToCacheEffect(query ServerQuery) Effect
	ClearAllEffect() Effect
	RegisterClientNameEffect(name string, query ServerQuery) Effect
}

type preparedStatements struct {
	cache       *pgwire.PreparedStatementCache
	clientNames map[string]uint64
}

var _ PreparedStatements = &preparedStatements{}

func (p *preparedStatements) ForQuery(query pgproto3.Parse) ServerQuery {
	hash := pgwire.HashQuery(query.Query)
	if existing, ok := p.cache.Get(hash); ok {
		// TODO: not pointer equal :(
		return &serverQuery{existing}
	}

	return &serverQuery{
		stmt: &pgwire.PreparedStatement{
			Query:         query.Query,
			QueryHash:     hash,
			ParameterOIDs: query.ParameterOIDs,
		},
	}
}

// todo: bool ok
func (p *preparedStatements) GetByClientName(name string) ServerQuery {
	hash, ok := p.clientNames[name]
	if !ok {
		return nil
	}
	stmt, ok := p.cache.Get(hash)
	if !ok {
		return nil
	}
	// todo: fix
	return &serverQuery{stmt}
}

func (p *preparedStatements) AddToCacheEffect(query ServerQuery) Effect {
	return effect{
		name: fmt.Sprintf("AddToCacheEffect(%s)", query.ServerName()),
		apply: func(ctx context.Context) (cleanup Effect, err error) {
			// TODO: safety
			p.cache.Put(query.(*serverQuery).stmt)
			return effect{
				name: fmt.Sprintf("RemoveFromCacheEffect(%s)", query.ServerName()),
				apply: func(ctx context.Context) (cleanup Effect, err error) {
					p.cache.Delete(query.QueryHash())
					return nil, nil
				},
			}, nil
		},
	}
}

func (p *preparedStatements) ClearAllEffect() Effect {
	return effect{
		name: "ClearAllEffect",
		apply: func(ctx context.Context) (cleanup Effect, err error) {
			p.cache.Clear()
			return nil, nil
		},
	}
}

func (p *preparedStatements) RegisterClientNameEffect(name string, query ServerQuery) Effect {
	return effect{
		name: fmt.Sprintf("RegisterClientNameEffect(%s -> %s)", name, query.ServerName()),
		apply: func(ctx context.Context) (cleanup Effect, err error) {
			p.clientNames[name] = query.QueryHash()
			return effect{
				name: fmt.Sprintf("UnregisterClientNameEffect(%s)", name),
				apply: func(ctx context.Context) (cleanup Effect, err error) {
					delete(p.clientNames, name)
					return nil, nil
				},
			}, nil
		},
	}
}

type ServerQuery interface {
	ServerName() string
	QueryHash() uint64
	// ParseRequest returns a Parse request that will create the appropriate
	// PreparedStatement on the server.
	ParseRequest() *pgwire.ClientParse
	// BindRequest returns a rewritten Bind request that shares its parameter
	// values with the original Bind request
	BindRequest(original *pgwire.ClientBind) *pgwire.ClientBind
}

type serverQuery struct {
	stmt *pgwire.PreparedStatement
}

func (s *serverQuery) QueryHash() uint64 {
	return s.stmt.QueryHash
}

func (s *serverQuery) ServerName() string {
	return fmt.Sprintf("pgwire_hash_%d", s.stmt.QueryHash)
}

func (s *serverQuery) ParseRequest() *pgwire.ClientParse {
	return (*pgwire.ClientParse)(pgwire.ClientParsed(&pgproto3.Parse{
		Name:          s.ServerName(),
		Query:         s.stmt.Query,
		ParameterOIDs: s.stmt.ParameterOIDs,
	}))
}

func (s *serverQuery) BindRequest(original *pgwire.ClientBind) *pgwire.ClientBind {
	// TODO: structural sharing of the parameter values w/ `original`
	data := original.Parse()
	return (*pgwire.ClientBind)(pgwire.ClientParsed(&pgproto3.Bind{
		PreparedStatement:    s.ServerName(),
		DestinationPortal:    data.DestinationPortal,
		ParameterFormatCodes: data.ParameterFormatCodes,
		Parameters:           data.Parameters,
		ResultFormatCodes:    data.ResultFormatCodes,
	}))
}

type Logging interface {
	DebugEffect(msg string) Effect
}

type logging struct {
	ctx    context.Context
	logger *slog.Logger
}

func (l *logging) DebugEffect(msg string) Effect {
	if l.logger.Enabled(l.ctx, slog.LevelDebug) {
		return effect{
			name: "DebugEffect",
			apply: func(ctx context.Context) (cleanup Effect, err error) {
				l.logger.Debug(msg)
				return nil, nil
			},
		}
	} else {
		return EmptyEffect()
	}
}

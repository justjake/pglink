package client

import (
	"bytes"
	"context"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/pgwire"
)

// Ideas:
// - No mutation in handlers
//   - All mutation thru effects
//
// - Handlers entirely focus on "what", not "how"
//   - So far, we assume "the framework" turns Action into registered PendingRequest
//     automatically...
//
// - any handler checking state set by another handler is a smell
//   - can u use callback/continuation in the requester instead?
type SessionModel struct {
	PendingRequests    PendingRequests
	Backend            Backend
	PreparedStatements PreparedStatements
	Logging            Logging
}

func (s *SessionModel) HandleParse(msg *pgwire.ClientExtendedQueryParse) Action {
	data := msg.Parse()
	if data.Name == "" {
		return Forward(msg)
	}

	serverQuery := s.PreparedStatements.ForQuery(data)
	registerEffect := s.PreparedStatements.RegisterClientNameEffect(data.Name, serverQuery)
	if s.Backend.HasServerQuery(serverQuery) {
		return Respond(msg, pgwire.ToServer(&pgproto3.ParseComplete{}), registerEffect)
	} else {
		return RewriteAndHandleResponse(msg, serverQuery.ParseRequest(), func(res *pgwire.ServerExtendedQueryParseComplete) Action {
			return Forward(res, s.PreparedStatements.AddToCacheEffect(serverQuery))
		}, registerEffect)
	}
}

func (s *SessionModel) HandleBind(msg *pgwire.ClientExtendedQueryBind) Action {
	// TODO: avoid parsing all the parameters
	clientStatementName := msg.Parse().PreparedStatement
	if clientStatementName == "" {
		return Forward(msg)
	}

	if serverQuery := s.PreparedStatements.GetByClientName(clientStatementName); serverQuery != nil {
		if s.Backend.HasServerQuery(serverQuery) {
			return Rewrite(msg, serverQuery.BindRequest(msg))
		} else {
			// compare to pgbouncer: ensure_statement_is_prepared_on_server
			return Actions(
				SendToServerAndHandleResponse(serverQuery.ParseRequest(), func(res *pgwire.ServerExtendedQueryParseComplete) Action {
					return Skip(res)
				}, s.PreparedStatements.AddToCacheEffect(serverQuery)),
				Rewrite(msg, serverQuery.BindRequest(msg)),
			)
		}
	}

	// If this prepared statement name is unknown to us, we'll just trust the
	// client that it exists.
	// TODO: perhaps pgbouncer errors here?
	return Forward(msg)
}

func (s *SessionModel) HandleExecute(msg *pgwire.ClientExtendedQueryExecute) Action {
	return Forward(msg)
}

func (s *SessionModel) HandleParseComplete(msg *pgwire.ServerExtendedQueryParseComplete) Action {
	pendingRequest := s.PendingRequests.WaitingFor(msg)
	if pendingRequest != nil {
		return pendingRequest.Handle(msg)
	}

	return Forward(msg)
}

func (s *SessionModel) HandleBindComplete(msg *pgwire.ServerExtendedQueryBindComplete) Action {
	pendingRequest := s.PendingRequests.WaitingFor(msg)
	if pendingRequest != nil {
		return pendingRequest.Handle(msg)
	}

	return Forward(msg)
}

func (s *SessionModel) HandleErrorResponse(msg *pgwire.ServerResponseErrorResponse) Action {
	var effects []Effect
	if s.CopyMode.IsCopy() {
		// pgbouncer:
		/*
		 * Clear until next CopyDone or CopyFail message in the
		 * queue. This is needed to remove any Sync messages
		 * from the outstanding requests queue, for which we
		 * don't expect a response from the server.
		 *
		 * It isn't a problem if the CopyDone or CopyFail
		 * message has not been received yet. This message will
		 * be removed from the queue later when the server
		 * sends a ReadyForQuery message and we clear the queue
		 * until the next Sync.
		 *
		 * NOTE: CopyFail is the obvious error case, because
		 * here the client triggers a failure of the COPY.
		 * But CopyDone is also included in the search. The
		 * reason for that being that the server might fail the
		 * COPY for some reason unknown to the client (e.g. a
		 * unique constraint violation).
		 */
		effects = append(
			effects,
			s.Logging.DebugEffect("COPY failed"),
			s.PendingRequests.FailPendingUntilEffect([]pgwire.MsgType{pgwire.MsgClientCopyDone, pgwire.MsgClientCopyFail}),
		)

	}

	// pbouncer set server->query_failed = true
	// us: ???
	pendingRequest := s.PendingRequests.WaitingFor(msg)
	if pendingRequest != nil {
		// Note: we assume pendingRequest.Handle() returns a RemovePendingRequestEffect
		// to remove the request from the queue
		return RunEffectsFirst(effects, pendingRequest.Handle(msg))
	}
	return Forward(msg, effects...)
}

func (s *SessionModel) HandleCommandComplete(msg *pgwire.ServerResponseCommandComplete) Action {
	var effects []Effect
	// TODO: should this just be a callback from CopyStart?
	// Do we need copy_mode explicitly across multiple handlers?
	if s.CopyMode.IsCopy() {
		// pgbouncer: remove Sync messages for which we don't expect a response from the server
		effects = append(effects, s.CopyMode.EndEffect())
		effects = append(effects, s.PendingRequests.FailPendingUntilEffect([]pgwire.MsgType{pgwire.MsgClientCopyDone}))
	}

	// pgbouncer: clean up prepared statements if client sent DEALLOCATE ALL or DISCARD ALL
	// todo: us: perhaps this should be in callback for Execute?
	if s.PreparedStatements.Enabled() {
		tag := msg.Parse().CommandTag
		if bytes.Equal(tag, []byte("DEALLOCATE ALL")) || bytes.Equal(tag, []byte("DISCARD ALL")) {
			effects = append(effects, s.PreparedStatements.ClearAllEffect())
		}
	}

	// pgbouncer: pop outstanding Execute request
	// us: pop whoever wants it
	pendingRequest := s.PendingRequests.WaitingFor(msg)
	if pendingRequest != nil {
		return RunEffectsFirst(effects, pendingRequest.Handle(msg))
	}

	return Forward(msg, effects...)
}

func HandleReadyForQuery(msg *pgwire.ServerResponseReadyForQuery) Action {

}

type PendingRequests interface {
	WaitingFor(msg pgwire.ServerMessage) PendingRequest
	FailPendingUntilEffect(msgTypes []pgwire.MsgType) Effect
	RemovePendingRequestEffect(request PendingRequest) Effect
}

type PendingRequest interface {
	Handle(msg pgwire.ServerMessage) Action
}

type Backend interface {
	HasServerQuery(query ServerQuery) bool
}

type RewriteAction struct {
	msg              pgwire.Message
	rewritten        pgwire.Message
	responseCallback func(res pgwire.ServerMessage) Action
	effects          []Effect
}

type Effect interface {
	Apply(ctx context.Context) (cleanup Effect, err error)
}

func Rewrite(msg pgwire.Message, rewritten pgwire.Message, effects ...Effect) RewriteAction {
	return RewriteAction{
		msg:       msg,
		rewritten: rewritten,
		effects:   effects,
	}
}

func RewriteAndHandleResponse(msg pgwire.Message, rewritten any, responseCallback func(res pgwire.ServerMessage) Action, effects ...Effect) RewriteAction {
	return RewriteAction{
		msg:              msg,
		rewritten:        rewritten,
		responseCallback: responseCallback,
	}
}

func Forward(msg pgwire.Message, effects ...Effect) Action {
	return ForwardAction{
		msg:     msg,
		effects: effects,
	}
}

type ForwardAction struct {
	msg              pgwire.Message
	responseCallback func(res pgwire.Message) Action
	effects          []Effect
}

type Action interface {
	// TODO: design
}

func RunEffectsFirst(effects []Effect, action Action) Action {
	return WithEffectsAction{
		before: effects,
		action: action,
	}
}

type WithEffectsAction struct {
	before []Effect
	action Action
}

func SendToServerAndHandleResponse(msg pgwire.ClientMessage, responseCallback func(res pgwire.ServerMessage) Action, effects ...Effect) Action {
	return SendToServerAction{
		msg:              msg,
		responseCallback: responseCallback,
		effects:          effects,
	}
}

func Respond(req pgwire.ClientMessage, res pgwire.ServerMessage, effects ...Effect) Action {
	return RespondAction{
		req:     req,
		res:     res,
		effects: effects,
	}
}

type RespondAction struct {
	req     pgwire.ClientMessage
	res     pgwire.ServerMessage
	effects []Effect
}

func Actions(a Action, b Action, rest ...Action) Action {
	return CompoundAction{
		a:    a,
		b:    b,
		rest: rest,
	}
}

type CompoundAction struct {
	a    Action
	b    Action
	rest []Action
}

type SendToServerAction struct {
	msg              pgwire.ClientMessage
	responseCallback func(res pgwire.ServerMessage) Action
	effects          []Effect
}

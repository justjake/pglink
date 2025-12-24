package pgwire

import (
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Flow tracks a multi-message protocol flow (query, prepare, copy, etc.)
// The lifecycle of a flow:
// 1. A FlowRecognizer detects the start of a flow and creates a new Flow
// 2. For each message, UpdateHandlers is called - handlers return true to continue, false when complete
// 3. When complete (handler returns false), Close() is called
type Flow interface {
	// UpdateHandlers returns handlers to update flow state from messages.
	// Handlers return true to continue the flow, false when the flow is complete.
	// The default behavior for unhandled messages is to continue (return true).
	UpdateHandlers(state *ProtocolState) FlowUpdateHandlers

	// Close is called when flow ends (complete or timeout/eviction).
	// The flow implementation handles its own cleanup and callbacks.
	Close()
}

// FlowRecognizer detects when new flows start.
type FlowRecognizer interface {
	// StartHandlers returns handlers that detect flow starts.
	// Handlers return a non-nil Flow when a new flow begins, nil otherwise.
	StartHandlers(state *ProtocolState) FlowStartHandlers
}

// FlowUpdateHandlers provides handlers for updating a flow's state from messages.
// Each handler returns true to continue the flow, false when complete.
type FlowUpdateHandlers struct {
	Client FlowUpdateClientHandlers
	Server FlowUpdateServerHandlers
}

// Update calls the appropriate handler based on message type and returns whether to continue.
// Returns true (continue) if no handler is defined for the message type.
func (h FlowUpdateHandlers) Update(msg Message) bool {
	if m, ok := msg.(ClientMessage); ok {
		return h.Client.Update(m)
	} else if m, ok := msg.(ServerMessage); ok {
		return h.Server.Update(m)
	}
	return true // Unknown message type, continue
}

// FlowUpdateClientHandlers provides handlers for client messages.
type FlowUpdateClientHandlers struct {
	SimpleQuery   func(msg ClientSimpleQuery) bool
	ExtendedQuery func(msg ClientExtendedQuery) bool
	Copy          func(msg ClientCopy) bool
}

// Update calls the appropriate handler and returns whether to continue.
func (h FlowUpdateClientHandlers) Update(msg ClientMessage) bool {
	switch m := msg.(type) {
	case ClientSimpleQuery:
		if h.SimpleQuery != nil {
			return h.SimpleQuery(m)
		}
	case ClientExtendedQuery:
		if h.ExtendedQuery != nil {
			return h.ExtendedQuery(m)
		}
	case ClientCopy:
		if h.Copy != nil {
			return h.Copy(m)
		}
	}
	return true // No handler, continue
}

// FlowUpdateServerHandlers provides handlers for server messages.
type FlowUpdateServerHandlers struct {
	Response      func(msg ServerResponse) bool
	ExtendedQuery func(msg ServerExtendedQuery) bool
	Copy          func(msg ServerCopy) bool
	Async         func(msg ServerAsync) bool
}

// Update calls the appropriate handler and returns whether to continue.
func (h FlowUpdateServerHandlers) Update(msg ServerMessage) bool {
	switch m := msg.(type) {
	case ServerResponse:
		if h.Response != nil {
			return h.Response(m)
		}
	case ServerExtendedQuery:
		if h.ExtendedQuery != nil {
			return h.ExtendedQuery(m)
		}
	case ServerCopy:
		if h.Copy != nil {
			return h.Copy(m)
		}
	case ServerAsync:
		if h.Async != nil {
			return h.Async(m)
		}
	}
	return true // No handler, continue
}

// FlowStartHandlers provides handlers for detecting when flows start.
// Each handler returns a non-nil Flow when a new flow begins, nil otherwise.
type FlowStartHandlers struct {
	Client FlowStartClientHandlers
	Server FlowStartServerHandlers
}

// Start calls the appropriate handler and returns a Flow if one started.
func (h FlowStartHandlers) Start(msg Message) Flow {
	if m, ok := msg.(ClientMessage); ok {
		return h.Client.Start(m)
	} else if m, ok := msg.(ServerMessage); ok {
		return h.Server.Start(m)
	}
	return nil
}

// FlowStartClientHandlers provides handlers for detecting client-initiated flows.
type FlowStartClientHandlers struct {
	SimpleQuery   func(msg ClientSimpleQuery) Flow
	ExtendedQuery func(msg ClientExtendedQuery) Flow
}

// Start calls the appropriate handler and returns a Flow if one started.
func (h FlowStartClientHandlers) Start(msg ClientMessage) Flow {
	switch m := msg.(type) {
	case ClientSimpleQuery:
		if h.SimpleQuery != nil {
			return h.SimpleQuery(m)
		}
	case ClientExtendedQuery:
		if h.ExtendedQuery != nil {
			return h.ExtendedQuery(m)
		}
	}
	return nil
}

// FlowStartServerHandlers provides handlers for detecting server-initiated flow changes.
// For example, CopyInResponse/CopyOutResponse indicate the server is entering copy mode.
type FlowStartServerHandlers struct {
	Copy func(msg ServerCopy) Flow
}

// Start calls the appropriate handler and returns a Flow if one started.
func (h FlowStartServerHandlers) Start(msg ServerMessage) Flow {
	switch m := msg.(type) {
	case ServerCopy:
		if h.Copy != nil {
			return h.Copy(m)
		}
	}
	return nil
}

// FlowType categorizes different types of protocol flows.
type FlowType int

const (
	FlowTypeSimpleQuery FlowType = iota
	FlowTypePrepare
	FlowTypeExecute
	FlowTypeCopyIn
	FlowTypeCopyOut
)

func (t FlowType) String() string {
	switch t {
	case FlowTypeSimpleQuery:
		return "simple_query"
	case FlowTypePrepare:
		return "prepare"
	case FlowTypeExecute:
		return "execute"
	case FlowTypeCopyIn:
		return "copy_in"
	case FlowTypeCopyOut:
		return "copy_out"
	default:
		return "unknown"
	}
}

// SimpleQueryFlow tracks a simple query from Query message to ReadyForQuery.
type SimpleQueryFlow struct {
	SQL        string
	StartTime  time.Time
	EndTime    time.Time
	CommandTag pgconn.CommandTag
	Err        *pgproto3.ErrorResponse
	RowCount   int64

	// OnClose is called when the flow completes. Injected by the recognizer.
	OnClose func(*SimpleQueryFlow)
}

// NewSimpleQueryFlow creates a new SimpleQueryFlow from a Query message.
func NewSimpleQueryFlow(msg ClientSimpleQueryQuery, onClose func(*SimpleQueryFlow)) *SimpleQueryFlow {
	return &SimpleQueryFlow{
		SQL:       msg.T.String,
		StartTime: time.Now(),
		OnClose:   onClose,
	}
}

func (f *SimpleQueryFlow) UpdateHandlers(state *ProtocolState) FlowUpdateHandlers {
	return FlowUpdateHandlers{
		Server: FlowUpdateServerHandlers{
			Response: func(msg ServerResponse) bool {
				switch m := msg.(type) {
				case ServerResponseCommandComplete:
					f.CommandTag = pgconn.NewCommandTag(string(m.T.CommandTag))
					return true // Continue until ReadyForQuery
				case ServerResponseErrorResponse:
					f.Err = m.T
					return true // Continue until ReadyForQuery
				case ServerResponseReadyForQuery:
					f.EndTime = time.Now()
					return false // FLOW COMPLETE
				case ServerResponseDataRow:
					f.RowCount++
					return true
				}
				return true
			},
		},
	}
}

func (f *SimpleQueryFlow) Close() {
	if f.OnClose != nil {
		f.OnClose(f)
	}
}

// ExtendedQueryFlow tracks an extended query flow.
// This covers both Prepare (Parse+Describe) and Execute (Bind+Execute) flows.
type ExtendedQueryFlow struct {
	Type          FlowType // FlowTypePrepare or FlowTypeExecute
	SQL           string   // Only set for Parse-based flows
	StatementName string
	PortalName    string
	StartTime     time.Time
	EndTime       time.Time
	CommandTag    pgconn.CommandTag
	Err           *pgproto3.ErrorResponse
	RowCount      int64

	// Internal state
	seenBind    bool
	seenExecute bool

	// OnClose is called when the flow completes. Injected by the recognizer.
	OnClose func(*ExtendedQueryFlow)
}

// NewExtendedQueryFlow creates a new ExtendedQueryFlow from a Parse message.
func NewExtendedQueryFlow(msg ClientExtendedQueryParse, onClose func(*ExtendedQueryFlow)) *ExtendedQueryFlow {
	return &ExtendedQueryFlow{
		Type:          FlowTypePrepare, // Will be updated to FlowTypeExecute if we see Bind+Execute
		SQL:           msg.T.Query,
		StatementName: msg.T.Name,
		StartTime:     time.Now(),
		OnClose:       onClose,
	}
}

func (f *ExtendedQueryFlow) UpdateHandlers(state *ProtocolState) FlowUpdateHandlers {
	return FlowUpdateHandlers{
		Client: FlowUpdateClientHandlers{
			ExtendedQuery: func(msg ClientExtendedQuery) bool {
				switch m := msg.(type) {
				case ClientExtendedQueryBind:
					f.seenBind = true
					f.PortalName = m.T.DestinationPortal
				case ClientExtendedQueryExecute:
					f.seenExecute = true
					if f.seenBind {
						f.Type = FlowTypeExecute
					}
				}
				return true
			},
		},
		Server: FlowUpdateServerHandlers{
			Response: func(msg ServerResponse) bool {
				switch m := msg.(type) {
				case ServerResponseCommandComplete:
					f.CommandTag = pgconn.NewCommandTag(string(m.T.CommandTag))
					return true
				case ServerResponseErrorResponse:
					f.Err = m.T
					return true
				case ServerResponseReadyForQuery:
					f.EndTime = time.Now()
					return false // FLOW COMPLETE
				case ServerResponseDataRow:
					f.RowCount++
					return true
				}
				return true
			},
		},
	}
}

func (f *ExtendedQueryFlow) Close() {
	if f.OnClose != nil {
		f.OnClose(f)
	}
}

// CopyFlow tracks a COPY operation from CopyInResponse/CopyOutResponse to ReadyForQuery.
type CopyFlow struct {
	Type       FlowType // FlowTypeCopyIn or FlowTypeCopyOut
	SQL        string   // The original SQL that initiated the COPY (from the preceding Query)
	StartTime  time.Time
	EndTime    time.Time
	CommandTag pgconn.CommandTag
	Err        *pgproto3.ErrorResponse
	ByteCount  int64

	// OnClose is called when the flow completes. Injected by the recognizer.
	OnClose func(*CopyFlow)
}

// NewCopyInFlow creates a new CopyFlow for COPY FROM (client -> server).
func NewCopyInFlow(sql string, onClose func(*CopyFlow)) *CopyFlow {
	return &CopyFlow{
		Type:      FlowTypeCopyIn,
		SQL:       sql,
		StartTime: time.Now(),
		OnClose:   onClose,
	}
}

// NewCopyOutFlow creates a new CopyFlow for COPY TO (server -> client).
func NewCopyOutFlow(sql string, onClose func(*CopyFlow)) *CopyFlow {
	return &CopyFlow{
		Type:      FlowTypeCopyOut,
		SQL:       sql,
		StartTime: time.Now(),
		OnClose:   onClose,
	}
}

func (f *CopyFlow) UpdateHandlers(state *ProtocolState) FlowUpdateHandlers {
	return FlowUpdateHandlers{
		Client: FlowUpdateClientHandlers{
			Copy: func(msg ClientCopy) bool {
				switch m := msg.(type) {
				case ClientCopyCopyData:
					f.ByteCount += int64(len(m.T.Data))
				}
				return true
			},
		},
		Server: FlowUpdateServerHandlers{
			Copy: func(msg ServerCopy) bool {
				switch m := msg.(type) {
				case ServerCopyCopyData:
					f.ByteCount += int64(len(m.T.Data))
				}
				return true
			},
			Response: func(msg ServerResponse) bool {
				switch m := msg.(type) {
				case ServerResponseCommandComplete:
					f.CommandTag = pgconn.NewCommandTag(string(m.T.CommandTag))
					return true
				case ServerResponseErrorResponse:
					f.Err = m.T
					return true
				case ServerResponseReadyForQuery:
					f.EndTime = time.Now()
					return false // FLOW COMPLETE
				}
				return true
			},
		},
	}
}

func (f *CopyFlow) Close() {
	if f.OnClose != nil {
		f.OnClose(f)
	}
}

// SimpleQueryRecognizer detects simple query flows.
type SimpleQueryRecognizer struct {
	// OnStart is called when a simple query flow starts. The returned function
	// becomes the flow's OnClose callback.
	OnStart func(msg ClientSimpleQueryQuery) func(*SimpleQueryFlow)
}

func (r *SimpleQueryRecognizer) StartHandlers(state *ProtocolState) FlowStartHandlers {
	return FlowStartHandlers{
		Client: FlowStartClientHandlers{
			SimpleQuery: func(msg ClientSimpleQuery) Flow {
				if query, ok := msg.(ClientSimpleQueryQuery); ok {
					var onClose func(*SimpleQueryFlow)
					if r.OnStart != nil {
						onClose = r.OnStart(query)
					}
					return NewSimpleQueryFlow(query, onClose)
				}
				return nil
			},
		},
	}
}

// ExtendedQueryRecognizer detects extended query flows starting with Parse.
type ExtendedQueryRecognizer struct {
	// OnStart is called when an extended query flow starts. The returned function
	// becomes the flow's OnClose callback.
	OnStart func(msg ClientExtendedQueryParse) func(*ExtendedQueryFlow)
}

func (r *ExtendedQueryRecognizer) StartHandlers(state *ProtocolState) FlowStartHandlers {
	return FlowStartHandlers{
		Client: FlowStartClientHandlers{
			ExtendedQuery: func(msg ClientExtendedQuery) Flow {
				if parse, ok := msg.(ClientExtendedQueryParse); ok {
					var onClose func(*ExtendedQueryFlow)
					if r.OnStart != nil {
						onClose = r.OnStart(parse)
					}
					return NewExtendedQueryFlow(parse, onClose)
				}
				return nil
			},
		},
	}
}

// CopyRecognizer detects copy flows starting from server CopyInResponse/CopyOutResponse.
// It needs to track the most recent SQL to associate with the copy operation.
type CopyRecognizer struct {
	// GetLastSQL returns the SQL from the most recent Query message.
	// This is used to associate the COPY with its originating query.
	GetLastSQL func() string

	// OnStart is called when a copy flow starts. The returned function
	// becomes the flow's OnClose callback.
	OnStart func(flowType FlowType, sql string) func(*CopyFlow)
}

func (r *CopyRecognizer) StartHandlers(state *ProtocolState) FlowStartHandlers {
	return FlowStartHandlers{
		Server: FlowStartServerHandlers{
			Copy: func(msg ServerCopy) Flow {
				var flowType FlowType
				switch msg.(type) {
				case ServerCopyCopyInResponse:
					flowType = FlowTypeCopyIn
				case ServerCopyCopyOutResponse:
					flowType = FlowTypeCopyOut
				default:
					return nil
				}

				sql := ""
				if r.GetLastSQL != nil {
					sql = r.GetLastSQL()
				}

				var onClose func(*CopyFlow)
				if r.OnStart != nil {
					onClose = r.OnStart(flowType, sql)
				}

				if flowType == FlowTypeCopyIn {
					return NewCopyInFlow(sql, onClose)
				}
				return NewCopyOutFlow(sql, onClose)
			},
		},
	}
}

package pgwire

import (
	"fmt"
	"io"
	"slices"
)

// Cursor provides zero-allocation iteration over messages in a RingBuffer.
// It implements RawMessageSource for the current message, enabling lazy parsing.
//
// Single-cursor usage:
//
//	cursor := NewClientCursor(ring)
//	for {
//	    if err := cursor.NextBatch(); err != nil {
//	        return err
//	    }
//	    for cursor.NextMsg() {
//	        msg, _ := cursor.AsClient()
//	        // Process msg...
//	    }
//	    cursor.WriteAll(dst)
//	}
//
// Multi-cursor usage (e.g., proxying between frontend and backend):
//
//	frontend := NewClientCursor(frontendRing)
//	backend := NewServerCursor(backendRing)
//	for {
//	    // Check both sides without blocking
//	    gotFrontend, errF := frontend.TryNextBatch()
//	    gotBackend, errB := backend.TryNextBatch()
//	    if errF != nil { return errF }
//	    if errB != nil { return errB }
//
//	    // Process server responses first, then client requests
//	    if gotBackend {
//	        for backend.NextMsg() { /* forward to client */ }
//	    }
//	    if gotFrontend {
//	        for frontend.NextMsg() { /* forward to server */ }
//	    }
//
//	    // If nothing available, wait for either side
//	    if !gotFrontend && !gotBackend {
//	        select {
//	        case <-backend.Ready():
//	        case <-frontend.Ready():
//	        case <-backend.Done():
//	            return backend.Err()
//	        case <-frontend.Done():
//	            return frontend.Err()
//	        }
//	    }
//	}
type Cursor struct {
	ring *RingBuffer

	// Batch state
	batchStart int64
	batchEnd   int64

	// Iteration state
	msgIdx   int64 // Current message index
	writePos int64 // Messages written so far in this batch

	// Flyweights - only one set allocated based on direction
	clientFlyweights *ClientFlyweights // nil for server cursor
	serverFlyweights *ServerFlyweights // nil for client cursor
}

// ClientFlyweights holds reusable message wrappers for client messages.
// One instance per message type, reused each iteration for zero allocation.
type ClientFlyweights struct {
	// Simple query
	query        ClientSimpleQueryQuery
	functionCall ClientSimpleQueryFunctionCall

	// Startup/Auth
	passwordMessage ClientStartupPasswordMessage

	// Extended query
	parse    ClientExtendedQueryParse
	bind     ClientExtendedQueryBind
	execute  ClientExtendedQueryExecute
	describe ClientExtendedQueryDescribe
	close    ClientExtendedQueryClose
	sync     ClientExtendedQuerySync
	flush    ClientExtendedQueryFlush

	// Copy
	copyData ClientCopyCopyData
	copyDone ClientCopyCopyDone
	copyFail ClientCopyCopyFail

	// Terminate
	terminate ClientTerminateConnTerminate
}

// ServerFlyweights holds reusable message wrappers for server messages.
type ServerFlyweights struct {
	// Response
	readyForQuery        ServerResponseReadyForQuery
	commandComplete      ServerResponseCommandComplete
	dataRow              ServerResponseDataRow
	emptyQueryResponse   ServerResponseEmptyQueryResponse
	errorResponse        ServerResponseErrorResponse
	functionCallResponse ServerResponseFunctionCallResponse

	// Extended query
	parseComplete        ServerExtendedQueryParseComplete
	bindComplete         ServerExtendedQueryBindComplete
	parameterDescription ServerExtendedQueryParameterDescription
	rowDescription       ServerExtendedQueryRowDescription
	noData               ServerExtendedQueryNoData
	portalSuspended      ServerExtendedQueryPortalSuspended
	closeComplete        ServerExtendedQueryCloseComplete

	// Copy
	copyInResponse   ServerCopyCopyInResponse
	copyOutResponse  ServerCopyCopyOutResponse
	copyBothResponse ServerCopyCopyBothResponse
	copyData         ServerCopyCopyData
	copyDone         ServerCopyCopyDone

	// Async
	noticeResponse       ServerAsyncNoticeResponse
	notificationResponse ServerAsyncNotificationResponse
	parameterStatus      ServerAsyncParameterStatus

	// Startup
	authenticationOk                ServerStartupAuthenticationOk
	authenticationCleartextPassword ServerStartupAuthenticationCleartextPassword
	authenticationMD5Password       ServerStartupAuthenticationMD5Password
	authenticationGSS               ServerStartupAuthenticationGSS
	authenticationGSSContinue       ServerStartupAuthenticationGSSContinue
	authenticationSASL              ServerStartupAuthenticationSASL
	authenticationSASLContinue      ServerStartupAuthenticationSASLContinue
	authenticationSASLFinal         ServerStartupAuthenticationSASLFinal
	backendKeyData                  ServerStartupBackendKeyData
}

// NewClientCursor creates a cursor for iterating client messages.
func NewClientCursor(ring *RingBuffer) *Cursor {
	return &Cursor{
		ring:             ring,
		clientFlyweights: &ClientFlyweights{},
	}
}

// NewServerCursor creates a cursor for iterating server messages.
func NewServerCursor(ring *RingBuffer) *Cursor {
	return &Cursor{
		ring:             ring,
		serverFlyweights: &ServerFlyweights{},
	}
}

// === RawMessageSource implementation ===

// MessageType returns the message type of the current message.
func (c *Cursor) MessageType() MsgType {
	return c.ring.MessageType(c.msgIdx)
}

// MessageBody returns the body bytes of the current message.
// This may allocate if the message wraps around the ring buffer.
func (c *Cursor) MessageBody() []byte {
	return c.ring.MessageBody(c.msgIdx)
}

// BodyLen returns the body length without copying.
func (c *Cursor) BodyLen() int {
	return int(c.ring.MessageLen(c.msgIdx) - 5)
}

// WriteTo writes the current message to dst.
// The ring buffer stores full wire bytes, so this is a direct write.
func (c *Cursor) WriteTo(w io.Writer) (int64, error) {
	return c.ring.WriteMessage(c.msgIdx, w)
}

// Retain returns an owned RawBody copy of the current message.
func (c *Cursor) Retain() RawMessageSource {
	return RawBody{
		Type: c.MessageType(),
		Body: slices.Clone(c.MessageBody()),
	}
}

// === Batch iteration ===

// NextBatch waits for new messages and advances the reader position.
// Returns error on EOF or context cancellation.
// For multi-cursor scenarios, use TryNextBatch with Ready/Done channels instead.
func (c *Cursor) NextBatch() error {
	// Release previous batch (if any)
	if c.batchEnd > 0 {
		c.ring.ReleaseThrough(c.batchEnd)
	}

	// Wait for new messages
	newEnd, err := c.ring.AvailableMessages(c.batchEnd)
	if err != nil {
		return err
	}

	c.batchStart = c.batchEnd
	c.batchEnd = newEnd
	c.msgIdx = c.batchStart - 1 // NextMsg will increment
	c.writePos = c.batchStart
	return nil
}

// TryNextBatch checks for new messages without blocking.
// Returns (true, nil) if messages are available for processing.
// Returns (false, nil) if no messages yet - use Ready() channel to wait.
// Returns (false, err) on EOF or error.
func (c *Cursor) TryNextBatch() (bool, error) {
	// Release previous batch (if any)
	if c.batchEnd > 0 {
		c.ring.ReleaseThrough(c.batchEnd)
	}

	// Check for error first
	if err := c.ring.Error(); err != nil {
		return false, err
	}

	// Non-blocking check for new messages
	newEnd := c.ring.PublishedMsgCount()
	if newEnd <= c.batchEnd {
		return false, nil
	}

	c.batchStart = c.batchEnd
	c.batchEnd = newEnd
	c.msgIdx = c.batchStart - 1 // NextMsg will increment
	c.writePos = c.batchStart
	return true, nil
}

// Ready returns a channel that signals when new messages may be available.
// Use in select with other cursors for multiplexing.
func (c *Cursor) Ready() <-chan struct{} {
	return c.ring.ReaderWake()
}

// Done returns a channel that's closed when the cursor is done (EOF or error).
func (c *Cursor) Done() <-chan struct{} {
	return c.ring.Done()
}

// Err returns the error that caused the cursor to close, or nil.
func (c *Cursor) Err() error {
	return c.ring.Error()
}

// NextMsg advances to the next message in the batch.
// Returns false when the batch is exhausted.
func (c *Cursor) NextMsg() bool {
	c.msgIdx++
	return c.msgIdx < c.batchEnd
}

// MsgIdx returns the current message index.
func (c *Cursor) MsgIdx() int64 {
	return c.msgIdx
}

// FirstMsgIdx returns the index of the first unwritten message in this batch.
func (c *Cursor) FirstMsgIdx() int64 {
	return c.writePos
}

// === Writing ===

// Write writes messages [fromMsg, toMsg) to dst.
// The ring buffer stores full wire bytes, so this writes directly without
// reconstructing headers.
func (c *Cursor) Write(fromMsg, toMsg int64, dst io.Writer) error {
	if fromMsg >= toMsg {
		return nil
	}

	_, err := c.ring.WriteBatch(fromMsg, toMsg, dst)
	if err != nil {
		return err
	}

	c.writePos = toMsg
	return nil
}

// WriteThrough writes all messages up to and including the current message.
func (c *Cursor) WriteThrough(dst io.Writer) error {
	return c.Write(c.writePos, c.msgIdx+1, dst)
}

// WriteAll writes all remaining messages in the batch.
func (c *Cursor) WriteAll(dst io.Writer) error {
	return c.Write(c.writePos, c.batchEnd, dst)
}

// SkipThrough marks messages up to and including current as written (without writing).
func (c *Cursor) SkipThrough() {
	c.writePos = c.msgIdx + 1
}

// === Typed message access (flyweight pattern) ===

// AsClient returns the current message as a ClientMessage using flyweights.
// The returned message is only valid until the next call to AsClient or NextMsg.
// Returns a pointer to a flyweight slot - zero allocation.
func (c *Cursor) AsClient() (ClientMessage, error) {
	if c.clientFlyweights == nil {
		panic("AsClient called on server cursor")
	}

	fw := c.clientFlyweights
	msgType := c.MessageType()

	switch msgType {
	// Simple query
	case MsgClientQuery:
		fw.query = ClientSimpleQueryQuery{source: c}
		return &fw.query, nil
	case MsgClientFunc:
		fw.functionCall = ClientSimpleQueryFunctionCall{source: c}
		return &fw.functionCall, nil

	// Extended query
	case MsgClientParse:
		fw.parse = ClientExtendedQueryParse{source: c}
		return &fw.parse, nil
	case MsgClientBind:
		fw.bind = ClientExtendedQueryBind{source: c}
		return &fw.bind, nil
	case MsgClientExecute:
		fw.execute = ClientExtendedQueryExecute{source: c}
		return &fw.execute, nil
	case MsgClientDescribe:
		fw.describe = ClientExtendedQueryDescribe{source: c}
		return &fw.describe, nil
	case MsgClientClose:
		fw.close = ClientExtendedQueryClose{source: c}
		return &fw.close, nil
	case MsgClientSync:
		fw.sync = ClientExtendedQuerySync{source: c}
		return &fw.sync, nil
	case MsgClientFlush:
		fw.flush = ClientExtendedQueryFlush{source: c}
		return &fw.flush, nil

	// Copy
	case MsgClientCopyData:
		fw.copyData = ClientCopyCopyData{source: c}
		return &fw.copyData, nil
	case MsgClientCopyDone:
		fw.copyDone = ClientCopyCopyDone{source: c}
		return &fw.copyDone, nil
	case MsgClientCopyFail:
		fw.copyFail = ClientCopyCopyFail{source: c}
		return &fw.copyFail, nil

	// Terminate
	case MsgClientTerminate:
		fw.terminate = ClientTerminateConnTerminate{source: c}
		return &fw.terminate, nil

	// Startup/Auth (p = password)
	case MsgClientPassword:
		fw.passwordMessage = ClientStartupPasswordMessage{source: c}
		return &fw.passwordMessage, nil

	default:
		return nil, fmt.Errorf("unknown client message type: %c (0x%02x)", msgType, msgType)
	}
}

// AsServer returns the current message as a ServerMessage using flyweights.
// The returned message is only valid until the next call to AsServer or NextMsg.
// Returns a pointer to a flyweight slot - zero allocation.
func (c *Cursor) AsServer() (ServerMessage, error) {
	if c.serverFlyweights == nil {
		panic("AsServer called on client cursor")
	}

	fw := c.serverFlyweights
	msgType := c.MessageType()

	switch msgType {
	// Response
	case MsgServerReadyForQuery:
		fw.readyForQuery = ServerResponseReadyForQuery{source: c}
		return &fw.readyForQuery, nil
	case MsgServerCommandComplete:
		fw.commandComplete = ServerResponseCommandComplete{source: c}
		return &fw.commandComplete, nil
	case MsgServerDataRow:
		fw.dataRow = ServerResponseDataRow{source: c}
		return &fw.dataRow, nil
	case MsgServerEmptyQueryResponse:
		fw.emptyQueryResponse = ServerResponseEmptyQueryResponse{source: c}
		return &fw.emptyQueryResponse, nil
	case MsgServerErrorResponse:
		fw.errorResponse = ServerResponseErrorResponse{source: c}
		return &fw.errorResponse, nil
	case MsgServerFuncCallResponse:
		fw.functionCallResponse = ServerResponseFunctionCallResponse{source: c}
		return &fw.functionCallResponse, nil

	// Extended query
	case MsgServerParseComplete:
		fw.parseComplete = ServerExtendedQueryParseComplete{source: c}
		return &fw.parseComplete, nil
	case MsgServerBindComplete:
		fw.bindComplete = ServerExtendedQueryBindComplete{source: c}
		return &fw.bindComplete, nil
	case MsgServerParameterDescription:
		fw.parameterDescription = ServerExtendedQueryParameterDescription{source: c}
		return &fw.parameterDescription, nil
	case MsgServerRowDescription:
		fw.rowDescription = ServerExtendedQueryRowDescription{source: c}
		return &fw.rowDescription, nil
	case MsgServerNoData:
		fw.noData = ServerExtendedQueryNoData{source: c}
		return &fw.noData, nil
	case MsgServerPortalSuspended:
		fw.portalSuspended = ServerExtendedQueryPortalSuspended{source: c}
		return &fw.portalSuspended, nil
	case MsgServerCloseComplete:
		fw.closeComplete = ServerExtendedQueryCloseComplete{source: c}
		return &fw.closeComplete, nil

	// Copy
	case MsgServerCopyInResponse:
		fw.copyInResponse = ServerCopyCopyInResponse{source: c}
		return &fw.copyInResponse, nil
	case MsgServerCopyOutResponse:
		fw.copyOutResponse = ServerCopyCopyOutResponse{source: c}
		return &fw.copyOutResponse, nil
	case MsgServerCopyBothResponse:
		fw.copyBothResponse = ServerCopyCopyBothResponse{source: c}
		return &fw.copyBothResponse, nil
	case MsgServerCopyData:
		fw.copyData = ServerCopyCopyData{source: c}
		return &fw.copyData, nil
	case MsgServerCopyDone:
		fw.copyDone = ServerCopyCopyDone{source: c}
		return &fw.copyDone, nil

	// Async
	case MsgServerNoticeResponse:
		fw.noticeResponse = ServerAsyncNoticeResponse{source: c}
		return &fw.noticeResponse, nil
	case MsgServerNotificationResponse:
		fw.notificationResponse = ServerAsyncNotificationResponse{source: c}
		return &fw.notificationResponse, nil
	case MsgServerParameterStatus:
		fw.parameterStatus = ServerAsyncParameterStatus{source: c}
		return &fw.parameterStatus, nil

	// Startup/Auth
	case MsgServerAuth:
		return c.asServerAuth()
	case MsgServerBackendKeyData:
		fw.backendKeyData = ServerStartupBackendKeyData{source: c}
		return &fw.backendKeyData, nil

	default:
		return nil, fmt.Errorf("unknown server message type: %c (0x%02x)", msgType, msgType)
	}
}

// asServerAuth handles the 'R' authentication message subtypes.
func (c *Cursor) asServerAuth() (ServerMessage, error) {
	fw := c.serverFlyweights
	body := c.MessageBody()

	if len(body) < 4 {
		return nil, fmt.Errorf("authentication message too short")
	}

	authType := uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3])

	switch authType {
	case 0:
		fw.authenticationOk = ServerStartupAuthenticationOk{source: c}
		return &fw.authenticationOk, nil
	case 3:
		fw.authenticationCleartextPassword = ServerStartupAuthenticationCleartextPassword{source: c}
		return &fw.authenticationCleartextPassword, nil
	case 5:
		fw.authenticationMD5Password = ServerStartupAuthenticationMD5Password{source: c}
		return &fw.authenticationMD5Password, nil
	case 7:
		fw.authenticationGSS = ServerStartupAuthenticationGSS{source: c}
		return &fw.authenticationGSS, nil
	case 8:
		fw.authenticationGSSContinue = ServerStartupAuthenticationGSSContinue{source: c}
		return &fw.authenticationGSSContinue, nil
	case 10:
		fw.authenticationSASL = ServerStartupAuthenticationSASL{source: c}
		return &fw.authenticationSASL, nil
	case 11:
		fw.authenticationSASLContinue = ServerStartupAuthenticationSASLContinue{source: c}
		return &fw.authenticationSASLContinue, nil
	case 12:
		fw.authenticationSASLFinal = ServerStartupAuthenticationSASLFinal{source: c}
		return &fw.authenticationSASLFinal, nil
	default:
		return nil, fmt.Errorf("unknown authentication type: %d", authType)
	}
}

// Compile-time check that Cursor implements RawMessageSource
var _ RawMessageSource = (*Cursor)(nil)

package pgwire

import "fmt"

// ClientFlyweights holds reusable message wrappers for client messages.
// One instance per message type, reused each iteration for zero allocation.
type ClientFlyweights struct {
	// Simple query
	query        ClientQuery
	functionCall ClientFunctionCall

	// Startup/Auth
	passwordMessage ClientPasswordMessage

	// Extended query
	parse    ClientParse
	bind     ClientBind
	execute  ClientExecute
	describe ClientDescribe
	close    ClientClose
	sync     ClientSync
	flush    ClientFlush

	// Copy
	copyData OldClientCopyData
	copyDone OldClientCopyDone
	copyFail ClientCopyFail

	// Terminate
	terminate ClientTerminate
}

// Parse returns the current message as a ClientMessage using flyweights.
// The returned message is only valid until the next call to Parse.
// Returns a pointer to a flyweight slot - zero allocation.
func (fw *ClientFlyweights) Parse(source RawMessageSource) (ClientMessage, error) {
	msgType := source.MessageType()

	switch msgType {
	// Simple query
	case MsgClientQuery:
		fw.query = ClientQuery{source: source}
		return &fw.query, nil
	case MsgClientFunc:
		fw.functionCall = ClientFunctionCall{source: source}
		return &fw.functionCall, nil

	// Extended query
	case MsgClientParse:
		fw.parse = ClientParse{source: source}
		return &fw.parse, nil
	case MsgClientBind:
		fw.bind = ClientBind{source: source}
		return &fw.bind, nil
	case MsgClientExecute:
		fw.execute = ClientExecute{source: source}
		return &fw.execute, nil
	case MsgClientDescribe:
		fw.describe = ClientDescribe{source: source}
		return &fw.describe, nil
	case MsgClientClose:
		fw.close = ClientClose{source: source}
		return &fw.close, nil
	case MsgClientSync:
		fw.sync = ClientSync{source: source}
		return &fw.sync, nil
	case MsgClientFlush:
		fw.flush = ClientFlush{source: source}
		return &fw.flush, nil

	// Copy
	case MsgClientCopyData:
		fw.copyData = OldClientCopyData{source: source}
		return &fw.copyData, nil
	case MsgClientCopyDone:
		fw.copyDone = OldClientCopyDone{source: source}
		return &fw.copyDone, nil
	case MsgClientCopyFail:
		fw.copyFail = ClientCopyFail{source: source}
		return &fw.copyFail, nil

	// Terminate
	case MsgClientTerminate:
		fw.terminate = ClientTerminate{source: source}
		return &fw.terminate, nil

	// Startup/Auth (p = password)
	case MsgClientPassword:
		fw.passwordMessage = ClientPasswordMessage{source: source}
		return &fw.passwordMessage, nil

	default:
		return nil, fmt.Errorf("unknown client message type: %c (0x%02x)", msgType, msgType)
	}
}

// ServerFlyweights holds reusable message wrappers for server messages.
type ServerFlyweights struct {
	// Response
	readyForQuery        ServerReadyForQuery
	commandComplete      ServerCommandComplete
	dataRow              ServerDataRow
	emptyQueryResponse   ServerEmptyQueryResponse
	errorResponse        ServerErrorResponse
	functionCallResponse ServerFunctionCallResponse

	// Extended query
	parseComplete        ServerParseComplete
	bindComplete         ServerBindComplete
	parameterDescription ServerParameterDescription
	rowDescription       ServerRowDescription
	noData               ServerNoData
	portalSuspended      ServerPortalSuspended
	closeComplete        ServerCloseComplete

	// Copy
	copyInResponse   ServerCopyInResponse
	copyOutResponse  ServerCopyOutResponse
	copyBothResponse ServerCopyBothResponse
	copyData         OldServerCopyData
	copyDone         OldServerCopyDone

	// Async
	noticeResponse       ServerNoticeResponse
	notificationResponse ServerNotificationResponse
	parameterStatus      ServerParameterStatus

	// Startup
	authenticationOk                ServerAuthenticationOk
	authenticationCleartextPassword ServerAuthenticationCleartextPassword
	authenticationMD5Password       ServerAuthenticationMD5Password
	authenticationGSS               ServerAuthenticationGSS
	authenticationGSSContinue       ServerAuthenticationGSSContinue
	authenticationSASL              ServerAuthenticationSASL
	authenticationSASLContinue      ServerAuthenticationSASLContinue
	authenticationSASLFinal         ServerAuthenticationSASLFinal
	backendKeyData                  ServerBackendKeyData
}

// Parse returns the current message as a ServerMessage using flyweights.
// The returned message is only valid until the next call to Parse.
// Returns a pointer to a flyweight slot - zero allocation.
func (fw *ServerFlyweights) Parse(source RawMessageSource) (ServerMessage, error) {
	msgType := source.MessageType()

	switch msgType {
	// Response
	case MsgServerReadyForQuery:
		fw.readyForQuery = ServerReadyForQuery{source: source}
		return &fw.readyForQuery, nil
	case MsgServerCommandComplete:
		fw.commandComplete = ServerCommandComplete{source: source}
		return &fw.commandComplete, nil
	case MsgServerDataRow:
		fw.dataRow = ServerDataRow{source: source}
		return &fw.dataRow, nil
	case MsgServerEmptyQueryResponse:
		fw.emptyQueryResponse = ServerEmptyQueryResponse{source: source}
		return &fw.emptyQueryResponse, nil
	case MsgServerErrorResponse:
		fw.errorResponse = ServerErrorResponse{source: source}
		return &fw.errorResponse, nil
	case MsgServerFuncCallResponse:
		fw.functionCallResponse = ServerFunctionCallResponse{source: source}
		return &fw.functionCallResponse, nil

	// Extended query
	case MsgServerParseComplete:
		fw.parseComplete = ServerParseComplete{source: source}
		return &fw.parseComplete, nil
	case MsgServerBindComplete:
		fw.bindComplete = ServerBindComplete{source: source}
		return &fw.bindComplete, nil
	case MsgServerParameterDescription:
		fw.parameterDescription = ServerParameterDescription{source: source}
		return &fw.parameterDescription, nil
	case MsgServerRowDescription:
		fw.rowDescription = ServerRowDescription{source: source}
		return &fw.rowDescription, nil
	case MsgServerNoData:
		fw.noData = ServerNoData{source: source}
		return &fw.noData, nil
	case MsgServerPortalSuspended:
		fw.portalSuspended = ServerPortalSuspended{source: source}
		return &fw.portalSuspended, nil
	case MsgServerCloseComplete:
		fw.closeComplete = ServerCloseComplete{source: source}
		return &fw.closeComplete, nil

	// Copy
	case MsgServerCopyInResponse:
		fw.copyInResponse = ServerCopyInResponse{source: source}
		return &fw.copyInResponse, nil
	case MsgServerCopyOutResponse:
		fw.copyOutResponse = ServerCopyOutResponse{source: source}
		return &fw.copyOutResponse, nil
	case MsgServerCopyBothResponse:
		fw.copyBothResponse = ServerCopyBothResponse{source: source}
		return &fw.copyBothResponse, nil
	case MsgServerCopyData:
		fw.copyData = OldServerCopyData{source: source}
		return &fw.copyData, nil
	case MsgServerCopyDone:
		fw.copyDone = OldServerCopyDone{source: source}
		return &fw.copyDone, nil

	// Async
	case MsgServerNoticeResponse:
		fw.noticeResponse = ServerNoticeResponse{source: source}
		return &fw.noticeResponse, nil
	case MsgServerNotificationResponse:
		fw.notificationResponse = ServerNotificationResponse{source: source}
		return &fw.notificationResponse, nil
	case MsgServerParameterStatus:
		fw.parameterStatus = ServerParameterStatus{source: source}
		return &fw.parameterStatus, nil

	// Startup/Auth
	case MsgServerAuth:
		return fw.parseAuth(source)
	case MsgServerBackendKeyData:
		fw.backendKeyData = ServerBackendKeyData{source: source}
		return &fw.backendKeyData, nil

	default:
		return nil, fmt.Errorf("unknown server message type: %c (0x%02x)", msgType, msgType)
	}
}

// parseAuth handles the 'R' authentication message subtypes.
func (fw *ServerFlyweights) parseAuth(source RawMessageSource) (ServerMessage, error) {
	body := source.Body()

	if len(body) < 4 {
		return nil, fmt.Errorf("authentication message too short")
	}

	authType := uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3])

	switch authType {
	case 0:
		fw.authenticationOk = ServerAuthenticationOk{source: source}
		return &fw.authenticationOk, nil
	case 3:
		fw.authenticationCleartextPassword = ServerAuthenticationCleartextPassword{source: source}
		return &fw.authenticationCleartextPassword, nil
	case 5:
		fw.authenticationMD5Password = ServerAuthenticationMD5Password{source: source}
		return &fw.authenticationMD5Password, nil
	case 7:
		fw.authenticationGSS = ServerAuthenticationGSS{source: source}
		return &fw.authenticationGSS, nil
	case 8:
		fw.authenticationGSSContinue = ServerAuthenticationGSSContinue{source: source}
		return &fw.authenticationGSSContinue, nil
	case 10:
		fw.authenticationSASL = ServerAuthenticationSASL{source: source}
		return &fw.authenticationSASL, nil
	case 11:
		fw.authenticationSASLContinue = ServerAuthenticationSASLContinue{source: source}
		return &fw.authenticationSASLContinue, nil
	case 12:
		fw.authenticationSASLFinal = ServerAuthenticationSASLFinal{source: source}
		return &fw.authenticationSASLFinal, nil
	default:
		return nil, fmt.Errorf("unknown authentication type: %d", authType)
	}
}

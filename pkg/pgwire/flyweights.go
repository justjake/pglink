package pgwire

import "fmt"

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

// Parse returns the current message as a ClientMessage using flyweights.
// The returned message is only valid until the next call to Parse.
// Returns a pointer to a flyweight slot - zero allocation.
func (fw *ClientFlyweights) Parse(source RawMessageSource) (ClientMessage, error) {
	msgType := source.MessageType()

	switch msgType {
	// Simple query
	case MsgClientQuery:
		fw.query = ClientSimpleQueryQuery{source: source}
		return &fw.query, nil
	case MsgClientFunc:
		fw.functionCall = ClientSimpleQueryFunctionCall{source: source}
		return &fw.functionCall, nil

	// Extended query
	case MsgClientParse:
		fw.parse = ClientExtendedQueryParse{source: source}
		return &fw.parse, nil
	case MsgClientBind:
		fw.bind = ClientExtendedQueryBind{source: source}
		return &fw.bind, nil
	case MsgClientExecute:
		fw.execute = ClientExtendedQueryExecute{source: source}
		return &fw.execute, nil
	case MsgClientDescribe:
		fw.describe = ClientExtendedQueryDescribe{source: source}
		return &fw.describe, nil
	case MsgClientClose:
		fw.close = ClientExtendedQueryClose{source: source}
		return &fw.close, nil
	case MsgClientSync:
		fw.sync = ClientExtendedQuerySync{source: source}
		return &fw.sync, nil
	case MsgClientFlush:
		fw.flush = ClientExtendedQueryFlush{source: source}
		return &fw.flush, nil

	// Copy
	case MsgClientCopyData:
		fw.copyData = ClientCopyCopyData{source: source}
		return &fw.copyData, nil
	case MsgClientCopyDone:
		fw.copyDone = ClientCopyCopyDone{source: source}
		return &fw.copyDone, nil
	case MsgClientCopyFail:
		fw.copyFail = ClientCopyCopyFail{source: source}
		return &fw.copyFail, nil

	// Terminate
	case MsgClientTerminate:
		fw.terminate = ClientTerminateConnTerminate{source: source}
		return &fw.terminate, nil

	// Startup/Auth (p = password)
	case MsgClientPassword:
		fw.passwordMessage = ClientStartupPasswordMessage{source: source}
		return &fw.passwordMessage, nil

	default:
		return nil, fmt.Errorf("unknown client message type: %c (0x%02x)", msgType, msgType)
	}
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

// Parse returns the current message as a ServerMessage using flyweights.
// The returned message is only valid until the next call to Parse.
// Returns a pointer to a flyweight slot - zero allocation.
func (fw *ServerFlyweights) Parse(source RawMessageSource) (ServerMessage, error) {
	msgType := source.MessageType()

	switch msgType {
	// Response
	case MsgServerReadyForQuery:
		fw.readyForQuery = ServerResponseReadyForQuery{source: source}
		return &fw.readyForQuery, nil
	case MsgServerCommandComplete:
		fw.commandComplete = ServerResponseCommandComplete{source: source}
		return &fw.commandComplete, nil
	case MsgServerDataRow:
		fw.dataRow = ServerResponseDataRow{source: source}
		return &fw.dataRow, nil
	case MsgServerEmptyQueryResponse:
		fw.emptyQueryResponse = ServerResponseEmptyQueryResponse{source: source}
		return &fw.emptyQueryResponse, nil
	case MsgServerErrorResponse:
		fw.errorResponse = ServerResponseErrorResponse{source: source}
		return &fw.errorResponse, nil
	case MsgServerFuncCallResponse:
		fw.functionCallResponse = ServerResponseFunctionCallResponse{source: source}
		return &fw.functionCallResponse, nil

	// Extended query
	case MsgServerParseComplete:
		fw.parseComplete = ServerExtendedQueryParseComplete{source: source}
		return &fw.parseComplete, nil
	case MsgServerBindComplete:
		fw.bindComplete = ServerExtendedQueryBindComplete{source: source}
		return &fw.bindComplete, nil
	case MsgServerParameterDescription:
		fw.parameterDescription = ServerExtendedQueryParameterDescription{source: source}
		return &fw.parameterDescription, nil
	case MsgServerRowDescription:
		fw.rowDescription = ServerExtendedQueryRowDescription{source: source}
		return &fw.rowDescription, nil
	case MsgServerNoData:
		fw.noData = ServerExtendedQueryNoData{source: source}
		return &fw.noData, nil
	case MsgServerPortalSuspended:
		fw.portalSuspended = ServerExtendedQueryPortalSuspended{source: source}
		return &fw.portalSuspended, nil
	case MsgServerCloseComplete:
		fw.closeComplete = ServerExtendedQueryCloseComplete{source: source}
		return &fw.closeComplete, nil

	// Copy
	case MsgServerCopyInResponse:
		fw.copyInResponse = ServerCopyCopyInResponse{source: source}
		return &fw.copyInResponse, nil
	case MsgServerCopyOutResponse:
		fw.copyOutResponse = ServerCopyCopyOutResponse{source: source}
		return &fw.copyOutResponse, nil
	case MsgServerCopyBothResponse:
		fw.copyBothResponse = ServerCopyCopyBothResponse{source: source}
		return &fw.copyBothResponse, nil
	case MsgServerCopyData:
		fw.copyData = ServerCopyCopyData{source: source}
		return &fw.copyData, nil
	case MsgServerCopyDone:
		fw.copyDone = ServerCopyCopyDone{source: source}
		return &fw.copyDone, nil

	// Async
	case MsgServerNoticeResponse:
		fw.noticeResponse = ServerAsyncNoticeResponse{source: source}
		return &fw.noticeResponse, nil
	case MsgServerNotificationResponse:
		fw.notificationResponse = ServerAsyncNotificationResponse{source: source}
		return &fw.notificationResponse, nil
	case MsgServerParameterStatus:
		fw.parameterStatus = ServerAsyncParameterStatus{source: source}
		return &fw.parameterStatus, nil

	// Startup/Auth
	case MsgServerAuth:
		return fw.parseAuth(source)
	case MsgServerBackendKeyData:
		fw.backendKeyData = ServerStartupBackendKeyData{source: source}
		return &fw.backendKeyData, nil

	default:
		return nil, fmt.Errorf("unknown server message type: %c (0x%02x)", msgType, msgType)
	}
}

// parseAuth handles the 'R' authentication message subtypes.
func (fw *ServerFlyweights) parseAuth(source RawMessageSource) (ServerMessage, error) {
	body := source.MessageBody()

	if len(body) < 4 {
		return nil, fmt.Errorf("authentication message too short")
	}

	authType := uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3])

	switch authType {
	case 0:
		fw.authenticationOk = ServerStartupAuthenticationOk{source: source}
		return &fw.authenticationOk, nil
	case 3:
		fw.authenticationCleartextPassword = ServerStartupAuthenticationCleartextPassword{source: source}
		return &fw.authenticationCleartextPassword, nil
	case 5:
		fw.authenticationMD5Password = ServerStartupAuthenticationMD5Password{source: source}
		return &fw.authenticationMD5Password, nil
	case 7:
		fw.authenticationGSS = ServerStartupAuthenticationGSS{source: source}
		return &fw.authenticationGSS, nil
	case 8:
		fw.authenticationGSSContinue = ServerStartupAuthenticationGSSContinue{source: source}
		return &fw.authenticationGSSContinue, nil
	case 10:
		fw.authenticationSASL = ServerStartupAuthenticationSASL{source: source}
		return &fw.authenticationSASL, nil
	case 11:
		fw.authenticationSASLContinue = ServerStartupAuthenticationSASLContinue{source: source}
		return &fw.authenticationSASLContinue, nil
	case 12:
		fw.authenticationSASLFinal = ServerStartupAuthenticationSASLFinal{source: source}
		return &fw.authenticationSASLFinal, nil
	default:
		return nil, fmt.Errorf("unknown authentication type: %d", authType)
	}
}

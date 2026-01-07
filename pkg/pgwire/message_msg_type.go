package pgwire

// MsgType() methods for all message wrapper types.
// These are written manually rather than generated because:
// 1. The pgproto3 types don't have a MessageType() method
// 2. Constructed messages (via ClientParsed/ServerParsed) have nil source
// 3. These mappings are static and will never change

// Client Startup messages - these don't have a standard message type byte
func (t *ClientGSSEncRequest) MsgType() MsgType       { return 0 }
func (t *ClientGSSResponse) MsgType() MsgType         { return MsgClientPassword } // 'p'
func (t *ClientPasswordMessage) MsgType() MsgType     { return MsgClientPassword } // 'p'
func (t *ClientSASLInitialResponse) MsgType() MsgType { return MsgClientPassword } // 'p'
func (t *ClientSASLResponse) MsgType() MsgType        { return MsgClientPassword } // 'p'
func (t *ClientSSLRequest) MsgType() MsgType          { return 0 }
func (t *ClientStartupMessage) MsgType() MsgType      { return 0 }

// Client SimpleQuery messages
func (t *ClientQuery) MsgType() MsgType        { return MsgClientQuery } // 'Q'
func (t *ClientFunctionCall) MsgType() MsgType { return MsgClientFunc }  // 'F'

// Client ExtendedQuery messages
func (t *ClientParse) MsgType() MsgType    { return MsgClientParse }    // 'P'
func (t *ClientBind) MsgType() MsgType     { return MsgClientBind }     // 'B'
func (t *ClientExecute) MsgType() MsgType  { return MsgClientExecute }  // 'E'
func (t *ClientSync) MsgType() MsgType     { return MsgClientSync }     // 'S'
func (t *ClientDescribe) MsgType() MsgType { return MsgClientDescribe } // 'D'
func (t *ClientClose) MsgType() MsgType    { return MsgClientClose }    // 'C'
func (t *ClientFlush) MsgType() MsgType    { return MsgClientFlush }    // 'H'

// Client Copy messages
func (t *OldClientCopyData) MsgType() MsgType { return MsgClientCopyData } // 'd'
func (t *OldClientCopyDone) MsgType() MsgType { return MsgClientCopyDone } // 'c'
func (t *ClientCopyFail) MsgType() MsgType    { return MsgClientCopyFail } // 'f'

// Client Cancel - special startup-like message
func (t *ClientCancelRequest) MsgType() MsgType { return 0 }

// Client Terminate
func (t *ClientTerminate) MsgType() MsgType { return MsgClientTerminate } // 'X'

// Server Startup/Auth messages - all use 'R'
func (t *ServerAuthenticationCleartextPassword) MsgType() MsgType { return MsgServerAuth }           // 'R'
func (t *ServerAuthenticationGSS) MsgType() MsgType               { return MsgServerAuth }           // 'R'
func (t *ServerAuthenticationGSSContinue) MsgType() MsgType       { return MsgServerAuth }           // 'R'
func (t *ServerAuthenticationMD5Password) MsgType() MsgType       { return MsgServerAuth }           // 'R'
func (t *ServerAuthenticationOk) MsgType() MsgType                { return MsgServerAuth }           // 'R'
func (t *ServerAuthenticationSASL) MsgType() MsgType              { return MsgServerAuth }           // 'R'
func (t *ServerAuthenticationSASLContinue) MsgType() MsgType      { return MsgServerAuth }           // 'R'
func (t *ServerAuthenticationSASLFinal) MsgType() MsgType         { return MsgServerAuth }           // 'R'
func (t *ServerBackendKeyData) MsgType() MsgType                  { return MsgServerBackendKeyData } // 'K'

// Server ExtendedQuery messages
func (t *ServerParseComplete) MsgType() MsgType        { return MsgServerParseComplete }        // '1'
func (t *ServerBindComplete) MsgType() MsgType         { return MsgServerBindComplete }         // '2'
func (t *ServerParameterDescription) MsgType() MsgType { return MsgServerParameterDescription } // 't'
func (t *ServerRowDescription) MsgType() MsgType       { return MsgServerRowDescription }       // 'T'
func (t *ServerNoData) MsgType() MsgType               { return MsgServerNoData }               // 'n'
func (t *ServerPortalSuspended) MsgType() MsgType      { return MsgServerPortalSuspended }      // 's'
func (t *ServerCloseComplete) MsgType() MsgType        { return MsgServerCloseComplete }        // '3'

// Server Copy messages
func (t *ServerCopyInResponse) MsgType() MsgType   { return MsgServerCopyInResponse }   // 'G'
func (t *ServerCopyOutResponse) MsgType() MsgType  { return MsgServerCopyOutResponse }  // 'H'
func (t *ServerCopyBothResponse) MsgType() MsgType { return MsgServerCopyBothResponse } // 'W'
func (t *OldServerCopyData) MsgType() MsgType      { return MsgServerCopyData }         // 'd'
func (t *OldServerCopyDone) MsgType() MsgType      { return MsgServerCopyDone }         // 'c'

// Server Response messages
func (t *ServerReadyForQuery) MsgType() MsgType        { return MsgServerReadyForQuery }      // 'Z'
func (t *ServerCommandComplete) MsgType() MsgType      { return MsgServerCommandComplete }    // 'C'
func (t *ServerDataRow) MsgType() MsgType              { return MsgServerDataRow }            // 'D'
func (t *ServerEmptyQueryResponse) MsgType() MsgType   { return MsgServerEmptyQueryResponse } // 'I'
func (t *ServerErrorResponse) MsgType() MsgType        { return MsgServerErrorResponse }      // 'E'
func (t *ServerFunctionCallResponse) MsgType() MsgType { return MsgServerFuncCallResponse }   // 'V'

// Server Async messages
func (t *ServerNoticeResponse) MsgType() MsgType       { return MsgServerNoticeResponse }       // 'N'
func (t *ServerNotificationResponse) MsgType() MsgType { return MsgServerNotificationResponse } // 'A'
func (t *ServerParameterStatus) MsgType() MsgType      { return MsgServerParameterStatus }      // 'S'

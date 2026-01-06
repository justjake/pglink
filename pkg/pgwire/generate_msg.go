//go:build ignore

package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/justjake/pglink/pkg/pgwire"
)

func main() {
	dir := "."
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}

	// Parse source files
	fset := token.NewFileSet()

	// Parse raw.go for RawMessageSource interface (need method signatures for delegation)
	rawFile, err := parser.ParseFile(fset, filepath.Join(dir, "raw.go"), nil, parser.ParseComments)
	if err != nil {
		log.Fatalf("failed to parse raw.go: %v", err)
	}

	methods := extractInterfaceMethods(fset, rawFile, "RawMessageSource")
	if len(methods) == 0 {
		log.Fatal("no methods found in RawMessageSource interface")
	}

	// Parse msg_type.go for const names (we need to map type bytes to const names like "MsgClientBind")
	msgTypeFile, err := parser.ParseFile(fset, filepath.Join(dir, "msg_type.go"), nil, parser.ParseComments)
	if err != nil {
		log.Fatalf("failed to parse msg_type.go: %v", err)
	}

	// Extract const name mappings: type byte -> const name
	clientConsts := extractMsgConsts(fset, msgTypeFile, "MsgClient")
	serverConsts := extractMsgConsts(fset, msgTypeFile, "MsgServer")
	startupConsts := extractStartupConsts(fset, msgTypeFile)

	// Build message entries from pgwire lookups + const names
	clientMsgs := buildMsgEntries(&pgwire.ClientMsgName, clientConsts, startupConsts)
	serverMsgs := buildMsgEntries(&pgwire.ServerMsgName, serverConsts, startupConsts)

	if len(clientMsgs) == 0 {
		log.Fatal("no entries found in ClientMsgName")
	}
	if len(serverMsgs) == 0 {
		log.Fatal("no entries found in ServerMsgName")
	}

	// Handle duplicate names (e.g., CopyData, CopyDone appear in both client and server)
	// Bidirectional messages will implement both ClientMsg and ServerMsg
	clientOnly, serverOnly, bidirectional := resolveDuplicateNames(clientMsgs, serverMsgs)

	// Build metadata for doc comments using pgwire lookups directly
	meta := buildMsgMetadata(clientOnly, serverOnly, bidirectional)

	// Generate the code
	output := generateCode(methods, clientOnly, serverOnly, bidirectional, meta, pgwire.MsgParsePriority)

	// Write output
	outPath := filepath.Join(dir, "typed_msg_generated.go")
	if err := os.WriteFile(outPath, output, 0644); err != nil {
		log.Fatalf("failed to write %s: %v", outPath, err)
	}

	fmt.Printf("Generated %s with %d client, %d server, %d bidirectional types\n",
		outPath, len(clientOnly), len(serverOnly), len(bidirectional))
}

// methodInfo holds information about an interface method
type methodInfo struct {
	name    string
	params  string // e.g., "buf []byte"
	returns string // e.g., "([]byte, error)"
}

// msgEntry holds information about a message type entry
type msgEntry struct {
	msgType   pgwire.MsgType // e.g., pgwire.MsgClientBind
	name      string         // e.g., "Bind"
	constName string         // e.g., "MsgClientBind"
}

// msgMetadata holds doc comment info for a message type
type msgMetadata struct {
	isRequest  bool             // has entries in MsgResponse
	isResponse bool             // appears as a response in MsgResponse
	responses  []pgwire.MsgType // response message types (if request)
	respondsTo []pgwire.MsgType // request types this responds to
	// Note: isStartup is computed in generateTypedMsg, not stored here,
	// because ambiguous types like 'S' need sender-specific handling.
}

func extractInterfaceMethods(fset *token.FileSet, file *ast.File, interfaceName string) []methodInfo {
	var methods []methodInfo

	ast.Inspect(file, func(n ast.Node) bool {
		typeSpec, ok := n.(*ast.TypeSpec)
		if !ok || typeSpec.Name.Name != interfaceName {
			return true
		}

		iface, ok := typeSpec.Type.(*ast.InterfaceType)
		if !ok {
			return true
		}

		for _, method := range iface.Methods.List {
			if len(method.Names) == 0 {
				continue // embedded interface
			}

			funcType, ok := method.Type.(*ast.FuncType)
			if !ok {
				continue
			}

			mi := methodInfo{
				name: method.Names[0].Name,
			}

			// Extract params
			if funcType.Params != nil && len(funcType.Params.List) > 0 {
				var params []string
				for _, p := range funcType.Params.List {
					var buf bytes.Buffer
					printer.Fprint(&buf, fset, p.Type)
					typeStr := buf.String()
					for _, name := range p.Names {
						params = append(params, name.Name+" "+typeStr)
					}
					if len(p.Names) == 0 {
						params = append(params, typeStr)
					}
				}
				mi.params = strings.Join(params, ", ")
			}

			// Extract returns
			if funcType.Results != nil && len(funcType.Results.List) > 0 {
				var returns []string
				for _, r := range funcType.Results.List {
					var buf bytes.Buffer
					printer.Fprint(&buf, fset, r.Type)
					returns = append(returns, buf.String())
				}
				if len(returns) == 1 {
					mi.returns = returns[0]
				} else {
					mi.returns = "(" + strings.Join(returns, ", ") + ")"
				}
			}

			methods = append(methods, mi)
		}

		return false
	})

	return methods
}

// extractMsgConsts extracts const declarations like MsgClientBind, MsgServerAuth
// Returns a map from type byte (e.g., "'B'") to const name (e.g., "MsgClientBind")
func extractMsgConsts(fset *token.FileSet, file *ast.File, prefix string) map[string]string {
	consts := make(map[string]string)

	ast.Inspect(file, func(n ast.Node) bool {
		genDecl, ok := n.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.CONST {
			return true
		}

		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok || len(valueSpec.Names) == 0 {
				continue
			}

			name := valueSpec.Names[0].Name
			if !strings.HasPrefix(name, prefix) {
				continue
			}

			if len(valueSpec.Values) == 0 {
				continue
			}

			// Get the value (type byte)
			var valBuf bytes.Buffer
			printer.Fprint(&valBuf, fset, valueSpec.Values[0])
			valStr := valBuf.String()

			consts[valStr] = name
		}

		return true
	})

	return consts
}

// extractStartupConsts extracts startup message consts (MsgStartup, MsgSSLRequest, etc.)
func extractStartupConsts(fset *token.FileSet, file *ast.File) map[string]string {
	consts := make(map[string]string)

	ast.Inspect(file, func(n ast.Node) bool {
		genDecl, ok := n.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.CONST {
			return true
		}

		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok || len(valueSpec.Names) == 0 {
				continue
			}

			name := valueSpec.Names[0].Name
			// Look for MsgStartup, MsgSSLRequest, MsgCancelRequest, MsgGSSENCRequest
			if !strings.HasPrefix(name, "Msg") || strings.HasPrefix(name, "MsgClient") || strings.HasPrefix(name, "MsgServer") {
				continue
			}

			if len(valueSpec.Values) == 0 {
				continue
			}

			// Get the value (type byte like 0x00, 0x01, etc.)
			var valBuf bytes.Buffer
			printer.Fprint(&valBuf, fset, valueSpec.Values[0])
			valStr := valBuf.String()

			consts[valStr] = name
		}

		return true
	})

	return consts
}

// buildMsgEntries builds msgEntry list from a pgwire.MsgLookup and const name maps.
// senderConsts maps type byte (as printed string like "'B'") to const name.
func buildMsgEntries(lookup *pgwire.MsgLookup[string], senderConsts, startupConsts map[string]string) []msgEntry {
	var entries []msgEntry

	for i := 0; i < 256; i++ {
		msgType := pgwire.MsgType(i)
		name := lookup.Get(msgType)
		if name == "" {
			continue
		}

		// Find const name - try formatting the byte different ways
		var constName string
		if msgType < 32 {
			// Startup messages use hex format: 0x00, 0x01, etc.
			key := fmt.Sprintf("0x%02x", byte(msgType))
			constName = startupConsts[key]
		} else {
			// Regular messages use char format: 'B', 'Q', etc.
			key := fmt.Sprintf("'%c'", byte(msgType))
			constName = senderConsts[key]
			if constName == "" {
				constName = startupConsts[key]
			}
		}

		if constName == "" {
			log.Printf("warning: no const found for MsgType %d (%q) with name %q", msgType, string([]byte{byte(msgType)}), name)
			continue
		}

		entries = append(entries, msgEntry{
			msgType:   msgType,
			name:      name,
			constName: constName,
		})
	}

	// Sort by name for consistent output
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].name < entries[j].name
	})

	return entries
}

// resolveDuplicateNames handles message names that appear in both client and server.
// Returns: client-only msgs, server-only msgs, bidirectional msgs (appear in both).
// Bidirectional messages will implement both ClientMsg and ServerMsg.
func resolveDuplicateNames(clientMsgs, serverMsgs []msgEntry) (clientOnly, serverOnly, bidirectional []msgEntry) {
	// Build maps by name
	clientByName := make(map[string]msgEntry)
	for _, e := range clientMsgs {
		clientByName[e.name] = e
	}

	// Find duplicates
	duplicateNames := make(map[string]bool)
	for _, e := range serverMsgs {
		if _, ok := clientByName[e.name]; ok {
			duplicateNames[e.name] = true
		}
	}

	// Separate into lists
	for _, e := range clientMsgs {
		if duplicateNames[e.name] {
			bidirectional = append(bidirectional, e)
		} else {
			clientOnly = append(clientOnly, e)
		}
	}
	for _, e := range serverMsgs {
		if !duplicateNames[e.name] {
			serverOnly = append(serverOnly, e)
		}
	}

	return clientOnly, serverOnly, bidirectional
}

// buildMsgMetadata builds documentation metadata for all messages using pgwire lookups directly.
// The map is keyed by message NAME (not type) because ambiguous types like 'S' have different
// meanings for client (Sync) vs server (ParameterStatus).
func buildMsgMetadata(clientOnly, serverOnly, bidirectional []msgEntry) map[string]*msgMetadata {
	meta := make(map[string]*msgMetadata)

	// Build lookup from msgType to entry name, separated by sender.
	// Client types are requests, server types are responses.
	clientByType := make(map[pgwire.MsgType]string)
	serverByType := make(map[pgwire.MsgType]string)

	for _, e := range clientOnly {
		meta[e.name] = &msgMetadata{}
		clientByType[e.msgType] = e.name
	}
	for _, e := range serverOnly {
		meta[e.name] = &msgMetadata{}
		serverByType[e.msgType] = e.name
	}
	for _, e := range bidirectional {
		meta[e.name] = &msgMetadata{}
		clientByType[e.msgType] = e.name
		serverByType[e.msgType] = e.name
	}

	// Helper to check if slice contains a type
	containsType := func(slice []pgwire.MsgType, t pgwire.MsgType) bool {
		for _, v := range slice {
			if v == t {
				return true
			}
		}
		return false
	}

	// Process request/response relationships.
	// Requests are CLIENT messages in MsgResponse/MsgTerminalResponse.
	// Responses are SERVER messages listed as values in those maps.
	processResponses := func(reqType pgwire.MsgType, respTypes pgwire.MsgTypeSet) {
		// Only client messages can be requests
		reqName, isClientReq := clientByType[reqType]
		if isClientReq {
			m := meta[reqName]
			m.isRequest = true
			for _, respType := range respTypes {
				if !containsType(m.responses, respType) {
					m.responses = append(m.responses, respType)
				}
			}
		}
		// Only server messages can be responses
		for _, respType := range respTypes {
			respName, isServerResp := serverByType[respType]
			if isServerResp {
				m := meta[respName]
				m.isResponse = true
				if !containsType(m.respondsTo, reqType) {
					m.respondsTo = append(m.respondsTo, reqType)
				}
			}
		}
	}

	// Mark requests from MsgTerminalResponse (has all request types with their terminal responses)
	for i := 0; i < 256; i++ {
		msgType := pgwire.MsgType(i)
		responses := pgwire.MsgTerminalResponse.Get(msgType)
		if len(responses) > 0 {
			processResponses(msgType, responses)
		}
	}

	// Mark requests from MsgResponse (may have additional non-terminal responses)
	for i := 0; i < 256; i++ {
		msgType := pgwire.MsgType(i)
		responses := pgwire.MsgResponse.Get(msgType)
		if len(responses) > 0 {
			processResponses(msgType, responses)
		}
	}

	// Note: isStartup is computed in generateTypedMsg based on sender context,
	// not stored in metadata, because ambiguous types like 'S' need sender-specific handling.

	return meta
}

// msgTypeToTypeName maps a MsgType to its generated type name.
func msgTypeToTypeName(msgType pgwire.MsgType, clientOnly, serverOnly, bidirectional []msgEntry) string {
	for _, e := range clientOnly {
		if e.msgType == msgType {
			return e.name
		}
	}
	for _, e := range serverOnly {
		if e.msgType == msgType {
			return e.name
		}
	}
	for _, e := range bidirectional {
		if e.msgType == msgType {
			return e.name
		}
	}
	// Fallback
	return pgwire.MsgName.Get(msgType)
}

func generateCode(methods []methodInfo, clientMsgs, serverMsgs, bidirectional []msgEntry, meta map[string]*msgMetadata, priority []pgwire.MsgType) []byte {
	var buf bytes.Buffer

	// Header
	buf.WriteString("// Code generated by generate_msg.go; DO NOT EDIT.\n")
	buf.WriteString("//go:generate go run generate_msg.go\n\n")
	buf.WriteString("package pgwire\n\n")

	// Imports
	buf.WriteString("import (\n\t\"fmt\"\n\t\"io\"\n)\n\n")

	// Generate client message types
	for _, entry := range clientMsgs {
		generateTypedMsg(&buf, entry, methods, "client", meta, clientMsgs, serverMsgs, bidirectional)
	}

	// Generate server message types
	for _, entry := range serverMsgs {
		generateTypedMsg(&buf, entry, methods, "server", meta, clientMsgs, serverMsgs, bidirectional)
	}

	// Generate bidirectional message types
	for _, entry := range bidirectional {
		generateTypedMsg(&buf, entry, methods, "both", meta, clientMsgs, serverMsgs, bidirectional)
	}

	// Generate UnknownMsg
	generateUnknownMsg(&buf, methods)

	// Generate Typed function - returns concrete types
	generateTypedFunc(&buf, clientMsgs, serverMsgs, bidirectional, priority)

	// Format the code
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		log.Printf("warning: generated code has formatting issues: %v", err)
		log.Printf("unformatted output:\n%s", buf.String())
		return buf.Bytes()
	}
	return formatted
}

// generateTypedMsg generates a typed message wrapper.
// sender is "client", "server", or "both" for bidirectional messages.
func generateTypedMsg(buf *bytes.Buffer, entry msgEntry, methods []methodInfo, sender string, meta map[string]*msgMetadata, clientMsgs, serverMsgs, bidirectional []msgEntry) {
	typeName := entry.name
	constName := entry.constName
	m := meta[entry.name]

	// Compute isStartup based on sender context.
	// MsgIsStartup entries are sender-specific:
	// - Client startup: synthetic types < 0x20 (StartupMessage, SSLRequest, etc.) and 'p' (PasswordMessage)
	// - Server startup: 'R', 'K', 'S', 'Z', 'E', 'N'
	// We must not mark client Sync ('S') as startup just because server ParameterStatus ('S') is.
	isStartup := false
	if pgwire.MsgIsStartup.Get(entry.msgType) {
		isClientStartup := entry.msgType < 0x20 || entry.msgType == 'p'
		isServerStartup := entry.msgType >= 0x20 && entry.msgType != 'p'
		if (sender == "client" && isClientStartup) || (sender == "server" && isServerStartup) || sender == "both" {
			isStartup = true
		}
	}

	// Doc comment
	var senderDesc string
	switch sender {
	case "client":
		senderDesc = "client"
	case "server":
		senderDesc = "server"
	case "both":
		senderDesc = "client or server"
	}

	var kindDesc string
	if m.isRequest {
		kindDesc = " request"
	} else if m.isResponse {
		kindDesc = " response"
	} else if isStartup {
		kindDesc = " startup"
	}

	fmt.Fprintf(buf, "// %s is a %s%s message.\n", typeName, senderDesc, kindDesc)

	// List responses if request
	if m.isRequest && len(m.responses) > 0 {
		var respNames []string
		for _, r := range m.responses {
			name := msgTypeToTypeName(r, clientMsgs, serverMsgs, bidirectional)
			respNames = append(respNames, "["+name+"]")
		}
		fmt.Fprintf(buf, "// Responses: %s.\n", strings.Join(respNames, ", "))
	}

	// List what it responds to if response
	if m.isResponse && len(m.respondsTo) > 0 {
		var reqNames []string
		seen := make(map[string]bool)
		for _, r := range m.respondsTo {
			name := msgTypeToTypeName(r, clientMsgs, serverMsgs, bidirectional)
			if !seen[name] {
				reqNames = append(reqNames, "["+name+"]")
				seen[name] = true
			}
		}
		fmt.Fprintf(buf, "// Response to %s.\n", strings.Join(reqNames, ", "))
	}

	// Startup note
	if isStartup {
		fmt.Fprintf(buf, "// Startup messages only appear before the first [ReadyForQuery] response.\n")
	}

	// Type definition
	fmt.Fprintf(buf, "type %s Msg\n\n", typeName)

	// 0. Own methods (String, ExpectedType, CopyTyped, Validate last since multi-line)
	fmt.Fprintf(buf, "func (m %s) String() string { return fmt.Sprintf(\"%s(%%v)\", Msg(m)) }\n", typeName, typeName)
	fmt.Fprintf(buf, "func (m %s) ExpectedType() MsgType { return %s }\n", typeName, constName)
	fmt.Fprintf(buf, "func (m %s) CopyTyped() %s { return %s(m.Msg().Copy()) }\n", typeName, typeName, typeName)
	fmt.Fprintf(buf, "func (m %s) Validate() error {\n", typeName)
	buf.WriteString("\tif err := Msg(m).Validate(); err != nil {\n")
	buf.WriteString("\t\treturn err\n")
	buf.WriteString("\t}\n")
	buf.WriteString("\tif m.MessageType() != m.ExpectedType() {\n")
	buf.WriteString("\t\treturn ErrMsgGoTypeMismatch\n")
	buf.WriteString("\t}\n")
	// For client-only and server-only types, check sender
	if sender == "client" || sender == "server" {
		buf.WriteString("\tif m.From() != m.ExpectedFrom() {\n")
		buf.WriteString("\t\treturn ErrMsgSenderMismatch\n")
		buf.WriteString("\t}\n")
	}
	buf.WriteString("\treturn nil\n")
	buf.WriteString("}\n\n")

	// 1. ClientMsg/ServerMsg methods
	switch sender {
	case "client":
		fmt.Fprintf(buf, "func (m %s) ExpectedFrom() Sender { return SenderClient }\n", typeName)
		fmt.Fprintf(buf, "func (m %s) ExpectedClientType() MsgType { return m.ExpectedType() }\n", typeName)
		fmt.Fprintf(buf, "func (m %s) CopyClient() ClientMsg { return m.CopyTyped() }\n\n", typeName)
	case "server":
		fmt.Fprintf(buf, "func (m %s) ExpectedFrom() Sender { return SenderServer }\n", typeName)
		fmt.Fprintf(buf, "func (m %s) ExpectedServerType() MsgType { return m.ExpectedType() }\n", typeName)
		fmt.Fprintf(buf, "func (m %s) CopyServer() ServerMsg { return m.CopyTyped() }\n\n", typeName)
	case "both":
		// Bidirectional: ExpectedFrom returns actual sender (accepts either)
		fmt.Fprintf(buf, "func (m %s) ExpectedFrom() Sender { return m.From() }\n", typeName)
		fmt.Fprintf(buf, "func (m %s) ExpectedClientType() MsgType { return m.ExpectedType() }\n", typeName)
		fmt.Fprintf(buf, "func (m %s) ExpectedServerType() MsgType { return m.ExpectedType() }\n", typeName)
		fmt.Fprintf(buf, "func (m %s) CopyClient() ClientMsg { return m.CopyTyped() }\n", typeName)
		fmt.Fprintf(buf, "func (m %s) CopyServer() ServerMsg { return m.CopyTyped() }\n\n", typeName)
	}

	// 2. TypedMsg methods
	fmt.Fprintf(buf, "func (m %s) From() Sender { return Msg(m).Sender }\n", typeName)
	fmt.Fprintf(buf, "func (m %s) Msg() Msg { return Msg(m) }\n", typeName)
	fmt.Fprintf(buf, "func (m %s) Copy() TypedMsg { return m.CopyTyped() }\n\n", typeName)

	// 3. RawMessageSource methods
	for _, method := range methods {
		generateDelegateMethod(buf, typeName, method)
	}
	buf.WriteString("\n")

	// Interface compliance checks
	switch sender {
	case "client":
		fmt.Fprintf(buf, "var _ ClientMsg = %s{}\n\n", typeName)
	case "server":
		fmt.Fprintf(buf, "var _ ServerMsg = %s{}\n\n", typeName)
	case "both":
		fmt.Fprintf(buf, "var _ ClientMsg = %s{}\n", typeName)
		fmt.Fprintf(buf, "var _ ServerMsg = %s{}\n\n", typeName)
	}
}

func generateDelegateMethod(buf *bytes.Buffer, typeName string, m methodInfo) {
	// Build parameter list for call (just names, not types)
	var callArgs []string
	if m.params != "" {
		parts := strings.Split(m.params, ", ")
		for _, p := range parts {
			nameParts := strings.Split(strings.TrimSpace(p), " ")
			if len(nameParts) > 0 {
				callArgs = append(callArgs, nameParts[0])
			}
		}
	}
	callArgsStr := strings.Join(callArgs, ", ")

	if m.params != "" {
		fmt.Fprintf(buf, "func (m %s) %s(%s) %s { return Msg(m).%s(%s) }\n",
			typeName, m.name, m.params, m.returns, m.name, callArgsStr)
	} else {
		if m.returns != "" {
			fmt.Fprintf(buf, "func (m %s) %s() %s { return Msg(m).%s() }\n",
				typeName, m.name, m.returns, m.name)
		} else {
			fmt.Fprintf(buf, "func (m %s) %s() { Msg(m).%s() }\n",
				typeName, m.name, m.name)
		}
	}
}

func generateUnknownMsg(buf *bytes.Buffer, methods []methodInfo) {
	buf.WriteString("// UnknownMsg wraps a message with an unrecognized type byte.\n")
	buf.WriteString("type UnknownMsg Msg\n\n")

	// 0. Own methods
	buf.WriteString("func (m UnknownMsg) String() string { return fmt.Sprintf(\"UnknownMsg(%v)\", Msg(m)) }\n")
	buf.WriteString("func (m UnknownMsg) ExpectedType() MsgType { return 0 }\n")
	buf.WriteString("func (m UnknownMsg) CopyTyped() UnknownMsg { return UnknownMsg(m.Msg().Copy()) }\n")
	buf.WriteString("func (m UnknownMsg) Validate() error {\n")
	buf.WriteString("\tif err := Msg(m).Validate(); err != nil {\n")
	buf.WriteString("\t\treturn err\n")
	buf.WriteString("\t}\n")
	buf.WriteString("\treturn ErrMsgGoTypeMismatch\n")
	buf.WriteString("}\n\n")

	// 1. TypedMsg methods
	buf.WriteString("func (m UnknownMsg) From() Sender { return Msg(m).Sender }\n")
	buf.WriteString("func (m UnknownMsg) Msg() Msg { return Msg(m) }\n")
	buf.WriteString("func (m UnknownMsg) Copy() TypedMsg { return m.CopyTyped() }\n\n")

	// 2. RawMessageSource methods
	for _, m := range methods {
		generateDelegateMethod(buf, "UnknownMsg", m)
	}
	buf.WriteString("\n")

	buf.WriteString("var _ TypedMsg = UnknownMsg{}\n\n")
}

func generateTypedFunc(buf *bytes.Buffer, clientMsgs, serverMsgs, bidirectional []msgEntry, priority []pgwire.MsgType) {
	// Build lookup maps by MsgType
	clientByType := make(map[pgwire.MsgType]msgEntry)
	for _, e := range clientMsgs {
		clientByType[e.msgType] = e
	}
	serverByType := make(map[pgwire.MsgType]msgEntry)
	for _, e := range serverMsgs {
		serverByType[e.msgType] = e
	}
	bidirByType := make(map[pgwire.MsgType]msgEntry)
	for _, e := range bidirectional {
		bidirByType[e.msgType] = e
	}

	// Collect all message types to process, in priority order
	var orderedTypes []pgwire.MsgType
	seenTypes := make(map[pgwire.MsgType]bool)

	// Add priority types first
	for _, mt := range priority {
		if seenTypes[mt] {
			continue
		}
		// Only add if it's a type we're generating
		_, inClient := clientByType[mt]
		_, inServer := serverByType[mt]
		_, inBidir := bidirByType[mt]
		if inClient || inServer || inBidir {
			orderedTypes = append(orderedTypes, mt)
			seenTypes[mt] = true
		}
	}

	// Add remaining client types
	for _, e := range clientMsgs {
		if !seenTypes[e.msgType] {
			orderedTypes = append(orderedTypes, e.msgType)
			seenTypes[e.msgType] = true
		}
	}

	// Add remaining server types
	for _, e := range serverMsgs {
		if !seenTypes[e.msgType] {
			orderedTypes = append(orderedTypes, e.msgType)
			seenTypes[e.msgType] = true
		}
	}

	// Add remaining bidirectional types
	for _, e := range bidirectional {
		if !seenTypes[e.msgType] {
			orderedTypes = append(orderedTypes, e.msgType)
			seenTypes[e.msgType] = true
		}
	}

	// Generate function
	buf.WriteString("// Typed converts a Msg to its specific TypedMsg wrapper based on Sender and MessageType.\n")
	buf.WriteString("// Returns the concrete type (e.g., Bind, Query, DataRow) based on the wire type byte.\n")
	buf.WriteString("// Returns UnknownMsg if the type byte is not recognized for the sender.\n")
	buf.WriteString("//\n")
	buf.WriteString("// The switch is ordered by MsgParsePriority for hot-path optimization:\n")
	buf.WriteString("// most frequent message types are checked first, and unambiguous types\n")
	buf.WriteString("// (unique to client or server) return immediately without checking Sender.\n")
	buf.WriteString("func Typed(m Msg) TypedMsg {\n")
	buf.WriteString("\tswitch m.MessageType() {\n")

	// Track which type bytes we've already handled
	handledTypes := make(map[pgwire.MsgType]bool)

	for _, mt := range orderedTypes {
		if handledTypes[mt] {
			continue
		}

		// Check if this type byte is ambiguous (different meaning for client/server)
		clientEntry, hasClient := clientByType[mt]
		serverEntry, hasServer := serverByType[mt]
		bidirEntry, hasBidir := bidirByType[mt]

		if hasBidir {
			// Bidirectional: same type for both, no sender check needed
			fmt.Fprintf(buf, "\tcase %s:\n", bidirEntry.constName)
			fmt.Fprintf(buf, "\t\treturn %s(m)\n", bidirEntry.name)
			handledTypes[mt] = true
		} else if hasClient && hasServer {
			// Ambiguous: need to check sender
			fmt.Fprintf(buf, "\tcase %s: // %s (client) / %s (server)\n", clientEntry.constName, clientEntry.name, serverEntry.name)
			buf.WriteString("\t\tif m.Sender == SenderClient {\n")
			fmt.Fprintf(buf, "\t\t\treturn %s(m)\n", clientEntry.name)
			buf.WriteString("\t\t}\n")
			fmt.Fprintf(buf, "\t\treturn %s(m)\n", serverEntry.name)
			handledTypes[mt] = true
		} else if hasClient {
			// Client only
			fmt.Fprintf(buf, "\tcase %s:\n", clientEntry.constName)
			fmt.Fprintf(buf, "\t\treturn %s(m)\n", clientEntry.name)
			handledTypes[mt] = true
		} else if hasServer {
			// Server only
			fmt.Fprintf(buf, "\tcase %s:\n", serverEntry.constName)
			fmt.Fprintf(buf, "\t\treturn %s(m)\n", serverEntry.name)
			handledTypes[mt] = true
		}
	}

	buf.WriteString("\t}\n")
	buf.WriteString("\treturn UnknownMsg(m)\n")
	buf.WriteString("}\n")
}

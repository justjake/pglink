package frontend

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/crypto/pbkdf2"
)

// SCRAMServer handles SCRAM-SHA-256 authentication for PostgreSQL clients.
// It specifically handles the PostgreSQL convention of omitting the username
// in the SCRAM messages (using n=,) since the username is already provided
// in the startup message.
type SCRAMServer struct {
	username       string
	password       string
	iterationCount int
	salt           []byte

	// State from the exchange
	clientFirstMsgBare string
	serverFirstMsg     string
	clientNonce        string
	serverNonce        string
}

// NewSCRAMServer creates a new SCRAM-SHA-256 server for the given credentials.
func NewSCRAMServer(username, password string, iterationCount int) (*SCRAMServer, error) {
	// Generate random salt
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	return &SCRAMServer{
		username:       username,
		password:       password,
		iterationCount: iterationCount,
		salt:           salt,
	}, nil
}

// ProcessClientFirstMessage processes the client-first-message and returns
// the server-first-message.
func (s *SCRAMServer) ProcessClientFirstMessage(clientFirstMsg string) (string, error) {
	// Parse the client-first-message
	// Format: gs2-header + client-first-message-bare
	// gs2-header: "n,," or "y,," or "p=..." for channel binding
	// client-first-message-bare: "n=username,r=nonce"

	// Find the end of GS2 header (after the second comma)
	parts := strings.SplitN(clientFirstMsg, ",", 3)
	if len(parts) < 3 {
		return "", errors.New("invalid client-first-message format")
	}

	// Store client-first-message-bare (everything after gs2-header)
	// This is EXACTLY what the client sent - we don't modify it
	s.clientFirstMsgBare = parts[2]

	// Parse the client-first-message-bare
	bareAttrs := parseAttributes(s.clientFirstMsgBare)

	// Extract client nonce
	clientNonce, ok := bareAttrs["r"]
	if !ok {
		return "", errors.New("missing client nonce in client-first-message")
	}
	s.clientNonce = clientNonce

	// Generate server nonce
	serverNonceBytes := make([]byte, 18)
	if _, err := rand.Read(serverNonceBytes); err != nil {
		return "", fmt.Errorf("failed to generate server nonce: %w", err)
	}
	s.serverNonce = base64.StdEncoding.EncodeToString(serverNonceBytes)

	// Build server-first-message
	// Format: "r=combined-nonce,s=base64-salt,i=iteration-count"
	combinedNonce := s.clientNonce + s.serverNonce
	saltB64 := base64.StdEncoding.EncodeToString(s.salt)
	s.serverFirstMsg = fmt.Sprintf("r=%s,s=%s,i=%d", combinedNonce, saltB64, s.iterationCount)

	return s.serverFirstMsg, nil
}

// ProcessClientFinalMessage processes the client-final-message and returns
// the server-final-message, or an error if authentication failed.
func (s *SCRAMServer) ProcessClientFinalMessage(clientFinalMsg string) (string, error) {
	// Parse client-final-message
	// Format: "c=base64-channel-binding,r=nonce,p=base64-proof"
	attrs := parseAttributes(clientFinalMsg)

	// Verify nonce matches
	receivedNonce, ok := attrs["r"]
	if !ok {
		return "", errors.New("missing nonce in client-final-message")
	}
	expectedNonce := s.clientNonce + s.serverNonce
	if receivedNonce != expectedNonce {
		return "", errors.New("nonce mismatch")
	}

	// Get client proof
	proofB64, ok := attrs["p"]
	if !ok {
		return "", errors.New("missing proof in client-final-message")
	}
	clientProof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil {
		return "", fmt.Errorf("invalid proof encoding: %w", err)
	}

	// Build client-final-message-without-proof for AuthMessage
	clientFinalWithoutProof := removeProof(clientFinalMsg)

	// Compute AuthMessage
	authMessage := s.clientFirstMsgBare + "," + s.serverFirstMsg + "," + clientFinalWithoutProof

	// Compute SaltedPassword
	saltedPassword := pbkdf2.Key([]byte(s.password), s.salt, s.iterationCount, 32, sha256.New)

	// Compute ClientKey = HMAC(SaltedPassword, "Client Key")
	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))

	// Compute StoredKey = SHA256(ClientKey)
	storedKeyHash := sha256.Sum256(clientKey)
	storedKey := storedKeyHash[:]

	// Compute ClientSignature = HMAC(StoredKey, AuthMessage)
	clientSignature := hmacSHA256(storedKey, []byte(authMessage))

	// Verify: ClientKey = ClientProof XOR ClientSignature
	if len(clientProof) != len(clientSignature) {
		return "", errors.New("proof length mismatch")
	}

	recoveredClientKey := make([]byte, len(clientProof))
	for i := range clientProof {
		recoveredClientKey[i] = clientProof[i] ^ clientSignature[i]
	}

	// Verify: StoredKey = SHA256(recovered ClientKey)
	recoveredStoredKeyHash := sha256.Sum256(recoveredClientKey)
	if !hmac.Equal(storedKey, recoveredStoredKeyHash[:]) {
		return "", errors.New("authentication failed")
	}

	// Compute ServerKey = HMAC(SaltedPassword, "Server Key")
	serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))

	// Compute ServerSignature = HMAC(ServerKey, AuthMessage)
	serverSignature := hmacSHA256(serverKey, []byte(authMessage))
	serverSignatureB64 := base64.StdEncoding.EncodeToString(serverSignature)

	// Build server-final-message
	serverFinalMsg := "v=" + serverSignatureB64

	return serverFinalMsg, nil
}

// parseAttributes parses a comma-separated list of key=value attributes.
func parseAttributes(msg string) map[string]string {
	attrs := make(map[string]string)
	for _, part := range strings.Split(msg, ",") {
		if len(part) >= 2 && part[1] == '=' {
			attrs[part[:1]] = part[2:]
		}
	}
	return attrs
}

// removeProof removes the proof attribute from a client-final-message.
func removeProof(msg string) string {
	// Find and remove ",p=..." from the message
	re := regexp.MustCompile(`,p=[^,]*$`)
	return re.ReplaceAllString(msg, "")
}

// hmacSHA256 computes HMAC-SHA256.
func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

// ParseChannelBindingFlag extracts the channel binding flag from a client-first-message.
// Returns the flag ('n', 'y', or 'p') and the channel binding type if applicable.
func ParseChannelBindingFlag(clientFirstMsg string) (flag byte, cbType string, err error) {
	if len(clientFirstMsg) == 0 {
		return 0, "", errors.New("empty client-first-message")
	}

	flag = clientFirstMsg[0]
	switch flag {
	case 'n', 'y':
		return flag, "", nil
	case 'p':
		// Extract channel binding type: "p=type,..."
		if len(clientFirstMsg) < 3 || clientFirstMsg[1] != '=' {
			return 0, "", errors.New("invalid channel binding format")
		}
		commaIdx := strings.Index(clientFirstMsg, ",")
		if commaIdx < 0 {
			return 0, "", errors.New("invalid client-first-message format")
		}
		cbType = clientFirstMsg[2:commaIdx]
		return flag, cbType, nil
	default:
		return 0, "", fmt.Errorf("invalid channel binding flag: %c", flag)
	}
}

// ParseClientFirstMessageUsername extracts the username from a client-first-message.
// Returns empty string if username is not provided (PostgreSQL convention).
func ParseClientFirstMessageUsername(clientFirstMsg string) string {
	// Skip GS2 header
	parts := strings.SplitN(clientFirstMsg, ",", 3)
	if len(parts) < 3 {
		return ""
	}
	bare := parts[2]
	attrs := parseAttributes(bare)
	return attrs["n"]
}

// SCRAMServerPlus extends SCRAMServer to support channel binding.
type SCRAMServerPlus struct {
	*SCRAMServer
	channelBindingData []byte
	gs2Header          string
}

// NewSCRAMServerPlus creates a new SCRAM-SHA-256-PLUS server with channel binding.
func NewSCRAMServerPlus(username, password string, iterationCount int, channelBindingData []byte) (*SCRAMServerPlus, error) {
	base, err := NewSCRAMServer(username, password, iterationCount)
	if err != nil {
		return nil, err
	}
	return &SCRAMServerPlus{
		SCRAMServer:        base,
		channelBindingData: channelBindingData,
	}, nil
}

// ProcessClientFirstMessage processes the client-first-message for SCRAM-SHA-256-PLUS.
func (s *SCRAMServerPlus) ProcessClientFirstMessage(clientFirstMsg string) (string, error) {
	// Store the GS2 header for channel binding verification
	parts := strings.SplitN(clientFirstMsg, ",", 3)
	if len(parts) < 3 {
		return "", errors.New("invalid client-first-message format")
	}
	s.gs2Header = parts[0] + "," + parts[1] + ","

	return s.SCRAMServer.ProcessClientFirstMessage(clientFirstMsg)
}

// ProcessClientFinalMessage processes the client-final-message for SCRAM-SHA-256-PLUS
// with channel binding verification.
func (s *SCRAMServerPlus) ProcessClientFinalMessage(clientFinalMsg string) (string, error) {
	// Parse client-final-message
	attrs := parseAttributes(clientFinalMsg)

	// Verify channel binding
	cbB64, ok := attrs["c"]
	if !ok {
		return "", errors.New("missing channel binding in client-final-message")
	}
	cbData, err := base64.StdEncoding.DecodeString(cbB64)
	if err != nil {
		return "", fmt.Errorf("invalid channel binding encoding: %w", err)
	}

	// Expected channel binding: gs2-header + channel-binding-data
	expectedCB := []byte(s.gs2Header)
	expectedCB = append(expectedCB, s.channelBindingData...)

	if !hmac.Equal(cbData, expectedCB) {
		return "", errors.New("channel binding verification failed")
	}

	return s.SCRAMServer.ProcessClientFinalMessage(clientFinalMsg)
}

// ParseIterationCount parses the iteration count from a server-first-message.
func ParseIterationCount(serverFirstMsg string) (int, error) {
	attrs := parseAttributes(serverFirstMsg)
	iStr, ok := attrs["i"]
	if !ok {
		return 0, errors.New("missing iteration count")
	}
	return strconv.Atoi(iStr)
}

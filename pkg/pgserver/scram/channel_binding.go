package scram

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	cb "github.com/golang-auth/go-channelbinding"
)

// ChannelBindingType represents the type of channel binding to use.
type ChannelBindingType string

const (
	// ChannelBindingNone means no channel binding is used.
	ChannelBindingNone ChannelBindingType = ""
	// ChannelBindingTLSUnique uses tls-unique channel binding (deprecated in TLS 1.3).
	ChannelBindingTLSUnique ChannelBindingType = "tls-unique"
	// ChannelBindingTLSExporter uses tls-exporter channel binding (TLS 1.3+).
	ChannelBindingTLSExporter ChannelBindingType = "tls-exporter"
	// ChannelBindingTLSServerEndPoint uses tls-server-end-point channel binding (RFC 5929).
	// This is the default channel binding type used by PostgreSQL/libpq.
	ChannelBindingTLSServerEndPoint ChannelBindingType = "tls-server-end-point"
)

func (t ChannelBindingType) String() string {
	return string(t)
}

// ParseChannelBindingType validates and converts a string to ChannelBindingType.
// Returns the type and true if valid, or empty string and false if invalid.
func ParseChannelBindingType(s string) (ChannelBindingType, bool) {
	switch s {
	case "tls-server-end-point":
		return ChannelBindingTLSServerEndPoint, true
	case "tls-exporter":
		return ChannelBindingTLSExporter, true
	case "tls-unique":
		return ChannelBindingTLSUnique, true
	default:
		return ChannelBindingNone, false
	}
}

// ChannelBindingData computes channel binding data for the requested type.
// serverCert is required for tls-server-end-point, can be nil for other types.
func ChannelBindingData(
	tlsState *tls.ConnectionState,
	serverCert *x509.Certificate,
	requestedType ChannelBindingType,
) ([]byte, error) {
	if tlsState == nil {
		return nil, fmt.Errorf("TLS connection required for channel binding")
	}

	var libType cb.TLSChannelBindingType
	switch requestedType {
	case ChannelBindingTLSServerEndPoint:
		libType = cb.TLSChannelBindingEndpoint
		if serverCert == nil {
			return nil, fmt.Errorf("server certificate required for tls-server-end-point channel binding")
		}
	case ChannelBindingTLSExporter:
		libType = cb.TLSChannelBindingExporter
	case ChannelBindingTLSUnique:
		libType = cb.TLSChannelBindingUnique
	default:
		return nil, fmt.Errorf("unsupported channel binding type: %s", requestedType)
	}

	return cb.MakeTLSChannelBinding(*tlsState, serverCert, libType)
}

const (
	SupportedChannelBindingFlag byte = 'p'
)

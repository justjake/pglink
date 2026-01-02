package scram

import (
	"crypto/tls"
	"fmt"
)

// ChannelBindingType represents the type of channel binding to use.
type ChannelBindingType int

const (
	// ChannelBindingNone means no channel binding is used.
	ChannelBindingNone ChannelBindingType = iota
	// ChannelBindingTLSUnique uses tls-unique channel binding (deprecated in TLS 1.3).
	ChannelBindingTLSUnique
	// ChannelBindingTLSExporter uses tls-exporter channel binding (TLS 1.3+).
	ChannelBindingTLSExporter
)

func (t ChannelBindingType) String() string {
	switch t {
	case ChannelBindingNone:
		return "none"
	case ChannelBindingTLSUnique:
		return "tls-unique"
	case ChannelBindingTLSExporter:
		return "tls-exporter"
	default:
		panic(fmt.Sprintf("invalid channel binding type: %d", t))
	}
}

// getChannelBindingData extracts channel binding data from a TLS connection state.
// For TLS 1.3+, it uses tls-exporter; for earlier versions, it uses tls-unique.
func ChannelBindingData(tlsState *tls.ConnectionState) ([]byte, ChannelBindingType, error) {
	if tlsState == nil {
		return nil, ChannelBindingNone, nil
	}

	// TLS 1.3 uses tls-exporter
	if tlsState.Version >= tls.VersionTLS13 {
		// The label and context are per RFC 9266
		data, err := tlsState.ExportKeyingMaterial("EXPORTER-Channel-Binding", nil, 32)
		if err != nil {
			return nil, ChannelBindingNone, fmt.Errorf("failed to export keying material: %w", err)
		}
		return data, ChannelBindingTLSExporter, nil
	}

	// TLS 1.2 and earlier use tls-unique (the finished message)
	// Note: tls-unique is not available in Go's TLS package directly
	// We need to use the TLSUnique field from ConnectionState
	if len(tlsState.TLSUnique) > 0 {
		return tlsState.TLSUnique, ChannelBindingTLSUnique, nil
	}

	return nil, ChannelBindingNone, nil
}

const (
	SupportedChannelBindingFlag byte = 'p'
)

package pgwire

type Sender byte

const (
	SenderNone   Sender = 0
	SenderClient Sender = 'C'
	SenderServer Sender = 'S'
	SenderProxy  Sender = 'P' // TODO: remove?
)

func (s Sender) IsZero() bool {
	return s == SenderNone
}

func (s Sender) String() string {
	switch s {
	case SenderClient:
		return "Client"
	case SenderServer:
		return "Server"
	case SenderNone:
		return "unknown"
	default:
		return "invalid"
	}
}

func (s Sender) IsClient() bool {
	return s == SenderClient
}

func (s Sender) IsServer() bool {
	return s == SenderServer
}

func (s Sender) Destination() Sender {
	switch s {
	case SenderClient:
		return SenderServer
	case SenderServer:
		return SenderClient
	default:
		return SenderNone
	}
}

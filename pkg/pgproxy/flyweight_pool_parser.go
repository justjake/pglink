package pgproxy

import (
	"sync"

	"github.com/justjake/pglink/pkg/pgwire"
)

type flyweightParser struct {
	client *pgwire.ClientFlyweights
	server *pgwire.ServerFlyweights
}

func (p *flyweightParser) Prepare(role ProxyRole) {
	if role == RoleClient && p.client == nil {
		p.client = clientFlyweightPool.Get().(*pgwire.ClientFlyweights)
	} else if role == RoleServer && p.server == nil {
		p.server = serverFlyweightPool.Get().(*pgwire.ServerFlyweights)
	}
}

func (p *flyweightParser) Parse(source pgwire.RawMessageSource) (pgwire.Message, error) {
	if p.client != nil {
		return p.client.Parse(source)
	} else {
		return p.server.Parse(source)
	}
}

func (p *flyweightParser) Release() {
	if p.client != nil {
		clientFlyweightPool.Put(p.client)
		p.client = nil
	}
	if p.server != nil {
		serverFlyweightPool.Put(p.server)
		p.server = nil
	}
}

var clientFlyweightPool = sync.Pool{
	New: func() any {
		return &pgwire.ClientFlyweights{}
	},
}

var serverFlyweightPool = sync.Pool{
	New: func() any {
		return &pgwire.ServerFlyweights{}
	},
}

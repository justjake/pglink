package proxy

import (
	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/justjake/pglink/pkg/pure"
)

type CopyModeService interface {
	IsCopy() bool
	CopyMode() pgwire.CopyMode
	Store() pure.Store[pgwire.CopyMode]
	// ResponseHandler returns a response handler that updates the copy mode.
	ResponseHandler() ResponseHandler
}

type copyModeService struct {
	store pure.Store[pgwire.CopyMode]
}

var _ CopyModeService = (*copyModeService)(nil)

func NewCopyModeService() CopyModeService {
	return &copyModeService{
		store: pure.NewStore(pgwire.CopyNone),
	}
}

func (s *copyModeService) IsCopy() bool {
	return s.store.Value() != pgwire.CopyNone
}

func (s *copyModeService) CopyMode() pgwire.CopyMode {
	return s.store.Value()
}

func (s *copyModeService) Store() pure.Store[pgwire.CopyMode] {
	return s.store
}

func (s *copyModeService) ResponseHandler() ResponseHandler {
	return s.waitingForCopyResponse
}

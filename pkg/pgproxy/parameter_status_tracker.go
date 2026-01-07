package pgproxy

import (
	"context"

	"github.com/justjake/pglink/pkg/pgwire"
)

type ParameterStatusTracker struct {
	pgwire.ParameterStatuses
	Parameters []string
}

var _ MessageTracker = (*ParameterStatusTracker)(nil)

func (t *ParameterStatusTracker) SetState(state pgwire.ParameterStatuses) {
	t.ParameterStatuses = state
	t.Parameters = make([]string, 0, len(state))
	for name := range state {
		t.Parameters = append(t.Parameters, name)
	}
}

func (t *ParameterStatusTracker) TrackNow(msg FlowMsg) {
}

func (t *ParameterStatusTracker) TrackMessage(ctx context.Context, msg FlowMsg) (context.Context, error) {
	switch msg := msg.Typed().(type) {
	case pgwire.ParameterStatus:
		name, value, err := msg.NameValue()
		if err != nil {
			return ctx, pgwire.NewProtocolViolation(err, msg)
		}
		if _, ok := t.ParameterStatuses[name]; !ok {
			t.Parameters = append(t.Parameters, name)
		}
		t.ParameterStatuses[name] = value
	}
	return ctx, nil
}

package proxy

import (
	"fmt"

	"github.com/justjake/pglink/pkg/pgwire"
	"github.com/justjake/pglink/pkg/pure"
)

type ParameterStatusTracker struct {
	pgwire.ParameterStatuses
	Parameters []string
}

var _ Tracker = (*ParameterStatusTracker)(nil)

func (t *ParameterStatusTracker) SetState(state pgwire.ParameterStatuses) {
	t.ParameterStatuses = state
	t.Parameters = make([]string, 0, len(state))
	for name := range state {
		t.Parameters = append(t.Parameters, name)
	}
}

func (t *ParameterStatusTracker) TrackNow(msg pgwire.Message) {
	switch msg := msg.(type) {
	case *pgwire.ServerParameterStatus:
		data := msg.Parse()
		if _, ok := t.ParameterStatuses[data.Name]; !ok {
			t.Parameters = append(t.Parameters, data.Name)
		}
		t.ParameterStatuses[data.Name] = data.Value
	}
}

func (t *ParameterStatusTracker) TrackEffect(msg pgwire.Message) pure.Effect {
	if msg, ok := msg.(*pgwire.ServerParameterStatus); ok {
		return pure.WithNameFunc(func() string {
			data := msg.Parse()
			return fmt.Sprintf("SetParameterStatus(%s=%q)", data.Name, data.Value)
		}, pure.Do(func() {
			t.TrackNow(msg)
		}))
	}
}

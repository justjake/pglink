package pure

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

const enableReflection = true

// Effect is a deferred side-effect of some pure operation.
type Effect interface {
	Apply(ctx context.Context) (cleanup Effect, err error)
	String() string
}

// Do creates an effect that calls apply.
func Do(apply func()) Effect {
	return doEffect{apply}
}

type doEffect struct {
	fn func()
}

func (e doEffect) String() string {
	return fmt.Sprintf("Do(%s)", DescribeFunction(e.fn))
}

func (e doEffect) Apply(ctx context.Context) (cleanup Effect, err error) {
	e.fn()
	return nil, nil
}

// DoCleanup creates an effect that calls apply, which takes a context and may return a cleanup effect or an error.
func DoCleanup(apply func(ctx context.Context) (cleanup Effect, err error)) Effect {
	return doCleanupEffect{apply}
}

type doCleanupEffect struct {
	fn func(ctx context.Context) (cleanup Effect, err error)
}

func (e doCleanupEffect) String() string {
	return fmt.Sprintf("DoCleanup(%s)", DescribeFunction(e.fn))
}

func (e doCleanupEffect) Apply(ctx context.Context) (cleanup Effect, err error) {
	return e.fn(ctx)
}

type doNamedEffect struct {
	name string
	doEffect
}

// DoNamed creates an effect with the given name and apply function.
func DoNamed(name string, apply func()) Effect {
	return doNamedEffect{name, doEffect{apply}}
}

func (e doNamedEffect) String() string {
	return fmt.Sprintf("%s(%s)", e.name, DescribeFunction(e.fn))
}

type doNamedCleanupEffect struct {
	name string
	doCleanupEffect
}

// DoNamedCleanup creates an effect with the given name and apply function that takes a context and may return a cleanup effect or an error.
func DoNamedCleanup(name string, apply func(ctx context.Context) (cleanup Effect, err error)) Effect {
	return doNamedCleanupEffect{name, doCleanupEffect{apply}}
}

func (e doNamedCleanupEffect) String() string {
	return fmt.Sprintf("%s(%s)", e.name, DescribeFunction(e.fn))
}

func DescribeFunction(f any) string {
	r := reflect.ValueOf(f)
	if r.Kind() != reflect.Func {
		panic(fmt.Sprintf("expected func, got %T", f))
	}
	var rt *runtime.Func
	if enableReflection {
		rt = runtime.FuncForPC(r.Pointer())
	}
	if rt == nil {
		return fmt.Sprintf("%#v", f)
	}
	return fmt.Sprintf("%s%#v", rt.Name(), f)
}

func WithName(name string, effect Effect) Effect {
	return namedEffect{name, effect}
}

func WithNameFunc(name func() string, effect Effect) Effect {
	return nameFuncEffect{name, effect}
}

type namedEffect struct {
	name string
	Effect
}

func (e namedEffect) String() string {
	return fmt.Sprintf("%s/%s", e.name, e.Effect.String())
}

type nameFuncEffect struct {
	name func() string
	Effect
}

func (e nameFuncEffect) String() string {
	return fmt.Sprintf("%s/%s", e.name(), e.Effect.String())
}

// Effects is a list of effects.
type Effects []Effect

func (e Effects) String() string {
	if len(e) == 0 {
		return "Pure"
	}
	var b strings.Builder
	b.WriteString("Effects[")
	for i, effect := range e {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(effect.String())
	}
	b.WriteString("]")
	return b.String()
}

type noOpEffect struct{}

func NoOp() Effect {
	return (*noOpEffect)(nil)
}

func (e *noOpEffect) String() string {
	return "NoOp"
}

func (e *noOpEffect) Apply(ctx context.Context) (cleanup Effect, err error) {
	return nil, nil
}

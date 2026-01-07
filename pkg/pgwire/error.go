package pgwire

import (
	"errors"
	"fmt"
	"runtime"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Err wraps a PostgreSQL error format.
type Err struct {
	pgproto3.ErrorResponse
	C error
}

// Ensure conformance
var _ error = &Err{}

func (e *Err) Error() string {
	// TODO: we could use filename + line number
	if e.C != nil {
		return fmt.Sprintf("%s %s: %s: %s", e.Severity, e.Code, e.Message, e.C.Error())
	}
	return fmt.Sprintf("%s %s: %s", e.Severity, e.Code, e.Message)
}

func (e *Err) Unwrap() error {
	return e.C
}

func (e *Err) Cause() error {
	return e.C
}

func (e *Err) ToMessage() Message {
	return Server(&e.ErrorResponse)
}

func (e *Err) EncodeTyped() (ErrorResponse, error) {
	encoded, err := e.ErrorResponse.Encode(nil)
	if err != nil {
		return ErrorResponse{}, err
	}
	return ErrorResponse{
		Sender: SenderServer,
		Data:   encoded,
	}, nil
}

func NewErr(severity Severity, code string, message string, cause error) *Err {
	_, file, line, _ := runtime.Caller(1)
	err := &Err{
		ErrorResponse: pgproto3.ErrorResponse{
			Severity: string(severity),
			Code:     code,
			Message:  message,
			File:     file,
			Line:     int32(line),
			Hint:     "pglink proxy error",
		},
		C: cause,
	}
	if cause != nil {
		err.Detail = cause.Error()
	}
	return err
}

func newProtocolViolationCaller(cause error, msg any, callerSkip int) *Err {
	var msgStr string
	if msg != nil {
		msgStr = fmt.Sprintf("unexpected message %T", msg)
	} else {
		msgStr = "invalid protocol state"
	}
	_, file, line, _ := runtime.Caller(callerSkip + 1)
	err := &Err{
		ErrorResponse: pgproto3.ErrorResponse{
			Severity: string(ErrorFatal),
			Code:     pgerrcode.ProtocolViolation,
			Message:  msgStr,
			File:     file,
			Line:     int32(line),
			Hint:     "pglink proxy error",
		},
		C: cause,
	}
	if cause != nil {
		err.Detail = cause.Error()
	}
	return err
}
func NewProtocolViolation(cause error, msg any) *Err {
	return newProtocolViolationCaller(cause, msg, 1)
}

func Expect[T pgproto3.Message](msg pgproto3.Message, err error) (T, error) {
	var zero T

	if err != nil {
		return zero, err
	}

	if m, ok := msg.(T); ok {
		return m, nil
	}

	err = newProtocolViolationCaller(fmt.Errorf("expected %T", zero), msg, 1)
	return zero, err
}

var ErrUnknownMessageType = errors.New("unknown message type")

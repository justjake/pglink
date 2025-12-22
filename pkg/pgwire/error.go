package pgwire

import (
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

func (e *Err) Cause() error {
	return e.C
}

func NewErr(severity Severity, code string, message string, cause error) *Err {
	_, file, line, _ := runtime.Caller(1)
	return &Err{
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
}

func NewProtocolViolation(cause error, msg Message) *Err {
	var msgStr string
	if msg != nil {
		msgStr = fmt.Sprintf("unexpected message %T", msg)
	} else {
		msgStr = "invalid protocol state"
	}
	_, file, line, _ := runtime.Caller(1)
	return &Err{
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
}

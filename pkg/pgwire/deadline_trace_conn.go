package pgwire

import (
	"log/slog"
	"net"
	"runtime"
	"strings"
	"time"
)

// DeadlineTraceConn wraps a net.Conn and logs stack traces for all deadline-setting calls.
// This is useful for debugging unexpected deadline behavior.
type DeadlineTraceConn struct {
	net.Conn
	Logger *slog.Logger
}

// NewDeadlineTraceConn wraps a net.Conn to log stack traces for deadline calls.
func NewDeadlineTraceConn(conn net.Conn, logger *slog.Logger) *DeadlineTraceConn {
	return &DeadlineTraceConn{
		Conn:   conn,
		Logger: logger,
	}
}

func (c *DeadlineTraceConn) SetDeadline(t time.Time) error {
	c.logDeadline("SetDeadline", t)
	return c.Conn.SetDeadline(t)
}

func (c *DeadlineTraceConn) SetReadDeadline(t time.Time) error {
	c.logDeadline("SetReadDeadline", t)
	return c.Conn.SetReadDeadline(t)
}

func (c *DeadlineTraceConn) SetWriteDeadline(t time.Time) error {
	c.logDeadline("SetWriteDeadline", t)
	return c.Conn.SetWriteDeadline(t)
}

func (c *DeadlineTraceConn) logDeadline(method string, t time.Time) {
	stack := captureStack(3) // Skip logDeadline, SetXxxDeadline, and caller

	var deadlineStr string
	if t.IsZero() {
		deadlineStr = "CLEAR (zero time)"
	} else {
		until := time.Until(t)
		if until < 0 {
			deadlineStr = "PAST (" + t.Format(time.RFC3339Nano) + ", " + until.String() + " ago)"
		} else if until < time.Second {
			deadlineStr = "IMMINENT (" + t.Format(time.RFC3339Nano) + ", in " + until.String() + ")"
		} else {
			deadlineStr = t.Format(time.RFC3339Nano) + " (in " + until.String() + ")"
		}
	}

	c.Logger.Debug("deadline set",
		"method", method,
		"deadline", deadlineStr,
		"localAddr", c.LocalAddr(),
		"remoteAddr", c.RemoteAddr(),
		"stack", stack,
	)
}

func captureStack(skip int) string {
	const maxFrames = 20
	pcs := make([]uintptr, maxFrames)
	n := runtime.Callers(skip+1, pcs)
	if n == 0 {
		return "(no stack)"
	}

	frames := runtime.CallersFrames(pcs[:n])
	var sb strings.Builder
	for {
		frame, more := frames.Next()
		// Skip runtime internals
		if strings.Contains(frame.Function, "runtime.") {
			if !more {
				break
			}
			continue
		}
		sb.WriteString("\n  ")
		sb.WriteString(frame.Function)
		sb.WriteString(" (")
		sb.WriteString(frame.File)
		sb.WriteString(":")
		sb.WriteString(itoa(frame.Line))
		sb.WriteString(")")
		if !more {
			break
		}
	}
	return sb.String()
}

func itoa(i int) string {
	if i < 0 {
		return "-" + itoa(-i)
	}
	if i < 10 {
		return string(rune('0' + i))
	}
	return itoa(i/10) + string(rune('0'+i%10))
}

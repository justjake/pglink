package frontend

import (
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/justjake/pglink/pkg/config"
	"github.com/justjake/pglink/pkg/pgwire"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// TimeoutType identifies the type of timeout that fired.
type TimeoutType string

const (
	// TimeoutQuery fires when a query exceeds query_timeout.
	TimeoutQuery TimeoutType = "query"

	// TimeoutIdleTransaction fires when idle in a transaction exceeds idle_transaction_timeout.
	TimeoutIdleTransaction TimeoutType = "idle_transaction"

	// TimeoutTransaction fires when total transaction time exceeds transaction_timeout.
	TimeoutTransaction TimeoutType = "transaction"
)

// String returns the string representation of the timeout type.
func (t TimeoutType) String() string {
	return string(t)
}

// TimeoutResult is returned by checkTimeouts when a timeout has fired.
type TimeoutResult struct {
	Type     TimeoutType
	Deadline time.Time
	Elapsed  time.Duration
}

// checkTimeouts checks if any timeout has fired based on current state.
// Returns nil if no timeout has fired.
// Uses else-if priority chain (query > idle_transaction > transaction) to avoid pile-up.
func (s *Session) checkTimeouts(now time.Time) *TimeoutResult {
	cfg := s.dbConfig
	if cfg == nil {
		return nil
	}

	// Priority 1: query_timeout (only when actively executing)
	if queryTimeout := cfg.QueryTimeout.Duration(); queryTimeout > 0 && s.state.ActiveRequestFlow != nil {
		deadline := s.state.ActiveRequestFlow.StartTime.Add(queryTimeout)
		if now.After(deadline) {
			return &TimeoutResult{
				Type:     TimeoutQuery,
				Deadline: deadline,
				Elapsed:  now.Sub(s.state.ActiveRequestFlow.StartTime),
			}
		}
	}

	// Priority 2: idle_transaction_timeout (only when idle in transaction)
	if idleTxTimeout := cfg.IdleTransactionTimeout.Duration(); idleTxTimeout > 0 &&
		s.state.TxStatus != pgwire.TxIdle &&
		s.state.ActiveRequestFlow == nil &&
		!s.state.LastReadyForQueryTime.IsZero() {
		deadline := s.state.LastReadyForQueryTime.Add(idleTxTimeout)
		if now.After(deadline) {
			return &TimeoutResult{
				Type:     TimeoutIdleTransaction,
				Deadline: deadline,
				Elapsed:  now.Sub(s.state.LastReadyForQueryTime),
			}
		}
	}

	// Priority 3: transaction_timeout (any time in transaction)
	if txTimeout := cfg.TransactionTimeout.Duration(); txTimeout > 0 && !s.state.TxStartTime.IsZero() {
		deadline := s.state.TxStartTime.Add(txTimeout)
		if now.After(deadline) {
			return &TimeoutResult{
				Type:     TimeoutTransaction,
				Deadline: deadline,
				Elapsed:  now.Sub(s.state.TxStartTime),
			}
		}
	}

	return nil
}

// nextTimeoutDeadline returns the earliest deadline for any configured timeout,
// or zero time if no timeouts are active.
func (s *Session) nextTimeoutDeadline() time.Time {
	cfg := s.dbConfig
	if cfg == nil {
		return time.Time{}
	}

	var earliest time.Time

	// Check query_timeout deadline
	if queryTimeout := cfg.QueryTimeout.Duration(); queryTimeout > 0 && s.state.ActiveRequestFlow != nil {
		deadline := s.state.ActiveRequestFlow.StartTime.Add(queryTimeout)
		if earliest.IsZero() || deadline.Before(earliest) {
			earliest = deadline
		}
	}

	// Check idle_transaction_timeout deadline
	if idleTxTimeout := cfg.IdleTransactionTimeout.Duration(); idleTxTimeout > 0 &&
		s.state.TxStatus != pgwire.TxIdle &&
		s.state.ActiveRequestFlow == nil &&
		!s.state.LastReadyForQueryTime.IsZero() {
		deadline := s.state.LastReadyForQueryTime.Add(idleTxTimeout)
		if earliest.IsZero() || deadline.Before(earliest) {
			earliest = deadline
		}
	}

	// Check transaction_timeout deadline
	if txTimeout := cfg.TransactionTimeout.Duration(); txTimeout > 0 && !s.state.TxStartTime.IsZero() {
		deadline := s.state.TxStartTime.Add(txTimeout)
		if earliest.IsZero() || deadline.Before(earliest) {
			earliest = deadline
		}
	}

	return earliest
}

// handleTimeout handles a fired timeout by sending error to client and terminating.
// Returns error to indicate session should end.
func (s *Session) handleTimeout(result *TimeoutResult) error {
	errCode, errMsg := timeoutTypeToError(result.Type)
	s.logger.Warn("timeout fired",
		"type", result.Type,
		"elapsed", result.Elapsed,
		"action", s.dbConfig.GetTimeoutAction(),
	)

	// Mark OTEL span with exception if we have an active request flow
	if s.tracingEnabled {
		// Get active span from context if available
		span := trace.SpanFromContext(s.ctx)
		if span.IsRecording() {
			timeoutErr := fmt.Errorf("%s: %s (elapsed: %v)", result.Type, errMsg, result.Elapsed)
			span.RecordError(timeoutErr)
			span.SetStatus(codes.Error, errMsg)
			span.SetAttributes(
				attribute.String("timeout.type", result.Type.String()),
				attribute.String("timeout.action", string(s.dbConfig.GetTimeoutAction())),
				attribute.Int64("timeout.elapsed_ms", result.Elapsed.Milliseconds()),
			)
		}
	}

	// Record metrics
	action := s.dbConfig.GetTimeoutAction()
	outcome := "terminated"

	switch action {
	case config.TimeoutActionTerminate:
		if s.metrics != nil {
			s.metrics.RecordTimeout(s.databaseName, result.Type.String(), outcome)
		}
		return s.terminateOnTimeout(errCode, errMsg, result)

	case config.TimeoutActionRequestCancel:
		// TODO: Implement cancel request logic
		// For now, fall back to terminate
		s.logger.Debug("request_cancel not yet implemented, using terminate")
		if s.metrics != nil {
			s.metrics.RecordTimeout(s.databaseName, result.Type.String(), outcome)
		}
		return s.terminateOnTimeout(errCode, errMsg, result)

	default:
		if s.metrics != nil {
			s.metrics.RecordTimeout(s.databaseName, result.Type.String(), outcome)
		}
		return s.terminateOnTimeout(errCode, errMsg, result)
	}
}

// terminateOnTimeout sends error to client, marks backend for destruction.
// This matches pgbouncer's behavior.
func (s *Session) terminateOnTimeout(errCode string, errMsg string, result *TimeoutResult) error {
	// Send ErrorResponse to client
	s.frontend.Send(&pgproto3.ErrorResponse{
		Severity: "FATAL",
		Code:     errCode,
		Message:  errMsg,
		Detail:   fmt.Sprintf("timeout type: %s, elapsed: %v", result.Type, result.Elapsed),
	})

	// Try to flush, but don't block on it
	if err := s.frontend.Flush(); err != nil {
		s.logger.Debug("failed to flush timeout error to client", "error", err)
	}

	// Mark backend for destruction so it's not reused
	if s.backend != nil {
		s.backend.MarkForDestroy(fmt.Errorf("%s: %s", result.Type, errMsg))
	}

	return pgwire.NewErr(pgwire.ErrorFatal, errCode, errMsg, nil)
}

// timeoutTypeToError returns the PostgreSQL error code and message for a timeout type.
func timeoutTypeToError(t TimeoutType) (code string, msg string) {
	// Use PostgreSQL standard error codes from pgerrcode package
	switch t {
	case TimeoutQuery:
		return pgerrcode.QueryCanceled, "canceling statement due to query timeout"
	case TimeoutIdleTransaction:
		return pgerrcode.IdleInTransactionSessionTimeout, "terminating connection due to idle in transaction timeout"
	case TimeoutTransaction:
		return pgerrcode.QueryCanceled, "canceling statement due to transaction timeout"
	default:
		return pgerrcode.QueryCanceled, "timeout"
	}
}

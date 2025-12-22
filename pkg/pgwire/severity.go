package pgwire

type Severity string

const (
	// Used in ErrorResponse messages.
	Error      Severity = "ERROR"
	ErrorFatal Severity = "FATAL"
	ErrorPanic Severity = "PANIC"

	// Used in NoticeResponse messages.
	NoticeWarning Severity = "WARNING"
	Notice        Severity = "NOTICE"
	NoticeDebug   Severity = "DEBUG"
	NoticeInfo    Severity = "INFO"
	NoticeLog     Severity = "LOG"
)

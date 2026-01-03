package pgwire

import (
	"fmt"
	"regexp"
	"strings"
)

// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-ASYNC
//
// ParameterStatus messages will be generated whenever the active value changes for
// any of the parameters the backend believes the frontend should know about. Most
// commonly this occurs in response to a SET SQL command executed by the frontend,
// and this case is effectively synchronous — but it is also possible for parameter
// status changes to occur because the administrator changed a configuration file
// and then sent the SIGHUP signal to the server. Also, if a SET command is rolled
// back, an appropriate ParameterStatus message will be generated to report the
// current effective value.
type ParameterStatuses map[string]string

func (p ParameterStatuses) User() string {
	return p[ParamUser]
}

// Database returns the `database` parameter value, or the `user` parameter value if `database` is not set.
func (p ParameterStatuses) Database() string {
	if db, ok := p[ParamDatabase]; ok {
		return db
	}
	return p.User()
}

// At present there is a hard-wired set of parameters for which ParameterStatus
// will be generated. They are:
const (
	ParamApplicationName            = "application_name"
	ParamScramIterations            = "scram_iterations"
	ParamClientEncoding             = "client_encoding"
	ParamSearchPath                 = "search_path"
	ParamDateStyle                  = "DateStyle"
	ParamServerEncoding             = "server_encoding"
	ParamDefaultTransactionReadOnly = "default_transaction_read_only"
	ParamServerVersion              = "server_version"
	ParamInHotStandby               = "in_hot_standby"
	ParamSessionAuthorization       = "session_authorization"
	ParamIntegerDatetimes           = "integer_datetimes"
	ParamStandardConformingStrings  = "standard_conforming_strings"
	ParamIntervalStyle              = "IntervalStyle"
	ParamTimeZone                   = "TimeZone"
	ParamIsSuperuser                = "is_superuser"

	// Startup parameters
	ParamUser     = "user"
	ParamDatabase = "database"
)

var BaseTrackedParameters = []string{
	ParamApplicationName,
	ParamScramIterations,
	ParamClientEncoding,
	ParamSearchPath,
	ParamDateStyle,
	ParamServerEncoding,
	ParamDefaultTransactionReadOnly,
	ParamServerVersion,
	ParamInHotStandby,
	ParamSessionAuthorization,
	ParamIntegerDatetimes,
	ParamStandardConformingStrings,
	ParamIntervalStyle,
	ParamTimeZone,
	ParamIsSuperuser,
}

// ImmutableParameters are parameters that cannot be changed via SET.
// They are fixed at server startup or connection time.
// From PgBouncer's immutable_vars list.
var ImmutableParameters = map[string]bool{
	ParamServerVersion:             true,
	ParamServerEncoding:            true,
	ParamIntegerDatetimes:          true,
	ParamStandardConformingStrings: true,
	ParamIsSuperuser:               true, // requires superuser to SET
	ParamSessionAuthorization:      true, // requires superuser to SET
	ParamInHotStandby:              true, // server state, not settable
}

var DefaultParameterStatuses = ParameterStatuses{
	ParamServerVersion:             "18.1 (pglink proxy)",
	ParamServerEncoding:            "UTF8",
	ParamClientEncoding:            "UTF8",
	ParamIntegerDatetimes:          "on",
	ParamStandardConformingStrings: "on",
	ParamIntervalStyle:             "postgres",
	ParamTimeZone:                  "UTC",
}

type ParameterStatusDiff map[string]*string

func (base ParameterStatuses) DiffToTip(tracked []string, tip ParameterStatuses) ParameterStatusDiff {
	var diff ParameterStatusDiff

	for _, key := range tracked {
		tipValue, tipHas := tip[key]
		baseValue, baseHas := base[key]
		if !tipHas && !baseHas {
			continue
		}

		if tipHas && baseHas && tipValue == baseValue {
			continue
		}

		if diff == nil {
			diff = make(ParameterStatusDiff)
		}
		if !tipHas {
			diff[key] = nil
		} else {
			diff[key] = &tipValue
		}
	}

	return diff
}

// BuildSetQuery builds SET statements to apply this diff.
// Returns empty string if no parameters need to be set.
//
// Note: Parameters with nil values (deletions) are skipped because
// PostgreSQL doesn't support unsetting session variables - they can only
// be RESET to their default values, which may differ from the client's expectations.
//
// Immutable parameters (server_version, server_encoding, etc.) are also skipped
// since they cannot be changed via SET.
func (diff ParameterStatusDiff) BuildSetQuery() string {
	if len(diff) == 0 {
		return ""
	}

	var parts []string
	for key, valuePtr := range diff {
		if valuePtr == nil {
			continue // Can't RESET parameters via diff
		}
		if ImmutableParameters[key] {
			continue // Cannot SET immutable parameters
		}
		parts = append(parts, buildSetStatement(key, *valuePtr))
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "; ")
}

// buildSetStatement builds a single SET statement for the given key/value pair.
func buildSetStatement(key, value string) string {
	// search_path uses GUC_LIST_QUOTE - PostgreSQL sends it already quoted
	// e.g., value might be: `"$user", public` or `""` for empty
	if strings.EqualFold(key, ParamSearchPath) {
		if value == `""` {
			// Empty search_path - PostgreSQL sends "" but we need ''
			value = `''`
		}
		return "SET " + key + " = " + value
	}
	// For other parameters, quote the value as a literal
	return "SET " + key + " = " + quoteLiteral(value)
}

// quoteLiteral quotes a string for use as a PostgreSQL string literal.
// It escapes single quotes by doubling them.
func quoteLiteral(s string) string {
	// Replace ' with ''
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'"
}

// optionRE matches -c key=value or --key=value with backslash escape support.
// Pattern: (?:[^\\\s]|\\.)* means "non-backslash-non-space OR backslash-anything, repeated"
var optionRE = regexp.MustCompile(`(?:-c\s*|--)((?:[^\\\s]|\\.)*)`)

// ParseOptionsParameter parses the PostgreSQL "options" startup parameter.
// It extracts session variables from formats like "-c key=value" and "--key=value".
//
// Escaping rules (per PostgreSQL libpq docs):
//   - Spaces separate arguments unless escaped with backslash (\)
//   - \\ represents a literal backslash
//   - Example: "-c search_path=schema1\ schema2" -> search_path = "schema1 schema2"
//
// Reference: https://www.postgresql.org/docs/current/libpq-connect.html
func ParseOptionsParameter(options string) (map[string]string, error) {
	result := make(map[string]string)
	for _, m := range optionRE.FindAllStringSubmatch(options, -1) {
		kv := unescapeOption(m[1])
		if eq := strings.Index(kv, "="); eq != -1 {
			result[kv[:eq]] = kv[eq+1:]
		} else {
			return nil, fmt.Errorf("invalid option: expected key=value, got %q", m[1])
		}
	}
	return result, nil
}

// unescapeOption removes backslash escapes from an option value.
// \X becomes X for any character X.
func unescapeOption(s string) string {
	if !strings.Contains(s, "\\") {
		return s // Fast path: no escapes
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++ // Skip backslash, take next char literally
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

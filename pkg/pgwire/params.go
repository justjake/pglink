package pgwire

import (
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

var BaseParameterStatuses = ParameterStatuses{
	ParamServerVersion:             "18.1 (pglink proxy)",
	ParamServerEncoding:            "UTF8",
	ParamClientEncoding:            "UTF8",
	ParamIntegerDatetimes:          "on",
	ParamStandardConformingStrings: "on",
	ParamIntervalStyle:             "postgres",
	ParamTimeZone:                  "UTC",
}

type ParameterStatusDiff map[string]*string

func (base ParameterStatuses) DiffToTip(tip ParameterStatuses) ParameterStatusDiff {
	var diff ParameterStatusDiff

	// Items in tip that are different are upserted.
	for tipKey, tipValue := range tip {
		if baseValue, baseHas := base[tipKey]; !baseHas || baseValue != tipValue {
			if diff == nil {
				diff = make(ParameterStatusDiff)
			}
			diff[tipKey] = &tipValue
		}
	}

	// Items in base that are not in tip are deleted.
	for baseKey := range base {
		if _, tipHas := tip[baseKey]; !tipHas {
			if diff == nil {
				diff = make(ParameterStatusDiff)
			}
			diff[baseKey] = nil
		}
	}

	return diff
}

// FilterKeys returns a new diff containing only the specified keys.
// This is used to restrict variable restoration to tracked parameters only.
func (diff ParameterStatusDiff) FilterKeys(keys []string) ParameterStatusDiff {
	if diff == nil {
		return nil
	}
	keySet := make(map[string]bool, len(keys))
	for _, k := range keys {
		keySet[k] = true
	}

	var filtered ParameterStatusDiff
	for k, v := range diff {
		if keySet[k] {
			if filtered == nil {
				filtered = make(ParameterStatusDiff)
			}
			filtered[k] = v
		}
	}
	return filtered
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

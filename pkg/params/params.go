package params

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

var BaseParameterStatuses = ParameterStatuses{
	ParamServerVersion:             "18.1 (pglink proxy)",
	ParamServerEncoding:            "UTF8",
	ParamIntegerDatetimes:          "on",
	ParamStandardConformingStrings: "on",
	ParamIntervalStyle:             "postgres",
	ParamTimeZone:                  "UTC",
}

type ParameterStatusDiff map[string]*string

func (base ParameterStatuses) DiffToTip(tip ParameterStatuses) ParameterStatusDiff {
	diff := ParameterStatusDiff{}

	// Items in tip that are different are upserted.
	for tipKey, tipValue := range tip {
		if baseValue, baseHas := base[tipKey]; !baseHas || baseValue != tipValue {
			diff[tipKey] = &tipValue
		}
	}

	// Items in base that are not in tip are deleted.
	for baseKey := range base {
		if _, tipHas := tip[baseKey]; !tipHas {
			diff[baseKey] = nil
		}
	}

	return diff
}

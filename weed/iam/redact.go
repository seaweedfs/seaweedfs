package iam

import "net/url"

// sensitiveFormKeys is the set of IAM request form parameters whose values
// must never be written to logs. Matching is case-sensitive and uses the
// exact AWS IAM parameter name. Extend this when adding IAM actions that
// accept credentials, passwords, session tokens, or private keys.
var sensitiveFormKeys = map[string]struct{}{
	"SecretAccessKey": {},
	"Password":        {},
	"NewPassword":     {},
	"OldPassword":     {},
	"PrivateKey":      {},
	"SessionToken":    {},
}

// RedactSensitiveFormValues returns a shallow copy of values with every
// sensitive key (see sensitiveFormKeys) replaced by "[REDACTED]". Intended
// for debug-level logging of IAM request forms so secrets do not leak into
// log sinks.
func RedactSensitiveFormValues(values url.Values) url.Values {
	safe := make(url.Values, len(values))
	for k, v := range values {
		if _, sensitive := sensitiveFormKeys[k]; sensitive {
			safe[k] = []string{"[REDACTED]"}
		} else {
			safe[k] = v
		}
	}
	return safe
}

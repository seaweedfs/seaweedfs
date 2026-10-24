package policy_engine

import (
	"context"
	"net/http"
)

// originalGrantConditionsKey prevents clients from forging original grant values through request headers.
type originalGrantConditionsKey struct{}

// isGrantConditionKey restricts original-value checks to the five upload grant condition keys.
func isGrantConditionKey(key string) bool {
	switch key {
	case "s3:x-amz-grant-read", "s3:x-amz-grant-write", "s3:x-amz-grant-read-acp", "s3:x-amz-grant-write-acp", "s3:x-amz-grant-full-control":
		return true
	}
	return false
}

// cloneGrantConditions isolates inputs and results so later mutations cannot change the original complete grant representation.
func cloneGrantConditions(values map[string][]string) map[string][]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string][]string, len(values))
	for key, grants := range values {
		if isGrantConditionKey(key) {
			cloned[key] = append([]string(nil), grants...)
		}
	}
	return cloned
}

// WithOriginalGrantConditions saves the complete list before upload normalization to supplement explicit deny checks only.
func WithOriginalGrantConditions(r *http.Request, values map[string][]string) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), originalGrantConditionsKey{}, cloneGrantConditions(values)))
}

// OriginalGrantConditionsFromRequest reads the internal snapshot; other operations have no such context.
func OriginalGrantConditionsFromRequest(r *http.Request) map[string][]string {
	if r == nil {
		return nil
	}
	values, _ := r.Context().Value(originalGrantConditionsKey{}).(map[string][]string)
	return cloneGrantConditions(values)
}

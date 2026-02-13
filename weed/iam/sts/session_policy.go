package sts

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
)

// NormalizeSessionPolicy validates and normalizes inline session policy JSON.
// It returns an empty string if the input is empty or whitespace.
func NormalizeSessionPolicy(policyJSON string) (string, error) {
	trimmed := strings.TrimSpace(policyJSON)
	if trimmed == "" {
		return "", nil
	}

	var policyDoc policy.PolicyDocument
	if err := json.Unmarshal([]byte(trimmed), &policyDoc); err != nil {
		return "", fmt.Errorf("invalid session policy JSON: %w", err)
	}
	if err := policy.ValidatePolicyDocument(&policyDoc); err != nil {
		return "", fmt.Errorf("invalid session policy document: %w", err)
	}
	normalized, err := json.Marshal(&policyDoc)
	if err != nil {
		return "", fmt.Errorf("failed to normalize session policy: %w", err)
	}
	return string(normalized), nil
}

package policy_engine

import (
	"testing"
)

// TestExtractPrincipalVariablesWithPaths tests ARN parsing with IAM path components
func TestExtractPrincipalVariablesWithPaths(t *testing.T) {
	tests := []struct {
		name      string
		principal string
		expected  map[string][]string
	}{
		{
			name:      "IAM User with path",
			principal: "arn:aws:iam::123456789012:user/division/team/alice",
			expected: map[string][]string{
				"aws:PrincipalAccount": {"123456789012"},
				"aws:principaltype":    {"IAMUser"},
				"aws:username":         {"alice"},
				"aws:userid":           {"alice"},
			},
		},
		{
			name:      "IAM Role with path",
			principal: "arn:aws:iam::123456789012:role/service-role/MyRole",
			expected: map[string][]string{
				"aws:PrincipalAccount": {"123456789012"},
				"aws:principaltype":    {"IAMRole"},
				"aws:username":         {"MyRole"},
				"aws:userid":           {"MyRole"},
			},
		},
		{
			name:      "Assumed Role with path",
			principal: "arn:aws:sts::123456789012:assumed-role/service-role/MyRole/session-name",
			expected: map[string][]string{
				"aws:PrincipalAccount": {"123456789012"},
				"aws:principaltype":    {"AssumedRole"},
				"aws:username":         {"session-name"},
				"aws:userid":           {"session-name"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractPrincipalVariables(tt.principal)

			// Check that all expected keys are present with correct values
			for key, expectedValues := range tt.expected {
				actualValues, ok := result[key]
				if !ok {
					t.Errorf("Expected key %s not found in result", key)
					continue
				}

				if len(actualValues) != len(expectedValues) {
					t.Errorf("For key %s: expected %d values, got %d", key, len(expectedValues), len(actualValues))
					continue
				}

				for i, expectedValue := range expectedValues {
					if actualValues[i] != expectedValue {
						t.Errorf("For key %s[%d]: expected %s, got %s", key, i, expectedValue, actualValues[i])
					}
				}
			}

			// Check that there are no unexpected keys
			for key := range result {
				if _, ok := tt.expected[key]; !ok {
					t.Errorf("Unexpected key %s in result", key)
				}
			}
		})
	}
}

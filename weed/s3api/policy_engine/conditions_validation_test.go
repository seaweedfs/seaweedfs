package policy_engine

import (
	"testing"
)

func TestValidatePolicyRejectsUnsupportedConditionOperator(t *testing.T) {
	tests := []struct {
		name        string
		policyJSON  string
		expectError bool
	}{
		{
			name: "unsupported operator rejected",
			policyJSON: `{
				"Version": "2012-10-17",
				"Statement": [{
					"Effect": "Allow",
					"Action": "s3:GetObject",
					"Resource": "arn:aws:s3:::test-bucket/*",
					"Condition": {"StringEqualsBogus": {"aws:username": "alice"}}
				}]
			}`,
			expectError: true,
		},
		{
			name: "supported IgnoreCase operator accepted",
			policyJSON: `{
				"Version": "2012-10-17",
				"Statement": [{
					"Effect": "Allow",
					"Action": "s3:GetObject",
					"Resource": "arn:aws:s3:::test-bucket/*",
					"Condition": {"StringEqualsIgnoreCase": {"aws:username": "alice"}}
				}]
			}`,
			expectError: false,
		},
		{
			name: "supported StringEquals operator accepted",
			policyJSON: `{
				"Version": "2012-10-17",
				"Statement": [{
					"Effect": "Allow",
					"Action": "s3:GetObject",
					"Resource": "arn:aws:s3:::test-bucket/*",
					"Condition": {"StringEquals": {"aws:username": "alice"}}
				}]
			}`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParsePolicy(tt.policyJSON)
			if (err != nil) != tt.expectError {
				t.Errorf("ParsePolicy expected error: %v, got: %v", tt.expectError, err)
			}
		})
	}
}

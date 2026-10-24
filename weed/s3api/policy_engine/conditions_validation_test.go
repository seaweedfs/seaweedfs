package policy_engine

import (
	"testing"
)

func TestValidateBucketPolicyRejectsUnsupportedConditionOperator(t *testing.T) {
	unsupported := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:GetObject",
			"Resource": "arn:aws:s3:::test-bucket/*",
			"Condition": {"StringEqualsBogus": {"aws:username": "alice"}}
		}]
	}`
	supportedIgnoreCase := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:GetObject",
			"Resource": "arn:aws:s3:::test-bucket/*",
			"Condition": {"StringEqualsIgnoreCase": {"aws:username": "alice"}}
		}]
	}`
	supportedStringEquals := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:GetObject",
			"Resource": "arn:aws:s3:::test-bucket/*",
			"Condition": {"StringEquals": {"aws:username": "alice"}}
		}]
	}`

	t.Run("upload rejects unsupported operator", func(t *testing.T) {
		policy, err := ParsePolicy(unsupported)
		if err != nil {
			t.Fatalf("ParsePolicy failed: %v", err)
		}
		if err := ValidateBucketPolicy(policy, "test-bucket"); err == nil {
			t.Fatalf("ValidateBucketPolicy expected error for unsupported operator, got nil")
		}
	})

	t.Run("upload accepts supported IgnoreCase operator", func(t *testing.T) {
		policy, err := ParsePolicy(supportedIgnoreCase)
		if err != nil {
			t.Fatalf("ParsePolicy failed: %v", err)
		}
		if err := ValidateBucketPolicy(policy, "test-bucket"); err != nil {
			t.Fatalf("ValidateBucketPolicy unexpected error: %v", err)
		}
	})

	t.Run("upload accepts supported StringEquals operator", func(t *testing.T) {
		policy, err := ParsePolicy(supportedStringEquals)
		if err != nil {
			t.Fatalf("ParsePolicy failed: %v", err)
		}
		if err := ValidateBucketPolicy(policy, "test-bucket"); err != nil {
			t.Fatalf("ValidateBucketPolicy unexpected error: %v", err)
		}
	})

	t.Run("load tolerates legacy unsupported operator", func(t *testing.T) {
		if _, err := ParsePolicy(unsupported); err != nil {
			t.Fatalf("ParsePolicy must not reject legacy unsupported operator at load time: %v", err)
		}
	})
}

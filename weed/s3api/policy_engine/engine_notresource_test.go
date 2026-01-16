package policy_engine

import (
	"testing"
)

func TestNotResourceWithVariables(t *testing.T) {
	engine := NewPolicyEngine()

	// Policy mirroring the isolation test
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Sid": "AllowOwnFolder",
				"Effect": "Allow",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::bucket/${aws:username}/*"
			},
			{
				"Sid": "DenyOtherFolders",
				"Effect": "Deny",
				"Action": "s3:GetObject",
				"NotResource": "arn:aws:s3:::bucket/${aws:username}/*"
			}
		]
	}`

	err := engine.SetBucketPolicy("test-bucket", policyJSON)
	if err != nil {
		t.Fatalf("Failed to set bucket policy: %v", err)
	}

	// Case 1: Alice accesses her own folder -> should match Allow, but NOT match Deny statement
	// (because Deny says NotResource is own folder, and she IS accessing her own folder, so NotResource check fails, statement doesn't apply)
	args := &PolicyEvaluationArgs{
		Action:    "s3:GetObject",
		Resource:  "arn:aws:s3:::bucket/alice/data.txt",
		Principal: "arn:aws:iam::123456789012:user/alice",
		Conditions: map[string][]string{
			"aws:username": {"alice"},
		},
	}

	result := engine.EvaluatePolicy("test-bucket", args)
	if result != PolicyResultAllow {
		t.Errorf("Alice should be allowed to her own folder, got %v", result)
	}

	// Case 2: Alice accesses Bob's folder -> should NOT match Allow, and SHOULD match Deny statement
	// (because Deny says NotResource is own folder, and she is NOT accessing her own folder, so NotResource matches, statement applies)
	args = &PolicyEvaluationArgs{
		Action:    "s3:GetObject",
		Resource:  "arn:aws:s3:::bucket/bob/data.txt",
		Principal: "arn:aws:iam::123456789012:user/alice",
		Conditions: map[string][]string{
			"aws:username": {"alice"},
		},
	}

	result = engine.EvaluatePolicy("test-bucket", args)
	if result != PolicyResultDeny {
		t.Errorf("Alice should be denied access to Bob folder, got %v", result)
	}
}

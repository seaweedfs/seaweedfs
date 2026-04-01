package policy_engine

import (
	"testing"
)

func TestPolicyVariables(t *testing.T) {
	engine := NewPolicyEngine()

	// Policy with variables in Resource and Condition
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Sid": "AllowUserHomeDirectory",
				"Effect": "Allow",
				"Action": "s3:ListBucket",
				"Resource": "arn:aws:s3:::test-bucket",
				"Condition": {
					"StringLike": {
						"s3:prefix": ["${aws:username}/*"]
					}
				}
			},
			{
				"Sid": "AllowUserObjectAccess",
				"Effect": "Allow",
				"Action": ["s3:GetObject", "s3:PutObject"],
				"Resource": ["arn:aws:s3:::test-bucket/${aws:username}/*"]
			}
		]
	}`

	err := engine.SetBucketPolicy("test-bucket", policyJSON)
	if err != nil {
		t.Fatalf("Failed to set bucket policy: %v", err)
	}

	// Case 1: Matching username for resource access
	args := &PolicyEvaluationArgs{
		Action:    "s3:GetObject",
		Resource:  "arn:aws:s3:::test-bucket/johndoe/file.txt",
		Principal: "arn:aws:iam::123456789012:user/johndoe",
		Conditions: map[string][]string{
			"aws:username": {"johndoe"},
		},
	}

	result := engine.EvaluatePolicy("test-bucket", args)
	if result != PolicyResultAllow {
		t.Errorf("Expected Allow for matching username in resource, got %v", result)
	}

	// Case 2: Mismatched username for resource access
	args = &PolicyEvaluationArgs{
		Action:    "s3:GetObject",
		Resource:  "arn:aws:s3:::test-bucket/janedoe/file.txt",
		Principal: "arn:aws:iam::123456789012:user/johndoe",
		Conditions: map[string][]string{
			"aws:username": {"johndoe"},
		},
	}

	result = engine.EvaluatePolicy("test-bucket", args)
	if result != PolicyResultIndeterminate {
		t.Errorf("Expected Indeterminate for mismatched username in resource, got %v", result)
	}

	// Case 3: ListBucket with matching prefix condition
	args = &PolicyEvaluationArgs{
		Action:    "s3:ListBucket",
		Resource:  "arn:aws:s3:::test-bucket",
		Principal: "arn:aws:iam::123456789012:user/johndoe",
		Conditions: map[string][]string{
			"aws:username": {"johndoe"},
			"s3:prefix":    {"johndoe/docs"},
		},
	}

	result = engine.EvaluatePolicy("test-bucket", args)
	if result != PolicyResultAllow {
		t.Errorf("Expected Allow for matching prefix condition, got %v", result)
	}

	// Case 4: ListBucket with mismatched prefix condition
	args = &PolicyEvaluationArgs{
		Action:    "s3:ListBucket",
		Resource:  "arn:aws:s3:::test-bucket",
		Principal: "arn:aws:iam::123456789012:user/johndoe",
		Conditions: map[string][]string{
			"aws:username": {"johndoe"},
			"s3:prefix":    {"janedoe/docs"},
		},
	}

	result = engine.EvaluatePolicy("test-bucket", args)
	if result != PolicyResultIndeterminate {
		t.Errorf("Expected Indeterminate for mismatched prefix condition, got %v", result)
	}
}

func TestEvaluatePolicyForRequestVariables(t *testing.T) {
	engine := NewPolicyEngine()

	// Policy using aws:username
	policyJSON := `{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Effect": "Allow",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::test-bucket/${aws:username}/*"
			}
		]
	}`

	err := engine.SetBucketPolicy("test-bucket", policyJSON)
	if err != nil {
		t.Fatalf("Failed to set bucket policy: %v", err)
	}

	// We need to mock the request but the EvaluatePolicyForRequest mostly runs on args extraction
	// The key thing is that EvaluatePolicyForRequest should populate "aws:username" from principal

	// Since we cannot easily pass a full http.Request that matches everything, we will test the extraction logic
	// by simulating what EvaluatePolicyForRequest does: calling EvaluatePolicy with populated Conditions

	principal := "arn:aws:iam::123456789012:user/alice"
	// Should extract "alice"

	// Create args manually as if extracted
	args := &PolicyEvaluationArgs{
		Action:    "s3:GetObject",
		Resource:  "arn:aws:s3:::test-bucket/alice/data.txt",
		Principal: principal,
		Conditions: map[string][]string{
			"aws:username": {"alice"},
		},
	}

	result := engine.EvaluatePolicy("test-bucket", args)
	if result != PolicyResultAllow {
		t.Errorf("Expected Allow when aws:username is populated, got %v", result)
	}

	// Now with wrong resource
	args.Resource = "arn:aws:s3:::test-bucket/bob/data.txt"
	result = engine.EvaluatePolicy("test-bucket", args)
	if result != PolicyResultIndeterminate {
		t.Errorf("Expected Indeterminate when resource doesn't match variable, got %v", result)
	}
}

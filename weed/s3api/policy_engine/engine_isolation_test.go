package policy_engine

import (
	"fmt"
	"testing"
)

func TestIsolationPolicy(t *testing.T) {
	engine := NewPolicyEngine()
	bucketName := "test-isolation"

	policyJSON := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Sid": "AllowOwnFolder",
			"Effect": "Allow",
			"Principal": "*",
			"Action": ["s3:GetObject", "s3:PutObject"],
			"Resource": "arn:aws:s3:::%s/${aws:username}/*"
		}, {
			"Sid": "AllowListOwnPrefix",
			"Effect": "Allow",
			"Principal": "*",
			"Action": "s3:ListBucket",
			"Resource": "arn:aws:s3:::%s",
			"Condition": {
				"StringLike": {
					"s3:prefix": ["${aws:username}/*", "${aws:username}"]
				}
			}
		}, {
			"Sid": "DenyOtherFolders",
			"Effect": "Deny",
			"Principal": "*",
			"Action": ["s3:GetObject", "s3:PutObject"],
			"NotResource": "arn:aws:s3:::%s/${aws:username}/*"
		}]
	}`, bucketName, bucketName, bucketName)

	err := engine.SetBucketPolicy(bucketName, policyJSON)
	if err != nil {
		t.Fatalf("Failed to set bucket policy: %v", err)
	}

	// Case 1: Alice accesses her own folder -> should be ALLOWED
	args := &PolicyEvaluationArgs{
		Action:    "s3:GetObject",
		Resource:  fmt.Sprintf("arn:aws:s3:::%s/alice/data.txt", bucketName),
		Principal: "arn:aws:sts::123456789012:assumed-role/TestReadOnlyRole/alice",
		Conditions: map[string][]string{
			"aws:username": {"alice"},
		},
	}
	result := engine.EvaluatePolicy(bucketName, args)
	if result != PolicyResultAllow {
		t.Errorf("Alice should be ALLOWED to her own folder, got %v", result)
	}

	// Case 2: Alice accesses Bob's folder -> should be DENIED
	args = &PolicyEvaluationArgs{
		Action:    "s3:GetObject",
		Resource:  fmt.Sprintf("arn:aws:s3:::%s/bob/data.txt", bucketName),
		Principal: "arn:aws:sts::123456789012:assumed-role/TestReadOnlyRole/alice",
		Conditions: map[string][]string{
			"aws:username": {"alice"},
		},
	}
	result = engine.EvaluatePolicy(bucketName, args)
	if result != PolicyResultDeny {
		t.Errorf("Alice should be DENIED access to Bob's folder, got %v", result)
	}

	// Case 3: Bob accesses Bob's folder -> should be ALLOWED
	args = &PolicyEvaluationArgs{
		Action:    "s3:GetObject",
		Resource:  fmt.Sprintf("arn:aws:s3:::%s/bob/data.txt", bucketName),
		Principal: "arn:aws:sts::123456789012:assumed-role/TestReadOnlyRole/bob",
		Conditions: map[string][]string{
			"aws:username": {"bob"},
		},
	}
	result = engine.EvaluatePolicy(bucketName, args)
	if result != PolicyResultAllow {
		t.Errorf("Bob should be ALLOWED to his own folder, got %v", result)
	}

	// Case 4: Bob accesses Alice's folder -> should be DENIED
	args = &PolicyEvaluationArgs{
		Action:    "s3:GetObject",
		Resource:  fmt.Sprintf("arn:aws:s3:::%s/alice/data.txt", bucketName),
		Principal: "arn:aws:sts::123456789012:assumed-role/TestReadOnlyRole/bob",
		Conditions: map[string][]string{
			"aws:username": {"bob"},
		},
	}
	result = engine.EvaluatePolicy(bucketName, args)
	if result != PolicyResultDeny {
		t.Errorf("Bob should be DENIED access to Alice's folder, got %v", result)
	}
}

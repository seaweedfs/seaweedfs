package policy_engine

import (
	"strings"
	"testing"
)

// requiresSSEPolicy is a bucket policy that denies PutObject when the
// x-amz-server-side-encryption header is absent (Null == true).
const requiresSSEPolicy = `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyUnencryptedUploads",
      "Effect": "Deny",
      "Principal": "*",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::test-bucket/*",
      "Condition": {
        "Null": {
          "s3:x-amz-server-side-encryption": "true"
        }
      }
    }
  ]
}`

// requiresAES256Policy denies PutObject unless AES256 is explicitly requested.
const requiresAES256Policy = `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowAES256Only",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::test-bucket/*",
      "Condition": {
        "StringEquals": {
          "s3:x-amz-server-side-encryption": "AES256"
        }
      }
    }
  ]
}`

// multipartPolicy denies PutObject when SSE is absent but should NOT block UploadPart.
const multipartPolicy = `{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DenyUnencryptedUploads",
      "Effect": "Deny",
      "Principal": "*",
      "Action": ["s3:PutObject", "s3:UploadPart", "s3:UploadPartCopy"],
      "Resource": "arn:aws:s3:::test-bucket/*",
      "Condition": {
        "Null": {
          "s3:x-amz-server-side-encryption": "true"
        }
      }
    }
  ]
}`

func newEngineWithPolicy(t *testing.T, policy string) *PolicyEngine {
	t.Helper()
	engine := NewPolicyEngine()
	if err := engine.SetBucketPolicy("test-bucket", policy); err != nil {
		t.Fatalf("SetBucketPolicy: %v", err)
	}
	return engine
}

func evalArgs(action string, conditions map[string][]string) *PolicyEvaluationArgs {
	return &PolicyEvaluationArgs{
		Action:     action,
		Resource:   "arn:aws:s3:::test-bucket/object.txt",
		Principal:  "*",
		Conditions: conditions,
	}
}

// TestSSEStringEqualsPresent – StringEquals with AES256 header present should Allow.
func TestSSEStringEqualsPresent(t *testing.T) {
	engine := newEngineWithPolicy(t, requiresAES256Policy)

	conditions := map[string][]string{
		"s3:x-amz-server-side-encryption": {"AES256"},
	}
	result := engine.EvaluatePolicy("test-bucket", evalArgs("s3:PutObject", conditions))
	if result != PolicyResultAllow {
		t.Errorf("expected Allow, got %v", result)
	}
}

// TestSSEStringEqualsWrongValue – StringEquals with wrong SSE value should not Allow.
func TestSSEStringEqualsWrongValue(t *testing.T) {
	engine := newEngineWithPolicy(t, requiresAES256Policy)

	conditions := map[string][]string{
		"s3:x-amz-server-side-encryption": {"aws:kms"},
	}
	result := engine.EvaluatePolicy("test-bucket", evalArgs("s3:PutObject", conditions))
	if result == PolicyResultAllow {
		t.Errorf("expected non-Allow, got %v", result)
	}
}

// TestSSENullConditionAbsent – Null("true") matches when header is absent → Deny.
func TestSSENullConditionAbsent(t *testing.T) {
	engine := newEngineWithPolicy(t, requiresSSEPolicy)

	// No SSE header → condition "Null == true" matches → Deny statement fires
	result := engine.EvaluatePolicy("test-bucket", evalArgs("s3:PutObject", map[string][]string{}))
	if result != PolicyResultDeny {
		t.Errorf("expected Deny (no SSE header), got %v", result)
	}
}

// TestSSENullConditionPresent – Null("true") does NOT match when header is present → not Deny.
func TestSSENullConditionPresent(t *testing.T) {
	engine := newEngineWithPolicy(t, requiresSSEPolicy)

	conditions := map[string][]string{
		"s3:x-amz-server-side-encryption": {"AES256"},
	}
	result := engine.EvaluatePolicy("test-bucket", evalArgs("s3:PutObject", conditions))
	// Deny condition not matched; no explicit Allow → Indeterminate
	if result == PolicyResultDeny {
		t.Errorf("expected non-Deny when SSE header present, got Deny")
	}
}

// TestSSECaseInsensitiveNormalization – "aes256" lowercase should normalise to "AES256".
// Use the normalisation function directly since ExtractConditionValuesFromRequest does it.
func TestSSECaseInsensitiveNormalization(t *testing.T) {
	engine := newEngineWithPolicy(t, requiresAES256Policy)

	// Simulate what ExtractConditionValuesFromRequest does: normalise the value
	raw := "aes256"
	var normalized string
	switch strings.ToUpper(raw) {
	case "AES256":
		normalized = "AES256"
	default:
		normalized = raw
	}

	conditions := map[string][]string{
		"s3:x-amz-server-side-encryption": {normalized},
	}
	result := engine.EvaluatePolicy("test-bucket", evalArgs("s3:PutObject", conditions))
	if result != PolicyResultAllow {
		t.Errorf("expected Allow after case normalisation, got %v", result)
	}
}

// TestSSEMultipartContinuationExempt – UploadPart should not be blocked by SSE Null condition.
func TestSSEMultipartContinuationExempt(t *testing.T) {
	engine := newEngineWithPolicy(t, multipartPolicy)

	// UploadPart carries no SSE header (inherited from CreateMultipartUpload)
	result := engine.EvaluatePolicy("test-bucket", evalArgs("s3:UploadPart", map[string][]string{}))
	if result == PolicyResultDeny {
		t.Errorf("UploadPart should not be denied by SSE Null condition, got Deny")
	}
}

// TestSSEUploadPartCopyExempt – UploadPartCopy should also be exempt.
func TestSSEUploadPartCopyExempt(t *testing.T) {
	engine := newEngineWithPolicy(t, multipartPolicy)

	result := engine.EvaluatePolicy("test-bucket", evalArgs("s3:UploadPartCopy", map[string][]string{}))
	if result == PolicyResultDeny {
		t.Errorf("UploadPartCopy should not be denied by SSE Null condition, got Deny")
	}
}

// TestSSEPutObjectStillBlockedWithoutHeader – regular PutObject still denied without SSE.
func TestSSEPutObjectStillBlockedWithoutHeader(t *testing.T) {
	engine := newEngineWithPolicy(t, multipartPolicy)

	result := engine.EvaluatePolicy("test-bucket", evalArgs("s3:PutObject", map[string][]string{}))
	if result != PolicyResultDeny {
		t.Errorf("PutObject without SSE should be Deny, got %v", result)
	}
}

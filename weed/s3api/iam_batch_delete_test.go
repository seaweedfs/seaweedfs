package s3api

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/require"
)

// TestAuthorizeBatchDeleteKey_AwsCanonicalPolicy: a policy granting s3:DeleteObject
// on <bucket>/* must allow per-key batch deletes. Pre-fix the bucket-level check
// built arn:aws:s3:::<bucket> and never matched the object-scoped policy.
func TestAuthorizeBatchDeleteKey_AwsCanonicalPolicy(t *testing.T) {
	const bucket = "test-bucket"
	const policyName = "delete-test-bucket-objects"

	policyDoc, err := json.Marshal(map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Effect":   "Allow",
				"Action":   "s3:DeleteObject",
				"Resource": "arn:aws:s3:::" + bucket + "/*",
			},
		},
	})
	require.NoError(t, err)

	iam := &IdentityAccessManagement{
		isAuthEnabled: true,
	}
	require.NoError(t, iam.PutPolicy(policyName, string(policyDoc)))

	identity := &Identity{
		Name:        "alice",
		Account:     &AccountAdmin,
		PolicyNames: []string{policyName},
		Credentials: []*Credential{{AccessKey: "AKIAEXAMPLE", SecretKey: "secret"}},
	}

	r := httptest.NewRequest("POST", "/"+bucket+"?delete", nil)

	require.Equal(t, s3err.ErrNone,
		iam.AuthorizeBatchDeleteKey(r, identity, bucket, "objects/a.txt", ""),
		"s3:DeleteObject on arn:aws:s3:::%s/* must allow deleting %s/objects/a.txt", bucket, bucket)

	require.Equal(t, s3err.ErrAccessDenied,
		iam.AuthorizeBatchDeleteKey(r, identity, "other-bucket", "objects/a.txt", ""),
		"keys outside the granted bucket must be denied")
}

// TestAuthorizeBatchDeleteKey_PrefixScopedPolicy: a prefix-scoped policy must allow
// batch deletes under the prefix and deny keys outside it, per-key.
func TestAuthorizeBatchDeleteKey_PrefixScopedPolicy(t *testing.T) {
	const bucket = "test-bucket"
	const policyName = "delete-prefix-only"

	policyDoc, err := json.Marshal(map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{
			{
				"Effect":   "Allow",
				"Action":   "s3:DeleteObject",
				"Resource": "arn:aws:s3:::" + bucket + "/safe/*",
			},
		},
	})
	require.NoError(t, err)

	iam := &IdentityAccessManagement{
		isAuthEnabled: true,
	}
	require.NoError(t, iam.PutPolicy(policyName, string(policyDoc)))

	identity := &Identity{
		Name:        "alice",
		Account:     &AccountAdmin,
		PolicyNames: []string{policyName},
		Credentials: []*Credential{{AccessKey: "AKIAEXAMPLE", SecretKey: "secret"}},
	}

	r := httptest.NewRequest("POST", "/"+bucket+"?delete", nil)

	require.Equal(t, s3err.ErrNone,
		iam.AuthorizeBatchDeleteKey(r, identity, bucket, "safe/inside.txt", ""),
		"key under granted prefix must be allowed")

	require.Equal(t, s3err.ErrAccessDenied,
		iam.AuthorizeBatchDeleteKey(r, identity, bucket, "danger/outside.txt", ""),
		"key outside the granted prefix must be denied per-key, not at the batch level")
}

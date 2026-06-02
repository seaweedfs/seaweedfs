package s3api

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/require"
)

// ListObjects requests carry their key scope in the ?prefix= query parameter,
// which authRequestWithAuthType promotes into object so the legacy CanDo path
// can honor prefix-scoped Action strings. That promoted object must not reach
// the policy resource ARN: s3:ListBucket is a bucket-level action, so the
// resource stays arn:aws:s3:::bucket and any prefix scoping is expressed via
// the s3:prefix Condition.
func TestEvaluateIAMPolicies_ListBucketWithPrefix(t *testing.T) {
	const bucket = "test-bucket"

	iam := &IdentityAccessManagement{}
	require.NoError(t, iam.PutPolicy("list-bucket", mustPolicy(t, map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{{
			"Effect":   "Allow",
			"Action":   "s3:ListBucket",
			"Resource": "arn:aws:s3:::" + bucket,
		}},
	})))

	identity := &Identity{
		Name:        "alice",
		Account:     &AccountAdmin,
		PolicyNames: []string{"list-bucket"},
		Credentials: []*Credential{{AccessKey: "AKIAEXAMPLE", SecretKey: "secret"}},
	}

	// authRequestWithAuthType promotes prefix into object before reaching the
	// IAM evaluator; pass the post-promotion value to mirror that flow.
	withPrefix := httptest.NewRequest("GET", "/"+bucket+"?list-type=2&prefix=foo/", nil)
	require.True(t, iam.evaluateIAMPolicies(withPrefix, identity, s3_constants.ACTION_LIST, bucket, "foo/"),
		"s3:ListBucket on the bucket ARN must allow listing with a prefix")

	noPrefix := httptest.NewRequest("GET", "/"+bucket+"?list-type=2", nil)
	require.True(t, iam.evaluateIAMPolicies(noPrefix, identity, s3_constants.ACTION_LIST, bucket, ""),
		"s3:ListBucket on the bucket ARN must allow listing without a prefix")
}

// Prefix scoping still works once it moves to the Condition: a policy that
// grants s3:ListBucket on the bucket only under an s3:prefix StringLike must
// allow a matching prefix and deny a non-matching one.
func TestEvaluateIAMPolicies_ListBucketPrefixCondition(t *testing.T) {
	const bucket = "test-bucket"

	iam := &IdentityAccessManagement{}
	require.NoError(t, iam.PutPolicy("list-scoped", mustPolicy(t, map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{{
			"Effect":    "Allow",
			"Action":    "s3:ListBucket",
			"Resource":  "arn:aws:s3:::" + bucket,
			"Condition": map[string]any{"StringLike": map[string]any{"s3:prefix": "warehouse/*"}},
		}},
	})))

	identity := &Identity{
		Name:        "bob",
		Account:     &AccountAdmin,
		PolicyNames: []string{"list-scoped"},
		Credentials: []*Credential{{AccessKey: "AKIAEXAMPLE", SecretKey: "secret"}},
	}

	matching := httptest.NewRequest("GET", "/"+bucket+"?list-type=2&prefix=warehouse/data", nil)
	require.True(t, iam.evaluateIAMPolicies(matching, identity, s3_constants.ACTION_LIST, bucket, "warehouse/data"),
		"prefix matching the s3:prefix condition must be allowed")

	nonMatching := httptest.NewRequest("GET", "/"+bucket+"?list-type=2&prefix=secrets/", nil)
	require.False(t, iam.evaluateIAMPolicies(nonMatching, identity, s3_constants.ACTION_LIST, bucket, "secrets/"),
		"prefix outside the s3:prefix condition must be denied")
}

func mustPolicy(t *testing.T, doc map[string]any) string {
	t.Helper()
	b, err := json.Marshal(doc)
	require.NoError(t, err)
	return string(b)
}

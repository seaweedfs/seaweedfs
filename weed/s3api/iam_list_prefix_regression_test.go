package s3api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
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

// Listing variants keep their specific action even when a prefix is promoted
// into object: ?versions resolves to s3:ListBucketVersions, not s3:ListBucket.
func TestEvaluateIAMPolicies_ListBucketVersionsWithPrefix(t *testing.T) {
	const bucket = "test-bucket"

	iam := &IdentityAccessManagement{}
	require.NoError(t, iam.PutPolicy("list-versions", mustPolicy(t, map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{{
			"Effect":   "Allow",
			"Action":   "s3:ListBucketVersions",
			"Resource": "arn:aws:s3:::" + bucket,
		}},
	})))

	identity := &Identity{
		Name:        "carol",
		Account:     &AccountAdmin,
		PolicyNames: []string{"list-versions"},
		Credentials: []*Credential{{AccessKey: "AKIAEXAMPLE", SecretKey: "secret"}},
	}

	r := httptest.NewRequest("GET", "/"+bucket+"?versions&prefix=foo/", nil)
	require.True(t, iam.evaluateIAMPolicies(r, identity, s3_constants.ACTION_LIST, bucket, "foo/"),
		"s3:ListBucketVersions must still resolve when listing with a prefix")
}

// The IAM-integration authorizer must resolve the listing variant the same way
// evaluateIAMPolicies does: a prefix promoted into the object argument is not
// part of the request URL, so ?versions&prefix=... still resolves to
// s3:ListBucketVersions and an s3:ListBucket grant must not cover it.
func TestAuthorizeAction_ListVersionsWithPromotedPrefix(t *testing.T) {
	const bucket = "test-bucket"

	ctx := context.Background()
	iamManager := integration.NewIAMManager()
	require.NoError(t, iamManager.Initialize(&integration.IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
		},
		Policy: &policy.PolicyEngineConfig{DefaultEffect: "Deny", StoreType: "memory"},
		Roles:  &integration.RoleStoreConfig{StoreType: "memory"},
	}, func() string { return "localhost:8888" }))

	require.NoError(t, iamManager.CreatePolicy(ctx, "", "ListBucketOnly", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect:    "Allow",
			Action:    []string{"s3:ListBucket"},
			Resource:  []string{"arn:aws:s3:::" + bucket},
			Condition: map[string]map[string]interface{}{"StringLike": {"s3:prefix": "b/*"}},
		}},
	}))
	require.NoError(t, iamManager.CreatePolicy(ctx, "", "ListVersionsOnly", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect:   "Allow",
			Action:   []string{"s3:ListBucketVersions"},
			Resource: []string{"arn:aws:s3:::" + bucket},
		}},
	}))

	s3iam := NewS3IAMIntegration(iamManager, "localhost:8888")
	reader := &IAMIdentity{
		Name:        "reader",
		Principal:   "arn:aws:iam::000000000000:user/reader",
		PolicyNames: []string{"ListBucketOnly"},
	}

	versionsReq := httptest.NewRequest("GET", "/"+bucket+"?versions&prefix=b/", nil)
	// object carries the promoted prefix, matching authRequestWithAuthType
	require.Equal(t, s3err.ErrAccessDenied,
		s3iam.AuthorizeAction(ctx, reader, s3_constants.ACTION_LIST, bucket, "b/", versionsReq),
		"an s3:ListBucket grant must not cover ?versions listing")

	listReq := httptest.NewRequest("GET", "/"+bucket+"?list-type=2&prefix=b/", nil)
	require.Equal(t, s3err.ErrNone,
		s3iam.AuthorizeAction(ctx, reader, s3_constants.ACTION_LIST, bucket, "b/", listReq),
		"s3:ListBucket with a matching s3:prefix still lists")

	versionsReader := &IAMIdentity{
		Name:        "versions-reader",
		Principal:   "arn:aws:iam::000000000000:user/versions-reader",
		PolicyNames: []string{"ListVersionsOnly"},
	}
	require.Equal(t, s3err.ErrNone,
		s3iam.AuthorizeAction(ctx, versionsReader, s3_constants.ACTION_LIST, bucket, "b/", versionsReq),
		"s3:ListBucketVersions allows the versions listing")
}

func mustPolicy(t *testing.T, doc map[string]any) string {
	t.Helper()
	b, err := json.Marshal(doc)
	require.NoError(t, err)
	return string(b)
}

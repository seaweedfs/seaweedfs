package s3api

import (
	"context"
	"encoding/json"
	"net/http"
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

// A listing without ?prefix is the empty prefix, so the home-directory policy
// that grants s3:prefix "" for the root level matches it in attached and
// bucket policies alike.
func TestListBucketWithoutPrefixMatchesEmptyPrefix(t *testing.T) {
	const bucket = "test-bucket"
	statement := map[string]any{
		"Effect":   "Allow",
		"Action":   "s3:ListBucket",
		"Resource": "arn:aws:s3:::" + bucket,
		"Condition": map[string]any{"StringEquals": map[string]any{
			"s3:prefix":    []string{"", "home/"},
			"s3:delimiter": "/",
		}},
	}
	iam := &IdentityAccessManagement{}
	require.NoError(t, iam.PutPolicy("home-root", mustPolicy(t, map[string]any{
		"Version": "2012-10-17", "Statement": []map[string]any{statement},
	})))
	identity := &Identity{Name: "carol", Account: &AccountAdmin, PolicyNames: []string{"home-root"}}

	statement["Principal"] = "*"
	engine := NewBucketPolicyEngine()
	require.NoError(t, engine.engine.SetBucketPolicy(bucket, mustPolicy(t, map[string]any{
		"Version": "2012-10-17", "Statement": []map[string]any{statement},
	})))

	for _, tt := range []struct {
		query   string
		allowed bool
	}{
		{"list-type=2&delimiter=/", true},
		{"list-type=2&prefix=&delimiter=/", true},
		{"list-type=2&prefix=home/&delimiter=/", true},
		{"list-type=2&prefix=other/&delimiter=/", false},
		{"list-type=2", false},
	} {
		req := httptest.NewRequest("GET", "/"+bucket+"?"+tt.query, nil)
		require.Equal(t, tt.allowed, iam.evaluateIAMPolicies(req, identity, s3_constants.ACTION_LIST, bucket, ""), "attached policy, %s", tt.query)
		allowed, evaluated, err := engine.EvaluatePolicy(bucket, "", string(s3_constants.ACTION_LIST), "arn:aws:iam::000000000000:user/carol", req, nil, nil)
		require.NoError(t, err)
		require.Equal(t, tt.allowed, evaluated && allowed, "bucket policy, %s", tt.query)
	}
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

// GET ?uploads routes under ACTION_READ, not ACTION_LIST, but it is a bucket
// listing: a promoted prefix must not resolve it to s3:GetObject, and only
// s3:ListBucketMultipartUploads on the bucket ARN may allow it.
func TestEvaluateIAMPolicies_ListMultipartUploadsWithPrefix(t *testing.T) {
	const bucket = "test-bucket"

	iam := &IdentityAccessManagement{}
	require.NoError(t, iam.PutPolicy("list-uploads", mustPolicy(t, map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{{
			"Effect":   "Allow",
			"Action":   "s3:ListBucketMultipartUploads",
			"Resource": "arn:aws:s3:::" + bucket,
		}},
	})))
	require.NoError(t, iam.PutPolicy("get-object", mustPolicy(t, map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{{
			"Effect":   "Allow",
			"Action":   "s3:GetObject",
			"Resource": "arn:aws:s3:::" + bucket + "/*",
		}},
	})))

	uploadsReader := &Identity{
		Name:        "dave",
		Account:     &AccountAdmin,
		PolicyNames: []string{"list-uploads"},
		Credentials: []*Credential{{AccessKey: "AKIAEXAMPLE", SecretKey: "secret"}},
	}
	getReader := &Identity{
		Name:        "erin",
		Account:     &AccountAdmin,
		PolicyNames: []string{"get-object"},
		Credentials: []*Credential{{AccessKey: "AKIAEXAMPLE", SecretKey: "secret"}},
	}

	r := httptest.NewRequest("GET", "/"+bucket+"?uploads&prefix=uploads/", nil)
	require.True(t, iam.evaluateIAMPolicies(r, uploadsReader, s3_constants.ACTION_READ, bucket, "uploads/"),
		"s3:ListBucketMultipartUploads on the bucket must allow the uploads listing")
	require.False(t, iam.evaluateIAMPolicies(r, getReader, s3_constants.ACTION_READ, bucket, "uploads/"),
		"s3:GetObject must not satisfy the uploads listing")
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
	require.NoError(t, iamManager.CreatePolicy(ctx, "", "ListUploadsOnly", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect:   "Allow",
			Action:   []string{"s3:ListBucketMultipartUploads"},
			Resource: []string{"arn:aws:s3:::" + bucket},
		}},
	}))
	require.NoError(t, iamManager.CreatePolicy(ctx, "", "GetObjectOnly", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect:   "Allow",
			Action:   []string{"s3:GetObject"},
			Resource: []string{"arn:aws:s3:::" + bucket + "/*"},
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

	// GET ?uploads routes under ACTION_READ; a promoted prefix must still
	// resolve it to s3:ListBucketMultipartUploads, not s3:GetObject.
	uploadsReq := httptest.NewRequest("GET", "/"+bucket+"?uploads&prefix=b/", nil)
	uploadsReader := &IAMIdentity{
		Name:        "uploads-reader",
		Principal:   "arn:aws:iam::000000000000:user/uploads-reader",
		PolicyNames: []string{"ListUploadsOnly"},
	}
	require.Equal(t, s3err.ErrNone,
		s3iam.AuthorizeAction(ctx, uploadsReader, s3_constants.ACTION_READ, bucket, "b/", uploadsReq),
		"s3:ListBucketMultipartUploads allows the uploads listing")
	getReader := &IAMIdentity{
		Name:        "get-reader",
		Principal:   "arn:aws:iam::000000000000:user/get-reader",
		PolicyNames: []string{"GetObjectOnly"},
	}
	require.Equal(t, s3err.ErrAccessDenied,
		s3iam.AuthorizeAction(ctx, getReader, s3_constants.ACTION_READ, bucket, "b/", uploadsReq),
		"s3:GetObject must not satisfy the uploads listing")
}

// Bucket-listing policies are evaluated by the IAM integration. Its authorizer
// must expose prefix, delimiter and max-keys to conditions, and resolve the
// listing variant (?versions, ?uploads) to its own action.
func TestAuthorizeAction_BucketListingConditions(t *testing.T) {
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

	allow := func(name, action string, condition map[string]map[string]interface{}) {
		t.Helper()
		require.NoError(t, iamManager.CreatePolicy(ctx, "", name, &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{{
				Effect:    "Allow",
				Action:    []string{action},
				Resource:  []string{"arn:aws:s3:::" + bucket},
				Condition: condition,
			}},
		}))
	}
	// One-level listing of home/: a prefix alone is recursive, so the
	// delimiter is what narrows the grant.
	homeOneLevel := map[string]map[string]interface{}{"StringEquals": {
		"s3:prefix":    "home/",
		"s3:delimiter": "/",
	}}
	allow("HomeOneLevel", "s3:ListBucket", homeOneLevel)
	allow("VersionsHomeOneLevel", "s3:ListBucketVersions", homeOneLevel)
	allow("UploadsHomeOneLevel", "s3:ListBucketMultipartUploads", homeOneLevel)
	allow("MaxKeysTen", "s3:ListBucket", map[string]map[string]interface{}{
		"NumericLessThanEquals": {"s3:max-keys": 10},
	})
	allow("VersionsMaxKeysTen", "s3:ListBucketVersions", map[string]map[string]interface{}{
		"NumericLessThanEquals": {"s3:max-keys": 10},
	})
	allow("UploadsMaxKeysTen", "s3:ListBucketMultipartUploads", map[string]map[string]interface{}{
		"NumericLessThanEquals": {"s3:max-keys": 10},
	})

	s3iam := NewS3IAMIntegration(iamManager, "localhost:8888")

	tests := []struct {
		name     string
		policy   string
		url      string
		action   Action
		object   string
		expected s3err.ErrorCode
	}{
		{"ListObjectsV2 one level with the permitted delimiter", "HomeOneLevel", "/" + bucket + "?list-type=2&prefix=home/&delimiter=/", s3_constants.ACTION_LIST, "home/", s3err.ErrNone},
		{"recursive ListObjectsV2 without a delimiter is denied", "HomeOneLevel", "/" + bucket + "?list-type=2&prefix=home/", s3_constants.ACTION_LIST, "home/", s3err.ErrAccessDenied},
		{"nested prefix with the permitted delimiter is denied", "HomeOneLevel", "/" + bucket + "?list-type=2&prefix=home/bob/&delimiter=/", s3_constants.ACTION_LIST, "home/bob/", s3err.ErrAccessDenied},
		{"different delimiter is denied", "HomeOneLevel", "/" + bucket + "?list-type=2&prefix=home/&delimiter=.", s3_constants.ACTION_LIST, "home/", s3err.ErrAccessDenied},
		{"bucket root listing is denied", "HomeOneLevel", "/" + bucket + "?list-type=2&delimiter=/", s3_constants.ACTION_LIST, "", s3err.ErrAccessDenied},
		{"ListObjectVersions one level is allowed", "VersionsHomeOneLevel", "/" + bucket + "?versions&prefix=home/&delimiter=/", s3_constants.ACTION_LIST, "home/", s3err.ErrNone},
		{"ListObjectVersions without a delimiter is denied", "VersionsHomeOneLevel", "/" + bucket + "?versions&prefix=home/", s3_constants.ACTION_LIST, "home/", s3err.ErrAccessDenied},
		{"ListMultipartUploads one level is allowed", "UploadsHomeOneLevel", "/" + bucket + "?uploads&prefix=home/&delimiter=/", s3_constants.ACTION_READ, "", s3err.ErrNone},
		{"ListMultipartUploads without a delimiter is denied", "UploadsHomeOneLevel", "/" + bucket + "?uploads&prefix=home/", s3_constants.ACTION_READ, "", s3err.ErrAccessDenied},
		{"max-keys within s3:max-keys is allowed", "MaxKeysTen", "/" + bucket + "?list-type=2&prefix=home/&delimiter=/&max-keys=5", s3_constants.ACTION_LIST, "home/", s3err.ErrNone},
		{"max-keys above s3:max-keys is denied", "MaxKeysTen", "/" + bucket + "?list-type=2&prefix=home/&delimiter=/&max-keys=20", s3_constants.ACTION_LIST, "home/", s3err.ErrAccessDenied},
		{"versions max-keys within limit is allowed", "VersionsMaxKeysTen", "/" + bucket + "?versions&max-keys=5", s3_constants.ACTION_LIST, "", s3err.ErrNone},
		{"versions max-keys above limit is denied", "VersionsMaxKeysTen", "/" + bucket + "?versions&max-keys=20", s3_constants.ACTION_LIST, "", s3err.ErrAccessDenied},
		{"uploads max-uploads does not satisfy max-keys", "UploadsMaxKeysTen", "/" + bucket + "?uploads&max-uploads=5", s3_constants.ACTION_READ, "", s3err.ErrAccessDenied},
		{"uploads spoofed max-keys does not satisfy policy", "UploadsMaxKeysTen", "/" + bucket + "?uploads&max-uploads=1000&max-keys=5", s3_constants.ACTION_READ, "", s3err.ErrAccessDenied},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identity := &IAMIdentity{
				Name:        "reader",
				Principal:   "arn:aws:iam::000000000000:user/reader",
				PolicyNames: []string{tt.policy},
			}
			req := httptest.NewRequest("GET", tt.url, http.NoBody)
			require.Equal(t, tt.expected,
				s3iam.AuthorizeAction(ctx, identity, tt.action, bucket, tt.object, req))
		})
	}
}

func mustPolicy(t *testing.T, doc map[string]any) string {
	t.Helper()
	b, err := json.Marshal(doc)
	require.NoError(t, err)
	return string(b)
}

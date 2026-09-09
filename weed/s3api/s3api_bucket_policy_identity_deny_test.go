package s3api

import (
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Reproducer for GHSA-xxc3-72qf-3g5r: a bucket-policy Allow must not override an
// applicable explicit Deny in an authenticated identity's attached policy.
//
// restricted-reader holds an attached IAM policy that allows GetObject on
// allowed/*, explicitly denies GetObject on secret/*, and may install a bucket
// policy. The bucket policy separately allows it GetObject on secret/* and
// other/*. AWS evaluation lets a resource policy supply the Allow an identity
// policy omits (other/*), but an explicit Deny must still win (secret/*).

const (
	bpdBucket       = "victim-bucket"
	bpdAccessKey    = "LOCALDENYKEY00000001"
	bpdSecretKey    = "local-deny-secret-for-loopback-only"
	bpdPrincipal    = "arn:aws:iam::000000000000:user/restricted-reader"
	bpdPolicyName   = "RestrictedRead"
	bpdIdentityName = "restricted-reader"
	bpdAccountID    = "000000000000"
	bpdAccountName  = "restricted-reader"
)

const bpdIdentityPolicy = `{
  "Version":"2012-10-17",
  "Statement":[
    {"Effect":"Allow","Action":"s3:PutBucketPolicy","Resource":"arn:aws:s3:::` + bpdBucket + `"},
    {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::` + bpdBucket + `/allowed/*"},
    {"Effect":"Deny","Action":"s3:GetObject","Resource":"arn:aws:s3:::` + bpdBucket + `/secret/*"}
  ]
}`

// bpdBucketPolicyDoc returns a bucket policy that allows restricted-reader
// GetObject on the given prefixes. The secret/* statement is the bypass under
// test; the other/* statement supplies an Allow over an implicit identity
// denial, which the fix must preserve.
func bpdBucketPolicyDoc(t *testing.T, prefixes ...string) string {
	t.Helper()
	stmts := ""
	for i, p := range prefixes {
		if i > 0 {
			stmts += ","
		}
		stmts += fmt.Sprintf(
			`{"Effect":"Allow","Principal":{"AWS":"%s"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::%s/%s"}`,
			bpdPrincipal, bpdBucket, p)
	}
	return fmt.Sprintf(`{"Version":"2012-10-17","Statement":[%s]}`, stmts)
}

// newBucketPolicyDenyIAM builds an IAM whose restricted-reader identity carries
// the attached RestrictedRead policy, plus a bucket policy engine that allows
// the reader on the supplied prefixes.
func newBucketPolicyDenyIAM(t *testing.T, bucketPolicy string) *IdentityAccessManagement {
	t.Helper()
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}
	err := iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Accounts: []*iam_pb.Account{
			{Id: bpdAccountID, DisplayName: bpdAccountName},
		},
		Identities: []*iam_pb.Identity{
			{
				Name:    "admin",
				Actions: []string{"Admin"},
				Credentials: []*iam_pb.Credential{
					{AccessKey: "LOCALADMINKEY0000001", SecretKey: "local-admin-secret-for-loopback-only"},
				},
			},
			{
				Name:        bpdIdentityName,
				Account:     &iam_pb.Account{Id: bpdAccountID, DisplayName: bpdAccountName},
				PolicyNames: []string{bpdPolicyName},
				Credentials: []*iam_pb.Credential{
					{AccessKey: bpdAccessKey, SecretKey: bpdSecretKey},
				},
			},
		},
		Policies: []*iam_pb.Policy{
			{Name: bpdPolicyName, Content: bpdIdentityPolicy},
		},
	})
	require.NoError(t, err)

	engine := NewBucketPolicyEngine()
	require.NoError(t, engine.engine.SetBucketPolicy(bpdBucket, bucketPolicy))
	iam.policyEngine = engine
	return iam
}

// bpdSignedRequest builds a SigV4-signed S3 request for the given key, signed
// as the restricted-reader, with mux vars set so GetBucketAndObject resolves.
func bpdSignedRequest(t *testing.T, method, objectKey string) *http.Request {
	t.Helper()
	urlStr := fmt.Sprintf("http://127.0.0.1:9000/%s/%s", bpdBucket, objectKey)
	req := mustNewRequest(method, urlStr, 0, nil, t)
	require.NoError(t, signRequestV4(req, bpdAccessKey, bpdSecretKey))
	return mux.SetURLVars(req, map[string]string{"bucket": bpdBucket, "object": objectKey})
}

// bpdReader looks up the loaded restricted-reader identity.
func bpdReader(t *testing.T, iam *IdentityAccessManagement) *Identity {
	t.Helper()
	ident := iam.lookupByIdentityName(bpdIdentityName)
	require.NotNil(t, ident, "restricted-reader must be loaded")
	return ident
}

// TestBucketPolicyAllowDoesNotOverrideIdentityExplicitDeny is the primary
// reproducer: a bucket-policy Allow on secret/* must not let restricted-reader
// read an object its attached policy explicitly denies.
func TestBucketPolicyAllowDoesNotOverrideIdentityExplicitDeny(t *testing.T) {
	iam := newBucketPolicyDenyIAM(t, bpdBucketPolicyDoc(t, "secret/*", "other/*"))

	// Precondition: the bucket policy alone would allow the reader on secret/*.
	req := bpdSignedRequest(t, http.MethodGet, "secret/payroll.txt")
	allowed, evaluated, err := iam.policyEngine.EvaluatePolicy(
		bpdBucket, "secret/payroll.txt", s3_constants.ACTION_READ, bpdPrincipal, req, nil, nil)
	require.NoError(t, err)
	require.True(t, evaluated && allowed, "bucket policy must allow the reader on secret/*")

	_, errCode := iam.authRequest(req, s3_constants.ACTION_READ)
	assert.Equal(t, s3err.ErrAccessDenied, errCode,
		"a bucket-policy Allow must not override an explicit Deny in the identity policy")
}

// TestBucketPolicyAllowSuppliesImplicitIdentityDeny preserves the cross-account
// behavior: a bucket-policy Allow on a prefix the identity policy does not
// mention (other/*) must still grant access.
func TestBucketPolicyAllowSuppliesImplicitIdentityDeny(t *testing.T) {
	iam := newBucketPolicyDenyIAM(t, bpdBucketPolicyDoc(t, "secret/*", "other/*"))

	_, errCode := iam.authRequest(bpdSignedRequest(t, http.MethodGet, "other/data.txt"), s3_constants.ACTION_READ)
	assert.Equal(t, s3err.ErrNone, errCode,
		"a bucket-policy Allow must supply the Allow an identity policy omits (implicit denial)")
}

// TestBucketPolicyAllowControlPathUnaffected checks the path the bucket policy
// does not match: allowed/* falls through to the identity policy, which allows.
func TestBucketPolicyAllowControlPathUnaffected(t *testing.T) {
	iam := newBucketPolicyDenyIAM(t, bpdBucketPolicyDoc(t, "secret/*", "other/*"))

	_, errCode := iam.authRequest(bpdSignedRequest(t, http.MethodGet, "allowed/report.txt"), s3_constants.ACTION_READ)
	assert.Equal(t, s3err.ErrNone, errCode,
		"a key the bucket policy does not match must fall through to the identity policy")
}

// TestAuthorizeObjectKeyActionBucketAllowDoesNotOverrideIdentityDeny covers the
// secondary object-key authorization path (CopySource, DeleteObjects body
// keys, POST Object form keys): the same bucket-Allow short-circuit must also
// honor an applicable explicit identity Deny. Added with the secondary-path
// fix in a follow-up commit.

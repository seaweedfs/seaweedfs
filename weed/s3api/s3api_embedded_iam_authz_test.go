package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	dataPlanePolicy        = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	iamAdminPolicy         = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iam:*","Resource":"*"}]}`
	iamCreateSvcAcctPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["iam:CreateServiceAccount"],"Resource":["*"]}]}`
	// The wildcard ARN shape this authorization used pre-#11218: it must keep
	// matching any parent, so existing grants survive the change.
	iamCreateSvcAcctWildcardArnPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["iam:CreateServiceAccount"],"Resource":["arn:aws:iam:::*"]}]}`
	// Scoped to one parent identity: the keymaker use case — mint service
	// accounts for the shared zone parent, and for nothing else.
	iamCreateSvcAcctZonePolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["iam:CreateServiceAccount"],"Resource":["arn:aws:iam:::user/team-parent"]}]}`
)

func newIamAuthzTestIam(t *testing.T) *IdentityAccessManagement {
	t.Helper()
	iam := &IdentityAccessManagement{}
	require.NoError(t, iam.PutPolicy("PowerUserPolicy", dataPlanePolicy))
	require.NoError(t, iam.PutPolicy("IamAdminPolicy", iamAdminPolicy))
	return iam
}

func iamPostRequest(rawQuery string) *http.Request {
	url := "http://s3.example.com/"
	if rawQuery != "" {
		url += "?" + rawQuery
	}
	return httptest.NewRequest(http.MethodPost, url, nil)
}

// A policy granting the S3 data plane must not reach IAM management, including
// when the request carries an S3 query parameter — the action resolver used to
// read the request shape even for an iam: action.
func TestAuthorizeIamActionDeniesDataPlanePolicy(t *testing.T) {
	iam := newIamAuthzTestIam(t)
	identity := &Identity{Name: "power_user", PolicyNames: []string{"PowerUserPolicy"}}

	for _, rawQuery := range []string{"", "delete", "acl", "tagging", "policy", "versions"} {
		t.Run("query="+rawQuery, func(t *testing.T) {
			assert.Equal(t, s3err.ErrAccessDenied,
				iam.AuthorizeIamAction(iamPostRequest(rawQuery), identity, "CreateUser", "victim"))
			assert.Equal(t, s3err.ErrAccessDenied,
				iam.AuthorizeIamAction(iamPostRequest(rawQuery), identity, "CreateAccessKey", "victim"))
		})
	}
}

func TestAuthorizeIamActionAllowsIamPolicy(t *testing.T) {
	iam := newIamAuthzTestIam(t)
	identity := &Identity{Name: "iam_operator", PolicyNames: []string{"IamAdminPolicy"}}

	for _, rawQuery := range []string{"", "delete"} {
		t.Run("query="+rawQuery, func(t *testing.T) {
			assert.Equal(t, s3err.ErrNone,
				iam.AuthorizeIamAction(iamPostRequest(rawQuery), identity, "CreateUser", "victim"))
		})
	}
}

func TestAuthorizeIamActionSelfServiceAndAdmin(t *testing.T) {
	iam := newIamAuthzTestIam(t)
	powerUser := &Identity{Name: "power_user", PolicyNames: []string{"PowerUserPolicy"}}
	admin := &Identity{Name: "admin", Actions: []Action{s3_constants.ACTION_ADMIN}}

	assert.Equal(t, s3err.ErrNone, iam.AuthorizeIamAction(iamPostRequest(""), powerUser, "CreateAccessKey", ""))
	assert.Equal(t, s3err.ErrNone, iam.AuthorizeIamAction(iamPostRequest(""), powerUser, "CreateAccessKey", "power_user"))
	assert.Equal(t, s3err.ErrNone, iam.AuthorizeIamAction(iamPostRequest(""), admin, "CreateUser", "victim"))
	assert.Equal(t, s3err.ErrAccessDenied, iam.AuthorizeIamAction(iamPostRequest(""), nil, "CreateUser", "victim"))
}

// An identity with no grant at all is the negative control: the route itself
// was never open, so a denial here has to come from the policy check.
func TestAuthorizeIamActionDeniesUngrantedIdentity(t *testing.T) {
	iam := newIamAuthzTestIam(t)
	identity := &Identity{Name: "nobody"}

	assert.Equal(t, s3err.ErrAccessDenied,
		iam.AuthorizeIamAction(iamPostRequest(""), identity, "CreateUser", "victim"))
}

// A configured anonymous identity must not slip in through the self-service
// carve-out, which would otherwise hand it the caller-implied user name.
func TestAuthorizeIamActionDeniesAnonymous(t *testing.T) {
	iam := newIamAuthzTestIam(t)
	anonymous := &Identity{Name: s3_constants.AccountAnonymousId, PolicyNames: []string{"IamAdminPolicy"}}

	assert.Equal(t, s3err.ErrAccessDenied,
		iam.AuthorizeIamAction(iamPostRequest(""), anonymous, "CreateAccessKey", ""))
	assert.Equal(t, s3err.ErrAccessDenied,
		iam.AuthorizeIamAction(iamPostRequest(""), anonymous, "CreateUser", "victim"))
}

// CreateServiceAccount takes its target from the ParentUser parameter, not
// UserName. A non-admin holding iam:CreateServiceAccount targets the
// identities its policy resource covers: a wildcard resource is the
// operator's explicit "any target" choice (as in AWS), a scoped resource
// confines the grant to the named parent identity — e.g. a keymaker role
// that may mint service accounts only for the shared zone parent.
func TestAuthorizeIamActionCreateServiceAccountScopedByPolicyResource(t *testing.T) {
	iam := newIamAuthzTestIam(t)
	require.NoError(t, iam.PutPolicy("CreateSvcAcctPolicy", iamCreateSvcAcctPolicy))
	require.NoError(t, iam.PutPolicy("CreateSvcAcctWildcardArnPolicy", iamCreateSvcAcctWildcardArnPolicy))
	require.NoError(t, iam.PutPolicy("CreateSvcAcctZonePolicy", iamCreateSvcAcctZonePolicy))
	dev := &Identity{Name: "dev", PolicyNames: []string{"CreateSvcAcctPolicy"}}
	wildcardArn := &Identity{Name: "wildcard-arn", PolicyNames: []string{"CreateSvcAcctWildcardArnPolicy"}}
	keymaker := &Identity{Name: "keymaker", PolicyNames: []string{"CreateSvcAcctZonePolicy"}}
	admin := &Identity{Name: "admin", Actions: []Action{s3_constants.ACTION_ADMIN}}

	// Wildcard resource: any target, including the caller itself.
	assert.Equal(t, s3err.ErrNone,
		iam.AuthorizeIamAction(iamPostRequest(""), dev, "CreateServiceAccount", "dev"))
	assert.Equal(t, s3err.ErrNone,
		iam.AuthorizeIamAction(iamPostRequest(""), dev, "CreateServiceAccount", ""))
	assert.Equal(t, s3err.ErrNone,
		iam.AuthorizeIamAction(iamPostRequest(""), dev, "CreateServiceAccount", "admin"))

	// The historical wildcard ARN shape (arn:aws:iam:::*) keeps matching any
	// parent — existing grants survive the switch to per-target resources.
	assert.Equal(t, s3err.ErrNone,
		iam.AuthorizeIamAction(iamPostRequest(""), wildcardArn, "CreateServiceAccount", "team-parent"))
	assert.Equal(t, s3err.ErrNone,
		iam.AuthorizeIamAction(iamPostRequest(""), wildcardArn, "CreateServiceAccount", "admin"))

	// Scoped resource: the named parent identity, and no other. Resource
	// matching is case-sensitive (as in AWS): a case variant is a different
	// identity and must not match the grant.
	assert.Equal(t, s3err.ErrNone,
		iam.AuthorizeIamAction(iamPostRequest(""), keymaker, "CreateServiceAccount", "team-parent"))
	assert.Equal(t, s3err.ErrAccessDenied,
		iam.AuthorizeIamAction(iamPostRequest(""), keymaker, "CreateServiceAccount", "Team-Parent"))
	assert.Equal(t, s3err.ErrAccessDenied,
		iam.AuthorizeIamAction(iamPostRequest(""), keymaker, "CreateServiceAccount", "admin"))
	assert.Equal(t, s3err.ErrAccessDenied,
		iam.AuthorizeIamAction(iamPostRequest(""), keymaker, "CreateServiceAccount", "keymaker"))
	assert.Equal(t, s3err.ErrAccessDenied,
		iam.AuthorizeIamAction(iamPostRequest(""), keymaker, "CreateServiceAccount", ""))

	// Admins remain unrestricted.
	assert.Equal(t, s3err.ErrNone,
		iam.AuthorizeIamAction(iamPostRequest(""), admin, "CreateServiceAccount", "victim"))
}

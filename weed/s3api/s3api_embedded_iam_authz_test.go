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
	dataPlanePolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	iamAdminPolicy  = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iam:*","Resource":"*"}]}`
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

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
	// The wildcard ARN shape this authorization always used; it must keep
	// matching any parent so existing grants survive.
	iamCreateSvcAcctWildcardArnPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["iam:CreateServiceAccount"],"Resource":["arn:aws:iam:::*"]}]}`
	// Scoped to one parent identity.
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

// CreateServiceAccount takes its target from the ParentUser parameter. A
// non-admin holding iam:CreateServiceAccount targets the identities its
// policy resource covers: a wildcard resource allows any target, a scoped
// resource confines the grant to the named parent identity.
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

	// Wildcard ARN: any target.
	assert.Equal(t, s3err.ErrNone,
		iam.AuthorizeIamAction(iamPostRequest(""), wildcardArn, "CreateServiceAccount", "team-parent"))
	assert.Equal(t, s3err.ErrNone,
		iam.AuthorizeIamAction(iamPostRequest(""), wildcardArn, "CreateServiceAccount", "admin"))

	// Scoped resource: the named parent only. IAM resource matching is
	// case-sensitive, so a case variant is a different identity.
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

var iamSelfServiceActionNames = []string{"CreateAccessKey", "DeleteAccessKey", "UpdateAccessKey", "ListAccessKeys", "GetUser"}

func ownKeysPolicy(resource string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["iam:CreateAccessKey","iam:DeleteAccessKey","iam:UpdateAccessKey","iam:ListAccessKeys","iam:GetUser"],"Resource":["` + resource + `"]}]}`
}

// With selfServiceRequiresGrant, an identity holding only data-plane rights may
// no longer manage its own access keys: a leaked key cannot mint further keys
// that outlive its own revocation, nor deactivate the keys it was issued.
func TestAuthorizeIamActionSelfServiceRequiresGrantWhenConfigured(t *testing.T) {
	iam := newIamAuthzTestIam(t)
	iam.selfServiceRequiresGrant = true
	powerUser := &Identity{Name: "power_user", PolicyNames: []string{"PowerUserPolicy"}}
	bare := &Identity{Name: "bare_user"}

	for _, action := range iamSelfServiceActionNames {
		for _, identity := range []*Identity{powerUser, bare} {
			for _, target := range []string{"", identity.Name, "victim"} {
				assert.Equal(t, s3err.ErrAccessDenied, iam.AuthorizeIamAction(iamPostRequest(""), identity, action, target),
					"%s by %s on %q", action, identity.Name, target)
			}
		}
	}
}

// The grant is authorized against the target user's ARN, the caller's own when
// UserName is empty, so a policy can confine it to the caller's own keys.
func TestAuthorizeIamActionSelfServiceGrantIsScopedByResource(t *testing.T) {
	for _, resource := range []string{"arn:aws:iam:::user/key_owner", "arn:aws:iam:::user/${aws:username}"} {
		t.Run(resource, func(t *testing.T) {
			iam := newIamAuthzTestIam(t)
			iam.selfServiceRequiresGrant = true
			require.NoError(t, iam.PutPolicy("OwnKeysPolicy", ownKeysPolicy(resource)))
			owner := &Identity{Name: "key_owner", PolicyNames: []string{"OwnKeysPolicy"}}

			for _, action := range iamSelfServiceActionNames {
				assert.Equal(t, s3err.ErrNone, iam.AuthorizeIamAction(iamPostRequest(""), owner, action, ""), "%s on itself, implicit", action)
				assert.Equal(t, s3err.ErrNone, iam.AuthorizeIamAction(iamPostRequest(""), owner, action, "key_owner"), "%s on itself", action)
				assert.Equal(t, s3err.ErrAccessDenied, iam.AuthorizeIamAction(iamPostRequest(""), owner, action, "victim"), "%s on another user", action)
			}
			assert.Equal(t, s3err.ErrAccessDenied, iam.AuthorizeIamAction(iamPostRequest(""), owner, "CreateUser", "victim"))
		})
	}
}

// The setting changes nothing for the admin, nor for an iam:* grant.
func TestAuthorizeIamActionSelfServiceRequiresGrantKeepsAdmin(t *testing.T) {
	iam := newIamAuthzTestIam(t)
	iam.selfServiceRequiresGrant = true
	admin := &Identity{Name: "admin", Actions: []Action{s3_constants.ACTION_ADMIN}}
	operator := &Identity{Name: "iam_operator", PolicyNames: []string{"IamAdminPolicy"}}

	for _, action := range iamSelfServiceActionNames {
		for _, identity := range []*Identity{admin, operator} {
			for _, target := range []string{"", identity.Name, "victim"} {
				assert.Equal(t, s3err.ErrNone, iam.AuthorizeIamAction(iamPostRequest(""), identity, action, target),
					"%s by %s on %q", action, identity.Name, target)
			}
		}
	}
}

// Off by default: self-service stays allowed without a grant.
func TestAuthorizeIamActionSelfServiceDefaultUnchanged(t *testing.T) {
	iam := newIamAuthzTestIam(t)
	powerUser := &Identity{Name: "power_user", PolicyNames: []string{"PowerUserPolicy"}}
	for _, action := range iamSelfServiceActionNames {
		assert.Equal(t, s3err.ErrNone, iam.AuthorizeIamAction(iamPostRequest(""), powerUser, action, ""), action)
		assert.Equal(t, s3err.ErrNone, iam.AuthorizeIamAction(iamPostRequest(""), powerUser, action, "power_user"), action)
	}
}

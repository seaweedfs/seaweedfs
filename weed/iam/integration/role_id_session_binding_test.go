package integration

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A session is bound to the role it was issued for, not to the role's name:
// deleting the role ends it, and a role created later under the same name —
// by whoever may create roles — must not inherit it.
func TestSessionIsBoundToTheRoleNotItsName(t *testing.T) {
	ctx := context.Background()
	m := setupIntegratedIAMSystem(t)

	assume := func() (token, principal string) {
		resp, err := m.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityRequest{
			RoleArn:          "arn:aws:iam::role/S3ReadOnlyRole",
			WebIdentityToken: createTestJWT(t, "https://test-issuer.com", "test-user-123", "test-signing-key"),
			RoleSessionName:  "binding-test",
		})
		require.NoError(t, err)
		return resp.Credentials.SessionToken, resp.AssumedRoleUser.Arn
	}
	allowed := func(token, principal string) bool {
		ok, _ := m.IsActionAllowed(ctx, &ActionRequest{
			Principal:    principal,
			Action:       "s3:GetObject",
			Resource:     "arn:aws:s3:::test-bucket/file.txt",
			SessionToken: token,
		})
		return ok
	}

	original, err := m.GetRole(ctx, "S3ReadOnlyRole")
	require.NoError(t, err)
	require.NotEmpty(t, original.RoleId, "a created role has an ID")

	token, principal := assume()
	require.True(t, allowed(token, principal), "precondition: the session works")

	require.NoError(t, m.DeleteRole(ctx, "S3ReadOnlyRole"))
	assert.False(t, allowed(token, principal), "the session outlived its role's deletion")

	recreated := *original
	recreated.RoleId = ""
	require.NoError(t, m.CreateRole(ctx, "", "S3ReadOnlyRole", &recreated))
	assert.NotEqual(t, original.RoleId, recreated.RoleId, "a role created again under the same name got the same ID")
	assert.False(t, allowed(token, principal), "a session of the deleted role works again under a new role of the same name")

	fresh, freshPrincipal := assume()
	assert.True(t, allowed(fresh, freshPrincipal), "a session issued for the new role is refused")
}

// A session carries its role's attached policies, and S3 evaluates those
// rather than looking the role up; the binding must hold on that path too.
func TestASessionCarryingItsPoliciesIsStillBoundToItsRole(t *testing.T) {
	ctx := context.Background()
	m := setupIntegratedIAMSystem(t)

	original, err := m.GetRole(ctx, "S3ReadOnlyRole")
	require.NoError(t, err)
	require.NotEmpty(t, original.AttachedPolicies, "precondition: the role attaches a policy")
	resp, err := m.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/S3ReadOnlyRole",
		WebIdentityToken: createTestJWT(t, "https://test-issuer.com", "test-user-123", "test-signing-key"),
		RoleSessionName:  "binding-test",
	})
	require.NoError(t, err)
	allowed := func() bool {
		ok, _ := m.IsActionAllowed(ctx, &ActionRequest{
			Principal:    resp.AssumedRoleUser.Arn,
			Action:       "s3:GetObject",
			Resource:     "arn:aws:s3:::test-bucket/file.txt",
			SessionToken: resp.Credentials.SessionToken,
			PolicyNames:  original.AttachedPolicies,
		})
		return ok
	}
	require.True(t, allowed(), "precondition: the session works")

	require.NoError(t, m.DeleteRole(ctx, "S3ReadOnlyRole"))
	assert.False(t, allowed(), "the session's embedded policies outlived its role's deletion")

	recreated := *original
	recreated.RoleId = ""
	require.NoError(t, m.CreateRole(ctx, "", "S3ReadOnlyRole", &recreated))
	assert.False(t, allowed(), "the session's embedded policies work again under a new role of the same name")
}

func TestStaticRoleIDIsStableAndRuntimeIDsAreUnique(t *testing.T) {
	trusting := func(principal string) *policy.PolicyDocument {
		return &policy.PolicyDocument{Version: "2012-10-17", Statement: []policy.Statement{{
			Effect: "Allow", Action: []string{"sts:AssumeRoleWithWebIdentity"},
			Principal: map[string]interface{}{"Federated": principal},
		}}}
	}
	app := &RoleDefinition{RoleName: "app", TrustPolicy: trusting("https://a.example")}
	restarted := &RoleDefinition{RoleName: "app", TrustPolicy: trusting("https://a.example")}
	replaced := &RoleDefinition{RoleName: "app", TrustPolicy: trusting("https://b.example")}

	assert.Equal(t, StaticRoleID(app), StaticRoleID(restarted), "a config-file role must keep its ID across restarts")
	assert.NotEqual(t, StaticRoleID(app), StaticRoleID(replaced), "a different role under the same name inherits the old one's ID")
	assert.NotEqual(t, StaticRoleID(app), StaticRoleID(&RoleDefinition{RoleName: "other", TrustPolicy: trusting("https://a.example")}))
	assert.Regexp(t, `^AROA[A-Z0-9]{17}$`, StaticRoleID(app))
	assert.NotEqual(t, NewRoleID(), NewRoleID())
	assert.Regexp(t, `^AROA[A-Z2-7]{17}$`, NewRoleID())
}

// replacedAfterFirstReadStore serves the stored role on its first read and a
// replacement of the same name afterwards: a role replaced while a request
// is being authorized.
type replacedAfterFirstReadStore struct {
	RoleStore
	reads       int
	replacement *RoleDefinition
}

func (s *replacedAfterFirstReadStore) GetRole(ctx context.Context, addr, name string) (*RoleDefinition, error) {
	s.reads++
	if s.reads > 1 && name == s.replacement.RoleName {
		return copyRoleDefinition(s.replacement), nil
	}
	return s.RoleStore.GetRole(ctx, addr, name)
}

// The session's binding is checked against one definition of its role, and
// that definition's policies are the ones evaluated: a replacement read in
// between must not lend the session its permissions.
func TestAuthorizationEvaluatesTheRoleTheBindingCheckSaw(t *testing.T) {
	ctx := context.Background()
	m := setupIntegratedIAMSystem(t)
	require.NoError(t, m.CreatePolicy(ctx, "", "S3WritePolicy", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect: "Allow", Action: []string{"s3:PutObject"},
			Resource: []string{"arn:aws:s3:::test-bucket/*"},
		}},
	}))
	resp, err := m.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/S3ReadOnlyRole",
		WebIdentityToken: createTestJWT(t, "https://test-issuer.com", "test-user-123", "test-signing-key"),
		RoleSessionName:  "snapshot-test",
	})
	require.NoError(t, err)

	original, err := m.GetRole(ctx, "S3ReadOnlyRole")
	require.NoError(t, err)
	replacement := *original
	replacement.RoleId = NewRoleID()
	replacement.AttachedPolicies = []string{"S3WritePolicy"}
	m.roleStore = &replacedAfterFirstReadStore{RoleStore: m.roleStore, replacement: &replacement}

	allowed, _ := m.IsActionAllowed(ctx, &ActionRequest{
		Principal:    resp.AssumedRoleUser.Arn,
		Action:       "s3:PutObject",
		Resource:     "arn:aws:s3:::test-bucket/file.txt",
		SessionToken: resp.Credentials.SessionToken,
	})
	assert.False(t, allowed, "the session was authorized by the replacement role's policies")
}

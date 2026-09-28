package integration

import (
	"context"
	"testing"

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

func TestStaticRoleIDIsStableAndRuntimeIDsAreUnique(t *testing.T) {
	assert.Equal(t, StaticRoleID("app"), StaticRoleID("app"), "a config-file role must keep its ID across restarts")
	assert.NotEqual(t, StaticRoleID("app"), StaticRoleID("other"))
	assert.NotEqual(t, NewRoleID(), NewRoleID())
	assert.Regexp(t, `^AROA[A-Z2-7]{17}$`, NewRoleID())
}

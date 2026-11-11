package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
)

// ${aws:username} of a role session is the user the session was issued to,
// not the session name its caller chose or a value forwarded in the request
// context.
func TestAssumedRoleUsernameIsTheSessionUser(t *testing.T) {
	ctx := context.Background()
	manager := setupIntegratedIAMSystem(t)
	require.NoError(t, manager.CreatePolicy(ctx, "", "OwnFolderPolicy", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect:   "Allow",
			Action:   []string{"s3:GetObject"},
			Resource: []string{"arn:aws:s3:::home/${aws:username}/*"},
		}},
	}))
	require.NoError(t, manager.CreateRole(ctx, "", "SharedRole", &RoleDefinition{
		RoleName: "SharedRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{{
				Effect:    "Allow",
				Principal: map[string]interface{}{"Federated": "test-oidc"},
				Action:    []string{"sts:AssumeRoleWithWebIdentity"},
			}},
		},
		AttachedPolicies: []string{"OwnFolderPolicy"},
	}))

	response, err := manager.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/SharedRole",
		WebIdentityToken: createTestJWT(t, "https://test-issuer.com", "test-user-123", "test-signing-key"),
		RoleSessionName:  "alice",
	})
	require.NoError(t, err)
	session, err := manager.GetSTSService().ValidateSessionToken(ctx, response.Credentials.SessionToken)
	require.NoError(t, err)
	require.NotEmpty(t, session.ExternalUserId)
	require.NotEqual(t, "alice", session.ExternalUserId)

	allowed := func(resource string) bool {
		t.Helper()
		ok, err := manager.IsActionAllowed(ctx, &ActionRequest{
			Principal:      response.AssumedRoleUser.Arn,
			Action:         "s3:GetObject",
			Resource:       resource,
			SessionToken:   response.Credentials.SessionToken,
			RequestContext: map[string]interface{}{"aws:username": "alice"},
		})
		require.NoError(t, err)
		return ok
	}
	assert.False(t, allowed("arn:aws:s3:::home/alice/notes.txt"), "the session name must not select another user's folder")
	assert.True(t, allowed("arn:aws:s3:::home/"+session.ExternalUserId+"/notes.txt"), "the session's user keeps its folder")
}

package integration

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCredentialsTrustPolicyExplicitDeny(t *testing.T) {
	iamManager := setupIntegratedIAMSystem(t)
	ctx := context.Background()

	statement := func(effect, provider string) policy.Statement {
		return policy.Statement{
			Effect:    effect,
			Principal: map[string]interface{}{"Federated": provider},
			Action:    []string{"sts:AssumeRoleWithCredentials"},
		}
	}
	roles := map[string][]policy.Statement{
		"LDAPAllowed": {statement("Allow", "test-ldap")},
		"LDAPRevoked": {statement("Allow", "test-ldap"), statement("Deny", "test-ldap")},
		"LDAPOther":   {statement("Allow", "other-ldap")},
	}
	for name, statements := range roles {
		require.NoError(t, iamManager.CreateRole(ctx, "", name, &RoleDefinition{
			RoleName:         name,
			TrustPolicy:      &policy.PolicyDocument{Version: "2012-10-17", Statement: statements},
			AttachedPolicies: []string{"LDAPUserPolicy"},
		}))
	}

	tests := []struct {
		role  string
		allow bool
	}{
		{"LDAPAllowed", true},
		{"LDAPRevoked", false},
		{"LDAPOther", false},
	}
	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			roleArn := "arn:aws:iam::role/" + tt.role

			response, err := iamManager.AssumeRoleWithCredentials(ctx, &sts.AssumeRoleWithCredentialsRequest{
				RoleArn:         roleArn,
				Username:        "testuser",
				Password:        "testpass",
				RoleSessionName: "ldap-session",
				ProviderName:    "test-ldap",
			})
			validateErr := iamManager.ValidateTrustPolicyForCredentials(ctx, roleArn, &providers.ExternalIdentity{
				UserID:   "testuser",
				Provider: "test-ldap",
			})

			if tt.allow {
				require.NoError(t, err)
				require.NotNil(t, response)
				assert.NoError(t, validateErr)
				return
			}
			assert.Error(t, err)
			assert.Nil(t, response)
			assert.Error(t, validateErr)
		})
	}
}

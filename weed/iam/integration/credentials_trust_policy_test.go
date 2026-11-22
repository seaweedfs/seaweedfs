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

	statement := func(effect string, principal interface{}, condition map[string]map[string]interface{}) policy.Statement {
		return policy.Statement{
			Effect:    effect,
			Principal: principal,
			Action:    []string{"sts:AssumeRoleWithCredentials"},
			Condition: condition,
		}
	}
	federated := func(provider interface{}) map[string]interface{} {
		return map[string]interface{}{"Federated": provider}
	}
	allowLDAP := statement("Allow", federated("test-ldap"), nil)
	providerIs := func(provider string) map[string]map[string]interface{} {
		return map[string]map[string]interface{}{"StringEquals": {"aws:FederatedProvider": provider}}
	}
	longSession := map[string]map[string]interface{}{"NumericGreaterThan": {"sts:DurationSeconds": 3600}}

	tests := []struct {
		role       string
		statements []policy.Statement
		duration   int64
		allow      bool
		allowVend  bool
	}{
		{"Allowed", []policy.Statement{allowLDAP}, 0, true, true},
		{"Revoked", []policy.Statement{allowLDAP, statement("Deny", federated("test-ldap"), nil)}, 0, false, false},
		{"RevokedByArray", []policy.Statement{allowLDAP, statement("Deny", federated([]interface{}{"other-ldap", "test-ldap"}), nil)}, 0, false, false},
		{"RevokedForEveryone", []policy.Statement{allowLDAP, statement("Deny", "*", nil)}, 0, false, false},
		{"RevokedByCondition", []policy.Statement{allowLDAP, statement("Deny", federated("*"), providerIs("test-ldap"))}, 0, false, false},
		{"OtherProviderRevoked", []policy.Statement{allowLDAP, statement("Deny", federated("*"), providerIs("other-ldap"))}, 0, true, true},
		{"LongSessionRevoked", []policy.Statement{allowLDAP, statement("Deny", federated("test-ldap"), longSession)}, 7200, false, true},
		{"ShortSessionKept", []policy.Statement{allowLDAP, statement("Deny", federated("test-ldap"), longSession)}, 1800, true, true},
		{"OtherProvider", []policy.Statement{statement("Allow", federated("other-ldap"), nil)}, 0, false, false},
		{"AWSWildcard", []policy.Statement{statement("Allow", map[string]interface{}{"AWS": "*"}, nil)}, 0, false, false},
		{"EmptyPrincipal", []policy.Statement{statement("Allow", map[string]interface{}{}, nil)}, 0, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			require.NoError(t, iamManager.CreateRole(ctx, "", tt.role, &RoleDefinition{
				RoleName:         tt.role,
				TrustPolicy:      &policy.PolicyDocument{Version: "2012-10-17", Statement: tt.statements},
				AttachedPolicies: []string{"LDAPUserPolicy"},
			}))
			roleArn := "arn:aws:iam::role/" + tt.role

			request := &sts.AssumeRoleWithCredentialsRequest{
				RoleArn:         roleArn,
				Username:        "testuser",
				Password:        "testpass",
				RoleSessionName: "ldap-session",
				ProviderName:    "test-ldap",
			}
			if tt.duration > 0 {
				request.DurationSeconds = &tt.duration
			}
			response, err := iamManager.AssumeRoleWithCredentials(ctx, request)
			if tt.allow {
				require.NoError(t, err)
				assert.NotNil(t, response)
			} else {
				assert.Error(t, err)
				assert.Nil(t, response)
			}

			err = iamManager.ValidateTrustPolicyForCredentials(ctx, roleArn, &providers.ExternalIdentity{
				UserID:   "testuser",
				Provider: "test-ldap",
			})
			if tt.allowVend {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

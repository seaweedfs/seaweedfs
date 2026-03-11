package integration

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTrustPolicyAWSUserPrincipal tests that trust policies with specific AWS user
// principals work correctly with ValidateTrustPolicyForPrincipal.
// This is a regression test for the case where setting a specific user principal like:
//
//	"Principal": {"AWS": "arn:aws:iam::000000000000:user/backend"}
//
// would fail with "trust policy denies access to principal" even though the
// caller's principal ARN matched.
func TestTrustPolicyAWSUserPrincipal(t *testing.T) {
	iamManager := setupIntegratedIAMSystem(t)
	ctx := context.Background()

	const (
		accountID    = "000000000000"
		backendUser  = "backend"
		backendArn   = "arn:aws:iam::" + accountID + ":user/" + backendUser
		otherUser    = "other"
		otherArn     = "arn:aws:iam::" + accountID + ":user/" + otherUser
		clientRoleN  = "ClientRole"
		clientRoleA  = "arn:aws:iam::role/" + clientRoleN
	)

	// Create role with trust policy restricted to a specific AWS user principal
	// (the exact scenario from discussion #8588)
	err := iamManager.CreateRole(ctx, "", clientRoleN, &RoleDefinition{
		RoleName: clientRoleN,
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"AWS": backendArn,
					},
					Action: []string{"sts:AssumeRole"},
				},
			},
		},
		AttachedPolicies: []string{"S3ReadOnlyPolicy"},
	})
	require.NoError(t, err)

	tests := []struct {
		name         string
		principalArn string
		expectErr    bool
		description  string
	}{
		{
			name:         "matching user principal should be allowed",
			principalArn: backendArn,
			expectErr:    false,
			description:  "Backend user ARN matches the trust policy principal exactly",
		},
		{
			name:         "non-matching user principal should be denied",
			principalArn: otherArn,
			expectErr:    true,
			description:  "Other user ARN does not match the trust policy principal",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := iamManager.ValidateTrustPolicyForPrincipal(ctx, clientRoleA, tt.principalArn)
			if tt.expectErr {
				assert.Error(t, err, tt.description)
				assert.Contains(t, err.Error(), "trust policy denies access")
			} else {
				assert.NoError(t, err, tt.description)
			}
		})
	}
}

// TestTrustPolicyAWSWildcardPrincipal tests that wildcard AWS principals
// continue to work correctly (this already works, serves as a regression guard).
func TestTrustPolicyAWSWildcardPrincipal(t *testing.T) {
	iamManager := setupIntegratedIAMSystem(t)
	ctx := context.Background()

	const roleName = "WildcardAWSRole"
	const roleArn = "arn:aws:iam::role/" + roleName

	err := iamManager.CreateRole(ctx, "", roleName, &RoleDefinition{
		RoleName: roleName,
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"AWS": "*",
					},
					Action: []string{"sts:AssumeRole"},
				},
			},
		},
		AttachedPolicies: []string{"S3ReadOnlyPolicy"},
	})
	require.NoError(t, err)

	// Any user should be able to assume a role with wildcard AWS principal
	err = iamManager.ValidateTrustPolicyForPrincipal(ctx, roleArn, "arn:aws:iam::000000000000:user/anyuser")
	assert.NoError(t, err, "Wildcard AWS principal should allow any user")
}

// TestTrustPolicyAWSUserArrayPrincipal tests trust policies with an array of
// AWS user principals.
func TestTrustPolicyAWSUserArrayPrincipal(t *testing.T) {
	iamManager := setupIntegratedIAMSystem(t)
	ctx := context.Background()

	const (
		accountID = "000000000000"
		roleName  = "MultiUserRole"
		roleArn   = "arn:aws:iam::role/" + roleName
		user1Arn  = "arn:aws:iam::" + accountID + ":user/user1"
		user2Arn  = "arn:aws:iam::" + accountID + ":user/user2"
		user3Arn  = "arn:aws:iam::" + accountID + ":user/user3"
	)

	// Create role with multiple allowed user principals
	err := iamManager.CreateRole(ctx, "", roleName, &RoleDefinition{
		RoleName: roleName,
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"AWS": []interface{}{user1Arn, user2Arn},
					},
					Action: []string{"sts:AssumeRole"},
				},
			},
		},
		AttachedPolicies: []string{"S3ReadOnlyPolicy"},
	})
	require.NoError(t, err)

	tests := []struct {
		name         string
		principalArn string
		expectErr    bool
	}{
		{
			name:         "first listed user is allowed",
			principalArn: user1Arn,
			expectErr:    false,
		},
		{
			name:         "second listed user is allowed",
			principalArn: user2Arn,
			expectErr:    false,
		},
		{
			name:         "unlisted user is denied",
			principalArn: user3Arn,
			expectErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := iamManager.ValidateTrustPolicyForPrincipal(ctx, roleArn, tt.principalArn)
			if tt.expectErr {
				if assert.Error(t, err) {
					assert.Contains(t, err.Error(), "trust policy denies access")
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestTrustPolicyPlainStringPrincipal tests trust policies with a plain string
// principal (not wrapped in {"AWS": ...}).
func TestTrustPolicyPlainStringPrincipal(t *testing.T) {
	iamManager := setupIntegratedIAMSystem(t)
	ctx := context.Background()

	const (
		roleName = "PlainStringPrincipalRole"
		roleArn  = "arn:aws:iam::role/" + roleName
		userArn  = "arn:aws:iam::000000000000:user/backend"
	)

	// Plain string wildcard - should work
	err := iamManager.CreateRole(ctx, "", roleName, &RoleDefinition{
		RoleName: roleName,
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect:    "Allow",
					Principal: "*",
					Action:    []string{"sts:AssumeRole"},
				},
			},
		},
		AttachedPolicies: []string{"S3ReadOnlyPolicy"},
	})
	require.NoError(t, err)

	err = iamManager.ValidateTrustPolicyForPrincipal(ctx, roleArn, userArn)
	assert.NoError(t, err, "Plain wildcard principal should allow any user")

	// Plain string with specific ARN
	const roleName2 = "PlainStringSpecificRole"
	const roleArn2 = "arn:aws:iam::role/" + roleName2

	err = iamManager.CreateRole(ctx, "", roleName2, &RoleDefinition{
		RoleName: roleName2,
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect:    "Allow",
					Principal: userArn,
					Action:    []string{"sts:AssumeRole"},
				},
			},
		},
		AttachedPolicies: []string{"S3ReadOnlyPolicy"},
	})
	require.NoError(t, err)

	err = iamManager.ValidateTrustPolicyForPrincipal(ctx, roleArn2, userArn)
	assert.NoError(t, err, "Matching plain string principal should allow user")

	err = iamManager.ValidateTrustPolicyForPrincipal(ctx, roleArn2, "arn:aws:iam::000000000000:user/other")
	if assert.Error(t, err, "Non-matching plain string principal should deny user") {
		assert.Contains(t, err.Error(), "trust policy denies access")
	}
}

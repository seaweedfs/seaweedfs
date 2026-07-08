package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/oidc"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGroupsRolesPolicyEnforcement verifies that OIDC groups and roles claims reach
// the request-time policy evaluation of an STS session, so a single role's permission
// policy can scope bucket access with ForAnyValue:StringEquals on jwt:groups / jwt:roles.
// The full path is exercised: AssumeRoleWithWebIdentity embeds the claims in the session
// JWT, AuthenticateJWT restores them (as []interface{} after the JSON round-trip), and
// AuthorizeAction evaluates the conditions.
func TestGroupsRolesPolicyEnforcement(t *testing.T) {
	ctx := context.Background()
	iamManager := setupGroupsRolesIAMManager(t)
	s3iam := NewS3IAMIntegration(iamManager, "localhost:8888")

	// One role for everyone; each bucket statement is gated on a groups or roles claim
	abacPolicy := &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{
			{
				Sid:    "TeamABucketByGroup",
				Effect: "Allow",
				Action: []string{"s3:GetObject", "s3:PutObject"},
				Resource: []string{
					"arn:aws:s3:::team-a-bucket/*",
				},
				Condition: map[string]map[string]interface{}{
					"ForAnyValue:StringEquals": {
						"jwt:groups": "team-a",
					},
				},
			},
			{
				Sid:    "TeamBBucketByRole",
				Effect: "Allow",
				Action: []string{"s3:GetObject", "s3:PutObject"},
				Resource: []string{
					"arn:aws:s3:::team-b-bucket/*",
				},
				Condition: map[string]map[string]interface{}{
					"ForAnyValue:StringEquals": {
						"jwt:roles": "team-b-rw",
					},
				},
			},
		},
	}
	require.NoError(t, iamManager.CreatePolicy(ctx, "", "TeamABACPolicy", abacPolicy))
	require.NoError(t, iamManager.CreateRole(ctx, "", "TeamABACRole", &integration.RoleDefinition{
		RoleName: "TeamABACRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"Federated": "test-oidc",
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
				},
			},
		},
		AttachedPolicies: []string{"TeamABACPolicy"},
	}))

	tests := []struct {
		name       string
		token      string
		allowTeamA bool
		allowTeamB bool
	}{
		{
			name:       "group member gets team-a bucket only",
			token:      groupsRolesJWT(t, "team-a-user", []string{"team-a"}, nil),
			allowTeamA: true,
			allowTeamB: false,
		},
		{
			name:       "role holder gets team-b bucket only",
			token:      groupsRolesJWT(t, "team-b-user", nil, []string{"team-b-rw"}),
			allowTeamA: false,
			allowTeamB: true,
		},
		{
			name:       "member of both gets both buckets in one session",
			token:      groupsRolesJWT(t, "both-teams-user", []string{"team-a", "team-b"}, []string{"team-a-rw", "team-b-rw"}),
			allowTeamA: true,
			allowTeamB: true,
		},
		{
			name:       "user without matching claims is denied everywhere",
			token:      groupsRolesJWT(t, "no-team-user", []string{"other-team"}, []string{"other-role"}),
			allowTeamA: false,
			allowTeamB: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, err := iamManager.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityRequest{
				RoleArn:          "arn:aws:iam::role/TeamABACRole",
				WebIdentityToken: tt.token,
				RoleSessionName:  "abac-test-session",
			})
			require.NoError(t, err)

			identity, errCode := authenticateSessionToken(t, s3iam, response.Credentials.SessionToken)
			require.Equal(t, s3err.ErrNone, errCode)

			assert.Equal(t, tt.allowTeamA, authorizeGet(ctx, s3iam, identity, "team-a-bucket"), "team-a-bucket access")
			assert.Equal(t, tt.allowTeamB, authorizeGet(ctx, s3iam, identity, "team-b-bucket"), "team-b-bucket access")
		})
	}
}

// TestGroupsRolesSessionRequestContext pins the session round-trip itself: the groups
// and roles claims must come back from the session JWT as multi-valued context keys.
func TestGroupsRolesSessionRequestContext(t *testing.T) {
	ctx := context.Background()
	iamManager := setupGroupsRolesIAMManager(t)

	require.NoError(t, iamManager.CreateRole(ctx, "", "ContextRole", &integration.RoleDefinition{
		RoleName: "ContextRole",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{
				{
					Effect: "Allow",
					Principal: map[string]interface{}{
						"Federated": "test-oidc",
					},
					Action: []string{"sts:AssumeRoleWithWebIdentity"},
				},
			},
		},
	}))

	response, err := iamManager.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/ContextRole",
		WebIdentityToken: groupsRolesJWT(t, "both-teams-user", []string{"team-a", "team-b"}, []string{"team-a-rw", "team-b-rw"}),
		RoleSessionName:  "context-test-session",
	})
	require.NoError(t, err)

	sessionInfo, err := iamManager.GetSTSService().ValidateSessionToken(ctx, response.Credentials.SessionToken)
	require.NoError(t, err)
	require.NotNil(t, sessionInfo.RequestContext)

	// JWT serialization turns []string into []interface{}; assert that shape so a
	// future change to the condition evaluator's type handling fails loudly here.
	assert.Equal(t, []interface{}{"team-a", "team-b"}, sessionInfo.RequestContext["groups"])
	assert.Equal(t, []interface{}{"team-a-rw", "team-b-rw"}, sessionInfo.RequestContext["roles"])
}

func setupGroupsRolesIAMManager(t *testing.T) *integration.IAMManager {
	manager := integration.NewIAMManager()
	config := &integration.IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: time.Hour * 12},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
		},
		Policy: &policy.PolicyEngineConfig{
			DefaultEffect: "Deny",
			StoreType:     "memory",
		},
		Roles: &integration.RoleStoreConfig{
			StoreType: "memory",
		},
	}
	require.NoError(t, manager.Initialize(config, func() string {
		return "localhost:8888"
	}))

	provider := oidc.NewMockOIDCProvider("test-oidc")
	require.NoError(t, provider.Initialize(&oidc.OIDCConfig{
		Issuer:   "https://test-issuer.com",
		ClientID: "test-client-id",
	}))

	require.NoError(t, manager.RegisterIdentityProvider(provider))
	return manager
}

// groupsRolesJWT builds a web identity token carrying groups and roles claims.
func groupsRolesJWT(t *testing.T, subject string, groups, roles []string) string {
	claims := jwt.MapClaims{
		"iss": "https://test-issuer.com",
		"sub": subject,
		"aud": "test-client-id",
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	}
	if groups != nil {
		claims["groups"] = groups
	}
	if roles != nil {
		claims["roles"] = roles
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte("test-signing-key"))
	require.NoError(t, err)
	return token
}

func authenticateSessionToken(t *testing.T, s3iam *S3IAMIntegration, sessionToken string) (*IAMIdentity, s3err.ErrorCode) {
	req := httptest.NewRequest("GET", "/", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+sessionToken)
	return s3iam.AuthenticateJWT(context.Background(), req)
}

func authorizeGet(ctx context.Context, s3iam *S3IAMIntegration, identity *IAMIdentity, bucket string) bool {
	req := httptest.NewRequest("GET", "/"+bucket+"/file.txt", http.NoBody)
	req.Header.Set("Authorization", "Bearer "+identity.SessionToken)
	return s3iam.AuthorizeAction(ctx, identity, s3_constants.ACTION_READ, bucket, "file.txt", req) == s3err.ErrNone
}

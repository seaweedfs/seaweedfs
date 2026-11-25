package integration

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/iam/oidc"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebIdentityRoleMappingLimitsNamedRoles(t *testing.T) {
	ctx := context.Background()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"keys": []map[string]interface{}{{
				"kty": "RSA",
				"kid": "mapping-key",
				"use": "sig",
				"alg": "RS256",
				"n":   base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
				"e":   "AQAB",
			}},
		})
	}))
	defer server.Close()

	provider := oidc.NewOIDCProvider("keycloak")
	require.NoError(t, provider.Initialize(&oidc.OIDCConfig{
		Issuer:   server.URL,
		ClientID: "test-client",
		JWKSUri:  server.URL + "/jwks",
		RoleMapping: &providers.RoleMapping{
			Rules: []providers.MappingRule{
				{Claim: "groups", Value: "admins", Role: "arn:aws:iam::role/AdminRole"},
				{Claim: "groups", Value: "readers", Role: "arn:aws:iam::role/ReadRole"},
				{Claim: "groups", Value: "auditors", Role: " arn:aws:iam::role/AuditRole "},
				{Claim: "groups", Value: "leads", Role: "arn:aws:iam::role/AdminRole,Lead"},
			},
			DefaultRole: "arn:aws:iam::role/GuestRole",
		},
	}))

	iamManager := NewIAMManager()
	require.NoError(t, iamManager.Initialize(&IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
		},
		Policy: &policy.PolicyEngineConfig{DefaultEffect: "Deny", StoreType: "memory"},
		Roles:  &RoleStoreConfig{StoreType: "memory"},
	}, func() string { return "localhost:8888" }))
	require.NoError(t, iamManager.RegisterIdentityProvider(provider))

	for _, roleName := range []string{"AdminRole", "ReadRole", "AuditRole", "AdminRole,Lead", "GuestRole", "UnmappedRole"} {
		require.NoError(t, iamManager.CreateRole(ctx, "", roleName, &RoleDefinition{
			RoleName: roleName,
			TrustPolicy: &policy.PolicyDocument{
				Version: "2012-10-17",
				Statement: []policy.Statement{{
					Effect:    "Allow",
					Principal: map[string]interface{}{"Federated": "keycloak"},
					Action:    []string{"sts:AssumeRoleWithWebIdentity"},
				}},
			},
		}))
	}

	signJWT := func(groups ...string) string {
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
			"iss":    server.URL,
			"sub":    "mapping-user",
			"aud":    "test-client",
			"exp":    time.Now().Add(time.Hour).Unix(),
			"iat":    time.Now().Unix(),
			"groups": groups,
		})
		token.Header["kid"] = "mapping-key"
		signed, err := token.SignedString(key)
		require.NoError(t, err)
		return signed
	}

	tests := []struct {
		groups  []string
		role    string
		allowed bool
	}{
		{[]string{"readers"}, "AdminRole", false},
		{[]string{"readers"}, "GuestRole", false},
		{[]string{"readers"}, "ReadRole", true},
		{[]string{"readers"}, "UnmappedRole", true},
		{[]string{"admins"}, "AdminRole", true},
		{[]string{"admins", "readers"}, "ReadRole", true},
		{[]string{"readers"}, "AuditRole", false},
		{[]string{"auditors"}, "AuditRole", true},
		{[]string{"leads"}, "AdminRole", false},
		{[]string{"leads"}, "AdminRole,Lead", true},
		{[]string{"admins"}, "AdminRole,Lead", false},
		{nil, "GuestRole", true},
		{nil, "AdminRole", false},
		{nil, "ReadRole", false},
	}
	for _, tc := range tests {
		resp, err := iamManager.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityRequest{
			RoleArn:          "arn:aws:iam::role/" + tc.role,
			WebIdentityToken: signJWT(tc.groups...),
			RoleSessionName:  "mapping-check",
		})
		if tc.allowed {
			require.NoError(t, err, "groups %v assuming %s", tc.groups, tc.role)
			session, err := iamManager.GetSTSService().ValidateSessionToken(ctx, resp.Credentials.SessionToken)
			require.NoError(t, err)
			assert.Equal(t, "arn:aws:iam::role/"+tc.role, session.RoleArn)
		} else {
			assert.Error(t, err, "groups %v assuming %s", tc.groups, tc.role)
			assert.Nil(t, resp, "groups %v assuming %s", tc.groups, tc.role)
		}
	}
}

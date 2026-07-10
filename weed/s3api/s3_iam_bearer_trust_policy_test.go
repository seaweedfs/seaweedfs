package s3api

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
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/oidc"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A raw OIDC token sent as Authorization: Bearer must clear the same role
// trust-policy gate as AssumeRoleWithWebIdentity. The provider's role mapping
// alone must not mint a principal for a role whose trust policy rejects the
// token's federated provider.
func TestOIDCBearerEnforcesRoleTrustPolicy(t *testing.T) {
	ctx := context.Background()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	jwks := map[string]interface{}{
		"keys": []map[string]interface{}{{
			"kty": "RSA",
			"kid": "bearer-test-key",
			"use": "sig",
			"alg": "RS256",
			"n":   base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
			"e":   "AQAB",
		}},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/jwks" {
			json.NewEncoder(w).Encode(jwks)
		}
	}))
	defer server.Close()

	provider := oidc.NewOIDCProvider("test-oidc")
	require.NoError(t, provider.Initialize(&oidc.OIDCConfig{
		Issuer:   server.URL,
		ClientID: "test-client",
		JWKSUri:  server.URL + "/jwks",
		RoleMapping: &providers.RoleMapping{
			Rules: []providers.MappingRule{
				{Claim: "groups", Value: "denied-group", Role: "arn:aws:iam::role/BearerDeniedRole"},
				{Claim: "groups", Value: "trusted-group", Role: "arn:aws:iam::role/BearerTrustedRole"},
			},
		},
	}))

	iamManager := integration.NewIAMManager()
	require.NoError(t, iamManager.Initialize(&integration.IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
			Issuer:           "test-sts",
			SigningKey:       []byte("test-signing-key-32-characters-long"),
		},
		Policy: &policy.PolicyEngineConfig{DefaultEffect: "Deny", StoreType: "memory"},
		Roles:  &integration.RoleStoreConfig{StoreType: "memory"},
	}, func() string { return "localhost:8888" }))
	require.NoError(t, iamManager.RegisterIdentityProvider(provider))

	require.NoError(t, iamManager.CreatePolicy(ctx, "", "BearerTestPolicy", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect:   "Allow",
			Action:   []string{"s3:*"},
			Resource: []string{"arn:aws:s3:::*", "arn:aws:s3:::*/*"},
		}},
	}))

	trustPolicy := func(federatedProvider string) *policy.PolicyDocument {
		return &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{{
				Effect:    "Allow",
				Principal: map[string]interface{}{"Federated": federatedProvider},
				Action:    []string{"sts:AssumeRoleWithWebIdentity"},
			}},
		}
	}
	// trusts a different federated provider than the one issuing the token
	require.NoError(t, iamManager.CreateRole(ctx, "", "BearerDeniedRole", &integration.RoleDefinition{
		RoleName:         "BearerDeniedRole",
		TrustPolicy:      trustPolicy("other-provider"),
		AttachedPolicies: []string{"BearerTestPolicy"},
	}))
	require.NoError(t, iamManager.CreateRole(ctx, "", "BearerTrustedRole", &integration.RoleDefinition{
		RoleName:         "BearerTrustedRole",
		TrustPolicy:      trustPolicy("test-oidc"),
		AttachedPolicies: []string{"BearerTestPolicy"},
	}))

	signJWT := func(groups ...string) string {
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
			"iss":    server.URL,
			"sub":    "bearer-user",
			"aud":    "test-client",
			"exp":    time.Now().Add(time.Hour).Unix(),
			"iat":    time.Now().Unix(),
			"groups": groups,
		})
		token.Header["kid"] = "bearer-test-key"
		signed, err := token.SignedString(key)
		require.NoError(t, err)
		return signed
	}

	s3iam := NewS3IAMIntegration(iamManager, "localhost:8888")
	authenticate := func(token string) (*IAMIdentity, s3err.ErrorCode) {
		req := httptest.NewRequest("GET", "/test-bucket/test-object", http.NoBody)
		req.Header.Set("Authorization", "Bearer "+token)
		return s3iam.AuthenticateJWT(ctx, req)
	}

	deniedToken := signJWT("denied-group")

	// negative control: STS refuses to issue a session for this token and role
	_, err = iamManager.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityRequest{
		RoleArn:          "arn:aws:iam::role/BearerDeniedRole",
		WebIdentityToken: deniedToken,
		RoleSessionName:  "trust-policy-check",
	})
	require.ErrorContains(t, err, "trust policy")

	// the direct bearer path must reject the same token
	identity, errCode := authenticate(deniedToken)
	assert.Equal(t, s3err.ErrAccessDenied, errCode)
	assert.Nil(t, identity)

	// a role whose trust policy trusts the issuing provider still works
	identity, errCode = authenticate(signJWT("trusted-group"))
	require.Equal(t, s3err.ErrNone, errCode)
	assert.Equal(t, "arn:aws:iam::role/BearerTrustedRole", identity.Principal)
}

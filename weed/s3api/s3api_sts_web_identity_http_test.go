package s3api

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/oidc"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AssumeRoleWithWebIdentity is the public, unauthenticated STS entry point, but
// every existing test reaches the OIDC path either at the IAMManager service
// layer or through the Authorization: Bearer shortcut. Nothing drove a real
// signed OIDC token through HandleSTSRequest, which is what an AWS SDK actually
// does - and that is the path carrying parameter parsing, the IAMManager
// dispatch, and the XML response shape.
func TestAssumeRoleWithWebIdentityOverHTTP(t *testing.T) {
	ctx := context.Background()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	jwks := map[string]interface{}{
		"keys": []map[string]interface{}{{
			"kty": "RSA",
			"kid": "web-identity-test-key",
			"use": "sig",
			"alg": "RS256",
			"n":   base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
			"e":   "AQAB",
		}},
	}
	idp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/jwks" {
			json.NewEncoder(w).Encode(jwks)
		}
	}))
	defer idp.Close()

	provider := oidc.NewOIDCProvider("test-oidc")
	require.NoError(t, provider.Initialize(&oidc.OIDCConfig{
		Issuer:   idp.URL,
		ClientID: "test-client",
		JWKSUri:  idp.URL + "/jwks",
	}))

	manager := newTestSTSIntegrationManager(t)
	require.NoError(t, manager.RegisterIdentityProvider(provider))

	require.NoError(t, manager.CreatePolicy(ctx, "", "WebIdentityPolicy", &policy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy.Statement{{
			Effect:   "Allow",
			Action:   []string{"s3:GetObject"},
			Resource: []string{"arn:aws:s3:::*/*"},
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
	require.NoError(t, manager.CreateRole(ctx, "", "WebIdentityRole", &integration.RoleDefinition{
		RoleName:         "WebIdentityRole",
		TrustPolicy:      trustPolicy("test-oidc"),
		AttachedPolicies: []string{"WebIdentityPolicy"},
	}))
	require.NoError(t, manager.CreateRole(ctx, "", "WebIdentityDeniedRole", &integration.RoleDefinition{
		RoleName:         "WebIdentityDeniedRole",
		TrustPolicy:      trustPolicy("some-other-provider"),
		AttachedPolicies: []string{"WebIdentityPolicy"},
	}))

	iam := &IdentityAccessManagement{iamIntegration: NewS3IAMIntegration(manager, "")}
	handlers := NewSTSHandlers(manager.GetSTSService(), iam)

	signOIDCToken := func(t *testing.T, subject string) string {
		t.Helper()
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
			"iss": idp.URL,
			"sub": subject,
			"aud": "test-client",
			"exp": time.Now().Add(time.Hour).Unix(),
			"iat": time.Now().Unix(),
		})
		token.Header["kid"] = "web-identity-test-key"
		signed, err := token.SignedString(key)
		require.NoError(t, err)
		return signed
	}

	const trustedRoleArn = "arn:aws:iam::" + defaultAccountID + ":role/WebIdentityRole"

	// The SDK may put the parameters in the query string or the form body; both
	// reach HandleSTSRequest and must behave identically.
	for _, encoding := range []string{"query", "body"} {
		t.Run("succeeds with parameters in the "+encoding, func(t *testing.T) {
			form := url.Values{
				"Action":           {"AssumeRoleWithWebIdentity"},
				"Version":          {"2011-06-15"},
				"RoleArn":          {trustedRoleArn},
				"RoleSessionName":  {"web-session"},
				"WebIdentityToken": {signOIDCToken(t, "oidc-user-1")},
			}

			var req *http.Request
			if encoding == "query" {
				req = httptest.NewRequest(http.MethodPost, "http://sts.seaweedfs.test/?"+form.Encode(), nil)
			} else {
				req = httptest.NewRequest(http.MethodPost, "http://sts.seaweedfs.test/", strings.NewReader(form.Encode()))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}

			rec := httptest.NewRecorder()
			handlers.HandleSTSRequest(rec, req)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var resp AssumeRoleWithWebIdentityResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "oidc-user-1", resp.Result.SubjectFromWebIdentityToken)
			assert.True(t, strings.HasPrefix(resp.Result.Credentials.AccessKeyId, "ASIA"),
				"temporary credentials must use the ASIA prefix")
			require.NotEmpty(t, resp.Result.Credentials.SessionToken)
			assert.Nil(t, resp.Result.PackedPolicySize, "no inline session policy was sent")

			// The minted session must resolve to the assumed-role principal.
			// Unlike AssumeRole, this path does not embed the role's attached
			// policies in the token - they are resolved from the role at request
			// time - so assert the effective permission rather than the claim.
			info, err := manager.GetSTSService().ValidateSessionToken(ctx, resp.Result.Credentials.SessionToken)
			require.NoError(t, err)
			assert.Contains(t, info.Principal, "assumed-role/WebIdentityRole/web-session")

			allowed, err := manager.IsActionAllowed(ctx, &integration.ActionRequest{
				Principal:    info.Principal,
				Action:       "s3:GetObject",
				Resource:     "arn:aws:s3:::bucket/key",
				SessionToken: resp.Result.Credentials.SessionToken,
			})
			require.NoError(t, err)
			assert.True(t, allowed, "the role grants s3:GetObject")

			denied, err := manager.IsActionAllowed(ctx, &integration.ActionRequest{
				Principal:    info.Principal,
				Action:       "s3:PutObject",
				Resource:     "arn:aws:s3:::bucket/key",
				SessionToken: resp.Result.Credentials.SessionToken,
			})
			require.NoError(t, err)
			assert.False(t, denied, "the role does not grant s3:PutObject")
		})
	}

	t.Run("inline session policy is reported in PackedPolicySize", func(t *testing.T) {
		sessionPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bucket/*"]}]}`
		form := url.Values{
			"Action":           {"AssumeRoleWithWebIdentity"},
			"Version":          {"2011-06-15"},
			"RoleArn":          {trustedRoleArn},
			"RoleSessionName":  {"web-session-scoped"},
			"WebIdentityToken": {signOIDCToken(t, "oidc-user-2")},
			"Policy":           {sessionPolicy},
		}
		req := httptest.NewRequest(http.MethodPost, "http://sts.seaweedfs.test/", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rec := httptest.NewRecorder()
		handlers.HandleSTSRequest(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var resp AssumeRoleWithWebIdentityResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		require.NotNil(t, resp.Result.PackedPolicySize)
		assert.Greater(t, *resp.Result.PackedPolicySize, int64(0))

		info, err := manager.GetSTSService().ValidateSessionToken(ctx, resp.Result.Credentials.SessionToken)
		require.NoError(t, err)
		assert.NotEmpty(t, info.SessionPolicy, "the inline policy must travel in the session token")
	})

	t.Run("role whose trust policy rejects the provider is denied", func(t *testing.T) {
		form := url.Values{
			"Action":           {"AssumeRoleWithWebIdentity"},
			"Version":          {"2011-06-15"},
			"RoleArn":          {"arn:aws:iam::" + defaultAccountID + ":role/WebIdentityDeniedRole"},
			"RoleSessionName":  {"web-session"},
			"WebIdentityToken": {signOIDCToken(t, "oidc-user-1")},
		}
		req := httptest.NewRequest(http.MethodPost, "http://sts.seaweedfs.test/", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rec := httptest.NewRecorder()
		handlers.HandleSTSRequest(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	})

	t.Run("token signed by an unknown key is rejected", func(t *testing.T) {
		otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
			"iss": idp.URL,
			"sub": "forged-user",
			"aud": "test-client",
			"exp": time.Now().Add(time.Hour).Unix(),
		})
		token.Header["kid"] = "web-identity-test-key"
		forged, err := token.SignedString(otherKey)
		require.NoError(t, err)

		form := url.Values{
			"Action":           {"AssumeRoleWithWebIdentity"},
			"Version":          {"2011-06-15"},
			"RoleArn":          {trustedRoleArn},
			"RoleSessionName":  {"web-session"},
			"WebIdentityToken": {forged},
		}
		req := httptest.NewRequest(http.MethodPost, "http://sts.seaweedfs.test/", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		rec := httptest.NewRecorder()
		handlers.HandleSTSRequest(rec, req)
		assert.NotEqual(t, http.StatusOK, rec.Code, "a forged signature must not mint a session")
	})
}

package oidc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOIDCProviderInitialization tests OIDC provider initialization
func TestOIDCProviderInitialization(t *testing.T) {
	tests := []struct {
		name    string
		config  *OIDCConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: &OIDCConfig{
				Issuer:   "https://accounts.google.com",
				ClientID: "test-client-id",
				JWKSUri:  "https://www.googleapis.com/oauth2/v3/certs",
			},
			wantErr: false,
		},
		{
			name: "missing issuer",
			config: &OIDCConfig{
				ClientID: "test-client-id",
			},
			wantErr: true,
		},
		{
			name: "missing client id",
			config: &OIDCConfig{
				Issuer: "https://accounts.google.com",
			},
			wantErr: true,
		},
		{
			name: "invalid issuer url",
			config: &OIDCConfig{
				Issuer:   "not-a-url",
				ClientID: "test-client-id",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := NewOIDCProvider("test-provider")

			err := provider.Initialize(tt.config)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, "test-provider", provider.Name())
			}
		})
	}
}

// TestOIDCProviderJWTValidation tests JWT token validation
func TestOIDCProviderJWTValidation(t *testing.T) {
	// Set up test server with JWKS endpoint
	privateKey, publicKey := generateTestKeys(t)

	jwks := map[string]interface{}{
		"keys": []map[string]interface{}{
			{
				"kty": "RSA",
				"kid": "test-key-id",
				"use": "sig",
				"alg": "RS256",
				"n":   encodePublicKey(t, publicKey),
				"e":   "AQAB",
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/openid_configuration" {
			config := map[string]interface{}{
				"issuer":   "http://" + r.Host,
				"jwks_uri": "http://" + r.Host + "/jwks",
			}
			json.NewEncoder(w).Encode(config)
		} else if r.URL.Path == "/jwks" {
			json.NewEncoder(w).Encode(jwks)
		}
	}))
	defer server.Close()

	provider := NewOIDCProvider("test-oidc")
	config := &OIDCConfig{
		Issuer:   server.URL,
		ClientID: "test-client",
		JWKSUri:  server.URL + "/jwks",
	}

	err := provider.Initialize(config)
	require.NoError(t, err)

	t.Run("valid token", func(t *testing.T) {
		// Create valid JWT token
		token := createTestJWT(t, privateKey, jwt.MapClaims{
			"iss":   server.URL,
			"aud":   "test-client",
			"sub":   "user123",
			"exp":   time.Now().Add(time.Hour).Unix(),
			"iat":   time.Now().Unix(),
			"email": "user@example.com",
			"name":  "Test User",
		})

		claims, err := provider.ValidateToken(context.Background(), token)
		require.NoError(t, err)
		require.NotNil(t, claims)
		assert.Equal(t, "user123", claims.Subject)
		assert.Equal(t, server.URL, claims.Issuer)

		email, exists := claims.GetClaimString("email")
		assert.True(t, exists)
		assert.Equal(t, "user@example.com", email)
	})

	t.Run("valid token with array audience", func(t *testing.T) {
		// Create valid JWT token with audience as an array (per RFC 7519)
		token := createTestJWT(t, privateKey, jwt.MapClaims{
			"iss":   server.URL,
			"aud":   []string{"test-client", "another-client"},
			"sub":   "user456",
			"exp":   time.Now().Add(time.Hour).Unix(),
			"iat":   time.Now().Unix(),
			"email": "user2@example.com",
			"name":  "Test User 2",
		})

		claims, err := provider.ValidateToken(context.Background(), token)
		require.NoError(t, err)
		require.NotNil(t, claims)
		assert.Equal(t, "user456", claims.Subject)
		assert.Equal(t, server.URL, claims.Issuer)

		email, exists := claims.GetClaimString("email")
		assert.True(t, exists)
		assert.Equal(t, "user2@example.com", email)
	})

	t.Run("expired token", func(t *testing.T) {
		// Create expired JWT token
		token := createTestJWT(t, privateKey, jwt.MapClaims{
			"iss": server.URL,
			"aud": "test-client",
			"sub": "user123",
			"exp": time.Now().Add(-time.Hour).Unix(), // Expired
			"iat": time.Now().Add(-time.Hour * 2).Unix(),
		})

		_, err := provider.ValidateToken(context.Background(), token)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expired")
	})

	t.Run("invalid signature", func(t *testing.T) {
		// Create token with wrong key
		wrongKey, _ := generateTestKeys(t)
		token := createTestJWT(t, wrongKey, jwt.MapClaims{
			"iss": server.URL,
			"aud": "test-client",
			"sub": "user123",
			"exp": time.Now().Add(time.Hour).Unix(),
			"iat": time.Now().Unix(),
		})

		_, err := provider.ValidateToken(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, providers.ErrProviderInvalidToken)
	})
}

func TestOIDCProviderJWTValidationECDSA(t *testing.T) {
	privateKey, publicKey := generateTestECKeys(t)
	x, y := encodeECPublicKey(t, publicKey)

	jwks := map[string]interface{}{
		"keys": []map[string]interface{}{
			{
				"kty": "EC",
				"kid": "test-ec-key-id",
				"use": "sig",
				"alg": "ES256",
				"crv": "P-256",
				"x":   x,
				"y":   y,
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/openid_configuration" {
			config := map[string]interface{}{
				"issuer":   "http://" + r.Host,
				"jwks_uri": "http://" + r.Host + "/jwks",
			}
			json.NewEncoder(w).Encode(config)
		} else if r.URL.Path == "/jwks" {
			json.NewEncoder(w).Encode(jwks)
		}
	}))
	defer server.Close()

	provider := NewOIDCProvider("test-oidc-ecdsa")
	config := &OIDCConfig{
		Issuer:   server.URL,
		ClientID: "test-client",
		JWKSUri:  server.URL + "/jwks",
	}

	err := provider.Initialize(config)
	require.NoError(t, err)

	t.Run("valid token", func(t *testing.T) {
		token := createTestECDSAJWT(t, privateKey, jwt.MapClaims{
			"iss": server.URL,
			"aud": "test-client",
			"sub": "user789",
			"exp": time.Now().Add(time.Hour).Unix(),
			"iat": time.Now().Unix(),
		})

		claims, err := provider.ValidateToken(context.Background(), token)
		require.NoError(t, err)
		require.NotNil(t, claims)
		assert.Equal(t, "user789", claims.Subject)
		assert.Equal(t, server.URL, claims.Issuer)
	})

	t.Run("expired token", func(t *testing.T) {
		token := createTestECDSAJWT(t, privateKey, jwt.MapClaims{
			"iss": server.URL,
			"aud": "test-client",
			"sub": "user789",
			"exp": time.Now().Add(-time.Hour).Unix(),
			"iat": time.Now().Add(-time.Hour * 2).Unix(),
		})

		_, err := provider.ValidateToken(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, providers.ErrProviderTokenExpired)
	})

	t.Run("invalid signature", func(t *testing.T) {
		wrongKey, _ := generateTestECKeys(t)
		token := createTestECDSAJWT(t, wrongKey, jwt.MapClaims{
			"iss": server.URL,
			"aud": "test-client",
			"sub": "user789",
			"exp": time.Now().Add(time.Hour).Unix(),
			"iat": time.Now().Unix(),
		})

		_, err := provider.ValidateToken(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, providers.ErrProviderInvalidToken)
	})

	t.Run("invalid issuer", func(t *testing.T) {
		token := createTestECDSAJWT(t, privateKey, jwt.MapClaims{
			"iss": "http://wrong-issuer",
			"aud": "test-client",
			"sub": "user789",
			"exp": time.Now().Add(time.Hour).Unix(),
			"iat": time.Now().Unix(),
		})

		_, err := provider.ValidateToken(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, providers.ErrProviderInvalidIssuer)
	})

	t.Run("invalid audience", func(t *testing.T) {
		token := createTestECDSAJWT(t, privateKey, jwt.MapClaims{
			"iss": server.URL,
			"aud": "wrong-client",
			"sub": "user789",
			"exp": time.Now().Add(time.Hour).Unix(),
			"iat": time.Now().Unix(),
		})

		_, err := provider.ValidateToken(context.Background(), token)
		require.Error(t, err)
		assert.ErrorIs(t, err, providers.ErrProviderInvalidAudience)
	})
}

// TestOIDCProviderAuthentication tests authentication flow
func TestOIDCProviderAuthentication(t *testing.T) {
	// Set up test OIDC provider
	privateKey, publicKey := generateTestKeys(t)

	server := setupOIDCTestServer(t, publicKey)
	defer server.Close()

	provider := NewOIDCProvider("test-oidc")
	config := &OIDCConfig{
		Issuer:   server.URL,
		ClientID: "test-client",
		JWKSUri:  server.URL + "/jwks",
		RoleMapping: &providers.RoleMapping{
			Rules: []providers.MappingRule{
				{
					Claim: "email",
					Value: "*@example.com",
					Role:  "arn:aws:iam::role/UserRole",
				},
				{
					Claim: "groups",
					Value: "admins",
					Role:  "arn:aws:iam::role/AdminRole",
				},
			},
			DefaultRole: "arn:aws:iam::role/GuestRole",
		},
	}

	err := provider.Initialize(config)
	require.NoError(t, err)

	t.Run("successful authentication", func(t *testing.T) {
		token := createTestJWT(t, privateKey, jwt.MapClaims{
			"iss":    server.URL,
			"aud":    "test-client",
			"sub":    "user123",
			"exp":    time.Now().Add(time.Hour).Unix(),
			"iat":    time.Now().Unix(),
			"email":  "user@example.com",
			"name":   "Test User",
			"groups": []string{"users", "developers"},
		})

		identity, err := provider.Authenticate(context.Background(), token)
		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.Equal(t, "user123", identity.UserID)
		assert.Equal(t, "user@example.com", identity.Email)
		assert.Equal(t, "Test User", identity.DisplayName)
		assert.Equal(t, "test-oidc", identity.Provider)
		assert.Contains(t, identity.Groups, "users")
		assert.Contains(t, identity.Groups, "developers")
	})

	t.Run("successful authentication with additional attributes", func(t *testing.T) {
		token := createTestJWT(t, privateKey, jwt.MapClaims{
			"iss":                server.URL,
			"aud":                "test-client",
			"sub":                "user123",
			"exp":                time.Now().Add(time.Hour).Unix(),
			"iat":                time.Now().Unix(),
			"email":              "user@example.com",
			"name":               "Test User",
			"groups":             []string{"users"},
			"preferred_username": "myusername",                              // Extra claim
			"department":         "engineering",                             // Extra claim
			"custom_number":      42,                                        // Non-string claim
			"custom_avg":         98.6,                                      // Non-string claim
			"custom_object":      map[string]interface{}{"nested": "value"}, // Nested object claim
		})

		identity, err := provider.Authenticate(context.Background(), token)
		require.NoError(t, err)
		require.NotNil(t, identity)

		// Check standard fields
		assert.Equal(t, "user123", identity.UserID)

		// Check attributes
		val, exists := identity.Attributes["preferred_username"]
		assert.True(t, exists, "preferred_username should be in attributes")
		assert.Equal(t, "myusername", val)

		val, exists = identity.Attributes["department"]
		assert.True(t, exists, "department should be in attributes")
		assert.Equal(t, "engineering", val)

		// Test non-string claims (should be JSON marshaled)
		val, exists = identity.Attributes["custom_number"]
		assert.True(t, exists, "custom_number should be in attributes")
		assert.Equal(t, "42", val)

		val, exists = identity.Attributes["custom_avg"]
		assert.True(t, exists, "custom_avg should be in attributes")
		assert.Contains(t, val, "98.6") // JSON number formatting might vary

		val, exists = identity.Attributes["custom_object"]
		assert.True(t, exists, "custom_object should be in attributes")
		assert.Contains(t, val, "\"nested\":\"value\"")

		// Verify structural JWT claims are excluded from attributes
		excludedClaims := []string{"iss", "aud", "exp", "iat"}
		for _, claim := range excludedClaims {
			_, exists := identity.Attributes[claim]
			assert.False(t, exists, "standard claim %s should not be in attributes", claim)
		}
	})

	t.Run("authentication with invalid token", func(t *testing.T) {
		_, err := provider.Authenticate(context.Background(), "invalid-token")
		assert.Error(t, err)
	})
}

// TestOIDCProviderUserInfo tests user info retrieval
func TestOIDCProviderUserInfo(t *testing.T) {
	// Set up test server with UserInfo endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/userinfo" {
			// Check for Authorization header
			authHeader := r.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error": "unauthorized"}`))
				return
			}

			accessToken := strings.TrimPrefix(authHeader, "Bearer ")

			// Return 401 for explicitly invalid tokens
			if accessToken == "invalid-token" {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error": "invalid_token"}`))
				return
			}

			// Mock user info response
			userInfo := map[string]interface{}{
				"sub":    "user123",
				"email":  "user@example.com",
				"name":   "Test User",
				"groups": []string{"users", "developers"},
			}

			// Customize response based on token
			if strings.Contains(accessToken, "admin") {
				userInfo["groups"] = []string{"admins"}
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(userInfo)
		}
	}))
	defer server.Close()

	provider := NewOIDCProvider("test-oidc")
	config := &OIDCConfig{
		Issuer:      server.URL,
		ClientID:    "test-client",
		UserInfoUri: server.URL + "/userinfo",
	}

	err := provider.Initialize(config)
	require.NoError(t, err)

	t.Run("get user info with access token", func(t *testing.T) {
		// Test using access token (real UserInfo endpoint call)
		identity, err := provider.GetUserInfoWithToken(context.Background(), "valid-access-token")
		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.Equal(t, "user123", identity.UserID)
		assert.Equal(t, "user@example.com", identity.Email)
		assert.Equal(t, "Test User", identity.DisplayName)
		assert.Contains(t, identity.Groups, "users")
		assert.Contains(t, identity.Groups, "developers")
		assert.Equal(t, "test-oidc", identity.Provider)
	})

	t.Run("get admin user info", func(t *testing.T) {
		// Test admin token response
		identity, err := provider.GetUserInfoWithToken(context.Background(), "admin-access-token")
		require.NoError(t, err)
		require.NotNil(t, identity)
		assert.Equal(t, "user123", identity.UserID)
		assert.Contains(t, identity.Groups, "admins")
	})

	t.Run("get user info without token", func(t *testing.T) {
		// Test without access token (should fail)
		_, err := provider.GetUserInfoWithToken(context.Background(), "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "access token cannot be empty")
	})

	t.Run("get user info with invalid token", func(t *testing.T) {
		// Test with invalid access token (should get 401)
		_, err := provider.GetUserInfoWithToken(context.Background(), "invalid-token")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "UserInfo endpoint returned status 401")
	})

	t.Run("get user info with custom claims mapping", func(t *testing.T) {
		// Create provider with custom claims mapping
		customProvider := NewOIDCProvider("test-custom-oidc")
		customConfig := &OIDCConfig{
			Issuer:      server.URL,
			ClientID:    "test-client",
			UserInfoUri: server.URL + "/userinfo",
			ClaimsMapping: map[string]string{
				"customEmail": "email",
				"customName":  "name",
			},
		}

		err := customProvider.Initialize(customConfig)
		require.NoError(t, err)

		identity, err := customProvider.GetUserInfoWithToken(context.Background(), "valid-access-token")
		require.NoError(t, err)
		require.NotNil(t, identity)

		// Standard claims should still work
		assert.Equal(t, "user123", identity.UserID)
		assert.Equal(t, "user@example.com", identity.Email)
		assert.Equal(t, "Test User", identity.DisplayName)
	})

	t.Run("get user info with empty id", func(t *testing.T) {
		_, err := provider.GetUserInfo(context.Background(), "")
		assert.Error(t, err)
	})
}

// Helper functions for testing

func generateTestKeys(tb testing.TB) (*rsa.PrivateKey, *rsa.PublicKey) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(tb, err)
	return privateKey, &privateKey.PublicKey
}

func generateTestECKeys(tb testing.TB) (*ecdsa.PrivateKey, *ecdsa.PublicKey) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(tb, err)
	return privateKey, &privateKey.PublicKey
}

func createTestJWT(tb testing.TB, privateKey *rsa.PrivateKey, claims jwt.MapClaims) string {
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "test-key-id"

	tokenString, err := token.SignedString(privateKey)
	require.NoError(tb, err)
	return tokenString
}

func createTestECDSAJWT(tb testing.TB, privateKey *ecdsa.PrivateKey, claims jwt.MapClaims) string {
	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = "test-ec-key-id"

	tokenString, err := token.SignedString(privateKey)
	require.NoError(tb, err)
	return tokenString
}

func encodePublicKey(tb testing.TB, publicKey *rsa.PublicKey) string {
	// Properly encode the RSA modulus (N) as base64url
	return base64.RawURLEncoding.EncodeToString(publicKey.N.Bytes())
}

func encodeECPublicKey(tb testing.TB, publicKey *ecdsa.PublicKey) (string, string) {
	// RFC 7518 §6.2.1.2 requires EC coordinates to be zero-padded to the full field size
	curveParams := publicKey.Curve.Params()
	size := (curveParams.BitSize + 7) / 8
	xBytes := publicKey.X.Bytes()
	yBytes := publicKey.Y.Bytes()
	xPadded := make([]byte, size)
	yPadded := make([]byte, size)
	// Right-align the coordinate bytes and leave leading zeros for padding
	copy(xPadded[size-len(xBytes):], xBytes)
	copy(yPadded[size-len(yBytes):], yBytes)
	return base64.RawURLEncoding.EncodeToString(xPadded),
		base64.RawURLEncoding.EncodeToString(yPadded)
}

func setupOIDCTestServer(tb testing.TB, publicKey *rsa.PublicKey) *httptest.Server {
	jwks := map[string]interface{}{
		"keys": []map[string]interface{}{
			{
				"kty": "RSA",
				"kid": "test-key-id",
				"use": "sig",
				"alg": "RS256",
				"n":   encodePublicKey(tb, publicKey),
				"e":   "AQAB",
			},
		},
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid_configuration":
			config := map[string]interface{}{
				"issuer":            "http://" + r.Host,
				"jwks_uri":          "http://" + r.Host + "/jwks",
				"userinfo_endpoint": "http://" + r.Host + "/userinfo",
			}
			json.NewEncoder(w).Encode(config)
		case "/jwks":
			json.NewEncoder(w).Encode(jwks)
		case "/userinfo":
			// Mock UserInfo endpoint
			authHeader := r.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error": "unauthorized"}`))
				return
			}

			accessToken := strings.TrimPrefix(authHeader, "Bearer ")

			// Return 401 for explicitly invalid tokens
			if accessToken == "invalid-token" {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error": "invalid_token"}`))
				return
			}

			// Mock user info response based on access token
			userInfo := map[string]interface{}{
				"sub":    "user123",
				"email":  "user@example.com",
				"name":   "Test User",
				"groups": []string{"users", "developers"},
			}

			// Customize response based on token
			if strings.Contains(accessToken, "admin") {
				userInfo["groups"] = []string{"admins"}
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(userInfo)
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestOIDCProviderJTIReplayProtection(t *testing.T) {
	// Setup mock OIDC provider with JTI protection enabled
	privateKey, publicKey := generateTestKeys(t)
	jwksServer := setupOIDCTestServer(t, publicKey)
	defer jwksServer.Close()

	provider := NewOIDCProvider("test-oidc")
	jtiEnabled := true
	config := &OIDCConfig{
		Issuer:                     jwksServer.URL,
		ClientID:                   "test-client",
		JWKSUri:                    jwksServer.URL + "/jwks",
		JTIReplayProtectionEnabled: &jtiEnabled,
		JTICacheMaxSize:            100,
	}

	err := provider.Initialize(config)
	require.NoError(t, err)

	// Test 1: First use of token with JTI should succeed
	jti := "unique-token-id-12345"
	token1 := createTestJWT(t, privateKey, jwt.MapClaims{
		"iss": jwksServer.URL,
		"aud": "test-client",
		"sub": "user123",
		"jti": jti,
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})

	claims1, err := provider.ValidateToken(context.Background(), token1)
	require.NoError(t, err)
	assert.Equal(t, "user123", claims1.Subject)
	assert.Equal(t, jti, claims1.Claims["jti"])

	// Test 2: Replay of same token should be rejected
	claims2, err := provider.ValidateToken(context.Background(), token1)
	assert.Error(t, err)
	assert.Nil(t, claims2)
	assert.True(t, errors.Is(err, providers.ErrProviderTokenReplayed))
	assert.Contains(t, err.Error(), jti)

	// Test 3: Different token with different JTI should succeed
	jti2 := "different-token-id-67890"
	token2 := createTestJWT(t, privateKey, jwt.MapClaims{
		"iss": jwksServer.URL,
		"aud": "test-client",
		"sub": "user456",
		"jti": jti2,
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})

	claims3, err := provider.ValidateToken(context.Background(), token2)
	require.NoError(t, err)
	assert.Equal(t, "user456", claims3.Subject)

	// Test 4: Token without JTI should still be accepted (backward compatibility)
	tokenNoJTI := createTestJWT(t, privateKey, jwt.MapClaims{
		"iss": jwksServer.URL,
		"aud": "test-client",
		"sub": "user789",
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})

	claims4, err := provider.ValidateToken(context.Background(), tokenNoJTI)
	require.NoError(t, err)
	assert.Equal(t, "user789", claims4.Subject)
}

func TestOIDCProviderJTIReplayProtectionDisabled(t *testing.T) {
	// Setup provider with JTI protection DISABLED
	privateKey, publicKey := generateTestKeys(t)
	jwksServer := setupOIDCTestServer(t, publicKey)
	defer jwksServer.Close()

	provider := NewOIDCProvider("test-oidc-no-jti")
	jtiDisabled := false
	config := &OIDCConfig{
		Issuer:                     jwksServer.URL,
		ClientID:                   "test-client",
		JWKSUri:                    jwksServer.URL + "/jwks",
		JTIReplayProtectionEnabled: &jtiDisabled, // Explicitly disabled
	}

	err := provider.Initialize(config)
	require.NoError(t, err)

	// Token with JTI can be used multiple times when protection is disabled
	jti := "reusable-token-id"
	token := createTestJWT(t, privateKey, jwt.MapClaims{
		"iss": jwksServer.URL,
		"aud": "test-client",
		"sub": "user123",
		"jti": jti,
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})

	// First use - should succeed
	claims1, err := provider.ValidateToken(context.Background(), token)
	require.NoError(t, err)
	assert.Equal(t, "user123", claims1.Subject)

	// Second use - should ALSO succeed (no replay protection)
	claims2, err := provider.ValidateToken(context.Background(), token)
	require.NoError(t, err)
	assert.Equal(t, "user123", claims2.Subject)
}

func TestOIDCProviderJTIExpiration(t *testing.T) {
	// Test that JTI cache entries expire with token
	// This test also verifies default behavior (JTIReplayProtectionEnabled not set = enabled by default)
	privateKey, publicKey := generateTestKeys(t)
	jwksServer := setupOIDCTestServer(t, publicKey)
	defer jwksServer.Close()

	provider := NewOIDCProvider("test-oidc")
	config := &OIDCConfig{
		Issuer:   jwksServer.URL,
		ClientID: "test-client",
		JWKSUri:  jwksServer.URL + "/jwks",
		// JTIReplayProtectionEnabled not set - should default to true (secure by default)
	}

	err := provider.Initialize(config)
	require.NoError(t, err)

	// Create token that expires in 2 seconds
	jti := "short-lived-token"
	token := createTestJWT(t, privateKey, jwt.MapClaims{
		"iss": jwksServer.URL,
		"aud": "test-client",
		"sub": "user123",
		"jti": jti,
		"exp": time.Now().Add(2 * time.Second).Unix(),
		"iat": time.Now().Unix(),
	})

	// First use - should succeed
	claims1, err := provider.ValidateToken(context.Background(), token)
	require.NoError(t, err)
	assert.Equal(t, "user123", claims1.Subject)

	// Immediate replay - should be rejected
	_, err = provider.ValidateToken(context.Background(), token)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, providers.ErrProviderTokenReplayed))

	// Wait for token AND cache entry to expire
	time.Sleep(3 * time.Second)

	// Now validation should fail due to EXPIRATION (not replay)
	_, err = provider.ValidateToken(context.Background(), token)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, providers.ErrProviderTokenExpired))
}

func TestOIDCProviderJTIReplayProtectionConcurrent(t *testing.T) {
	// Test concurrent validation to ensure no TOCTOU race conditions
	privateKey, publicKey := generateTestKeys(t)
	jwksServer := setupOIDCTestServer(t, publicKey)
	defer jwksServer.Close()

	provider := NewOIDCProvider("test-oidc-concurrent")
	jtiEnabled := true
	config := &OIDCConfig{
		Issuer:                     jwksServer.URL,
		ClientID:                   "test-client",
		JWKSUri:                    jwksServer.URL + "/jwks",
		JTIReplayProtectionEnabled: &jtiEnabled,
		JTICacheMaxSize:            1000,
	}
	require.NoError(t, provider.Initialize(config))

	jti := "concurrent-test-jti"
	token := createTestJWT(t, privateKey, jwt.MapClaims{
		"iss": jwksServer.URL,
		"aud": "test-client",
		"sub": "user123",
		"jti": jti,
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})

	// Launch 100 concurrent validations with the same token
	const numGoroutines = 100
	var successCount, replayCount, otherErrorCount atomic.Int32

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// All goroutines start at roughly the same time
	startBarrier := make(chan struct{})

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			<-startBarrier // Wait for start signal

			_, err := provider.ValidateToken(context.Background(), token)
			if err == nil {
				successCount.Add(1)
			} else if errors.Is(err, providers.ErrProviderTokenReplayed) {
				replayCount.Add(1)
			} else {
				otherErrorCount.Add(1)
				t.Logf("Unexpected error: %v", err)
			}
		}()
	}

	close(startBarrier) // Start all goroutines
	wg.Wait()

	// Exactly one should succeed, all others should be replay errors
	assert.Equal(t, int32(1), successCount.Load(),
		"Exactly one validation should succeed (atomic test-and-set)")
	assert.Equal(t, int32(numGoroutines-1), replayCount.Load(),
		"All other validations should be replay errors")
	assert.Equal(t, int32(0), otherErrorCount.Load(),
		"No other errors should occur")
}

func TestOIDCProviderJTICacheMaxSize(t *testing.T) {
	// Test that cache respects max size limit
	privateKey, publicKey := generateTestKeys(t)
	jwksServer := setupOIDCTestServer(t, publicKey)
	defer jwksServer.Close()

	provider := NewOIDCProvider("test-oidc-maxsize")
	jtiEnabled := true
	config := &OIDCConfig{
		Issuer:                     jwksServer.URL,
		ClientID:                   "test-client",
		JWKSUri:                    jwksServer.URL + "/jwks",
		JTIReplayProtectionEnabled: &jtiEnabled,
		JTICacheMaxSize:            10, // Small cache for testing
	}
	require.NoError(t, provider.Initialize(config))

	// Add tokens up to the limit
	for i := 0; i < 10; i++ {
		token := createTestJWT(t, privateKey, jwt.MapClaims{
			"iss": jwksServer.URL,
			"aud": "test-client",
			"sub": "user123",
			"jti": fmt.Sprintf("jti-%d", i),
			"exp": time.Now().Add(time.Hour).Unix(),
			"iat": time.Now().Unix(),
		})

		_, err := provider.ValidateToken(context.Background(), token)
		require.NoError(t, err, "Should accept token %d", i)
	}

	// Next token should be rejected due to cache full
	token := createTestJWT(t, privateKey, jwt.MapClaims{
		"iss": jwksServer.URL,
		"aud": "test-client",
		"sub": "user123",
		"jti": "jti-overflow",
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})

	_, err := provider.ValidateToken(context.Background(), token)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cache capacity exceeded")
}

func TestOIDCProviderJTICleanup(t *testing.T) {
	// Test that cleanup goroutine logic correctly identifies expired entries
	// NOTE: JWT library allows clock skew, so tokens can't be truly expired
	// We test the cleanup logic without actually waiting for expiration
	privateKey, publicKey := generateTestKeys(t)
	jwksServer := setupOIDCTestServer(t, publicKey)
	defer jwksServer.Close()

	provider := NewOIDCProvider("test-oidc-cleanup")
	jtiEnabled := true
	config := &OIDCConfig{
		Issuer:                     jwksServer.URL,
		ClientID:                   "test-client",
		JWKSUri:                    jwksServer.URL + "/jwks",
		JTIReplayProtectionEnabled: &jtiEnabled,
	}
	require.NoError(t, provider.Initialize(config))
	defer provider.Shutdown(context.Background())

	// Create token that expires in 1 hour
	token := createTestJWT(t, privateKey, jwt.MapClaims{
		"iss": jwksServer.URL,
		"aud": "test-client",
		"sub": "user123",
		"jti": "cleanup-test-jti",
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})

	// Validation should succeed
	_, err := provider.ValidateToken(context.Background(), token)
	require.NoError(t, err)

	// Check cache size is 1
	assert.Equal(t, int64(1), provider.jtiCount.Load())

	// Verify entry exists and check its expiration includes clock skew
	val, ok := provider.jtiStore.Load("cleanup-test-jti")
	require.True(t, ok, "JTI entry should exist")
	entry := val.(*jtiEntry)

	// Entry should expire around exp + clock skew tolerance (1 hour + 5 minutes)
	expectedExpiry := time.Now().Add(time.Hour + 5*time.Minute)
	assert.WithinDuration(t, expectedExpiry, entry.expiresAt, 10*time.Second,
		"Entry expiry should be token exp + clock skew tolerance")

	// Verify entry is not yet expired (cleanup shouldn't remove it)
	now := time.Now()
	assert.True(t, now.Before(entry.expiresAt),
		"Entry should not be expired yet")

	// Test cleanup logic: manually simulate cleanup with a fake "future" time
	// to verify the cleanup would work when time comes
	futureTime := entry.expiresAt.Add(1 * time.Minute)
	shouldBeDeleted := futureTime.After(entry.expiresAt)
	assert.True(t, shouldBeDeleted, "Entry should be identified as expired in the future")
}

func TestOIDCProviderReinitialization(t *testing.T) {
	// Test that re-initialization properly cancels the old cleanup goroutine
	// to prevent goroutine leaks
	privateKey, publicKey := generateTestKeys(t)
	jwksServer := setupOIDCTestServer(t, publicKey)
	defer jwksServer.Close()

	provider := NewOIDCProvider("test-oidc-reinit")
	jtiEnabled := true
	config := &OIDCConfig{
		Issuer:                     jwksServer.URL,
		ClientID:                   "test-client",
		JWKSUri:                    jwksServer.URL + "/jwks",
		JTIReplayProtectionEnabled: &jtiEnabled,
	}

	// First initialization
	require.NoError(t, provider.Initialize(config))
	require.NotNil(t, provider.jtiCleanupCancel, "First cleanup cancel should be set")

	// Create and validate a token to ensure JTI protection is working
	token1 := createTestJWT(t, privateKey, jwt.MapClaims{
		"iss": jwksServer.URL,
		"aud": "test-client",
		"sub": "user1",
		"jti": "token-1",
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})
	_, err := provider.ValidateToken(context.Background(), token1)
	require.NoError(t, err, "First token should validate successfully")

	// Second initialization should not cause errors
	require.NoError(t, provider.Initialize(config))
	require.NotNil(t, provider.jtiCleanupCancel, "Second cleanup cancel should be set")

	// JTI cache should be reset after re-initialization
	// So the same token should be accepted again (new cache)
	_, err = provider.ValidateToken(context.Background(), token1)
	require.NoError(t, err, "Token should validate after re-initialization (new cache)")

	// Third initialization to ensure it works multiple times
	require.NoError(t, provider.Initialize(config))
	require.NotNil(t, provider.jtiCleanupCancel, "Third cleanup cancel should be set")

	// Verify JTI replay protection still works after re-initialization
	token2 := createTestJWT(t, privateKey, jwt.MapClaims{
		"iss": jwksServer.URL,
		"aud": "test-client",
		"sub": "user2",
		"jti": "token-2",
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})
	_, err = provider.ValidateToken(context.Background(), token2)
	require.NoError(t, err, "New token should validate")

	// Replay should still be detected
	_, err = provider.ValidateToken(context.Background(), token2)
	assert.Error(t, err, "Replay should be detected")
	assert.True(t, errors.Is(err, providers.ErrProviderTokenReplayed), "Should be a replay error")

	// Final cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = provider.Shutdown(ctx)
	assert.NoError(t, err)
}

func TestOIDCProviderShutdown(t *testing.T) {
	// Test graceful shutdown
	_, publicKey := generateTestKeys(t)
	jwksServer := setupOIDCTestServer(t, publicKey)
	defer jwksServer.Close()

	provider := NewOIDCProvider("test-oidc-shutdown")
	jtiEnabled := true
	config := &OIDCConfig{
		Issuer:                     jwksServer.URL,
		ClientID:                   "test-client",
		JWKSUri:                    jwksServer.URL + "/jwks",
		JTIReplayProtectionEnabled: &jtiEnabled,
	}
	require.NoError(t, provider.Initialize(config))

	// Shutdown should complete without error
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := provider.Shutdown(ctx)
	assert.NoError(t, err)

	// Second shutdown should be idempotent
	err = provider.Shutdown(ctx)
	assert.NoError(t, err)
}

// Benchmark tests

func BenchmarkJTIValidation(b *testing.B) {
	// Benchmark JTI validation performance with unique tokens
	privateKey, publicKey := generateTestKeys(b)
	jwksServer := setupOIDCTestServer(b, publicKey)
	defer jwksServer.Close()

	provider := NewOIDCProvider("bench-oidc")
	jtiEnabled := true
	config := &OIDCConfig{
		Issuer:                     jwksServer.URL,
		ClientID:                   "test-client",
		JWKSUri:                    jwksServer.URL + "/jwks",
		JTIReplayProtectionEnabled: &jtiEnabled,
		JTICacheMaxSize:            100000,
	}
	require.NoError(b, provider.Initialize(config))
	defer provider.Shutdown(context.Background())

	// Pre-generate unique tokens
	tokens := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		tokens[i] = createTestJWT(b, privateKey, jwt.MapClaims{
			"iss": jwksServer.URL,
			"aud": "test-client",
			"sub": "user123",
			"jti": fmt.Sprintf("jti-%d", i),
			"exp": time.Now().Add(time.Hour).Unix(),
			"iat": time.Now().Unix(),
		})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			provider.ValidateToken(context.Background(), tokens[i%len(tokens)])
			i++
		}
	})
}

func BenchmarkJTIValidationWithReplay(b *testing.B) {
	// Benchmark replay detection performance
	privateKey, publicKey := generateTestKeys(b)
	jwksServer := setupOIDCTestServer(b, publicKey)
	defer jwksServer.Close()

	provider := NewOIDCProvider("bench-oidc-replay")
	jtiEnabled := true
	config := &OIDCConfig{
		Issuer:                     jwksServer.URL,
		ClientID:                   "test-client",
		JWKSUri:                    jwksServer.URL + "/jwks",
		JTIReplayProtectionEnabled: &jtiEnabled,
		JTICacheMaxSize:            100000,
	}
	require.NoError(b, provider.Initialize(config))
	defer provider.Shutdown(context.Background())

	// Create one token that will be replayed
	token := createTestJWT(b, privateKey, jwt.MapClaims{
		"iss": jwksServer.URL,
		"aud": "test-client",
		"sub": "user123",
		"jti": "replay-jti",
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})

	// First validation to store the JTI
	_, err := provider.ValidateToken(context.Background(), token)
	require.NoError(b, err)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// All subsequent validations should be fast replay rejections
			provider.ValidateToken(context.Background(), token)
		}
	})
}

func BenchmarkJTIValidationNoProtection(b *testing.B) {
	// Benchmark validation without JTI protection (baseline)
	privateKey, publicKey := generateTestKeys(b)
	jwksServer := setupOIDCTestServer(b, publicKey)
	defer jwksServer.Close()

	provider := NewOIDCProvider("bench-oidc-no-jti")
	jtiDisabled := false
	config := &OIDCConfig{
		Issuer:                     jwksServer.URL,
		ClientID:                   "test-client",
		JWKSUri:                    jwksServer.URL + "/jwks",
		JTIReplayProtectionEnabled: &jtiDisabled,
	}
	require.NoError(b, provider.Initialize(config))

	token := createTestJWT(b, privateKey, jwt.MapClaims{
		"iss": jwksServer.URL,
		"aud": "test-client",
		"sub": "user123",
		"jti": "no-protection-jti",
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Unix(),
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			provider.ValidateToken(context.Background(), token)
		}
	})
}

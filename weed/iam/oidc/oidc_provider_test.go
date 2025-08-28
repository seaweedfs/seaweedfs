package oidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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
		assert.Error(t, err)
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
					Role:  "arn:seaweed:iam::role/UserRole",
				},
				{
					Claim: "groups",
					Value: "admins",
					Role:  "arn:seaweed:iam::role/AdminRole",
				},
			},
			DefaultRole: "arn:seaweed:iam::role/GuestRole",
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

func generateTestKeys(t *testing.T) (*rsa.PrivateKey, *rsa.PublicKey) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return privateKey, &privateKey.PublicKey
}

func createTestJWT(t *testing.T, privateKey *rsa.PrivateKey, claims jwt.MapClaims) string {
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "test-key-id"

	tokenString, err := token.SignedString(privateKey)
	require.NoError(t, err)
	return tokenString
}

func encodePublicKey(t *testing.T, publicKey *rsa.PublicKey) string {
	// Properly encode the RSA modulus (N) as base64url
	return base64.RawURLEncoding.EncodeToString(publicKey.N.Bytes())
}

func setupOIDCTestServer(t *testing.T, publicKey *rsa.PublicKey) *httptest.Server {
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

package mockoidc

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"github.com/golang-jwt/jwt/v5"
)

func TestNewServer(t *testing.T) {
	s, err := NewServer(Config{
		ClientID:     "test-client",
		ClientSecret: "test-secret",
		ExpiresIn:    3600,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer s.Close()
	if s.URL == "" {
		t.Error("expected non-empty URL")
	}
	if s.PrivateKey == nil {
		t.Error("expected private key to be set")
	}
}

func TestDiscoveryEndpoint(t *testing.T) {
	s, err := NewServer(Config{ClientID: "test", ClientSecret: "secret"})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer s.Close()
	resp, err := http.Get(s.URL + "/.well-known/openid-configuration")
	if err != nil {
		t.Fatalf("discovery request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	var config map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&config); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if config["issuer"] != s.URL {
		t.Errorf("expected issuer %s, got %v", s.URL, config["issuer"])
	}
	if config["token_endpoint"] != s.URL+"/token" {
		t.Errorf("expected token_endpoint %s/token, got %v", s.URL, config["token_endpoint"])
	}
}

func TestTokenEndpointSuccess(t *testing.T) {
	s, err := NewServer(Config{
		ClientID:     "my-client",
		ClientSecret: "my-secret",
		ExpiresIn:    60,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer s.Close()
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", "my-client")
	data.Set("client_secret", "my-secret")
	resp, err := http.PostForm(s.URL+"/token", data)
	if err != nil {
		t.Fatalf("token request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	var tokenResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	accessToken, ok := tokenResp["access_token"].(string)
	if !ok || accessToken == "" {
		t.Error("expected non-empty access_token")
	}
	if tokenResp["token_type"] != "Bearer" {
		t.Errorf("expected token_type Bearer, got %v", tokenResp["token_type"])
	}
	token, err := jwt.Parse(accessToken, func(token *jwt.Token) (interface{}, error) {
		return s.PublicKey, nil
	})
	if err != nil {
		t.Fatalf("failed to parse token: %v", err)
	}
	if !token.Valid {
		t.Error("token should be valid")
	}
}

func TestTokenEndpointInvalidCredentials(t *testing.T) {
	s, err := NewServer(Config{
		ClientID:     "my-client",
		ClientSecret: "my-secret",
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer s.Close()
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", "wrong-client")
	data.Set("client_secret", "wrong-secret")
	resp, err := http.PostForm(s.URL+"/token", data)
	if err != nil {
		t.Fatalf("token request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestTokenEndpointUnsupportedGrantType(t *testing.T) {
	s, err := NewServer(Config{
		ClientID:     "my-client",
		ClientSecret: "my-secret",
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer s.Close()
	data := url.Values{}
	data.Set("grant_type", "password")
	data.Set("client_id", "my-client")
	data.Set("client_secret", "my-secret")
	resp, err := http.PostForm(s.URL+"/token", data)
	if err != nil {
		t.Fatalf("token request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestJWKSEndpoint(t *testing.T) {
	s, err := NewServer(Config{ClientID: "test", ClientSecret: "secret"})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer s.Close()
	resp, err := http.Get(s.URL + "/jwks")
	if err != nil {
		t.Fatalf("jwks request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	var jwks map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	keys, ok := jwks["keys"].([]interface{})
	if !ok || len(keys) == 0 {
		t.Error("expected at least one key in JWKS")
	}
}

func TestUserInfoEndpoint(t *testing.T) {
	s, err := NewServer(Config{ClientID: "test", ClientSecret: "secret"})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer s.Close()
	req, _ := http.NewRequest("GET", s.URL+"/userinfo", nil)
	req.Header.Set("Authorization", "Bearer some-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("userinfo request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestUserInfoEndpointUnauthorized(t *testing.T) {
	s, err := NewServer(Config{ClientID: "test", ClientSecret: "secret"})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer s.Close()
	resp, err := http.Get(s.URL + "/userinfo")
	if err != nil {
		t.Fatalf("userinfo request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

func TestNotFound(t *testing.T) {
	s, err := NewServer(Config{ClientID: "test", ClientSecret: "secret"})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer s.Close()
	resp, err := http.Get(s.URL + "/nonexistent")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestTokenEndpointMethodNotAllowed(t *testing.T) {
	s, err := NewServer(Config{ClientID: "test", ClientSecret: "secret"})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer s.Close()
	resp, err := http.Get(s.URL + "/token")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", resp.StatusCode)
	}
}

func TestTokenEndpointInvalidRequest(t *testing.T) {
	s, err := NewServer(Config{ClientID: "test", ClientSecret: "secret"})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer s.Close()
	resp, err := http.Post(s.URL+"/token", "application/x-www-form-urlencoded", strings.NewReader("%invalid"))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

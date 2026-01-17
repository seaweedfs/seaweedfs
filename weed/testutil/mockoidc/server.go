package mockoidc

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"time"
	"github.com/golang-jwt/jwt/v5"
)

type Server struct {
	HTTPServer           *httptest.Server
	URL                  string
	PrivateKey           *rsa.PrivateKey
	PublicKey            *rsa.PublicKey
	ExpectedClientID     string
	ExpectedClientSecret string
	ExpiresIn            int
	Issuer               string
}

type Config struct {
	ClientID     string
	ClientSecret string
	ExpiresIn    int
}

func NewServer(cfg Config) (*Server, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	s := &Server{
		PrivateKey:           privateKey,
		PublicKey:            &privateKey.PublicKey,
		ExpectedClientID:     cfg.ClientID,
		ExpectedClientSecret: cfg.ClientSecret,
		ExpiresIn:            cfg.ExpiresIn,
	}
	if s.ExpiresIn == 0 {
		s.ExpiresIn = 3600
	}
	s.HTTPServer = httptest.NewServer(http.HandlerFunc(s.handler))
	s.URL = s.HTTPServer.URL
	s.Issuer = s.URL
	return s, nil
}

func (s *Server) Close() {
	if s.HTTPServer != nil {
		s.HTTPServer.Close()
	}
}

func (s *Server) handler(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/.well-known/openid-configuration":
		s.handleDiscovery(w, r)
	case "/jwks":
		s.handleJWKS(w, r)
	case "/token":
		s.handleToken(w, r)
	case "/userinfo":
		s.handleUserInfo(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	config := map[string]interface{}{
		"issuer":                 s.Issuer,
		"token_endpoint":         s.URL + "/token",
		"jwks_uri":               s.URL + "/jwks",
		"userinfo_endpoint":      s.URL + "/userinfo",
		"grant_types_supported":  []string{"client_credentials"},
		"response_types_supported": []string{"token"},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(config)
}

func (s *Server) handleJWKS(w http.ResponseWriter, r *http.Request) {
	n := base64.RawURLEncoding.EncodeToString(s.PublicKey.N.Bytes())
	jwks := map[string]interface{}{
		"keys": []map[string]interface{}{
			{
				"kty": "RSA",
				"kid": "test-key-id",
				"use": "sig",
				"alg": "RS256",
				"n":   n,
				"e":   "AQAB",
			},
		},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jwks)
}

func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{"error": "method_not_allowed"})
		return
	}
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid_request"})
		return
	}
	grantType := r.FormValue("grant_type")
	if grantType != "client_credentials" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "unsupported_grant_type"})
		return
	}
	clientID := r.FormValue("client_id")
	clientSecret := r.FormValue("client_secret")
	if clientID != s.ExpectedClientID || clientSecret != s.ExpectedClientSecret {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid_client"})
		return
	}
	claims := jwt.MapClaims{
		"sub": clientID,
		"iss": s.Issuer,
		"aud": clientID,
		"exp": time.Now().Add(time.Duration(s.ExpiresIn) * time.Second).Unix(),
		"iat": time.Now().Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "test-key-id"
	tokenString, err := token.SignedString(s.PrivateKey)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "server_error"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"access_token": tokenString,
		"token_type":   "Bearer",
		"expires_in":   s.ExpiresIn,
	})
}

func (s *Server) handleUserInfo(w http.ResponseWriter, r *http.Request) {
	authHeader := r.Header.Get("Authorization")
	if len(authHeader) < 8 || authHeader[:7] != "Bearer " {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	userInfo := map[string]interface{}{
		"sub":   "test-user",
		"email": "test@example.com",
		"name":  "Test User",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(userInfo)
}

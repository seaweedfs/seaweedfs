package iceberg

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// OAuthTokenResponse is the response for POST /v1/oauth/tokens.
type OAuthTokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope,omitempty"`
}

// OAuthErrorResponse is the error response for the OAuth endpoint.
type OAuthErrorResponse struct {
	Error       string `json:"error"`
	Description string `json:"error_description,omitempty"`
}

// IcebergClaims are JWT claims for Iceberg catalog OAuth tokens.
type IcebergClaims struct {
	IdentityName string `json:"identity_name"`
	AccessKey    string `json:"access_key"`
	jwt.RegisteredClaims
}

const oauthTokenExpiry = 3600 // 1 hour in seconds

// handleOAuthTokens implements the OAuth2 client_credentials flow.
// POST /v1/oauth/tokens
func (s *Server) handleOAuthTokens(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "Could not parse form body")
		return
	}

	// Reject credentials in query string to prevent leaking secrets into logs and caches.
	if r.URL.Query().Get("client_secret") != "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "client_secret must not be sent in the URL")
		return
	}

	grantType := r.PostFormValue("grant_type")
	if grantType != "client_credentials" {
		writeOAuthError(w, http.StatusBadRequest, "unsupported_grant_type",
			fmt.Sprintf("Unsupported grant_type: %s", grantType))
		return
	}

	clientID := r.PostFormValue("client_id")
	clientSecret := r.PostFormValue("client_secret")

	// Also support HTTP Basic auth per OAuth2 spec
	if clientID == "" && clientSecret == "" {
		var ok bool
		clientID, clientSecret, ok = r.BasicAuth()
		if !ok {
			writeOAuthError(w, http.StatusUnauthorized, "invalid_client", "Missing client credentials")
			return
		}
	}

	if clientID == "" || clientSecret == "" {
		writeOAuthError(w, http.StatusUnauthorized, "invalid_client", "Missing client_id or client_secret")
		return
	}

	if s.credentialValidator == nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "Credential validation not configured")
		return
	}

	identityName, _, err := s.credentialValidator.ValidateS3Credential(clientID, clientSecret)
	if err != nil {
		glog.V(2).Infof("Iceberg OAuth: credential validation failed for client_id=%s: %v", clientID, err)
		writeOAuthError(w, http.StatusUnauthorized, "invalid_client", "Invalid client credentials")
		return
	}

	// Generate a JWT signed with a key derived from the client secret.
	// Include the access key in claims so we can look up the exact credential for verification.
	signingKey := deriveSigningKey(clientID, clientSecret)
	now := time.Now()
	claims := IcebergClaims{
		IdentityName: identityName,
		AccessKey:    clientID,
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Duration(oauthTokenExpiry) * time.Second)),
			Issuer:    "seaweedfs-iceberg",
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(signingKey)
	if err != nil {
		glog.Errorf("Iceberg OAuth: failed to sign token: %v", err)
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "Failed to generate token")
		return
	}

	scope := r.PostFormValue("scope")
	resp := OAuthTokenResponse{
		AccessToken: tokenString,
		TokenType:   "bearer",
		ExpiresIn:   oauthTokenExpiry,
		Scope:       scope,
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, resp)
}

// authenticateBearer validates a Bearer token from the Authorization header.
// Returns the identity name, identity object, and whether auth succeeded.
func (s *Server) authenticateBearer(r *http.Request) (string, interface{}, bool) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return "", nil, false
	}
	if !strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		return "", nil, false
	}
	tokenString := strings.TrimSpace(auth[7:])
	if tokenString == "" {
		return "", nil, false
	}

	if s.credentialValidator == nil {
		return "", nil, false
	}

	// Parse the token without verification first to get the access key,
	// then look up the exact credential to verify the signature.
	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	unverified := &IcebergClaims{}
	_, _, err := parser.ParseUnverified(tokenString, unverified)
	if err != nil {
		glog.V(2).Infof("Iceberg OAuth: failed to parse token: %v", err)
		return "", nil, false
	}

	if unverified.AccessKey == "" {
		return "", nil, false
	}

	// Look up the credential by access key to get the signing key for verification
	identityName, identity, secretKey, err := s.credentialValidator.GetCredentialByAccessKey(unverified.AccessKey)
	if err != nil {
		glog.V(2).Infof("Iceberg OAuth: failed to get credential for access key: %v", err)
		return "", nil, false
	}

	signingKey := deriveSigningKey(unverified.AccessKey, secretKey)
	claims := &IcebergClaims{}
	verified, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return signingKey, nil
	})
	if err != nil || !verified.Valid {
		glog.V(2).Infof("Iceberg OAuth: token verification failed: %v", err)
		return "", nil, false
	}

	return identityName, identity, true
}

// deriveSigningKey derives a signing key from the access key and secret using HMAC-SHA256.
// Including the access key prevents cross-credential token forgery when two
// credentials happen to share the same secret.
func deriveSigningKey(accessKey, secret string) []byte {
	h := hmac.New(sha256.New, []byte("seaweedfs-iceberg-oauth"))
	h.Write([]byte(accessKey))
	h.Write([]byte{0}) // null separator
	h.Write([]byte(secret))
	return h.Sum(nil)
}

func writeOAuthError(w http.ResponseWriter, status int, errCode, description string) {
	resp := OAuthErrorResponse{
		Error:       errCode,
		Description: description,
	}
	writeJSON(w, status, resp)
}

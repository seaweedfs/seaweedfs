package lance

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// OAuthTokenResponse is the response for POST /oauth/token.
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

// LanceClaims are JWT claims for Lance catalog OAuth tokens.
type LanceClaims struct {
	IdentityName string `json:"identity_name"`
	AccessKey    string `json:"access_key"`
	jwt.RegisteredClaims
}

const defaultOauthTokenExpiry = 3600

// maxOauthTokenExpiry bounds the configured TTL so seconds-to-Duration
// conversions cannot overflow into already-expired tokens.
const maxOauthTokenExpiry = 365 * 24 * 3600

const grantTypeClientCredentials = "client_credentials"

// oauthExpirySeconds returns the OAuth token TTL. Lance clients hold a static
// Authorization header and cannot refresh on 401, so deployments can raise
// this to survive beyond the default hour.
func oauthExpirySeconds() int {
	if v := os.Getenv("LANCE_OAUTH_TOKEN_EXPIRY"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			if n > maxOauthTokenExpiry {
				return maxOauthTokenExpiry
			}
			return int(n)
		}
	}
	return defaultOauthTokenExpiry
}

// handleOAuthTokens implements the OAuth2 client_credentials flow.
// POST /oauth/token
func (s *Server) handleOAuthTokens(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	if err := r.ParseForm(); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "Could not parse form body")
		return
	}

	// Reject credentials in query string to prevent leaking secrets into logs and caches.
	if r.URL.Query().Get("client_secret") != "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "client_secret must not be sent in the URL")
		return
	}

	if grantType := r.PostFormValue("grant_type"); grantType != grantTypeClientCredentials {
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
			writeInvalidClient(w, "Missing client credentials")
			return
		}
	}

	if clientID == "" || clientSecret == "" {
		writeInvalidClient(w, "Missing client_id or client_secret")
		return
	}

	if s.credentialValidator == nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "Credential validation not configured")
		return
	}

	identityName, _, err := s.credentialValidator.ValidateS3Credential(clientID, clientSecret)
	if err != nil {
		glog.V(2).Infof("Lance OAuth: credential validation failed for client_id=%s: %v", clientID, err)
		writeInvalidClient(w, "Invalid client credentials")
		return
	}

	tokenString, err := mintToken(identityName, clientID, clientSecret, oauthExpirySeconds())
	if err != nil {
		glog.Errorf("Lance OAuth: failed to sign token: %v", err)
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "Failed to generate token")
		return
	}

	resp := OAuthTokenResponse{
		AccessToken: tokenString,
		TokenType:   "bearer",
		ExpiresIn:   oauthExpirySeconds(),
		Scope:       r.PostFormValue("scope"),
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, resp)
}

// mintToken issues a signed access token for the given identity and
// credential, valid for ttlSeconds.
func mintToken(identityName, accessKey, secret string, ttlSeconds int) (string, error) {
	signingKey := deriveSigningKey(accessKey, secret)
	now := time.Now()
	claims := LanceClaims{
		IdentityName: identityName,
		AccessKey:    accessKey,
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Duration(ttlSeconds) * time.Second)),
			Issuer:    "seaweedfs-lance",
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(signingKey)
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
	unverified := &LanceClaims{}
	_, _, err := parser.ParseUnverified(tokenString, unverified)
	if err != nil {
		glog.V(2).Infof("Lance OAuth: failed to parse token: %v", err)
		return "", nil, false
	}

	if unverified.AccessKey == "" {
		return "", nil, false
	}

	identityName, identity, secretKey, err := s.credentialValidator.GetCredentialByAccessKey(unverified.AccessKey)
	if err != nil {
		glog.V(2).Infof("Lance OAuth: failed to get credential for access key: %v", err)
		return "", nil, false
	}

	signingKey := deriveSigningKey(unverified.AccessKey, secretKey)
	claims := &LanceClaims{}
	verified, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return signingKey, nil
	})
	if err != nil || !verified.Valid {
		glog.V(2).Infof("Lance OAuth: token verification failed: %v", err)
		return "", nil, false
	}

	return identityName, identity, true
}

// authenticateApiKey validates an x-api-key header carrying an S3 credential
// as "access_key:secret_key". Unlike a Bearer token it does not expire, which
// suits clients that hold static headers.
func (s *Server) authenticateApiKey(apiKey string) (string, interface{}, bool) {
	if s.credentialValidator == nil {
		return "", nil, false
	}
	accessKey, secretKey, ok := strings.Cut(apiKey, ":")
	if !ok || accessKey == "" || secretKey == "" {
		return "", nil, false
	}
	identityName, identity, err := s.credentialValidator.ValidateS3Credential(accessKey, secretKey)
	if err != nil {
		glog.V(2).Infof("Lance x-api-key: credential validation failed: %v", err)
		return "", nil, false
	}
	return identityName, identity, true
}

// deriveSigningKey derives a signing key from the access key and secret using HMAC-SHA256.
// Including the access key prevents cross-credential token forgery when two
// credentials happen to share the same secret.
func deriveSigningKey(accessKey, secret string) []byte {
	h := hmac.New(sha256.New, []byte("seaweedfs-lance-oauth"))
	h.Write([]byte(accessKey))
	h.Write([]byte{0}) // null separator
	h.Write([]byte(secret))
	return h.Sum(nil)
}

// writeInvalidClient answers 401 with the Basic challenge RFC 6749 §5.2
// requires, so a client knows which scheme to retry with.
func writeInvalidClient(w http.ResponseWriter, description string) {
	w.Header().Set("WWW-Authenticate", `Basic realm="lance"`)
	writeOAuthError(w, http.StatusUnauthorized, "invalid_client", description)
}

func writeOAuthError(w http.ResponseWriter, status int, errCode, description string) {
	resp := OAuthErrorResponse{
		Error:       errCode,
		Description: description,
	}
	writeJSON(w, status, resp)
}

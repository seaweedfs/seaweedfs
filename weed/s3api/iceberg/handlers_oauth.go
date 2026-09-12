package iceberg

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

// OAuthTokenResponse is the response for POST /v1/oauth/tokens.
type OAuthTokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope,omitempty"`
	// IssuedTokenType is set to the access-token URN on token-exchange
	// responses, where RFC 8693 requires it.
	IssuedTokenType string `json:"issued_token_type,omitempty"`
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

const defaultOauthTokenExpiry = 3600

// maxOauthTokenExpiry bounds the configured TTL so seconds-to-Duration
// conversions cannot overflow into already-expired tokens.
const maxOauthTokenExpiry = 365 * 24 * 3600

// accessTokenTokenType is the RFC 8693 token-type URN for access tokens,
// required in issued_token_type on token-exchange responses.
const accessTokenTokenType = "urn:ietf:params:oauth:token-type:access_token"

// grant types accepted by POST /v1/oauth/tokens. Iceberg Java 1.10.x
// refreshes tokens via token-exchange (exchangeEnabled defaults true), so a
// server that only accepts client_credentials leaves those clients unable
// to refresh.
const (
	grantTypeClientCredentials = "client_credentials"
	grantTypeTokenExchange     = "urn:ietf:params:oauth:grant-type:token-exchange"
)

// oauthExpirySeconds returns the OAuth token TTL. Deployments whose
// clients cannot refresh tokens on 401 can raise this to survive client
// restart cycles instead of dying every hour.
func oauthExpirySeconds() int {
	if v := os.Getenv("ICEBERG_OAUTH_TOKEN_EXPIRY"); v != "" {
		// Parse in 64-bit space: on 32-bit platforms Atoi overflows and
		// errors on oversized values, which would silently fall back to
		// the default instead of clamping.
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			if n > maxOauthTokenExpiry {
				return maxOauthTokenExpiry
			}
			return int(n)
		}
	}
	return defaultOauthTokenExpiry
}

// tokenExchangeGrace is how long an expired subject token may still be
// exchanged for a fresh access token. Signature verification is the real
// gate; the grace exists so a client holding a token that expired while the
// exchange grant was unsupported (or during a server outage) recovers
// without a process restart.
func tokenExchangeGrace() time.Duration {
	grace := 2 * oauthExpirySeconds()
	if grace < 3600 {
		grace = 3600
	}
	if grace > 86400 {
		grace = 86400
	}
	return time.Duration(grace) * time.Second
}

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
	switch grantType {
	case grantTypeClientCredentials:
		// handled below
	case grantTypeTokenExchange, "token_exchange":
		s.handleTokenExchange(w, r)
		return
	default:
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
	tokenString, err := mintIcebergToken(identityName, clientID, clientSecret, oauthExpirySeconds())
	if err != nil {
		glog.Errorf("Iceberg OAuth: failed to sign token: %v", err)
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "Failed to generate token")
		return
	}

	scope := r.PostFormValue("scope")
	resp := OAuthTokenResponse{
		AccessToken: tokenString,
		TokenType:   "bearer",
		ExpiresIn:   oauthExpirySeconds(),
		Scope:       scope,
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, resp)
}

// handleTokenExchange implements RFC 8693 token exchange for Iceberg REST
// clients: a previously issued access token (subject_token) is exchanged for
// a fresh one. Iceberg Java's OAuth2Manager refreshes via this grant
// (exchangeEnabled defaults true), so supporting it lets those clients
// self-heal before their token expires — no client restart needed.
//
// Client authentication (RFC 8693 §2.1) is accepted but not required:
// Iceberg Java's refreshExpiredToken sends Basic credentials with the
// exchange, while its proactive scheduled refresh sends only the Bearer
// session headers. An expired subject_token is only exchangeable with valid
// client credentials, and an unauthenticated exchange never extends the
// token's lifetime beyond the subject_token's own expiry.
func (s *Server) handleTokenExchange(w http.ResponseWriter, r *http.Request) {
	if s.credentialValidator == nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "Credential validation not configured")
		return
	}

	subjectToken := r.PostFormValue("subject_token")
	if subjectToken == "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "Missing subject_token")
		return
	}

	// Verify the subject token by signature (exp checked separately against
	// the recovery grace).
	unverified := &IcebergClaims{}
	parser := jwt.NewParser(jwt.WithoutClaimsValidation())
	if _, _, err := parser.ParseUnverified(subjectToken, unverified); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "Invalid subject_token")
		return
	}
	if unverified.AccessKey == "" || unverified.Issuer != "seaweedfs-iceberg" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "Invalid subject_token")
		return
	}
	identityName, _, secretKey, err := s.credentialValidator.GetCredentialByAccessKey(unverified.AccessKey)
	if err != nil {
		glog.V(2).Infof("Iceberg OAuth: token exchange failed to get credential for access key: %v", err)
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "Invalid subject_token")
		return
	}

	// Optional client authentication. When present, the client must
	// authenticate with the same credential that issued the subject_token.
	clientID, clientSecret, hasBasic := r.BasicAuth()
	if !hasBasic {
		clientID = r.PostFormValue("client_id")
		clientSecret = r.PostFormValue("client_secret")
		hasBasic = clientID != "" && clientSecret != ""
	}
	clientAuthenticated := false
	if hasBasic {
		if _, _, err := s.credentialValidator.ValidateS3Credential(clientID, clientSecret); err != nil {
			glog.V(2).Infof("Iceberg OAuth: token exchange client authentication failed for client_id=%s: %v", clientID, err)
			writeOAuthError(w, http.StatusUnauthorized, "invalid_client", "Invalid client credentials")
			return
		}
		if clientID != unverified.AccessKey {
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "subject_token was issued to a different client")
			return
		}
		clientAuthenticated = true
	}

	signingKey := deriveSigningKey(unverified.AccessKey, secretKey)
	claims := &IcebergClaims{}
	parsed, err := jwt.ParseWithClaims(subjectToken, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return signingKey, nil
	}, jwt.WithoutClaimsValidation())
	if err != nil || !parsed.Valid {
		glog.V(2).Infof("Iceberg OAuth: token exchange signature verification failed: %v", err)
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "Invalid subject_token")
		return
	}
	if claims.Issuer != "seaweedfs-iceberg" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "Invalid subject_token")
		return
	}
	if claims.ExpiresAt == nil {
		// Tokens minted by this server always carry an expiry; a subject
		// token without one must not be exchangeable forever.
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "subject_token has no expiry")
		return
	}

	ttlSeconds := oauthExpirySeconds()
	if time.Now().After(claims.ExpiresAt.Time) {
		// Expired subject tokens are only exchangeable by an authenticated
		// client, within the recovery grace.
		if !clientAuthenticated {
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant",
				"subject_token is expired; client authentication is required to exchange it")
			return
		}
		if time.Since(claims.ExpiresAt.Time) > tokenExchangeGrace() {
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "subject_token expired beyond the exchange grace window")
			return
		}
	} else if remaining := int(time.Until(claims.ExpiresAt.Time).Seconds()); !clientAuthenticated && remaining < ttlSeconds {
		// Unauthenticated exchange (proactive refresh carrying only the
		// Bearer) must not extend the lifetime past the subject's expiry;
		// otherwise a leaked token could chain-refresh forever. An
		// authenticated client gets a fresh full TTL — it could mint one
		// via client_credentials anyway.
		ttlSeconds = remaining
	}
	if ttlSeconds <= 0 {
		// A subject token with under a second left would otherwise be
		// exchanged for a token that is born expired.
		writeOAuthError(w, http.StatusBadRequest, "invalid_grant", "subject_token has no remaining lifetime")
		return
	}

	tokenString, err := mintIcebergToken(identityName, unverified.AccessKey, secretKey, ttlSeconds)
	if err != nil {
		glog.Errorf("Iceberg OAuth: failed to sign exchanged token: %v", err)
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "Failed to generate token")
		return
	}

	resp := OAuthTokenResponse{
		AccessToken:     tokenString,
		TokenType:       "bearer",
		ExpiresIn:       ttlSeconds,
		Scope:           r.PostFormValue("scope"),
		IssuedTokenType: accessTokenTokenType,
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, resp)
}

// mintIcebergToken issues a signed access token for the given identity and
// credential, valid for ttlSeconds.
func mintIcebergToken(identityName, accessKey, secret string, ttlSeconds int) (string, error) {
	signingKey := deriveSigningKey(accessKey, secret)
	now := time.Now()
	claims := IcebergClaims{
		IdentityName: identityName,
		AccessKey:    accessKey,
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Duration(ttlSeconds) * time.Second)),
			Issuer:    "seaweedfs-iceberg",
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

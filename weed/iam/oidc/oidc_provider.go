package oidc

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
)

// OIDCProvider implements OpenID Connect authentication
type OIDCProvider struct {
	name        string
	config      *OIDCConfig
	initialized bool
	jwksCache   *JWKS
	httpClient  *http.Client
}

// OIDCConfig holds OIDC provider configuration
type OIDCConfig struct {
	// Issuer is the OIDC issuer URL
	Issuer string `json:"issuer"`

	// ClientID is the OAuth2 client ID
	ClientID string `json:"clientId"`

	// ClientSecret is the OAuth2 client secret (optional for public clients)
	ClientSecret string `json:"clientSecret,omitempty"`

	// JWKSUri is the JSON Web Key Set URI
	JWKSUri string `json:"jwksUri,omitempty"`

	// UserInfoUri is the UserInfo endpoint URI
	UserInfoUri string `json:"userInfoUri,omitempty"`

	// Scopes are the OAuth2 scopes to request
	Scopes []string `json:"scopes,omitempty"`

	// RoleMapping defines how to map OIDC claims to roles
	RoleMapping *providers.RoleMapping `json:"roleMapping,omitempty"`

	// ClaimsMapping defines how to map OIDC claims to identity attributes
	ClaimsMapping map[string]string `json:"claimsMapping,omitempty"`
}

// JWKS represents JSON Web Key Set
type JWKS struct {
	Keys []JWK `json:"keys"`
}

// JWK represents a JSON Web Key
type JWK struct {
	Kty string `json:"kty"` // Key Type (RSA, EC, etc.)
	Kid string `json:"kid"` // Key ID
	Use string `json:"use"` // Usage (sig for signature)
	Alg string `json:"alg"` // Algorithm (RS256, etc.)
	N   string `json:"n"`   // RSA public key modulus
	E   string `json:"e"`   // RSA public key exponent
	X   string `json:"x"`   // EC public key x coordinate
	Y   string `json:"y"`   // EC public key y coordinate
	Crv string `json:"crv"` // EC curve
}

// NewOIDCProvider creates a new OIDC provider
func NewOIDCProvider(name string) *OIDCProvider {
	return &OIDCProvider{
		name:       name,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// Name returns the provider name
func (p *OIDCProvider) Name() string {
	return p.name
}

// Initialize initializes the OIDC provider with configuration
func (p *OIDCProvider) Initialize(config interface{}) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	oidcConfig, ok := config.(*OIDCConfig)
	if !ok {
		return fmt.Errorf("invalid config type for OIDC provider")
	}

	if err := p.validateConfig(oidcConfig); err != nil {
		return fmt.Errorf("invalid OIDC configuration: %w", err)
	}

	p.config = oidcConfig
	p.initialized = true

	// For testing, we'll skip the actual OIDC client initialization
	return nil
}

// validateConfig validates the OIDC configuration
func (p *OIDCProvider) validateConfig(config *OIDCConfig) error {
	if config.Issuer == "" {
		return fmt.Errorf("issuer is required")
	}

	if config.ClientID == "" {
		return fmt.Errorf("client ID is required")
	}

	// Basic URL validation for issuer
	if config.Issuer != "" && config.Issuer != "https://accounts.google.com" && config.Issuer[0:4] != "http" {
		return fmt.Errorf("invalid issuer URL format")
	}

	return nil
}

// Authenticate authenticates a user with an OIDC token
func (p *OIDCProvider) Authenticate(ctx context.Context, token string) (*providers.ExternalIdentity, error) {
	if !p.initialized {
		return nil, fmt.Errorf("provider not initialized")
	}

	if token == "" {
		return nil, fmt.Errorf("token cannot be empty")
	}

	// Validate token and get claims
	claims, err := p.ValidateToken(ctx, token)
	if err != nil {
		return nil, err
	}

	// Map claims to external identity
	email, _ := claims.GetClaimString("email")
	displayName, _ := claims.GetClaimString("name")
	groups, _ := claims.GetClaimStringSlice("groups")

	return &providers.ExternalIdentity{
		UserID:      claims.Subject,
		Email:       email,
		DisplayName: displayName,
		Groups:      groups,
		Provider:    p.name,
	}, nil
}

// GetUserInfo retrieves user information from the UserInfo endpoint
func (p *OIDCProvider) GetUserInfo(ctx context.Context, userID string) (*providers.ExternalIdentity, error) {
	if !p.initialized {
		return nil, fmt.Errorf("provider not initialized")
	}

	if userID == "" {
		return nil, fmt.Errorf("user ID cannot be empty")
	}

	// TODO: Implement UserInfo endpoint call
	// 1. Make HTTP request to UserInfo endpoint
	// 2. Parse response and extract user claims
	// 3. Map claims to ExternalIdentity structure

	return nil, fmt.Errorf("UserInfo endpoint integration not implemented yet")
}

// ValidateToken validates an OIDC JWT token
func (p *OIDCProvider) ValidateToken(ctx context.Context, token string) (*providers.TokenClaims, error) {
	if !p.initialized {
		return nil, fmt.Errorf("provider not initialized")
	}

	if token == "" {
		return nil, fmt.Errorf("token cannot be empty")
	}

	// Parse token without verification first to get header info
	parsedToken, _, err := new(jwt.Parser).ParseUnverified(token, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("failed to parse JWT token: %v", err)
	}

	// Get key ID from header
	kid, ok := parsedToken.Header["kid"].(string)
	if !ok {
		return nil, fmt.Errorf("missing key ID in JWT header")
	}

	// Get signing key from JWKS
	publicKey, err := p.getPublicKey(ctx, kid)
	if err != nil {
		return nil, fmt.Errorf("failed to get public key: %v", err)
	}

	// Parse and validate token with proper signature verification
	claims := jwt.MapClaims{}
	validatedToken, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
		// Verify signing method
		switch token.Method.(type) {
		case *jwt.SigningMethodRSA:
			return publicKey, nil
		default:
			return nil, fmt.Errorf("unsupported signing method: %v", token.Header["alg"])
		}
	})

	if err != nil {
		return nil, fmt.Errorf("failed to validate JWT token: %v", err)
	}

	if !validatedToken.Valid {
		return nil, fmt.Errorf("JWT token is invalid")
	}

	// Validate required claims
	issuer, ok := claims["iss"].(string)
	if !ok || issuer != p.config.Issuer {
		return nil, fmt.Errorf("invalid or missing issuer claim")
	}

	audience, ok := claims["aud"].(string)
	if !ok || audience != p.config.ClientID {
		return nil, fmt.Errorf("invalid or missing audience claim")
	}

	subject, ok := claims["sub"].(string)
	if !ok {
		return nil, fmt.Errorf("missing subject claim")
	}

	// Convert to our TokenClaims structure
	tokenClaims := &providers.TokenClaims{
		Subject: subject,
		Issuer:  issuer,
		Claims:  make(map[string]interface{}),
	}

	// Copy all claims
	for key, value := range claims {
		tokenClaims.Claims[key] = value
	}

	return tokenClaims, nil
}

// mapClaimsToRoles maps token claims to SeaweedFS roles
func (p *OIDCProvider) mapClaimsToRoles(claims *providers.TokenClaims) []string {
	roles := []string{}

	// Get groups from claims
	groups, _ := claims.GetClaimStringSlice("groups")

	// Basic role mapping based on groups
	for _, group := range groups {
		switch group {
		case "admins":
			roles = append(roles, "admin")
		case "developers":
			roles = append(roles, "readwrite")
		case "users":
			roles = append(roles, "readonly")
		}
	}

	if len(roles) == 0 {
		roles = []string{"readonly"} // Default role
	}

	return roles
}

// getPublicKey retrieves the public key for the given key ID from JWKS
func (p *OIDCProvider) getPublicKey(ctx context.Context, kid string) (interface{}, error) {
	// Fetch JWKS if not cached or refresh if needed
	if p.jwksCache == nil {
		if err := p.fetchJWKS(ctx); err != nil {
			return nil, fmt.Errorf("failed to fetch JWKS: %v", err)
		}
	}

	// Find the key with matching kid
	for _, key := range p.jwksCache.Keys {
		if key.Kid == kid {
			return p.parseJWK(&key)
		}
	}

	return nil, fmt.Errorf("key with ID %s not found in JWKS", kid)
}

// fetchJWKS fetches the JWKS from the provider
func (p *OIDCProvider) fetchJWKS(ctx context.Context) error {
	jwksURL := p.config.JWKSUri
	if jwksURL == "" {
		jwksURL = strings.TrimSuffix(p.config.Issuer, "/") + "/.well-known/jwks.json"
	}

	req, err := http.NewRequestWithContext(ctx, "GET", jwksURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create JWKS request: %v", err)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch JWKS: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("JWKS endpoint returned status: %d", resp.StatusCode)
	}

	var jwks JWKS
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return fmt.Errorf("failed to decode JWKS response: %v", err)
	}

	p.jwksCache = &jwks
	glog.V(3).Infof("Fetched JWKS with %d keys from %s", len(jwks.Keys), jwksURL)
	return nil
}

// parseJWK converts a JWK to a public key
func (p *OIDCProvider) parseJWK(key *JWK) (interface{}, error) {
	switch key.Kty {
	case "RSA":
		return p.parseRSAKey(key)
	default:
		return nil, fmt.Errorf("unsupported key type: %s", key.Kty)
	}
}

// parseRSAKey parses an RSA key from JWK
func (p *OIDCProvider) parseRSAKey(key *JWK) (*rsa.PublicKey, error) {
	// Decode the modulus (n)
	nBytes, err := base64.RawURLEncoding.DecodeString(key.N)
	if err != nil {
		return nil, fmt.Errorf("failed to decode RSA modulus: %v", err)
	}

	// Decode the exponent (e)
	eBytes, err := base64.RawURLEncoding.DecodeString(key.E)
	if err != nil {
		return nil, fmt.Errorf("failed to decode RSA exponent: %v", err)
	}

	// Convert exponent bytes to int
	var exponent int
	for _, b := range eBytes {
		exponent = exponent*256 + int(b)
	}

	// Create RSA public key
	pubKey := &rsa.PublicKey{
		E: exponent,
	}
	pubKey.N = new(big.Int).SetBytes(nBytes)

	return pubKey, nil
}

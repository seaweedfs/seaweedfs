package oidc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
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
	name          string
	config        *OIDCConfig
	initialized   bool
	jwksCache     *JWKS
	httpClient    *http.Client
	jwksFetchedAt time.Time
	jwksTTL       time.Duration
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

	// JWKSCacheTTLSeconds sets how long to cache JWKS before refresh (default 3600 seconds)
	JWKSCacheTTLSeconds int `json:"jwksCacheTTLSeconds,omitempty"`
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

// GetIssuer returns the configured issuer URL for efficient provider lookup
func (p *OIDCProvider) GetIssuer() string {
	if p.config == nil {
		return ""
	}
	return p.config.Issuer
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

	// Configure JWKS cache TTL
	if oidcConfig.JWKSCacheTTLSeconds > 0 {
		p.jwksTTL = time.Duration(oidcConfig.JWKSCacheTTLSeconds) * time.Second
	} else {
		p.jwksTTL = time.Hour
	}

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

	// Debug: Log available claims
	glog.V(3).Infof("Available claims: %+v", claims.Claims)
	if rolesFromClaims, exists := claims.GetClaimStringSlice("roles"); exists {
		glog.V(3).Infof("Roles claim found as string slice: %v", rolesFromClaims)
	} else if roleFromClaims, exists := claims.GetClaimString("roles"); exists {
		glog.V(3).Infof("Roles claim found as string: %s", roleFromClaims)
	} else {
		glog.V(3).Infof("No roles claim found in token")
	}

	// Map claims to roles using configured role mapping
	roles := p.mapClaimsToRolesWithConfig(claims)

	// Create attributes map and add roles
	attributes := make(map[string]string)
	if len(roles) > 0 {
		// Store roles as a comma-separated string in attributes
		attributes["roles"] = strings.Join(roles, ",")
	}

	identity := &providers.ExternalIdentity{
		UserID:      claims.Subject,
		Email:       email,
		DisplayName: displayName,
		Groups:      groups,
		Attributes:  attributes,
		Provider:    p.name,
	}

	// Pass the token expiration to limit session duration
	// This ensures the STS session doesn't exceed the source token's validity
	if !claims.ExpiresAt.IsZero() {
		identity.TokenExpiration = &claims.ExpiresAt
	}

	return identity, nil
}

// GetUserInfo retrieves user information from the UserInfo endpoint
func (p *OIDCProvider) GetUserInfo(ctx context.Context, userID string) (*providers.ExternalIdentity, error) {
	if !p.initialized {
		return nil, fmt.Errorf("provider not initialized")
	}

	if userID == "" {
		return nil, fmt.Errorf("user ID cannot be empty")
	}

	// For now, we'll use a token-based approach since OIDC UserInfo typically requires a token
	// In a real implementation, this would need an access token from the authentication flow
	return p.getUserInfoWithToken(ctx, userID, "")
}

// GetUserInfoWithToken retrieves user information using an access token
func (p *OIDCProvider) GetUserInfoWithToken(ctx context.Context, accessToken string) (*providers.ExternalIdentity, error) {
	if !p.initialized {
		return nil, fmt.Errorf("provider not initialized")
	}

	if accessToken == "" {
		return nil, fmt.Errorf("access token cannot be empty")
	}

	return p.getUserInfoWithToken(ctx, "", accessToken)
}

// getUserInfoWithToken is the internal implementation for UserInfo endpoint calls
func (p *OIDCProvider) getUserInfoWithToken(ctx context.Context, userID, accessToken string) (*providers.ExternalIdentity, error) {
	// Determine UserInfo endpoint URL
	userInfoUri := p.config.UserInfoUri
	if userInfoUri == "" {
		// Use standard OIDC discovery endpoint convention
		userInfoUri = strings.TrimSuffix(p.config.Issuer, "/") + "/userinfo"
	}

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "GET", userInfoUri, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create UserInfo request: %v", err)
	}

	// Set authorization header if access token is provided
	if accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+accessToken)
	}
	req.Header.Set("Accept", "application/json")

	// Make HTTP request
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to call UserInfo endpoint: %v", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("UserInfo endpoint returned status %d", resp.StatusCode)
	}

	// Parse JSON response
	var userInfo map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&userInfo); err != nil {
		return nil, fmt.Errorf("failed to decode UserInfo response: %v", err)
	}

	glog.V(4).Infof("Received UserInfo response: %+v", userInfo)

	// Map UserInfo claims to ExternalIdentity
	identity := p.mapUserInfoToIdentity(userInfo)

	// If userID was provided but not found in claims, use it
	if userID != "" && identity.UserID == "" {
		identity.UserID = userID
	}

	glog.V(3).Infof("Retrieved user info from OIDC provider: %s", identity.UserID)
	return identity, nil
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
		// Wrap JWT validation errors with typed provider errors
		errMsg := err.Error()
		if strings.Contains(errMsg, "expired") || strings.Contains(errMsg, "exp") {
			return nil, fmt.Errorf("%w: %v", providers.ErrProviderTokenExpired, err)
		}
		return nil, fmt.Errorf("%w: %v", providers.ErrProviderInvalidToken, err)
	}

	if !validatedToken.Valid {
		return nil, fmt.Errorf("%w: token validation failed", providers.ErrProviderInvalidToken)
	}

	// Validate required claims
	issuer, ok := claims["iss"].(string)
	if !ok || issuer != p.config.Issuer {
		return nil, fmt.Errorf("%w: expected %s, got %s", providers.ErrProviderInvalidIssuer, p.config.Issuer, issuer)
	}

	// Check audience claim (aud) or authorized party (azp) - Keycloak uses azp
	// Per RFC 7519, aud can be either a string or an array of strings
	var audienceMatched bool
	if audClaim, ok := claims["aud"]; ok {
		switch aud := audClaim.(type) {
		case string:
			if aud == p.config.ClientID {
				audienceMatched = true
			}
		case []interface{}:
			for _, a := range aud {
				if str, ok := a.(string); ok && str == p.config.ClientID {
					audienceMatched = true
					break
				}
			}
		}
	}

	if !audienceMatched {
		if azp, ok := claims["azp"].(string); ok && azp == p.config.ClientID {
			audienceMatched = true
		}
	}

	if !audienceMatched {
		return nil, fmt.Errorf("%w: expected client ID %s", providers.ErrProviderInvalidAudience, p.config.ClientID)
	}

	subject, ok := claims["sub"].(string)
	if !ok {
		return nil, fmt.Errorf("%w: missing subject claim", providers.ErrProviderMissingClaims)
	}

	// Convert to our TokenClaims structure
	tokenClaims := &providers.TokenClaims{
		Subject: subject,
		Issuer:  issuer,
		Claims:  make(map[string]interface{}),
	}

	// Extract time-based claims (exp, iat, nbf)
	for key, target := range map[string]*time.Time{
		"exp": &tokenClaims.ExpiresAt,
		"iat": &tokenClaims.IssuedAt,
		"nbf": &tokenClaims.NotBefore,
	} {
		if val, ok := claims[key]; ok {
			switch v := val.(type) {
			case float64:
				*target = time.Unix(int64(v), 0)
			case json.Number:
				if intVal, err := v.Int64(); err == nil {
					*target = time.Unix(intVal, 0)
				}
			}
		}
	}

	// Copy all claims
	for key, value := range claims {
		tokenClaims.Claims[key] = value
	}

	return tokenClaims, nil
}

// mapClaimsToRoles maps token claims to SeaweedFS roles (legacy method)
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

// mapClaimsToRolesWithConfig maps token claims to roles using configured role mapping
func (p *OIDCProvider) mapClaimsToRolesWithConfig(claims *providers.TokenClaims) []string {
	glog.V(3).Infof("mapClaimsToRolesWithConfig: RoleMapping is nil? %t", p.config.RoleMapping == nil)

	if p.config.RoleMapping == nil {
		glog.V(2).Infof("No role mapping configured for provider %s, using legacy mapping", p.name)
		// Fallback to legacy mapping if no role mapping configured
		return p.mapClaimsToRoles(claims)
	}

	glog.V(3).Infof("Applying %d role mapping rules", len(p.config.RoleMapping.Rules))
	roles := []string{}

	// Apply role mapping rules
	for i, rule := range p.config.RoleMapping.Rules {
		glog.V(3).Infof("Rule %d: claim=%s, value=%s, role=%s", i, rule.Claim, rule.Value, rule.Role)

		if rule.Matches(claims) {
			glog.V(2).Infof("Rule %d matched! Adding role: %s", i, rule.Role)
			roles = append(roles, rule.Role)
		} else {
			glog.V(3).Infof("Rule %d did not match", i)
		}
	}

	// Use default role if no rules matched
	if len(roles) == 0 && p.config.RoleMapping.DefaultRole != "" {
		glog.V(2).Infof("No rules matched, using default role: %s", p.config.RoleMapping.DefaultRole)
		roles = []string{p.config.RoleMapping.DefaultRole}
	}

	glog.V(2).Infof("Role mapping result: %v", roles)
	return roles
}

// getPublicKey retrieves the public key for the given key ID from JWKS
func (p *OIDCProvider) getPublicKey(ctx context.Context, kid string) (interface{}, error) {
	// Fetch JWKS if not cached or refresh if expired
	if p.jwksCache == nil || (!p.jwksFetchedAt.IsZero() && time.Since(p.jwksFetchedAt) > p.jwksTTL) {
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

	// Key not found in cache. Refresh JWKS once to handle key rotation and retry.
	if err := p.fetchJWKS(ctx); err != nil {
		return nil, fmt.Errorf("failed to refresh JWKS after key miss: %v", err)
	}
	for _, key := range p.jwksCache.Keys {
		if key.Kid == kid {
			return p.parseJWK(&key)
		}
	}
	return nil, fmt.Errorf("key with ID %s not found in JWKS after refresh", kid)
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
	p.jwksFetchedAt = time.Now()
	glog.V(3).Infof("Fetched JWKS with %d keys from %s", len(jwks.Keys), jwksURL)
	return nil
}

// parseJWK converts a JWK to a public key
func (p *OIDCProvider) parseJWK(key *JWK) (interface{}, error) {
	switch key.Kty {
	case "RSA":
		return p.parseRSAKey(key)
	case "EC":
		return p.parseECKey(key)
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

// parseECKey parses an Elliptic Curve key from JWK
func (p *OIDCProvider) parseECKey(key *JWK) (*ecdsa.PublicKey, error) {
	// Validate required fields
	if key.X == "" || key.Y == "" || key.Crv == "" {
		return nil, fmt.Errorf("incomplete EC key: missing x, y, or crv parameter")
	}

	// Get the curve
	var curve elliptic.Curve
	switch key.Crv {
	case "P-256":
		curve = elliptic.P256()
	case "P-384":
		curve = elliptic.P384()
	case "P-521":
		curve = elliptic.P521()
	default:
		return nil, fmt.Errorf("unsupported EC curve: %s", key.Crv)
	}

	// Decode x coordinate
	xBytes, err := base64.RawURLEncoding.DecodeString(key.X)
	if err != nil {
		return nil, fmt.Errorf("failed to decode EC x coordinate: %v", err)
	}

	// Decode y coordinate
	yBytes, err := base64.RawURLEncoding.DecodeString(key.Y)
	if err != nil {
		return nil, fmt.Errorf("failed to decode EC y coordinate: %v", err)
	}

	// Create EC public key
	pubKey := &ecdsa.PublicKey{
		Curve: curve,
		X:     new(big.Int).SetBytes(xBytes),
		Y:     new(big.Int).SetBytes(yBytes),
	}

	// Validate that the point is on the curve
	if !curve.IsOnCurve(pubKey.X, pubKey.Y) {
		return nil, fmt.Errorf("EC key coordinates are not on the specified curve")
	}

	return pubKey, nil
}

// mapUserInfoToIdentity maps UserInfo response to ExternalIdentity
func (p *OIDCProvider) mapUserInfoToIdentity(userInfo map[string]interface{}) *providers.ExternalIdentity {
	identity := &providers.ExternalIdentity{
		Provider:   p.name,
		Attributes: make(map[string]string),
	}

	// Map standard OIDC claims
	if sub, ok := userInfo["sub"].(string); ok {
		identity.UserID = sub
	}

	if email, ok := userInfo["email"].(string); ok {
		identity.Email = email
	}

	if name, ok := userInfo["name"].(string); ok {
		identity.DisplayName = name
	}

	// Handle groups claim (can be array of strings or single string)
	if groupsData, exists := userInfo["groups"]; exists {
		switch groups := groupsData.(type) {
		case []interface{}:
			// Array of groups
			for _, group := range groups {
				if groupStr, ok := group.(string); ok {
					identity.Groups = append(identity.Groups, groupStr)
				}
			}
		case []string:
			// Direct string array
			identity.Groups = groups
		case string:
			// Single group as string
			identity.Groups = []string{groups}
		}
	}

	// Map configured custom claims
	if p.config.ClaimsMapping != nil {
		for identityField, oidcClaim := range p.config.ClaimsMapping {
			if value, exists := userInfo[oidcClaim]; exists {
				if strValue, ok := value.(string); ok {
					switch identityField {
					case "email":
						if identity.Email == "" {
							identity.Email = strValue
						}
					case "displayName":
						if identity.DisplayName == "" {
							identity.DisplayName = strValue
						}
					case "userID":
						if identity.UserID == "" {
							identity.UserID = strValue
						}
					default:
						identity.Attributes[identityField] = strValue
					}
				}
			}
		}
	}

	// Store all additional claims as attributes
	for key, value := range userInfo {
		if key != "sub" && key != "email" && key != "name" && key != "groups" {
			if strValue, ok := value.(string); ok {
				identity.Attributes[key] = strValue
			} else if jsonValue, err := json.Marshal(value); err == nil {
				identity.Attributes[key] = string(jsonValue)
			}
		}
	}

	return identity
}

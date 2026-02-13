package oidc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
)

const (
	// clockSkewTolerance matches the typical JWT library clock skew allowance (RFC 7519)
	clockSkewTolerance = 5 * time.Minute
	// defaultJTICacheSize is the default maximum number of JTI entries to cache
	defaultJTICacheSize = 10000
	// defaultCleanupInterval is how often to scan for expired JTI entries
	defaultCleanupInterval = 5 * time.Minute
)

// jtiEntry represents a cached JTI with metadata
type jtiEntry struct {
	subject   string
	expiresAt time.Time
}

// OIDCProvider implements OpenID Connect authentication with JTI replay protection.
//
// Replay Protection:
// The provider validates the 'jti' (JWT ID) claim to prevent token replay attacks.
// Each JTI is stored in memory until the token expires. If a token with a duplicate
// JTI is presented, it is rejected with ErrProviderTokenReplayed.
//
// The JTI cache is bounded by JTICacheMaxSize and uses automatic TTL-based expiration.
// Tokens without JTI claims are accepted with a warning logged (backward compatibility).
//
// Thread Safety:
// All methods are safe for concurrent use. The JTI cache uses atomic LoadOrStore
// operations to prevent TOCTOU race conditions.
type OIDCProvider struct {
	name          string
	config        *OIDCConfig
	initialized   bool
	jwksCache     *JWKS
	httpClient    *http.Client
	jwksFetchedAt time.Time
	jwksTTL       time.Duration

	// JTI replay protection fields
	jtiEnabled       bool     // Explicit flag for JTI protection status
	jtiStore         sync.Map // map[string]*jtiEntry - atomic storage for JTI claims
	jtiCount         atomic.Int64
	jtiMaxSize       int64
	jtiCleanupCtx    context.Context
	jtiCleanupCancel context.CancelFunc
	jtiCleanupDone   chan struct{} // Closed when cleanup goroutine exits
	shutdownOnce     sync.Once
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

	// TLSCACert is the path to the CA certificate file for custom/self-signed certificates
	TLSCACert string `json:"tlsCaCert,omitempty"`

	// TLSInsecureSkipVerify controls whether to skip TLS verification.
	// WARNING: Should only be used in development/testing environments. Never use in production.
	TLSInsecureSkipVerify bool `json:"tlsInsecureSkipVerify,omitempty"`

	// JTIReplayProtectionEnabled enables token replay protection via JTI tracking (default: true)
	// Use pointer to distinguish between "not set" (nil = default true) and "explicitly false"
	JTIReplayProtectionEnabled *bool `json:"jtiReplayProtectionEnabled,omitempty"`

	// JTICacheMaxSize sets maximum number of JTI entries to cache (default: 10000)
	JTICacheMaxSize int `json:"jtiCacheMaxSize,omitempty"`
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

	// Initialize JTI replay protection (enabled by default for security)
	jtiEnabled := true // Default to enabled (secure by default)
	if oidcConfig.JTIReplayProtectionEnabled != nil {
		jtiEnabled = *oidcConfig.JTIReplayProtectionEnabled
	}

	if jtiEnabled {
		// Cancel any existing cleanup goroutine before re-initializing
		if p.jtiCleanupCancel != nil {
			glog.V(3).Infof("OIDC provider %q: Stopping existing JTI cleanup goroutine", p.name)
			p.jtiCleanupCancel()
			// Wait for old goroutine to exit (with timeout to prevent blocking indefinitely)
			if p.jtiCleanupDone != nil {
				select {
				case <-p.jtiCleanupDone:
					glog.V(3).Infof("OIDC provider %q: Old cleanup goroutine exited", p.name)
				case <-time.After(2 * time.Second):
					glog.Warningf("OIDC provider %q: Timeout waiting for old cleanup goroutine", p.name)
				}
			}
			p.jtiCleanupCancel = nil
		}

		p.jtiEnabled = true
		p.jtiMaxSize = int64(oidcConfig.JTICacheMaxSize)
		if p.jtiMaxSize <= 0 {
			p.jtiMaxSize = defaultJTICacheSize
		}

		p.jtiStore = sync.Map{}
		p.jtiCount = atomic.Int64{}
		p.jtiCleanupCtx, p.jtiCleanupCancel = context.WithCancel(context.Background())
		p.jtiCleanupDone = make(chan struct{})

		// Start background cleanup goroutine for expired JTI entries
		go p.cleanupExpiredJTIs(p.jtiCleanupCtx, defaultCleanupInterval)

		glog.V(2).Infof("OIDC provider %q: JTI replay protection enabled (max size: %d)", p.name, p.jtiMaxSize)
	} else {
		p.jtiEnabled = false
		glog.Warningf("OIDC provider %q: JTI replay protection is DISABLED", p.name)
	}

	// Configure HTTP client with TLS settings
	tlsConfig := &tls.Config{
		InsecureSkipVerify: oidcConfig.TLSInsecureSkipVerify,
		MinVersion:         tls.VersionTLS12, // Prevent TLS downgrade attacks
	}

	if oidcConfig.TLSInsecureSkipVerify {
		glog.Warningf("OIDC provider %q is configured to skip TLS verification. This is insecure and should not be used in production.", p.name)
	}

	if oidcConfig.TLSCACert != "" {
		// Validate that the CA cert path is absolute to prevent reading unintended files
		if !filepath.IsAbs(oidcConfig.TLSCACert) {
			return fmt.Errorf("TLSCACert must be an absolute path, got: %s", oidcConfig.TLSCACert)
		}

		caCert, err := os.ReadFile(oidcConfig.TLSCACert)
		if err != nil {
			return fmt.Errorf("failed to read CA cert file: %w", err)
		}
		// Start with the system cert pool to trust public CAs, then add the custom one.
		rootCAs, _ := x509.SystemCertPool()
		if rootCAs == nil {
			rootCAs = x509.NewCertPool()
		}
		if !rootCAs.AppendCertsFromPEM(caCert) {
			return fmt.Errorf("failed to append CA cert from file: %s", oidcConfig.TLSCACert)
		}
		tlsConfig.RootCAs = rootCAs
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	p.httpClient = &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
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

	// Store all additional claims as attributes
	processedClaims := map[string]struct{}{
		// user / business claims already handled elsewhere
		"sub":    {},
		"email":  {},
		"name":   {},
		"groups": {},
		"roles":  {},
		// standard structural OIDC/JWT claims that should not be exposed as attributes
		"iss": {},
		"aud": {},
		"exp": {},
		"iat": {},
		"nbf": {},
		"jti": {},
	}
	for key, value := range claims.Claims {
		if _, isProcessed := processedClaims[key]; !isProcessed {
			if strValue, ok := value.(string); ok {
				attributes[key] = strValue
			} else if jsonValue, err := json.Marshal(value); err == nil {
				attributes[key] = string(jsonValue)
			} else {
				glog.Warningf("failed to marshal claim %q to JSON for OIDC attributes: %v", key, err)
			}
		}
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
		case *jwt.SigningMethodRSA, *jwt.SigningMethodECDSA:
			return publicKey, nil
		default:
			return nil, fmt.Errorf("unsupported signing method: %v", token.Header["alg"])
		}
	})

	if err != nil {
		// Use JWT library's typed errors for robust error checking
		if errors.Is(err, jwt.ErrTokenExpired) {
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

	// Extract and validate JTI claim for replay protection
	if p.jtiEnabled { // Check if JTI protection is enabled
		jti, jtiExists := claims["jti"].(string)

		if jtiExists && len(jti) > 0 {
			currentCount := p.jtiCount.Load()

			// Trigger eager cleanup when cache reaches 90% capacity to prevent DoS
			if currentCount >= (p.jtiMaxSize * 9 / 10) {
				glog.Warningf("OIDC provider %q: JTI cache at %d%% capacity (%d/%d), triggering eager cleanup",
					p.name, (currentCount*100)/p.jtiMaxSize, currentCount, p.jtiMaxSize)
				go p.performEagerCleanup()
			}

			// Final check: reject if cache is completely full (prevent memory exhaustion)
			if currentCount >= p.jtiMaxSize {
				glog.Errorf("OIDC provider %q: JTI cache FULL (%d entries) - rejecting token. "+
					"This may indicate a DoS attack or insufficient cache size. Consider increasing JTICacheMaxSize.",
					p.name, p.jtiMaxSize)
				return nil, fmt.Errorf("JTI cache capacity exceeded")
			}

			// Create entry with expiration (add clock skew tolerance)
			entry := &jtiEntry{
				subject:   subject,
				expiresAt: tokenClaims.ExpiresAt.Add(clockSkewTolerance),
			}

			// Atomic test-and-set operation to prevent TOCTOU race conditions
			actual, loaded := p.jtiStore.LoadOrStore(jti, entry)
			if loaded {
				// JTI already exists - replay attack detected!
				existingEntry := actual.(*jtiEntry)
				glog.Warningf("OIDC provider %q: Token replay detected for JTI %s (subject: %s, existing subject: %s)",
					p.name, jti, subject, existingEntry.subject)
				return nil, fmt.Errorf("%w: jti %s has already been used",
					providers.ErrProviderTokenReplayed, jti)
			}

			// Increment count only if new entry was stored
			newCount := p.jtiCount.Add(1)

			// Calculate TTL with clock skew tolerance (match JWT library behavior)
			ttl := time.Until(entry.expiresAt)
			if ttl <= 0 {
				// Token is expired even with clock skew tolerance
				p.jtiStore.Delete(jti)
				p.jtiCount.Add(-1)
				glog.Warningf("OIDC provider %q: Token with JTI %s is already expired (exp: %v)",
					p.name, jti, tokenClaims.ExpiresAt)
				return nil, fmt.Errorf("%w: token expired", providers.ErrProviderTokenExpired)
			}

			glog.V(4).Infof("OIDC provider %q: Registered JTI %s (expires in %v, cache size: %d)",
				p.name, jti, ttl, newCount)
		} else {
			// No JTI claim - log warning but allow (for backward compatibility)
			glog.V(3).Infof("OIDC provider %q: Token has no jti claim, replay protection skipped (subject: %s)",
				p.name, subject)
		}
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

// performEagerCleanup performs an immediate cleanup of expired JTI entries.
// Called when cache approaches capacity to prevent DoS scenarios where
// legitimate tokens are rejected due to accumulated expired entries.
func (p *OIDCProvider) performEagerCleanup() {
	if !p.jtiEnabled {
		return
	}

	now := time.Now()
	count := 0
	beforeCount := p.jtiCount.Load()

	p.jtiStore.Range(func(key, value interface{}) bool {
		entry := value.(*jtiEntry)
		if now.After(entry.expiresAt) {
			p.jtiStore.Delete(key)
			p.jtiCount.Add(-1)
			count++
		}
		return true // continue iteration
	})

	if count > 0 {
		glog.Infof("OIDC provider %q: Eager cleanup removed %d expired JTI entries (%d -> %d)",
			p.name, count, beforeCount, p.jtiCount.Load())
	} else {
		glog.Warningf("OIDC provider %q: Eager cleanup found no expired entries to remove. "+
			"Cache may be filled with valid long-lived tokens. Consider increasing JTICacheMaxSize.",
			p.name)
	}
}

// cleanupExpiredJTIs periodically removes expired JTI entries from the cache.
// This prevents memory growth by removing JTIs after their tokens have expired.
// Runs in a background goroutine until the provider is shut down.
func (p *OIDCProvider) cleanupExpiredJTIs(ctx context.Context, interval time.Duration) {
	defer close(p.jtiCleanupDone) // Signal goroutine exit

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	glog.V(3).Infof("OIDC provider %q: JTI cleanup goroutine started (interval: %v)", p.name, interval)

	for {
		select {
		case <-ctx.Done():
			glog.V(3).Infof("OIDC provider %q: JTI cleanup goroutine stopping", p.name)
			return
		case <-ticker.C:
			now := time.Now()
			count := 0

			p.jtiStore.Range(func(key, value interface{}) bool {
				entry := value.(*jtiEntry)
				if now.After(entry.expiresAt) {
					p.jtiStore.Delete(key)
					p.jtiCount.Add(-1)
					count++
				}
				return true // continue iteration
			})

			if count > 0 {
				glog.V(4).Infof("OIDC provider %q: Cleaned up %d expired JTI entries (remaining: %d)",
					p.name, count, p.jtiCount.Load())
			}
		}
	}
}

// Shutdown gracefully stops the JTI cleanup goroutine.
// Should be called when the provider is being shut down to prevent goroutine leaks.
func (p *OIDCProvider) Shutdown(ctx context.Context) error {
	var err error
	p.shutdownOnce.Do(func() {
		if p.jtiCleanupCancel != nil {
			p.jtiCleanupCancel()

			// Wait for cleanup goroutine to exit
			if p.jtiCleanupDone != nil {
				select {
				case <-p.jtiCleanupDone:
					glog.V(2).Infof("OIDC provider %q: Cleanup goroutine shutdown complete", p.name)
				case <-ctx.Done():
					err = ctx.Err()
					glog.Warningf("OIDC provider %q: Shutdown timeout waiting for cleanup goroutine: %v", p.name, err)
				}
			}
		}
	})
	return err
}

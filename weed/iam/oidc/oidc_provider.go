package oidc

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
)

// OIDCProvider implements OpenID Connect authentication
type OIDCProvider struct {
	name        string
	config      *OIDCConfig
	initialized bool
	jwksCache   interface{} // Will store JWKS keys
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

// NewOIDCProvider creates a new OIDC provider
func NewOIDCProvider(name string) *OIDCProvider {
	return &OIDCProvider{
		name: name,
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

	// TODO: Implement actual JWT token validation
	// 1. Parse JWT token
	// 2. Verify signature using JWKS from provider
	// 3. Validate claims (iss, aud, exp, etc.)
	// 4. Extract user claims

	return nil, fmt.Errorf("JWT validation not implemented yet - requires JWKS integration")
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

package providers

import (
	"context"
	"fmt"
	"net/mail"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
)

// IdentityProvider defines the interface for external identity providers
type IdentityProvider interface {
	// Name returns the unique name of the provider
	Name() string

	// Initialize initializes the provider with configuration
	Initialize(config interface{}) error

	// Authenticate authenticates a user with a token and returns external identity
	Authenticate(ctx context.Context, token string) (*ExternalIdentity, error)

	// GetUserInfo retrieves user information by user ID
	GetUserInfo(ctx context.Context, userID string) (*ExternalIdentity, error)

	// ValidateToken validates a token and returns claims
	ValidateToken(ctx context.Context, token string) (*TokenClaims, error)
}

// ExternalIdentity represents an identity from an external provider
type ExternalIdentity struct {
	// UserID is the unique identifier from the external provider
	UserID string `json:"userId"`

	// Email is the user's email address
	Email string `json:"email"`

	// DisplayName is the user's display name
	DisplayName string `json:"displayName"`

	// Groups are the groups the user belongs to
	Groups []string `json:"groups,omitempty"`

	// Attributes are additional user attributes
	Attributes map[string]string `json:"attributes,omitempty"`

	// Provider is the name of the identity provider
	Provider string `json:"provider"`
}

// Validate validates the external identity structure
func (e *ExternalIdentity) Validate() error {
	if e.UserID == "" {
		return fmt.Errorf("user ID is required")
	}

	if e.Provider == "" {
		return fmt.Errorf("provider is required")
	}

	if e.Email != "" {
		if _, err := mail.ParseAddress(e.Email); err != nil {
			return fmt.Errorf("invalid email format: %w", err)
		}
	}

	return nil
}

// TokenClaims represents claims from a validated token
type TokenClaims struct {
	// Subject (sub) - user identifier
	Subject string `json:"sub"`

	// Issuer (iss) - token issuer
	Issuer string `json:"iss"`

	// Audience (aud) - intended audience
	Audience string `json:"aud"`

	// ExpiresAt (exp) - expiration time
	ExpiresAt time.Time `json:"exp"`

	// IssuedAt (iat) - issued at time
	IssuedAt time.Time `json:"iat"`

	// NotBefore (nbf) - not valid before time
	NotBefore time.Time `json:"nbf,omitempty"`

	// Claims are additional claims from the token
	Claims map[string]interface{} `json:"claims,omitempty"`
}

// IsValid checks if the token claims are valid (not expired, etc.)
func (c *TokenClaims) IsValid() bool {
	now := time.Now()

	// Check expiration
	if !c.ExpiresAt.IsZero() && now.After(c.ExpiresAt) {
		return false
	}

	// Check not before
	if !c.NotBefore.IsZero() && now.Before(c.NotBefore) {
		return false
	}

	// Check issued at (shouldn't be in the future)
	if !c.IssuedAt.IsZero() && now.Before(c.IssuedAt) {
		return false
	}

	return true
}

// GetClaimString returns a string claim value
func (c *TokenClaims) GetClaimString(key string) (string, bool) {
	if value, exists := c.Claims[key]; exists {
		if str, ok := value.(string); ok {
			return str, true
		}
	}
	return "", false
}

// GetClaimStringSlice returns a string slice claim value
func (c *TokenClaims) GetClaimStringSlice(key string) ([]string, bool) {
	if value, exists := c.Claims[key]; exists {
		switch v := value.(type) {
		case []string:
			return v, true
		case []interface{}:
			var result []string
			for _, item := range v {
				if str, ok := item.(string); ok {
					result = append(result, str)
				}
			}
			return result, len(result) > 0
		case string:
			// Single string can be treated as slice
			return []string{v}, true
		}
	}
	return nil, false
}

// ProviderConfig represents configuration for identity providers
type ProviderConfig struct {
	// Type of provider (oidc, ldap, saml)
	Type string `json:"type"`

	// Name of the provider instance
	Name string `json:"name"`

	// Enabled indicates if the provider is active
	Enabled bool `json:"enabled"`

	// Config is provider-specific configuration
	Config map[string]interface{} `json:"config"`

	// RoleMapping defines how to map external identities to roles
	RoleMapping *RoleMapping `json:"roleMapping,omitempty"`
}

// RoleMapping defines rules for mapping external identities to roles
type RoleMapping struct {
	// Rules are the mapping rules
	Rules []MappingRule `json:"rules"`

	// DefaultRole is assigned if no rules match
	DefaultRole string `json:"defaultRole,omitempty"`
}

// MappingRule defines a single mapping rule
type MappingRule struct {
	// Claim is the claim key to check
	Claim string `json:"claim"`

	// Value is the expected claim value (supports wildcards)
	Value string `json:"value"`

	// Role is the role ARN to assign
	Role string `json:"role"`

	// Condition is additional condition logic (optional)
	Condition string `json:"condition,omitempty"`
}

// Matches checks if a rule matches the given claims
func (r *MappingRule) Matches(claims *TokenClaims) bool {
	if r.Claim == "" || r.Value == "" {
		glog.V(3).Infof("Rule invalid: claim=%s, value=%s", r.Claim, r.Value)
		return false
	}

	claimValue, exists := claims.GetClaimString(r.Claim)
	if !exists {
		glog.V(3).Infof("Claim '%s' not found as string, trying as string slice", r.Claim)
		// Try as string slice
		if claimSlice, sliceExists := claims.GetClaimStringSlice(r.Claim); sliceExists {
			glog.V(3).Infof("Claim '%s' found as string slice: %v", r.Claim, claimSlice)
			for _, val := range claimSlice {
				glog.V(3).Infof("Checking if '%s' matches rule value '%s'", val, r.Value)
				if r.matchValue(val) {
					glog.V(3).Infof("Match found: '%s' matches '%s'", val, r.Value)
					return true
				}
			}
		} else {
			glog.V(3).Infof("Claim '%s' not found in any format", r.Claim)
		}
		return false
	}

	glog.V(3).Infof("Claim '%s' found as string: '%s'", r.Claim, claimValue)
	return r.matchValue(claimValue)
}

// matchValue checks if a value matches the rule value (with wildcard support)
// Uses AWS IAM-compliant case-insensitive wildcard matching for consistency with policy engine
func (r *MappingRule) matchValue(value string) bool {
	matched := policy.AwsWildcardMatch(r.Value, value)
	glog.V(3).Infof("AWS IAM pattern match result: '%s' matches '%s' = %t", value, r.Value, matched)
	return matched
}

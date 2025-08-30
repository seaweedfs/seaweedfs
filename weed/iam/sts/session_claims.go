package sts

import (
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// STSSessionClaims represents comprehensive session information embedded in JWT tokens
// This eliminates the need for separate session storage by embedding all session
// metadata directly in the token itself - enabling true stateless operation
type STSSessionClaims struct {
	jwt.RegisteredClaims

	// Session identification
	SessionId   string `json:"sid"`  // session_id (abbreviated for smaller tokens)
	SessionName string `json:"snam"` // session_name (abbreviated for smaller tokens)
	TokenType   string `json:"typ"`  // token_type

	// Role information
	RoleArn     string `json:"role"`      // role_arn
	AssumedRole string `json:"assumed"`   // assumed_role_user
	Principal   string `json:"principal"` // principal_arn

	// Authorization data
	Policies []string `json:"pol,omitempty"` // policies (abbreviated)

	// Identity provider information
	IdentityProvider string `json:"idp"`      // identity_provider
	ExternalUserId   string `json:"ext_uid"`  // external_user_id
	ProviderIssuer   string `json:"prov_iss"` // provider_issuer

	// Request context (optional, for policy evaluation)
	RequestContext map[string]interface{} `json:"req_ctx,omitempty"`

	// Session metadata
	AssumedAt   time.Time `json:"assumed_at"`        // when role was assumed
	MaxDuration int64     `json:"max_dur,omitempty"` // maximum session duration in seconds
}

// NewSTSSessionClaims creates new STS session claims with all required information
func NewSTSSessionClaims(sessionId, issuer string, expiresAt time.Time) *STSSessionClaims {
	now := time.Now()
	return &STSSessionClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    issuer,
			Subject:   sessionId,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			NotBefore: jwt.NewNumericDate(now),
		},
		SessionId: sessionId,
		TokenType: TokenTypeSession,
		AssumedAt: now,
	}
}

// ToSessionInfo converts JWT claims back to SessionInfo structure
// This enables seamless integration with existing code expecting SessionInfo
func (c *STSSessionClaims) ToSessionInfo() *SessionInfo {
	var expiresAt time.Time
	if c.ExpiresAt != nil {
		expiresAt = c.ExpiresAt.Time
	}

	return &SessionInfo{
		SessionId:        c.SessionId,
		SessionName:      c.SessionName,
		RoleArn:          c.RoleArn,
		AssumedRoleUser:  c.AssumedRole,
		Principal:        c.Principal,
		Policies:         c.Policies,
		ExpiresAt:        expiresAt,
		IdentityProvider: c.IdentityProvider,
		ExternalUserId:   c.ExternalUserId,
		ProviderIssuer:   c.ProviderIssuer,
		RequestContext:   c.RequestContext,
	}
}

// IsValid checks if the session claims are valid (not expired, etc.)
func (c *STSSessionClaims) IsValid() bool {
	now := time.Now()

	// Check expiration
	if c.ExpiresAt != nil && c.ExpiresAt.Before(now) {
		return false
	}

	// Check not-before
	if c.NotBefore != nil && c.NotBefore.After(now) {
		return false
	}

	// Ensure required fields are present
	if c.SessionId == "" || c.RoleArn == "" || c.Principal == "" {
		return false
	}

	return true
}

// GetSessionId returns the session identifier
func (c *STSSessionClaims) GetSessionId() string {
	return c.SessionId
}

// GetExpiresAt returns the expiration time
func (c *STSSessionClaims) GetExpiresAt() time.Time {
	if c.ExpiresAt != nil {
		return c.ExpiresAt.Time
	}
	return time.Time{}
}

// WithRoleInfo sets role-related information in the claims
func (c *STSSessionClaims) WithRoleInfo(roleArn, assumedRole, principal string) *STSSessionClaims {
	c.RoleArn = roleArn
	c.AssumedRole = assumedRole
	c.Principal = principal
	return c
}

// WithPolicies sets the policies associated with this session
func (c *STSSessionClaims) WithPolicies(policies []string) *STSSessionClaims {
	c.Policies = policies
	return c
}

// WithIdentityProvider sets identity provider information
func (c *STSSessionClaims) WithIdentityProvider(providerName, externalUserId, providerIssuer string) *STSSessionClaims {
	c.IdentityProvider = providerName
	c.ExternalUserId = externalUserId
	c.ProviderIssuer = providerIssuer
	return c
}

// WithRequestContext sets request context for policy evaluation
func (c *STSSessionClaims) WithRequestContext(ctx map[string]interface{}) *STSSessionClaims {
	c.RequestContext = ctx
	return c
}

// WithMaxDuration sets the maximum session duration
func (c *STSSessionClaims) WithMaxDuration(duration time.Duration) *STSSessionClaims {
	c.MaxDuration = int64(duration.Seconds())
	return c
}

// WithSessionName sets the session name
func (c *STSSessionClaims) WithSessionName(sessionName string) *STSSessionClaims {
	c.SessionName = sessionName
	return c
}

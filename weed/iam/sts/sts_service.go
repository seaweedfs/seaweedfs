package sts

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
	"github.com/seaweedfs/seaweedfs/weed/iam/utils"
)

// TrustPolicyValidator interface for validating trust policies during role assumption
type TrustPolicyValidator interface {
	// ValidateTrustPolicyForWebIdentity validates if a web identity token can assume a role
	ValidateTrustPolicyForWebIdentity(ctx context.Context, roleArn string, webIdentityToken string) error

	// ValidateTrustPolicyForCredentials validates if credentials can assume a role
	ValidateTrustPolicyForCredentials(ctx context.Context, roleArn string, identity *providers.ExternalIdentity) error
}

// FlexibleDuration wraps time.Duration to support both integer nanoseconds and duration strings in JSON
type FlexibleDuration struct {
	time.Duration
}

// UnmarshalJSON implements JSON unmarshaling for FlexibleDuration
// Supports both: 3600000000000 (nanoseconds) and "1h" (duration string)
func (fd *FlexibleDuration) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as a duration string first (e.g., "1h", "30m")
	var durationStr string
	if err := json.Unmarshal(data, &durationStr); err == nil {
		duration, parseErr := time.ParseDuration(durationStr)
		if parseErr != nil {
			return fmt.Errorf("invalid duration string %q: %w", durationStr, parseErr)
		}
		fd.Duration = duration
		return nil
	}

	// If that fails, try to unmarshal as an integer (nanoseconds for backward compatibility)
	var nanoseconds int64
	if err := json.Unmarshal(data, &nanoseconds); err == nil {
		fd.Duration = time.Duration(nanoseconds)
		return nil
	}

	// If both fail, try unmarshaling as a quoted number string (edge case)
	var numberStr string
	if err := json.Unmarshal(data, &numberStr); err == nil {
		if nanoseconds, parseErr := strconv.ParseInt(numberStr, 10, 64); parseErr == nil {
			fd.Duration = time.Duration(nanoseconds)
			return nil
		}
	}

	return fmt.Errorf("unable to parse duration from %s (expected duration string like \"1h\" or integer nanoseconds)", data)
}

// MarshalJSON implements JSON marshaling for FlexibleDuration
// Always marshals as a human-readable duration string
func (fd FlexibleDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(fd.Duration.String())
}

// STSService provides Security Token Service functionality
// This service is now completely stateless - all session information is embedded
// in JWT tokens, eliminating the need for session storage and enabling true
// distributed operation without shared state
type STSService struct {
	Config               *STSConfig // Public for access by other components
	initialized          bool
	providers            map[string]providers.IdentityProvider
	issuerToProvider     map[string]providers.IdentityProvider // Efficient issuer-based provider lookup
	tokenGenerator       *TokenGenerator
	trustPolicyValidator TrustPolicyValidator // Interface for trust policy validation
}

// STSConfig holds STS service configuration
type STSConfig struct {
	// TokenDuration is the default duration for issued tokens
	TokenDuration FlexibleDuration `json:"tokenDuration"`

	// MaxSessionLength is the maximum duration for any session
	MaxSessionLength FlexibleDuration `json:"maxSessionLength"`

	// Issuer is the STS issuer identifier
	Issuer string `json:"issuer"`

	// SigningKey is used to sign session tokens
	SigningKey []byte `json:"signingKey"`

	// Providers configuration - enables automatic provider loading
	Providers []*ProviderConfig `json:"providers,omitempty"`
}

// ProviderConfig holds identity provider configuration
type ProviderConfig struct {
	// Name is the unique identifier for the provider
	Name string `json:"name"`

	// Type specifies the provider type (oidc, ldap, etc.)
	Type string `json:"type"`

	// Config contains provider-specific configuration
	Config map[string]interface{} `json:"config"`

	// Enabled indicates if this provider should be active
	Enabled bool `json:"enabled"`
}

// AssumeRoleWithWebIdentityRequest represents a request to assume role with web identity
type AssumeRoleWithWebIdentityRequest struct {
	// RoleArn is the ARN of the role to assume
	RoleArn string `json:"RoleArn"`

	// WebIdentityToken is the OIDC token from the identity provider
	WebIdentityToken string `json:"WebIdentityToken"`

	// RoleSessionName is a name for the assumed role session
	RoleSessionName string `json:"RoleSessionName"`

	// DurationSeconds is the duration of the role session (optional)
	DurationSeconds *int64 `json:"DurationSeconds,omitempty"`

	// Policy is an optional session policy (optional)
	Policy *string `json:"Policy,omitempty"`
}

// AssumeRoleWithCredentialsRequest represents a request to assume role with username/password
type AssumeRoleWithCredentialsRequest struct {
	// RoleArn is the ARN of the role to assume
	RoleArn string `json:"RoleArn"`

	// Username is the username for authentication
	Username string `json:"Username"`

	// Password is the password for authentication
	Password string `json:"Password"`

	// RoleSessionName is a name for the assumed role session
	RoleSessionName string `json:"RoleSessionName"`

	// ProviderName is the name of the identity provider to use
	ProviderName string `json:"ProviderName"`

	// DurationSeconds is the duration of the role session (optional)
	DurationSeconds *int64 `json:"DurationSeconds,omitempty"`
}

// AssumeRoleResponse represents the response from assume role operations
type AssumeRoleResponse struct {
	// Credentials contains the temporary security credentials
	Credentials *Credentials `json:"Credentials"`

	// AssumedRoleUser contains information about the assumed role user
	AssumedRoleUser *AssumedRoleUser `json:"AssumedRoleUser"`

	// PackedPolicySize is the percentage of max policy size used (AWS compatibility)
	PackedPolicySize *int64 `json:"PackedPolicySize,omitempty"`
}

// Credentials represents temporary security credentials
type Credentials struct {
	// AccessKeyId is the access key ID
	AccessKeyId string `json:"AccessKeyId"`

	// SecretAccessKey is the secret access key
	SecretAccessKey string `json:"SecretAccessKey"`

	// SessionToken is the session token
	SessionToken string `json:"SessionToken"`

	// Expiration is when the credentials expire
	Expiration time.Time `json:"Expiration"`
}

// AssumedRoleUser contains information about the assumed role user
type AssumedRoleUser struct {
	// AssumedRoleId is the unique identifier of the assumed role
	AssumedRoleId string `json:"AssumedRoleId"`

	// Arn is the ARN of the assumed role user
	Arn string `json:"Arn"`

	// Subject is the subject identifier from the identity provider
	Subject string `json:"Subject,omitempty"`
}

// SessionInfo represents information about an active session
type SessionInfo struct {
	// SessionId is the unique identifier for the session
	SessionId string `json:"sessionId"`

	// SessionName is the name of the role session
	SessionName string `json:"sessionName"`

	// RoleArn is the ARN of the assumed role
	RoleArn string `json:"roleArn"`

	// AssumedRoleUser contains information about the assumed role user
	AssumedRoleUser string `json:"assumedRoleUser"`

	// Principal is the principal ARN
	Principal string `json:"principal"`

	// Subject is the subject identifier from the identity provider
	Subject string `json:"subject"`

	// Provider is the identity provider used (legacy field)
	Provider string `json:"provider"`

	// IdentityProvider is the identity provider used
	IdentityProvider string `json:"identityProvider"`

	// ExternalUserId is the external user identifier from the provider
	ExternalUserId string `json:"externalUserId"`

	// ProviderIssuer is the issuer from the identity provider
	ProviderIssuer string `json:"providerIssuer"`

	// Policies are the policies associated with this session
	Policies []string `json:"policies"`

	// RequestContext contains additional request context for policy evaluation
	RequestContext map[string]interface{} `json:"requestContext,omitempty"`

	// CreatedAt is when the session was created
	CreatedAt time.Time `json:"createdAt"`

	// ExpiresAt is when the session expires
	ExpiresAt time.Time `json:"expiresAt"`

	// Credentials are the temporary credentials for this session
	Credentials *Credentials `json:"credentials"`
}

// NewSTSService creates a new STS service
func NewSTSService() *STSService {
	return &STSService{
		providers:        make(map[string]providers.IdentityProvider),
		issuerToProvider: make(map[string]providers.IdentityProvider),
	}
}

// Initialize initializes the STS service with configuration
func (s *STSService) Initialize(config *STSConfig) error {
	if config == nil {
		return fmt.Errorf(ErrConfigCannotBeNil)
	}

	if err := s.validateConfig(config); err != nil {
		return fmt.Errorf("invalid STS configuration: %w", err)
	}

	s.Config = config

	// Initialize token generator for stateless JWT operations
	s.tokenGenerator = NewTokenGenerator(config.SigningKey, config.Issuer)

	// Load identity providers from configuration
	if err := s.loadProvidersFromConfig(config); err != nil {
		return fmt.Errorf("failed to load identity providers: %w", err)
	}

	s.initialized = true
	return nil
}

// validateConfig validates the STS configuration
func (s *STSService) validateConfig(config *STSConfig) error {
	if config.TokenDuration.Duration <= 0 {
		return fmt.Errorf(ErrInvalidTokenDuration)
	}

	if config.MaxSessionLength.Duration <= 0 {
		return fmt.Errorf(ErrInvalidMaxSessionLength)
	}

	if config.Issuer == "" {
		return fmt.Errorf(ErrIssuerRequired)
	}

	if len(config.SigningKey) < MinSigningKeyLength {
		return fmt.Errorf(ErrSigningKeyTooShort, MinSigningKeyLength)
	}

	return nil
}

// loadProvidersFromConfig loads identity providers from configuration
func (s *STSService) loadProvidersFromConfig(config *STSConfig) error {
	if len(config.Providers) == 0 {
		glog.V(2).Infof("No providers configured in STS config")
		return nil
	}

	factory := NewProviderFactory()

	// Load all providers from configuration
	providersMap, err := factory.LoadProvidersFromConfig(config.Providers)
	if err != nil {
		return fmt.Errorf("failed to load providers from config: %w", err)
	}

	// Replace current providers with new ones
	s.providers = providersMap

	// Also populate the issuerToProvider map for efficient and secure JWT validation
	s.issuerToProvider = make(map[string]providers.IdentityProvider)
	for name, provider := range s.providers {
		issuer := s.extractIssuerFromProvider(provider)
		if issuer != "" {
			if _, exists := s.issuerToProvider[issuer]; exists {
				glog.Warningf("Duplicate issuer %s found for provider %s. Overwriting.", issuer, name)
			}
			s.issuerToProvider[issuer] = provider
			glog.V(2).Infof("Registered provider %s with issuer %s for efficient lookup", name, issuer)
		}
	}

	glog.V(1).Infof("Successfully loaded %d identity providers: %v",
		len(s.providers), s.getProviderNames())

	return nil
}

// getProviderNames returns list of loaded provider names
func (s *STSService) getProviderNames() []string {
	names := make([]string, 0, len(s.providers))
	for name := range s.providers {
		names = append(names, name)
	}
	return names
}

// IsInitialized returns whether the service is initialized
func (s *STSService) IsInitialized() bool {
	return s.initialized
}

// RegisterProvider registers an identity provider
func (s *STSService) RegisterProvider(provider providers.IdentityProvider) error {
	if provider == nil {
		return fmt.Errorf(ErrProviderCannotBeNil)
	}

	name := provider.Name()
	if name == "" {
		return fmt.Errorf(ErrProviderNameEmpty)
	}

	s.providers[name] = provider

	// Try to extract issuer information for efficient lookup
	// This is a best-effort approach for different provider types
	issuer := s.extractIssuerFromProvider(provider)
	if issuer != "" {
		s.issuerToProvider[issuer] = provider
		glog.V(2).Infof("Registered provider %s with issuer %s for efficient lookup", name, issuer)
	}

	return nil
}

// extractIssuerFromProvider attempts to extract issuer information from different provider types
func (s *STSService) extractIssuerFromProvider(provider providers.IdentityProvider) string {
	// Handle different provider types
	switch p := provider.(type) {
	case interface{ GetIssuer() string }:
		// For providers that implement GetIssuer() method
		return p.GetIssuer()
	default:
		// For other provider types, we'll rely on JWT parsing during validation
		// This is still more efficient than the current brute-force approach
		return ""
	}
}

// GetProviders returns all registered identity providers
func (s *STSService) GetProviders() map[string]providers.IdentityProvider {
	return s.providers
}

// SetTrustPolicyValidator sets the trust policy validator for role assumption validation
func (s *STSService) SetTrustPolicyValidator(validator TrustPolicyValidator) {
	s.trustPolicyValidator = validator
}

// AssumeRoleWithWebIdentity assumes a role using a web identity token (OIDC)
// This method is now completely stateless - all session information is embedded in the JWT token
func (s *STSService) AssumeRoleWithWebIdentity(ctx context.Context, request *AssumeRoleWithWebIdentityRequest) (*AssumeRoleResponse, error) {
	if !s.initialized {
		return nil, fmt.Errorf(ErrSTSServiceNotInitialized)
	}

	if request == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	// Validate request parameters
	if err := s.validateAssumeRoleWithWebIdentityRequest(request); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	// Check for unsupported session policy
	if request.Policy != nil {
		return nil, fmt.Errorf("session policies are not currently supported - Policy parameter must be omitted")
	}

	// 1. Validate the web identity token with appropriate provider
	externalIdentity, provider, err := s.validateWebIdentityToken(ctx, request.WebIdentityToken)
	if err != nil {
		return nil, fmt.Errorf("failed to validate web identity token: %w", err)
	}

	// 2. Check if the role exists and can be assumed (includes trust policy validation)
	if err := s.validateRoleAssumptionForWebIdentity(ctx, request.RoleArn, request.WebIdentityToken); err != nil {
		return nil, fmt.Errorf("role assumption denied: %w", err)
	}

	// 3. Calculate session duration, capping at the source token's expiration
	// This ensures sessions from short-lived tokens (e.g., GitLab CI job tokens) don't outlive their source
	sessionDuration := s.calculateSessionDuration(request.DurationSeconds, externalIdentity.TokenExpiration)
	expiresAt := time.Now().Add(sessionDuration)

	// 4. Generate session ID and credentials
	sessionId, err := GenerateSessionId()
	if err != nil {
		return nil, fmt.Errorf("failed to generate session ID: %w", err)
	}

	credGenerator := NewCredentialGenerator()
	credentials, err := credGenerator.GenerateTemporaryCredentials(sessionId, expiresAt)
	if err != nil {
		return nil, fmt.Errorf("failed to generate credentials: %w", err)
	}

	// 5. Create comprehensive JWT session token with all session information embedded
	assumedRoleUser := &AssumedRoleUser{
		AssumedRoleId: request.RoleArn,
		Arn:           GenerateAssumedRoleArn(request.RoleArn, request.RoleSessionName),
		Subject:       externalIdentity.UserID,
	}

	// Create rich JWT claims with all session information
	sessionClaims := NewSTSSessionClaims(sessionId, s.Config.Issuer, expiresAt).
		WithSessionName(request.RoleSessionName).
		WithRoleInfo(request.RoleArn, assumedRoleUser.Arn, assumedRoleUser.Arn).
		WithIdentityProvider(provider.Name(), externalIdentity.UserID, "").
		WithMaxDuration(sessionDuration)

	// Generate self-contained JWT token with all session information
	jwtToken, err := s.tokenGenerator.GenerateJWTWithClaims(sessionClaims)
	if err != nil {
		return nil, fmt.Errorf("failed to generate JWT session token: %w", err)
	}
	credentials.SessionToken = jwtToken

	// 6. Build and return response (no session storage needed!)

	return &AssumeRoleResponse{
		Credentials:     credentials,
		AssumedRoleUser: assumedRoleUser,
	}, nil
}

// AssumeRoleWithCredentials assumes a role using username/password credentials
// This method is now completely stateless - all session information is embedded in the JWT token
func (s *STSService) AssumeRoleWithCredentials(ctx context.Context, request *AssumeRoleWithCredentialsRequest) (*AssumeRoleResponse, error) {
	if !s.initialized {
		return nil, fmt.Errorf("STS service not initialized")
	}

	if request == nil {
		return nil, fmt.Errorf("request cannot be nil")
	}

	// Validate request parameters
	if err := s.validateAssumeRoleWithCredentialsRequest(request); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	// 1. Get the specified provider
	provider, exists := s.providers[request.ProviderName]
	if !exists {
		return nil, fmt.Errorf("identity provider not found: %s", request.ProviderName)
	}

	// 2. Validate credentials with the specified provider
	credentials := request.Username + ":" + request.Password
	externalIdentity, err := provider.Authenticate(ctx, credentials)
	if err != nil {
		return nil, fmt.Errorf("failed to authenticate credentials: %w", err)
	}

	// 3. Check if the role exists and can be assumed (includes trust policy validation)
	if err := s.validateRoleAssumptionForCredentials(ctx, request.RoleArn, externalIdentity); err != nil {
		return nil, fmt.Errorf("role assumption denied: %w", err)
	}

	// 4. Calculate session duration
	// For credential-based auth, there's no source token with expiration to cap against
	sessionDuration := s.calculateSessionDuration(request.DurationSeconds, nil)
	expiresAt := time.Now().Add(sessionDuration)

	// 5. Generate session ID and temporary credentials
	sessionId, err := GenerateSessionId()
	if err != nil {
		return nil, fmt.Errorf("failed to generate session ID: %w", err)
	}

	credGenerator := NewCredentialGenerator()
	tempCredentials, err := credGenerator.GenerateTemporaryCredentials(sessionId, expiresAt)
	if err != nil {
		return nil, fmt.Errorf("failed to generate credentials: %w", err)
	}

	// 6. Create comprehensive JWT session token with all session information embedded
	assumedRoleUser := &AssumedRoleUser{
		AssumedRoleId: request.RoleArn,
		Arn:           GenerateAssumedRoleArn(request.RoleArn, request.RoleSessionName),
		Subject:       externalIdentity.UserID,
	}

	// Create rich JWT claims with all session information
	sessionClaims := NewSTSSessionClaims(sessionId, s.Config.Issuer, expiresAt).
		WithSessionName(request.RoleSessionName).
		WithRoleInfo(request.RoleArn, assumedRoleUser.Arn, assumedRoleUser.Arn).
		WithIdentityProvider(provider.Name(), externalIdentity.UserID, "").
		WithMaxDuration(sessionDuration)

	// Generate self-contained JWT token with all session information
	jwtToken, err := s.tokenGenerator.GenerateJWTWithClaims(sessionClaims)
	if err != nil {
		return nil, fmt.Errorf("failed to generate JWT session token: %w", err)
	}
	tempCredentials.SessionToken = jwtToken

	// 7. Build and return response (no session storage needed!)

	return &AssumeRoleResponse{
		Credentials:     tempCredentials,
		AssumedRoleUser: assumedRoleUser,
	}, nil
}

// ValidateSessionToken validates a session token and returns session information
// This method is now completely stateless - all session information is extracted from the JWT token
func (s *STSService) ValidateSessionToken(ctx context.Context, sessionToken string) (*SessionInfo, error) {
	if !s.initialized {
		return nil, fmt.Errorf(ErrSTSServiceNotInitialized)
	}

	if sessionToken == "" {
		return nil, fmt.Errorf(ErrSessionTokenCannotBeEmpty)
	}

	// Validate JWT and extract comprehensive session claims
	claims, err := s.tokenGenerator.ValidateJWTWithClaims(sessionToken)
	if err != nil {
		return nil, fmt.Errorf(ErrSessionValidationFailed, err)
	}

	// Convert JWT claims back to SessionInfo
	// All session information is embedded in the JWT token itself
	return claims.ToSessionInfo(), nil
}

// NOTE: Session revocation is not supported in the stateless JWT design.
//
// In a stateless JWT system, tokens cannot be revoked without implementing a token blacklist,
// which would break the stateless architecture. Tokens remain valid until their natural
// expiration time.
//
// For applications requiring token revocation, consider:
// 1. Using shorter token lifespans (e.g., 15-30 minutes)
// 2. Implementing a distributed token blacklist (breaks stateless design)
// 3. Including a "jti" (JWT ID) claim for tracking specific tokens
//
// Use ValidateSessionToken() to verify if a token is valid and not expired.

// Helper methods for AssumeRoleWithWebIdentity

// validateAssumeRoleWithWebIdentityRequest validates the request parameters
func (s *STSService) validateAssumeRoleWithWebIdentityRequest(request *AssumeRoleWithWebIdentityRequest) error {
	if request.RoleArn == "" {
		return fmt.Errorf("RoleArn is required")
	}

	if request.WebIdentityToken == "" {
		return fmt.Errorf("WebIdentityToken is required")
	}

	if request.RoleSessionName == "" {
		return fmt.Errorf("RoleSessionName is required")
	}

	// Validate session duration if provided
	if request.DurationSeconds != nil {
		if *request.DurationSeconds < 900 || *request.DurationSeconds > 43200 { // 15min to 12 hours
			return fmt.Errorf("DurationSeconds must be between 900 and 43200 seconds")
		}
	}

	return nil
}

// validateWebIdentityToken validates the web identity token with strict issuer-to-provider mapping
// SECURITY: JWT tokens with a specific issuer claim MUST only be validated by the provider for that issuer
// SECURITY: This method only accepts JWT tokens. Non-JWT authentication must use AssumeRoleWithCredentials with explicit ProviderName.
func (s *STSService) validateWebIdentityToken(ctx context.Context, token string) (*providers.ExternalIdentity, providers.IdentityProvider, error) {
	// Try to extract issuer from JWT token for strict validation
	issuer, err := s.extractIssuerFromJWT(token)
	if err != nil {
		// Token is not a valid JWT or cannot be parsed
		// SECURITY: Web identity tokens MUST be JWT tokens. Non-JWT authentication flows
		// should use AssumeRoleWithCredentials with explicit ProviderName to prevent
		// security vulnerabilities from non-deterministic provider selection.
		return nil, nil, fmt.Errorf("web identity token must be a valid JWT token: %w", err)
	}

	// Look up the specific provider for this issuer
	provider, exists := s.issuerToProvider[issuer]
	if !exists {
		// SECURITY: If no provider is registered for this issuer, fail immediately
		// This prevents JWT tokens from being validated by unintended providers
		return nil, nil, fmt.Errorf("no identity provider registered for issuer: %s", issuer)
	}

	// Authenticate with the correct provider for this issuer
	identity, err := provider.Authenticate(ctx, token)
	if err != nil {
		return nil, nil, fmt.Errorf("token validation failed with provider for issuer %s: %w", issuer, err)
	}

	if identity == nil {
		return nil, nil, fmt.Errorf("authentication succeeded but no identity returned for issuer %s", issuer)
	}

	return identity, provider, nil
}

// ValidateWebIdentityToken is a public method that exposes secure token validation for external use
// This method uses issuer-based lookup to select the correct provider, ensuring security and efficiency
func (s *STSService) ValidateWebIdentityToken(ctx context.Context, token string) (*providers.ExternalIdentity, providers.IdentityProvider, error) {
	return s.validateWebIdentityToken(ctx, token)
}

// extractIssuerFromJWT extracts the issuer (iss) claim from a JWT token without verification
func (s *STSService) extractIssuerFromJWT(token string) (string, error) {
	// Parse token without verification to get claims
	parsedToken, _, err := new(jwt.Parser).ParseUnverified(token, jwt.MapClaims{})
	if err != nil {
		return "", fmt.Errorf("failed to parse JWT token: %v", err)
	}

	// Extract claims
	claims, ok := parsedToken.Claims.(jwt.MapClaims)
	if !ok {
		return "", fmt.Errorf("invalid token claims")
	}

	// Get issuer claim
	issuer, ok := claims["iss"].(string)
	if !ok || issuer == "" {
		return "", fmt.Errorf("missing or invalid issuer claim")
	}

	return issuer, nil
}

// validateRoleAssumptionForWebIdentity validates role assumption for web identity tokens
// This method performs complete trust policy validation to prevent unauthorized role assumptions
func (s *STSService) validateRoleAssumptionForWebIdentity(ctx context.Context, roleArn string, webIdentityToken string) error {
	if roleArn == "" {
		return fmt.Errorf("role ARN cannot be empty")
	}

	if webIdentityToken == "" {
		return fmt.Errorf("web identity token cannot be empty")
	}

	// Basic role ARN format validation
	expectedPrefix := "arn:aws:iam::role/"
	if len(roleArn) < len(expectedPrefix) || roleArn[:len(expectedPrefix)] != expectedPrefix {
		return fmt.Errorf("invalid role ARN format: got %s, expected format: %s*", roleArn, expectedPrefix)
	}

	// Extract role name and validate ARN format
	roleName := utils.ExtractRoleNameFromArn(roleArn)
	if roleName == "" {
		return fmt.Errorf("invalid role ARN format: %s", roleArn)
	}

	// CRITICAL SECURITY: Perform trust policy validation
	if s.trustPolicyValidator != nil {
		if err := s.trustPolicyValidator.ValidateTrustPolicyForWebIdentity(ctx, roleArn, webIdentityToken); err != nil {
			return fmt.Errorf("trust policy validation failed: %w", err)
		}
	} else {
		// If no trust policy validator is configured, fail closed for security
		glog.Errorf("SECURITY WARNING: No trust policy validator configured - denying role assumption for security")
		return fmt.Errorf("trust policy validation not available - role assumption denied for security")
	}

	return nil
}

// validateRoleAssumptionForCredentials validates role assumption for credential-based authentication
// This method performs complete trust policy validation to prevent unauthorized role assumptions
func (s *STSService) validateRoleAssumptionForCredentials(ctx context.Context, roleArn string, identity *providers.ExternalIdentity) error {
	if roleArn == "" {
		return fmt.Errorf("role ARN cannot be empty")
	}

	if identity == nil {
		return fmt.Errorf("identity cannot be nil")
	}

	// Basic role ARN format validation
	expectedPrefix := "arn:aws:iam::role/"
	if len(roleArn) < len(expectedPrefix) || roleArn[:len(expectedPrefix)] != expectedPrefix {
		return fmt.Errorf("invalid role ARN format: got %s, expected format: %s*", roleArn, expectedPrefix)
	}

	// Extract role name and validate ARN format
	roleName := utils.ExtractRoleNameFromArn(roleArn)
	if roleName == "" {
		return fmt.Errorf("invalid role ARN format: %s", roleArn)
	}

	// CRITICAL SECURITY: Perform trust policy validation
	if s.trustPolicyValidator != nil {
		if err := s.trustPolicyValidator.ValidateTrustPolicyForCredentials(ctx, roleArn, identity); err != nil {
			return fmt.Errorf("trust policy validation failed: %w", err)
		}
	} else {
		// If no trust policy validator is configured, fail closed for security
		glog.Errorf("SECURITY WARNING: No trust policy validator configured - denying role assumption for security")
		return fmt.Errorf("trust policy validation not available - role assumption denied for security")
	}

	return nil
}

// calculateSessionDuration calculates the session duration, respecting the source token's expiration
// If the incoming web identity token has an exp claim, the session duration is capped to not exceed it
// This ensures that sessions from short-lived tokens (e.g., GitLab CI job tokens) don't outlive their source
func (s *STSService) calculateSessionDuration(durationSeconds *int64, tokenExpiration *time.Time) time.Duration {
	var duration time.Duration
	if durationSeconds != nil {
		duration = time.Duration(*durationSeconds) * time.Second
	} else {
		// Use default from config
		duration = s.Config.TokenDuration.Duration
	}

	// If the source token has an expiration, cap the session duration to not exceed it
	// This follows the principle: "if calculated exp > incoming exp claim, then limit outgoing exp to incoming exp"
	if tokenExpiration != nil && !tokenExpiration.IsZero() {
		timeUntilTokenExpiry := time.Until(*tokenExpiration)
		if timeUntilTokenExpiry > 0 && timeUntilTokenExpiry < duration {
			glog.V(2).Infof("Limiting session duration from %v to %v based on source token expiration",
				duration, timeUntilTokenExpiry)
			duration = timeUntilTokenExpiry
		}
	}

	return duration
}

// extractSessionIdFromToken extracts session ID from JWT session token
func (s *STSService) extractSessionIdFromToken(sessionToken string) string {
	// Parse JWT and extract session ID from claims
	claims, err := s.tokenGenerator.ValidateJWTWithClaims(sessionToken)
	if err != nil {
		// For test compatibility, also handle direct session IDs
		if len(sessionToken) == 32 { // Typical session ID length
			return sessionToken
		}
		return ""
	}

	return claims.SessionId
}

// validateAssumeRoleWithCredentialsRequest validates the credentials request parameters
func (s *STSService) validateAssumeRoleWithCredentialsRequest(request *AssumeRoleWithCredentialsRequest) error {
	if request.RoleArn == "" {
		return fmt.Errorf("RoleArn is required")
	}

	if request.Username == "" {
		return fmt.Errorf("Username is required")
	}

	if request.Password == "" {
		return fmt.Errorf("Password is required")
	}

	if request.RoleSessionName == "" {
		return fmt.Errorf("RoleSessionName is required")
	}

	if request.ProviderName == "" {
		return fmt.Errorf("ProviderName is required")
	}

	// Validate session duration if provided
	if request.DurationSeconds != nil {
		if *request.DurationSeconds < 900 || *request.DurationSeconds > 43200 { // 15min to 12 hours
			return fmt.Errorf("DurationSeconds must be between 900 and 43200 seconds")
		}
	}

	return nil
}

// ExpireSessionForTesting manually expires a session for testing purposes
func (s *STSService) ExpireSessionForTesting(ctx context.Context, sessionToken string) error {
	if !s.initialized {
		return fmt.Errorf("STS service not initialized")
	}

	if sessionToken == "" {
		return fmt.Errorf("session token cannot be empty")
	}

	// Validate JWT token format
	_, err := s.tokenGenerator.ValidateJWTWithClaims(sessionToken)
	if err != nil {
		return fmt.Errorf("invalid session token format: %w", err)
	}

	// In a stateless system, we cannot manually expire JWT tokens
	// The token expiration is embedded in the token itself and handled by JWT validation
	glog.V(1).Infof("Manual session expiration requested for stateless token - cannot expire JWT tokens manually")

	return fmt.Errorf("manual session expiration not supported in stateless JWT system")
}

package sts

// Store Types
const (
	StoreTypeMemory = "memory"
	StoreTypeFiler  = "filer"
	StoreTypeRedis  = "redis"
)

// Provider Types
const (
	ProviderTypeOIDC = "oidc"
	ProviderTypeLDAP = "ldap"
	ProviderTypeSAML = "saml"
)

// Policy Effects
const (
	EffectAllow = "Allow"
	EffectDeny  = "Deny"
)

// Default Paths - aligned with filer /etc/ convention
const (
	DefaultSessionBasePath = "/etc/iam/sessions"
	DefaultPolicyBasePath  = "/etc/iam/policies"
	DefaultRoleBasePath    = "/etc/iam/roles"
)

// Default Values
const (
	DefaultTokenDuration    = 3600  // 1 hour in seconds
	DefaultMaxSessionLength = 43200 // 12 hours in seconds
	DefaultIssuer           = "seaweedfs-sts"
	DefaultStoreType        = StoreTypeFiler // Default store type for persistence
	MinSigningKeyLength     = 16             // Minimum signing key length in bytes
)

// Configuration Field Names
const (
	ConfigFieldFilerAddress = "filerAddress"
	ConfigFieldBasePath     = "basePath"
	ConfigFieldIssuer       = "issuer"
	ConfigFieldClientID     = "clientId"
	ConfigFieldClientSecret = "clientSecret"
	ConfigFieldJWKSUri      = "jwksUri"
	ConfigFieldScopes       = "scopes"
	ConfigFieldUserInfoUri  = "userInfoUri"
	ConfigFieldRedirectUri  = "redirectUri"
)

// Error Messages
const (
	ErrConfigCannotBeNil         = "config cannot be nil"
	ErrProviderCannotBeNil       = "provider cannot be nil"
	ErrProviderNameEmpty         = "provider name cannot be empty"
	ErrProviderTypeEmpty         = "provider type cannot be empty"
	ErrTokenCannotBeEmpty        = "token cannot be empty"
	ErrSessionTokenCannotBeEmpty = "session token cannot be empty"
	ErrSessionIDCannotBeEmpty    = "session ID cannot be empty"
	ErrSTSServiceNotInitialized  = "STS service not initialized"
	ErrProviderNotInitialized    = "provider not initialized"
	ErrInvalidTokenDuration      = "token duration must be positive"
	ErrInvalidMaxSessionLength   = "max session length must be positive"
	ErrIssuerRequired            = "issuer is required"
	ErrSigningKeyTooShort        = "signing key must be at least %d bytes"
	ErrFilerAddressRequired      = "filer address is required"
	ErrClientIDRequired          = "clientId is required for OIDC provider"
	ErrUnsupportedStoreType      = "unsupported store type: %s"
	ErrUnsupportedProviderType   = "unsupported provider type: %s"
	ErrInvalidTokenFormat        = "invalid session token format: %w"
	ErrSessionValidationFailed   = "session validation failed: %w"
	ErrInvalidToken              = "invalid token: %w"
	ErrTokenNotValid             = "token is not valid"
	ErrInvalidTokenClaims        = "invalid token claims"
	ErrInvalidIssuer             = "invalid issuer"
	ErrMissingSessionID          = "missing session ID"
)

// JWT Claims
const (
	JWTClaimIssuer     = "iss"
	JWTClaimSubject    = "sub"
	JWTClaimAudience   = "aud"
	JWTClaimExpiration = "exp"
	JWTClaimIssuedAt   = "iat"
	JWTClaimTokenType  = "token_type"
)

// Token Types
const (
	TokenTypeSession = "session"
	TokenTypeAccess  = "access"
	TokenTypeRefresh = "refresh"
)

// AWS STS Actions
const (
	ActionAssumeRole                = "sts:AssumeRole"
	ActionAssumeRoleWithWebIdentity = "sts:AssumeRoleWithWebIdentity"
	ActionAssumeRoleWithCredentials = "sts:AssumeRoleWithCredentials"
	ActionValidateSession           = "sts:ValidateSession"
)

// Session File Prefixes
const (
	SessionFilePrefix = "session_"
	SessionFileExt    = ".json"
	PolicyFilePrefix  = "policy_"
	PolicyFileExt     = ".json"
	RoleFileExt       = ".json"
)

// HTTP Headers
const (
	HeaderAuthorization = "Authorization"
	HeaderContentType   = "Content-Type"
	HeaderUserAgent     = "User-Agent"
)

// Content Types
const (
	ContentTypeJSON           = "application/json"
	ContentTypeFormURLEncoded = "application/x-www-form-urlencoded"
)

// Default Test Values
const (
	TestSigningKey32Chars = "test-signing-key-32-characters-long"
	TestIssuer            = "test-sts"
	TestClientID          = "test-client"
	TestSessionID         = "test-session-123"
	TestValidToken        = "valid_test_token"
	TestInvalidToken      = "invalid_token"
	TestExpiredToken      = "expired_token"
)

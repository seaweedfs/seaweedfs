package providers

import "errors"

// Typed errors for identity provider operations
// These enable robust error checking with errors.Is() throughout the stack
var (
	// ErrProviderTokenExpired indicates that the provided token has expired
	ErrProviderTokenExpired = errors.New("provider: token has expired")

	// ErrProviderInvalidToken indicates that the token format is invalid or malformed
	ErrProviderInvalidToken = errors.New("provider: invalid token format")

	// ErrProviderInvalidIssuer indicates that the token issuer is not trusted
	ErrProviderInvalidIssuer = errors.New("provider: invalid token issuer")

	// ErrProviderInvalidAudience indicates that the token audience doesn't match expected value
	ErrProviderInvalidAudience = errors.New("provider: invalid token audience")

	// ErrProviderMissingClaims indicates that required claims are missing from the token
	ErrProviderMissingClaims = errors.New("provider: missing required claims")

	// ErrProviderTokenReplayed is returned when a token with duplicate JTI is detected
	ErrProviderTokenReplayed = errors.New("provider: token has been replayed")
)

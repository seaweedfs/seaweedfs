package sts

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/iam/utils"
)

// TokenGenerator handles token generation and validation
type TokenGenerator struct {
	signingKey []byte
	issuer     string
}

// NewTokenGenerator creates a new token generator
func NewTokenGenerator(signingKey []byte, issuer string) *TokenGenerator {
	return &TokenGenerator{
		signingKey: signingKey,
		issuer:     issuer,
	}
}

// GenerateSessionToken creates a signed JWT session token (legacy method for compatibility)
func (t *TokenGenerator) GenerateSessionToken(sessionId string, expiresAt time.Time) (string, error) {
	claims := NewSTSSessionClaims(sessionId, t.issuer, expiresAt)
	return t.GenerateJWTWithClaims(claims)
}

// GenerateJWTWithClaims creates a signed JWT token with comprehensive session claims
func (t *TokenGenerator) GenerateJWTWithClaims(claims *STSSessionClaims) (string, error) {
	if claims == nil {
		return "", fmt.Errorf("claims cannot be nil")
	}

	// Ensure issuer is set from token generator
	if claims.Issuer == "" {
		claims.Issuer = t.issuer
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(t.signingKey)
}

// ValidateSessionToken validates and extracts claims from a session token
func (t *TokenGenerator) ValidateSessionToken(tokenString string) (*SessionTokenClaims, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return t.signingKey, nil
	})

	if err != nil {
		return nil, fmt.Errorf(ErrInvalidToken, err)
	}

	if !token.Valid {
		return nil, fmt.Errorf(ErrTokenNotValid)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf(ErrInvalidTokenClaims)
	}

	// Verify issuer
	if iss, ok := claims[JWTClaimIssuer].(string); !ok || iss != t.issuer {
		return nil, fmt.Errorf(ErrInvalidIssuer)
	}

	// Extract session ID
	sessionId, ok := claims[JWTClaimSubject].(string)
	if !ok {
		return nil, fmt.Errorf(ErrMissingSessionID)
	}

	return &SessionTokenClaims{
		SessionId: sessionId,
		ExpiresAt: time.Unix(int64(claims[JWTClaimExpiration].(float64)), 0),
		IssuedAt:  time.Unix(int64(claims[JWTClaimIssuedAt].(float64)), 0),
	}, nil
}

// ValidateJWTWithClaims validates and extracts comprehensive session claims from a JWT token
func (t *TokenGenerator) ValidateJWTWithClaims(tokenString string) (*STSSessionClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &STSSessionClaims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return t.signingKey, nil
	})

	if err != nil {
		return nil, fmt.Errorf(ErrInvalidToken, err)
	}

	if !token.Valid {
		return nil, fmt.Errorf(ErrTokenNotValid)
	}

	claims, ok := token.Claims.(*STSSessionClaims)
	if !ok {
		return nil, fmt.Errorf(ErrInvalidTokenClaims)
	}

	// Validate issuer
	if claims.Issuer != t.issuer {
		return nil, fmt.Errorf(ErrInvalidIssuer)
	}

	// Validate that required fields are present
	if claims.SessionId == "" {
		return nil, fmt.Errorf(ErrMissingSessionID)
	}

	// Additional validation using the claims' own validation method
	if !claims.IsValid() {
		return nil, fmt.Errorf(ErrTokenNotValid)
	}

	return claims, nil
}

// SessionTokenClaims represents parsed session token claims
type SessionTokenClaims struct {
	SessionId string
	ExpiresAt time.Time
	IssuedAt  time.Time
}

// CredentialGenerator generates AWS-compatible temporary credentials
type CredentialGenerator struct{}

// NewCredentialGenerator creates a new credential generator
func NewCredentialGenerator() *CredentialGenerator {
	return &CredentialGenerator{}
}

// GenerateTemporaryCredentials creates temporary AWS credentials
func (c *CredentialGenerator) GenerateTemporaryCredentials(sessionId string, expiration time.Time) (*Credentials, error) {
	accessKeyId, err := c.generateAccessKeyId(sessionId)
	if err != nil {
		return nil, fmt.Errorf("failed to generate access key ID: %w", err)
	}

	secretAccessKey, err := c.generateSecretAccessKey()
	if err != nil {
		return nil, fmt.Errorf("failed to generate secret access key: %w", err)
	}

	sessionToken, err := c.generateSessionTokenId(sessionId)
	if err != nil {
		return nil, fmt.Errorf("failed to generate session token: %w", err)
	}

	return &Credentials{
		AccessKeyId:     accessKeyId,
		SecretAccessKey: secretAccessKey,
		SessionToken:    sessionToken,
		Expiration:      expiration,
	}, nil
}

// generateAccessKeyId generates an AWS-style access key ID
func (c *CredentialGenerator) generateAccessKeyId(sessionId string) (string, error) {
	// Create a deterministic but unique access key ID based on session
	hash := sha256.Sum256([]byte("access-key:" + sessionId))
	return "AKIA" + hex.EncodeToString(hash[:8]), nil // AWS format: AKIA + 16 chars
}

// generateSecretAccessKey generates a random secret access key
func (c *CredentialGenerator) generateSecretAccessKey() (string, error) {
	// Generate 32 random bytes for secret key
	secretBytes := make([]byte, 32)
	_, err := rand.Read(secretBytes)
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(secretBytes), nil
}

// generateSessionTokenId generates a session token identifier
func (c *CredentialGenerator) generateSessionTokenId(sessionId string) (string, error) {
	// Create session token with session ID embedded
	hash := sha256.Sum256([]byte("session-token:" + sessionId))
	return "ST" + hex.EncodeToString(hash[:16]), nil // Custom format
}

// generateSessionId generates a unique session ID
func GenerateSessionId() (string, error) {
	randomBytes := make([]byte, 16)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(randomBytes), nil
}

// generateAssumedRoleArn generates the ARN for an assumed role user
func GenerateAssumedRoleArn(roleArn, sessionName string) string {
	// Convert role ARN to assumed role user ARN
	// arn:seaweed:iam::role/RoleName -> arn:seaweed:sts::assumed-role/RoleName/SessionName
	roleName := utils.ExtractRoleNameFromArn(roleArn)
	if roleName == "" {
		// This should not happen if validation is done properly upstream
		return fmt.Sprintf("arn:seaweed:sts::assumed-role/INVALID-ARN/%s", sessionName)
	}
	return fmt.Sprintf("arn:seaweed:sts::assumed-role/%s/%s", roleName, sessionName)
}

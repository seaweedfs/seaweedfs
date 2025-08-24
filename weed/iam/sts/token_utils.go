package sts

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
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

// GenerateSessionToken creates a signed JWT session token
func (t *TokenGenerator) GenerateSessionToken(sessionId string, expiresAt time.Time) (string, error) {
	claims := jwt.MapClaims{
		"iss":        t.issuer,
		"sub":        sessionId,
		"iat":        time.Now().Unix(),
		"exp":        expiresAt.Unix(),
		"token_type": "session",
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
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	if !token.Valid {
		return nil, fmt.Errorf("token is not valid")
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("invalid token claims")
	}

	// Verify issuer
	if iss, ok := claims["iss"].(string); !ok || iss != t.issuer {
		return nil, fmt.Errorf("invalid issuer")
	}

	// Extract session ID
	sessionId, ok := claims["sub"].(string)
	if !ok {
		return nil, fmt.Errorf("missing session ID")
	}

	return &SessionTokenClaims{
		SessionId: sessionId,
		ExpiresAt: time.Unix(int64(claims["exp"].(float64)), 0),
		IssuedAt:  time.Unix(int64(claims["iat"].(float64)), 0),
	}, nil
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
	return fmt.Sprintf("arn:seaweed:sts::assumed-role/%s/%s", extractRoleNameFromArn(roleArn), sessionName)
}

// extractRoleNameFromArn extracts the role name from a role ARN
func extractRoleNameFromArn(roleArn string) string {
	// Simple extraction for arn:seaweed:iam::role/RoleName
	prefix := "arn:seaweed:iam::role/"
	if len(roleArn) > len(prefix) && roleArn[:len(prefix)] == prefix {
		return roleArn[len(prefix):]
	}
	return "UnknownRole"
}

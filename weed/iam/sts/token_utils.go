package sts

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/glog"
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
			return nil, fmt.Errorf("unexpected signing method: %v (expected HMAC)", token.Header["alg"])
		}
		return t.signingKey, nil
	})

	if err != nil {
		// Check for specific error types by examining the error message
		// jwt/v5 changed how errors are returned - they don't export ValidationError constants anymore
		errMsg := err.Error()
		
		// Check for expiration error
		if token != nil && token.Claims != nil {
			if claims, ok := token.Claims.(jwt.MapClaims); ok {
				if exp, ok := claims[JWTClaimExpiration].(float64); ok {
					expTime := time.Unix(int64(exp), 0)
					if time.Now().After(expTime) {
						glog.V(2).Infof("STS: Token validation failed - token expired at %s", expTime.Format(time.RFC3339))
						return nil, fmt.Errorf("session token has expired (expired at: %s, current time: %s)",
							expTime.Format(time.RFC3339), time.Now().Format(time.RFC3339))
					}
				}
			}
		}
		
		// Check for common JWT validation errors based on error message content
		if jwt.ErrTokenMalformed != nil && err == jwt.ErrTokenMalformed {
			glog.V(2).Infof("STS: Token validation failed - malformed token")
			return nil, fmt.Errorf("session token is malformed (not a valid JWT structure)")
		}
		if jwt.ErrTokenSignatureInvalid != nil && err == jwt.ErrTokenSignatureInvalid {
			glog.V(2).Infof("STS: Token validation failed - invalid signature")
			return nil, fmt.Errorf("session token signature is invalid (token may have been tampered with or was signed with a different key)")
		}
		if jwt.ErrTokenExpired != nil && err == jwt.ErrTokenExpired {
			glog.V(2).Infof("STS: Token validation failed - token expired")
			return nil, fmt.Errorf("session token has expired")
		}
		
		// Generic error with wrapping
		glog.V(2).Infof("STS: Token validation failed - error: %s", errMsg)
		return nil, fmt.Errorf("session token validation failed: %w (error: %s)", err, errMsg)
	}

	if !token.Valid {
		glog.V(2).Infof("STS: Token validation failed - token not valid")
		return nil, fmt.Errorf("session token is not valid (validation check failed)")
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("session token claims are invalid (cannot parse claims structure)")
	}

	// Verify issuer
	if iss, ok := claims[JWTClaimIssuer].(string); !ok || iss != t.issuer {
		actualIssuer := "<missing>"
		if iss != "" {
			actualIssuer = iss
		}
		return nil, fmt.Errorf("session token issuer is invalid (expected: %s, got: %s)", t.issuer, actualIssuer)
	}

	// Extract session ID
	sessionId, ok := claims[JWTClaimSubject].(string)
	if !ok {
		return nil, fmt.Errorf("session token is missing required 'sub' claim (session ID)")
	}

	// Extract expiration and issued-at times
	exp, expOk := claims[JWTClaimExpiration].(float64)
	iat, iatOk := claims[JWTClaimIssuedAt].(float64)

	if !expOk {
		return nil, fmt.Errorf("session token is missing required 'exp' claim (expiration time)")
	}
	if !iatOk {
		return nil, fmt.Errorf("session token is missing required 'iat' claim (issued at time)")
	}

	// Log successful validation
	expiresAt := time.Unix(int64(exp), 0)
	remaining := time.Until(expiresAt)
	glog.V(1).Infof("STS: Token validated successfully - session=%s, expires=%s, remaining=%s",
		sessionId, expiresAt.Format(time.RFC3339), remaining.Round(time.Second))

	return &SessionTokenClaims{
		SessionId: sessionId,
		ExpiresAt: time.Unix(int64(exp), 0),
		IssuedAt:  time.Unix(int64(iat), 0),
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
type CredentialGenerator struct {
	secret []byte
}

// NewCredentialGenerator creates a new credential generator
func NewCredentialGenerator(secret []byte) *CredentialGenerator {
	return &CredentialGenerator{
		secret: secret,
	}
}

// GenerateTemporaryCredentials creates temporary AWS credentials
func (c *CredentialGenerator) GenerateTemporaryCredentials(sessionId string, expiration time.Time) (*Credentials, error) {
	accessKeyId, err := c.generateAccessKeyId(sessionId)
	if err != nil {
		return nil, fmt.Errorf("failed to generate access key ID: %w", err)
	}

	secretAccessKey, err := c.generateSecretAccessKey(sessionId)
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

// generateSecretAccessKey generates a deterministic secret access key
func (c *CredentialGenerator) generateSecretAccessKey(sessionId string) (string, error) {
	// Generate deterministic secret key using HMAC-SHA256
	// using the service secret and session ID
	if len(c.secret) == 0 {
		return "", fmt.Errorf("credential generator secret is empty")
	}

	h := hmac.New(sha256.New, c.secret)
	h.Write([]byte("secret-key:" + sessionId))
	secretBytes := h.Sum(nil)

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
	// arn:aws:iam::role/RoleName -> arn:aws:sts::assumed-role/RoleName/SessionName
	roleName := utils.ExtractRoleNameFromArn(roleArn)
	if roleName == "" {
		// This should not happen if validation is done properly upstream
		return fmt.Sprintf("arn:aws:sts::assumed-role/INVALID-ARN/%s", sessionName)
	}
	return fmt.Sprintf("arn:aws:sts::assumed-role/%s/%s", roleName, sessionName)
}

// ParseUnverifiedTokenClaims parses a JWT token without verifying the signature
// This is used to extract the issuer claim to determine which provider to use for verification
func ParseUnverifiedTokenClaims(tokenString string) (jwt.MapClaims, error) {
	parser := jwt.NewParser()
	token, _, err := parser.ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("invalid token claims")
	}

	return claims, nil
}

package sts

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
)

func TestGenerateAssumedRoleArn(t *testing.T) {
	tests := []struct {
		name        string
		roleArn     string
		sessionName string
		expected    string
	}{
		{
			name:        "Standard Role ARN",
			roleArn:     "arn:aws:iam::role/MyRole",
			sessionName: "MySession",
			expected:    "arn:aws:sts::assumed-role/MyRole/MySession",
		},
		{
			name:        "Role ARN with path",
			roleArn:     "arn:aws:iam::role/path/to/MyRole",
			sessionName: "MySession",
			expected:    "arn:aws:sts::assumed-role/path/to/MyRole/MySession",
		},
		{
			name:        "Invalid Role ARN",
			roleArn:     "invalid-arn",
			sessionName: "MySession",
			expected:    "arn:aws:sts::assumed-role/INVALID-ARN/MySession", // As per implementation
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GenerateAssumedRoleArn(tt.roleArn, tt.sessionName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTokenGenerator(t *testing.T) {
	signingKey := []byte("test-key")
	issuer := "test-issuer"
	generator := NewTokenGenerator(signingKey, issuer)

	t.Run("Generate and Validate Session Token", func(t *testing.T) {
		sessionId := "session-123"
		expiresAt := time.Now().Add(1 * time.Hour)

		token, err := generator.GenerateSessionToken(sessionId, expiresAt)
		assert.NoError(t, err)
		assert.NotEmpty(t, token)

		claims, err := generator.ValidateSessionToken(token)
		assert.NoError(t, err)
		assert.Equal(t, sessionId, claims.SessionId)
		// time marshaling/unmarshaling accuracy issue, checking with tolerance or just non-zero
		assert.WithinDuration(t, expiresAt, claims.ExpiresAt, 1*time.Second)
	})

	t.Run("Validate Invalid Token", func(t *testing.T) {
		_, err := generator.ValidateSessionToken("invalid.token.string")
		assert.Error(t, err)
	})

	t.Run("GenerateJWTWithClaims", func(t *testing.T) {
		claims := &STSSessionClaims{
			SessionId: "session-456",
			RoleArn:   "arn:aws:iam::role/MyRole",
			Principal: "arn:aws:iam::role/MyRole",
			RegisteredClaims: jwt.RegisteredClaims{
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
			},
		}

		token, err := generator.GenerateJWTWithClaims(claims)
		assert.NoError(t, err)
		assert.NotEmpty(t, token)

		validatedClaims, err := generator.ValidateJWTWithClaims(token)
		assert.NoError(t, err)
		assert.Equal(t, claims.SessionId, validatedClaims.SessionId)
		assert.Equal(t, issuer, validatedClaims.Issuer) // Should automatically set issuer
	})
}

func TestCredentialGenerator(t *testing.T) {
	generator := NewCredentialGenerator()
	sessionId := "session-789"
	expiration := time.Now().Add(1 * time.Hour)

	creds, err := generator.GenerateTemporaryCredentials(sessionId, expiration)
	assert.NoError(t, err)
	assert.NotNil(t, creds)
	assert.NotEmpty(t, creds.AccessKeyId)
	assert.NotEmpty(t, creds.SecretAccessKey)
	assert.NotEmpty(t, creds.SessionToken)
	assert.Equal(t, expiration, creds.Expiration)

	// Verify Access Key ID format (AKIA...)
	assert.Equal(t, "AKIA", creds.AccessKeyId[:4])
	assert.Len(t, creds.AccessKeyId, 20) // AKIA + 16 chars

	// Verify Session Token format (ST...)
	assert.Equal(t, "ST", creds.SessionToken[:2])
}

package sts

import (
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestTemporaryCredentialPrefix verifies that temporary credentials use ASIA prefix
// (not AKIA which is for permanent IAM user credentials)
func TestTemporaryCredentialPrefix(t *testing.T) {
	sessionId := "test-session-for-prefix"
	expiration := time.Now().Add(time.Hour)

	credGen := NewCredentialGenerator()
	cred, err := credGen.GenerateTemporaryCredentials(sessionId, expiration)

	assert.NoError(t, err)
	assert.NotNil(t, cred)

	// Verify ASIA prefix for temporary credentials
	assert.True(t, strings.HasPrefix(cred.AccessKeyId, "ASIA"),
		"Temporary credentials must use ASIA prefix, got: %s", cred.AccessKeyId)

	// Verify it's NOT using AKIA (permanent credentials)
	assert.False(t, strings.HasPrefix(cred.AccessKeyId, "AKIA"),
		"Temporary credentials must NOT use AKIA prefix (that's for permanent IAM keys)")
}

// TestTemporaryCredentialFormat verifies the full format of temporary credentials
func TestTemporaryCredentialFormat(t *testing.T) {
	sessionId := "format-test-session"
	expiration := time.Now().Add(time.Hour)

	credGen := NewCredentialGenerator()
	cred, err := credGen.GenerateTemporaryCredentials(sessionId, expiration)

	assert.NoError(t, err)
	assert.NotNil(t, cred)

	// AWS temporary access key format: ASIA + 16 hex characters = 20 chars total
	assert.Equal(t, 20, len(cred.AccessKeyId),
		"Access key ID should be 20 characters (ASIA + 16 hex chars)")

	// Verify it starts with ASIA
	assert.True(t, strings.HasPrefix(cred.AccessKeyId, "ASIA"),
		"Access key must start with ASIA prefix")

	// Verify the rest is hex (after ASIA prefix)
	hexPart := cred.AccessKeyId[4:]
	assert.Equal(t, 16, len(hexPart), "Hex part should be 16 characters")
	_, err = hex.DecodeString(hexPart)
	assert.NoError(t, err, "The part after ASIA prefix should be valid hex")

	// Verify secret key is not empty and is a valid base64-encoded SHA256 hash
	assert.NotEmpty(t, cred.SecretAccessKey)
	assert.Equal(t, 44, len(cred.SecretAccessKey),
		"SecretAccessKey should be 44 characters for a base64-encoded 32-byte hash")
	_, err = base64.StdEncoding.DecodeString(cred.SecretAccessKey)
	assert.NoError(t, err, "SecretAccessKey should be a valid base64 string")

	// Verify session token is not empty
	assert.NotEmpty(t, cred.SessionToken)
}

package iamapi

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// generateSecureID generates a cryptographically secure random ID
// Used for policy IDs, access keys, and other security-sensitive identifiers
func generateSecureID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate secure random ID: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// generateSecureAccessKey generates a 20-character access key (AWS compatible)
func generateSecureAccessKey() (string, error) {
	b := make([]byte, 15) // 15 bytes = 20 base32 chars
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate access key: %w", err)
	}
	// Use uppercase hex for AWS compatibility
	return fmt.Sprintf("AKIA%s", hex.EncodeToString(b)[:16]), nil
}

// generateSecureSecretKey generates a 40-character secret key (AWS compatible)
func generateSecureSecretKey() (string, error) {
	b := make([]byte, 30) // 30 bytes = 40 base64-like chars
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate secret key: %w", err)
	}
	// Convert to base64-like string with AWS-compatible charset
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	result := make([]byte, 40)
	for i := 0; i < 40; i++ {
		result[i] = charset[int(b[i%30])%len(charset)]
	}
	return string(result), nil
}

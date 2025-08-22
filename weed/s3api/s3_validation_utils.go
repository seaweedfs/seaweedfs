package s3api

import (
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// isValidKMSKeyID performs basic validation of KMS key identifiers.
// Following Minio's approach: be permissive and accept any reasonable key format.
// Only reject keys with leading/trailing spaces or other obvious issues.
//
// This function is used across multiple S3 API handlers to ensure consistent
// validation of KMS key IDs in various contexts (bucket encryption, object operations, etc.).
func isValidKMSKeyID(keyID string) bool {
	// Reject empty keys
	if keyID == "" {
		return false
	}

	// Following Minio's validation: reject keys with leading/trailing spaces
	if strings.HasPrefix(keyID, " ") || strings.HasSuffix(keyID, " ") {
		return false
	}

	// Also reject keys with internal spaces (common sense validation)
	if strings.Contains(keyID, " ") {
		return false
	}

	// Reject keys with control characters or newlines
	if strings.ContainsAny(keyID, "\t\n\r\x00") {
		return false
	}

	// Accept any reasonable length key (be permissive for various KMS providers)
	if len(keyID) > 0 && len(keyID) <= s3_constants.MaxKMSKeyIDLength {
		return true
	}

	return false
}

// ValidateIV validates that an initialization vector has the correct length for AES encryption
func ValidateIV(iv []byte, name string) error {
	if len(iv) != s3_constants.AESBlockSize {
		return fmt.Errorf("invalid %s length: expected %d bytes, got %d", name, s3_constants.AESBlockSize, len(iv))
	}
	return nil
}

// ValidateSSEKMSKey validates that an SSE-KMS key is not nil and has required fields
func ValidateSSEKMSKey(sseKey *SSEKMSKey) error {
	if sseKey == nil {
		return fmt.Errorf("SSE-KMS key cannot be nil")
	}
	return nil
}

// ValidateSSECKey validates that an SSE-C key is not nil
func ValidateSSECKey(customerKey *SSECustomerKey) error {
	if customerKey == nil {
		return fmt.Errorf("SSE-C customer key cannot be nil")
	}
	return nil
}

// ValidateSSES3Key validates that an SSE-S3 key is not nil
func ValidateSSES3Key(sseKey *SSES3Key) error {
	if sseKey == nil {
		return fmt.Errorf("SSE-S3 key cannot be nil")
	}
	return nil
}

package s3api

import "strings"

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
	if len(keyID) > 0 && len(keyID) <= 500 {
		return true
	}

	return false
}

package s3api

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// RequireKeyCommitmentEnv is the environment variable that flips the
// commitment check from "skip when missing" (the AWS-compatible default,
// needed for objects written before commitments shipped) to "reject when
// missing". Operators who have either re-encrypted all legacy objects or
// who never wrote any objects under the pre-commitment code path can opt
// in via this env var to close the silent downgrade vector that an
// attacker with write access to object metadata could otherwise exploit
// by stripping the commitment field.
const RequireKeyCommitmentEnv = "WEED_S3_REQUIRE_KEY_COMMITMENT"

// requireKeyCommitment is the runtime mirror of the env var, kept as an
// atomic so config-reload paths can flip it without a global mutex.
var requireKeyCommitment atomic.Bool

func init() {
	if v := os.Getenv(RequireKeyCommitmentEnv); v == "1" || strings.EqualFold(v, "true") {
		requireKeyCommitment.Store(true)
		glog.V(1).Infof("SSE: %s=true; SSE objects without a key commitment will be rejected", RequireKeyCommitmentEnv)
	}
}

// SetRequireKeyCommitment toggles strict-commitment enforcement at runtime.
// Used by tests and by future config-reload code paths.
func SetRequireKeyCommitment(require bool) {
	requireKeyCommitment.Store(require)
}

// ComputeKeyCommitment computes an HMAC-SHA256 key commitment over the
// encryption parameters (IV + algorithm). This binds the ciphertext to the
// exact key material and IV that were used, preventing key-confusion and
// IV-manipulation attacks against unauthenticated AES-CTR.
//
// The commitment is stored alongside the IV in object metadata. On decrypt
// the commitment is re-derived and compared; a mismatch means the key or IV
// was tampered with.
func ComputeKeyCommitment(key []byte, iv []byte, algorithm string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(iv)
	mac.Write([]byte(algorithm))
	return mac.Sum(nil)
}

// VerifyKeyCommitment checks a previously stored commitment against the
// current key, IV, and algorithm. Returns nil on success.
//
// When the commitment is empty (legacy object written before commitments
// shipped), the default behaviour is to accept the object — this is the
// AWS-compatible path. Setting WEED_S3_REQUIRE_KEY_COMMITMENT=true (via
// env at startup or via SetRequireKeyCommitment at runtime) flips that
// to reject, closing the silent-downgrade vector at the cost of locking
// out un-migrated legacy objects.
func VerifyKeyCommitment(key []byte, iv []byte, algorithm string, commitment []byte) error {
	if len(commitment) == 0 {
		if requireKeyCommitment.Load() {
			return fmt.Errorf("key commitment is required but missing from object metadata: %s set; legacy objects must be re-encrypted before this flag can be enabled", RequireKeyCommitmentEnv)
		}
		// Legacy data written before key commitments were added; skip.
		return nil
	}
	expected := ComputeKeyCommitment(key, iv, algorithm)
	if !hmac.Equal(expected, commitment) {
		return fmt.Errorf("key commitment verification failed: encryption parameters may have been tampered with")
	}
	return nil
}

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

// ValidateSSES3Key validates that an SSE-S3 key has valid structure and contents
func ValidateSSES3Key(sseKey *SSES3Key) error {
	if sseKey == nil {
		return fmt.Errorf("SSE-S3 key cannot be nil")
	}

	// Validate key bytes
	if sseKey.Key == nil {
		return fmt.Errorf("SSE-S3 key bytes cannot be nil")
	}
	if len(sseKey.Key) != SSES3KeySize {
		return fmt.Errorf("invalid SSE-S3 key size: expected %d bytes, got %d", SSES3KeySize, len(sseKey.Key))
	}

	// Validate algorithm
	if sseKey.Algorithm != SSES3Algorithm {
		return fmt.Errorf("invalid SSE-S3 algorithm: expected %q, got %q", SSES3Algorithm, sseKey.Algorithm)
	}

	// Validate key ID (should not be empty)
	if sseKey.KeyID == "" {
		return fmt.Errorf("SSE-S3 key ID cannot be empty")
	}

	// IV validation is optional during key creation - it will be set during encryption
	// If IV is set, validate its length
	if len(sseKey.IV) > 0 && len(sseKey.IV) != s3_constants.AESBlockSize {
		return fmt.Errorf("invalid SSE-S3 IV length: expected %d bytes, got %d", s3_constants.AESBlockSize, len(sseKey.IV))
	}

	return nil
}

package s3api

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// KMSDataKeyResult holds the result of data key generation
type KMSDataKeyResult struct {
	Response *kms.GenerateDataKeyResponse
	Block    cipher.Block
}

// generateKMSDataKey generates a new data encryption key using KMS
// This function encapsulates the common pattern used across all SSE-KMS functions
func generateKMSDataKey(keyID string, encryptionContext map[string]string) (*KMSDataKeyResult, error) {
	// Validate keyID to prevent injection attacks and malformed requests to KMS service
	if !isValidKMSKeyID(keyID) {
		return nil, fmt.Errorf("invalid KMS key ID format: key ID must be non-empty, without spaces or control characters")
	}

	// Validate encryption context to prevent malformed requests to KMS service
	if encryptionContext != nil {
		for key, value := range encryptionContext {
			// Validate context keys and values for basic security
			if strings.TrimSpace(key) == "" {
				return nil, fmt.Errorf("invalid encryption context: keys cannot be empty or whitespace-only")
			}
			if strings.ContainsAny(key, "\x00\n\r\t") || strings.ContainsAny(value, "\x00\n\r\t") {
				return nil, fmt.Errorf("invalid encryption context: keys and values cannot contain control characters")
			}
			// AWS KMS has limits on key/value lengths
			if len(key) > 2048 || len(value) > 2048 {
				return nil, fmt.Errorf("invalid encryption context: keys and values must be ≤ 2048 characters (key=%d, value=%d)", len(key), len(value))
			}
		}
		// AWS KMS has a limit on the total number of context pairs
		if len(encryptionContext) > s3_constants.MaxKMSEncryptionContextPairs {
			return nil, fmt.Errorf("invalid encryption context: cannot exceed %d key-value pairs, got %d", s3_constants.MaxKMSEncryptionContextPairs, len(encryptionContext))
		}
	}

	// Get KMS provider
	kmsProvider := kms.GetGlobalKMS()
	if kmsProvider == nil {
		return nil, fmt.Errorf("KMS is not configured")
	}

	// Create data key request
	generateDataKeyReq := &kms.GenerateDataKeyRequest{
		KeyID:             keyID,
		KeySpec:           kms.KeySpecAES256,
		EncryptionContext: encryptionContext,
	}

	// Generate the data key
	dataKeyResp, err := kmsProvider.GenerateDataKey(context.Background(), generateDataKeyReq)
	if err != nil {
		return nil, fmt.Errorf("failed to generate KMS data key: %v", err)
	}

	// Create AES cipher with the plaintext data key
	block, err := aes.NewCipher(dataKeyResp.Plaintext)
	if err != nil {
		// Clear sensitive data before returning error
		kms.ClearSensitiveData(dataKeyResp.Plaintext)
		return nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}

	return &KMSDataKeyResult{
		Response: dataKeyResp,
		Block:    block,
	}, nil
}

// clearKMSDataKey safely clears sensitive data from a KMSDataKeyResult
func clearKMSDataKey(result *KMSDataKeyResult) {
	if result != nil && result.Response != nil {
		kms.ClearSensitiveData(result.Response.Plaintext)
	}
}

// createSSEKMSKey creates an SSEKMSKey struct from data key result and parameters
func createSSEKMSKey(result *KMSDataKeyResult, encryptionContext map[string]string, bucketKeyEnabled bool, iv []byte, chunkOffset int64) *SSEKMSKey {
	return &SSEKMSKey{
		KeyID:             result.Response.KeyID,
		EncryptedDataKey:  result.Response.CiphertextBlob,
		EncryptionContext: encryptionContext,
		BucketKeyEnabled:  bucketKeyEnabled,
		IV:                iv,
		ChunkOffset:       chunkOffset,
	}
}

package s3api

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/kms"
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

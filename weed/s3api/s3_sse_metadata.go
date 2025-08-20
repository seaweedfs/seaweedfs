package s3api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// SSE metadata keys for storing encryption information in entry metadata
const (
	// MetaSSEIV is the initialization vector used for encryption
	MetaSSEIV = "X-SeaweedFS-Server-Side-Encryption-Iv"

	// MetaSSEAlgorithm is the encryption algorithm used
	MetaSSEAlgorithm = "X-SeaweedFS-Server-Side-Encryption-Algorithm"

	// MetaSSECKeyMD5 is the MD5 hash of the SSE-C customer key
	MetaSSECKeyMD5 = "X-SeaweedFS-Server-Side-Encryption-Customer-Key-MD5"

	// MetaSSEKMSKeyID is the KMS key ID used for encryption
	MetaSSEKMSKeyID = "X-SeaweedFS-Server-Side-Encryption-KMS-Key-Id"

	// MetaSSEKMSEncryptedKey is the encrypted data key from KMS
	MetaSSEKMSEncryptedKey = "X-SeaweedFS-Server-Side-Encryption-KMS-Encrypted-Key"

	// MetaSSEKMSContext is the encryption context for KMS
	MetaSSEKMSContext = "X-SeaweedFS-Server-Side-Encryption-KMS-Context"

	// MetaSSES3KeyID is the key ID for SSE-S3 encryption
	MetaSSES3KeyID = "X-SeaweedFS-Server-Side-Encryption-S3-Key-Id"
)

// StoreIVInMetadata stores the IV in entry metadata as base64 encoded string
func StoreIVInMetadata(metadata map[string][]byte, iv []byte) {
	if len(iv) > 0 {
		metadata[MetaSSEIV] = []byte(base64.StdEncoding.EncodeToString(iv))
	}
}

// GetIVFromMetadata retrieves the IV from entry metadata
func GetIVFromMetadata(metadata map[string][]byte) ([]byte, error) {
	if ivBase64, exists := metadata[MetaSSEIV]; exists {
		iv, err := base64.StdEncoding.DecodeString(string(ivBase64))
		if err != nil {
			return nil, fmt.Errorf("failed to decode IV from metadata: %w", err)
		}
		return iv, nil
	}
	return nil, fmt.Errorf("IV not found in metadata")
}

// StoreSSECMetadata stores SSE-C related metadata
func StoreSSECMetadata(metadata map[string][]byte, iv []byte, keyMD5 string) {
	StoreIVInMetadata(metadata, iv)
	metadata[MetaSSEAlgorithm] = []byte("AES256")
	if keyMD5 != "" {
		metadata[MetaSSECKeyMD5] = []byte(keyMD5)
	}
}

// StoreSSEKMSMetadata stores SSE-KMS related metadata
func StoreSSEKMSMetadata(metadata map[string][]byte, iv []byte, keyID string, encryptedKey []byte, context map[string]string) {
	StoreIVInMetadata(metadata, iv)
	metadata[MetaSSEAlgorithm] = []byte("aws:kms")
	if keyID != "" {
		metadata[MetaSSEKMSKeyID] = []byte(keyID)
	}
	if len(encryptedKey) > 0 {
		metadata[MetaSSEKMSEncryptedKey] = []byte(base64.StdEncoding.EncodeToString(encryptedKey))
	}
	if len(context) > 0 {
		// Marshal context to JSON to handle special characters correctly
		contextBytes, err := json.Marshal(context)
		if err == nil {
			metadata[MetaSSEKMSContext] = contextBytes
		}
		// Note: json.Marshal for map[string]string should never fail, but we handle it gracefully
	}
}

// StoreSSES3Metadata stores SSE-S3 related metadata
func StoreSSES3Metadata(metadata map[string][]byte, iv []byte, keyID string) {
	StoreIVInMetadata(metadata, iv)
	metadata[MetaSSEAlgorithm] = []byte("AES256")
	if keyID != "" {
		metadata[MetaSSES3KeyID] = []byte(keyID)
	}
}

// GetSSECMetadata retrieves SSE-C metadata
func GetSSECMetadata(metadata map[string][]byte) (iv []byte, keyMD5 string, err error) {
	iv, err = GetIVFromMetadata(metadata)
	if err != nil {
		return nil, "", err
	}

	if keyMD5Bytes, exists := metadata[MetaSSECKeyMD5]; exists {
		keyMD5 = string(keyMD5Bytes)
	}

	return iv, keyMD5, nil
}

// GetSSEKMSMetadata retrieves SSE-KMS metadata
func GetSSEKMSMetadata(metadata map[string][]byte) (iv []byte, keyID string, encryptedKey []byte, context map[string]string, err error) {
	iv, err = GetIVFromMetadata(metadata)
	if err != nil {
		return nil, "", nil, nil, err
	}

	if keyIDBytes, exists := metadata[MetaSSEKMSKeyID]; exists {
		keyID = string(keyIDBytes)
	}

	if encKeyBase64, exists := metadata[MetaSSEKMSEncryptedKey]; exists {
		encryptedKey, err = base64.StdEncoding.DecodeString(string(encKeyBase64))
		if err != nil {
			return nil, "", nil, nil, fmt.Errorf("failed to decode encrypted key: %w", err)
		}
	}

	// Parse context from JSON
	if contextBytes, exists := metadata[MetaSSEKMSContext]; exists {
		context = make(map[string]string)
		if err := json.Unmarshal(contextBytes, &context); err != nil {
			return nil, "", nil, nil, fmt.Errorf("failed to parse KMS context JSON: %w", err)
		}
	}

	return iv, keyID, encryptedKey, context, nil
}

// GetSSES3Metadata retrieves SSE-S3 metadata
func GetSSES3Metadata(metadata map[string][]byte) (iv []byte, keyID string, err error) {
	iv, err = GetIVFromMetadata(metadata)
	if err != nil {
		return nil, "", err
	}

	if keyIDBytes, exists := metadata[MetaSSES3KeyID]; exists {
		keyID = string(keyIDBytes)
	}

	return iv, keyID, nil
}

// IsSSEEncrypted checks if the metadata indicates any form of SSE encryption
func IsSSEEncrypted(metadata map[string][]byte) bool {
	_, exists := metadata[MetaSSEIV]
	return exists
}

// GetSSEAlgorithm returns the SSE algorithm from metadata
func GetSSEAlgorithm(metadata map[string][]byte) string {
	if alg, exists := metadata[MetaSSEAlgorithm]; exists {
		return string(alg)
	}
	return ""
}

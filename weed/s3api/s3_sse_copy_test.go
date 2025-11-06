package s3api

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestSSECObjectCopy tests copying SSE-C encrypted objects with different keys
func TestSSECObjectCopy(t *testing.T) {
	// Original key for source object
	sourceKey := GenerateTestSSECKey(1)
	sourceCustomerKey := &SSECustomerKey{
		Algorithm: "AES256",
		Key:       sourceKey.Key,
		KeyMD5:    sourceKey.KeyMD5,
	}

	// Destination key for target object
	destKey := GenerateTestSSECKey(2)
	destCustomerKey := &SSECustomerKey{
		Algorithm: "AES256",
		Key:       destKey.Key,
		KeyMD5:    destKey.KeyMD5,
	}

	testData := "Hello, SSE-C copy world!"

	// Encrypt with source key
	encryptedReader, iv, err := CreateSSECEncryptedReader(strings.NewReader(testData), sourceCustomerKey)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader: %v", err)
	}

	encryptedData, err := io.ReadAll(encryptedReader)
	if err != nil {
		t.Fatalf("Failed to read encrypted data: %v", err)
	}

	// Test copy strategy determination
	sourceMetadata := make(map[string][]byte)
	StoreSSECIVInMetadata(sourceMetadata, iv)
	sourceMetadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] = []byte("AES256")
	sourceMetadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5] = []byte(sourceKey.KeyMD5)

	t.Run("Same key copy (direct copy)", func(t *testing.T) {
		strategy, err := DetermineSSECCopyStrategy(sourceMetadata, sourceCustomerKey, sourceCustomerKey)
		if err != nil {
			t.Fatalf("Failed to determine copy strategy: %v", err)
		}

		if strategy != SSECCopyStrategyDirect {
			t.Errorf("Expected direct copy strategy for same key, got %v", strategy)
		}
	})

	t.Run("Different key copy (decrypt-encrypt)", func(t *testing.T) {
		strategy, err := DetermineSSECCopyStrategy(sourceMetadata, sourceCustomerKey, destCustomerKey)
		if err != nil {
			t.Fatalf("Failed to determine copy strategy: %v", err)
		}

		if strategy != SSECCopyStrategyDecryptEncrypt {
			t.Errorf("Expected decrypt-encrypt copy strategy for different keys, got %v", strategy)
		}
	})

	t.Run("Can direct copy check", func(t *testing.T) {
		// Same key should allow direct copy
		canDirect := CanDirectCopySSEC(sourceMetadata, sourceCustomerKey, sourceCustomerKey)
		if !canDirect {
			t.Error("Should allow direct copy with same key")
		}

		// Different key should not allow direct copy
		canDirect = CanDirectCopySSEC(sourceMetadata, sourceCustomerKey, destCustomerKey)
		if canDirect {
			t.Error("Should not allow direct copy with different keys")
		}
	})

	// Test actual copy operation (decrypt with source key, encrypt with dest key)
	t.Run("Full copy operation", func(t *testing.T) {
		// Decrypt with source key
		decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(encryptedData), sourceCustomerKey, iv)
		if err != nil {
			t.Fatalf("Failed to create decrypted reader: %v", err)
		}

		// Re-encrypt with destination key
		reEncryptedReader, destIV, err := CreateSSECEncryptedReader(decryptedReader, destCustomerKey)
		if err != nil {
			t.Fatalf("Failed to create re-encrypted reader: %v", err)
		}

		reEncryptedData, err := io.ReadAll(reEncryptedReader)
		if err != nil {
			t.Fatalf("Failed to read re-encrypted data: %v", err)
		}

		// Verify we can decrypt with destination key
		finalDecryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(reEncryptedData), destCustomerKey, destIV)
		if err != nil {
			t.Fatalf("Failed to create final decrypted reader: %v", err)
		}

		finalData, err := io.ReadAll(finalDecryptedReader)
		if err != nil {
			t.Fatalf("Failed to read final decrypted data: %v", err)
		}

		if string(finalData) != testData {
			t.Errorf("Expected %s, got %s", testData, string(finalData))
		}
	})
}

// TestSSEKMSObjectCopy tests copying SSE-KMS encrypted objects
func TestSSEKMSObjectCopy(t *testing.T) {
	kmsKey := SetupTestKMS(t)
	defer kmsKey.Cleanup()

	testData := "Hello, SSE-KMS copy world!"
	encryptionContext := BuildEncryptionContext("test-bucket", "test-object", false)

	// Encrypt with SSE-KMS
	encryptedReader, sseKey, err := CreateSSEKMSEncryptedReader(strings.NewReader(testData), kmsKey.KeyID, encryptionContext)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader: %v", err)
	}

	encryptedData, err := io.ReadAll(encryptedReader)
	if err != nil {
		t.Fatalf("Failed to read encrypted data: %v", err)
	}

	t.Run("Same KMS key copy", func(t *testing.T) {
		// Decrypt with original key
		decryptedReader, err := CreateSSEKMSDecryptedReader(bytes.NewReader(encryptedData), sseKey)
		if err != nil {
			t.Fatalf("Failed to create decrypted reader: %v", err)
		}

		// Re-encrypt with same KMS key
		reEncryptedReader, newSseKey, err := CreateSSEKMSEncryptedReader(decryptedReader, kmsKey.KeyID, encryptionContext)
		if err != nil {
			t.Fatalf("Failed to create re-encrypted reader: %v", err)
		}

		reEncryptedData, err := io.ReadAll(reEncryptedReader)
		if err != nil {
			t.Fatalf("Failed to read re-encrypted data: %v", err)
		}

		// Verify we can decrypt with new key
		finalDecryptedReader, err := CreateSSEKMSDecryptedReader(bytes.NewReader(reEncryptedData), newSseKey)
		if err != nil {
			t.Fatalf("Failed to create final decrypted reader: %v", err)
		}

		finalData, err := io.ReadAll(finalDecryptedReader)
		if err != nil {
			t.Fatalf("Failed to read final decrypted data: %v", err)
		}

		if string(finalData) != testData {
			t.Errorf("Expected %s, got %s", testData, string(finalData))
		}
	})
}

// TestSSECToSSEKMSCopy tests cross-encryption copy (SSE-C to SSE-KMS)
func TestSSECToSSEKMSCopy(t *testing.T) {
	// Setup SSE-C key
	ssecKey := GenerateTestSSECKey(1)
	ssecCustomerKey := &SSECustomerKey{
		Algorithm: "AES256",
		Key:       ssecKey.Key,
		KeyMD5:    ssecKey.KeyMD5,
	}

	// Setup SSE-KMS
	kmsKey := SetupTestKMS(t)
	defer kmsKey.Cleanup()

	testData := "Hello, cross-encryption copy world!"

	// Encrypt with SSE-C
	encryptedReader, ssecIV, err := CreateSSECEncryptedReader(strings.NewReader(testData), ssecCustomerKey)
	if err != nil {
		t.Fatalf("Failed to create SSE-C encrypted reader: %v", err)
	}

	encryptedData, err := io.ReadAll(encryptedReader)
	if err != nil {
		t.Fatalf("Failed to read SSE-C encrypted data: %v", err)
	}

	// Decrypt SSE-C data
	decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(encryptedData), ssecCustomerKey, ssecIV)
	if err != nil {
		t.Fatalf("Failed to create SSE-C decrypted reader: %v", err)
	}

	// Re-encrypt with SSE-KMS
	encryptionContext := BuildEncryptionContext("test-bucket", "test-object", false)
	reEncryptedReader, sseKmsKey, err := CreateSSEKMSEncryptedReader(decryptedReader, kmsKey.KeyID, encryptionContext)
	if err != nil {
		t.Fatalf("Failed to create SSE-KMS encrypted reader: %v", err)
	}

	reEncryptedData, err := io.ReadAll(reEncryptedReader)
	if err != nil {
		t.Fatalf("Failed to read SSE-KMS encrypted data: %v", err)
	}

	// Decrypt with SSE-KMS
	finalDecryptedReader, err := CreateSSEKMSDecryptedReader(bytes.NewReader(reEncryptedData), sseKmsKey)
	if err != nil {
		t.Fatalf("Failed to create SSE-KMS decrypted reader: %v", err)
	}

	finalData, err := io.ReadAll(finalDecryptedReader)
	if err != nil {
		t.Fatalf("Failed to read final decrypted data: %v", err)
	}

	if string(finalData) != testData {
		t.Errorf("Expected %s, got %s", testData, string(finalData))
	}
}

// TestSSEKMSToSSECCopy tests cross-encryption copy (SSE-KMS to SSE-C)
func TestSSEKMSToSSECCopy(t *testing.T) {
	// Setup SSE-KMS
	kmsKey := SetupTestKMS(t)
	defer kmsKey.Cleanup()

	// Setup SSE-C key
	ssecKey := GenerateTestSSECKey(1)
	ssecCustomerKey := &SSECustomerKey{
		Algorithm: "AES256",
		Key:       ssecKey.Key,
		KeyMD5:    ssecKey.KeyMD5,
	}

	testData := "Hello, reverse cross-encryption copy world!"
	encryptionContext := BuildEncryptionContext("test-bucket", "test-object", false)

	// Encrypt with SSE-KMS
	encryptedReader, sseKmsKey, err := CreateSSEKMSEncryptedReader(strings.NewReader(testData), kmsKey.KeyID, encryptionContext)
	if err != nil {
		t.Fatalf("Failed to create SSE-KMS encrypted reader: %v", err)
	}

	encryptedData, err := io.ReadAll(encryptedReader)
	if err != nil {
		t.Fatalf("Failed to read SSE-KMS encrypted data: %v", err)
	}

	// Decrypt SSE-KMS data
	decryptedReader, err := CreateSSEKMSDecryptedReader(bytes.NewReader(encryptedData), sseKmsKey)
	if err != nil {
		t.Fatalf("Failed to create SSE-KMS decrypted reader: %v", err)
	}

	// Re-encrypt with SSE-C
	reEncryptedReader, reEncryptedIV, err := CreateSSECEncryptedReader(decryptedReader, ssecCustomerKey)
	if err != nil {
		t.Fatalf("Failed to create SSE-C encrypted reader: %v", err)
	}

	reEncryptedData, err := io.ReadAll(reEncryptedReader)
	if err != nil {
		t.Fatalf("Failed to read SSE-C encrypted data: %v", err)
	}

	// Decrypt with SSE-C
	finalDecryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(reEncryptedData), ssecCustomerKey, reEncryptedIV)
	if err != nil {
		t.Fatalf("Failed to create SSE-C decrypted reader: %v", err)
	}

	finalData, err := io.ReadAll(finalDecryptedReader)
	if err != nil {
		t.Fatalf("Failed to read final decrypted data: %v", err)
	}

	if string(finalData) != testData {
		t.Errorf("Expected %s, got %s", testData, string(finalData))
	}
}

// TestSSECopyWithCorruptedSource tests copy operations with corrupted source data
func TestSSECopyWithCorruptedSource(t *testing.T) {
	ssecKey := GenerateTestSSECKey(1)
	ssecCustomerKey := &SSECustomerKey{
		Algorithm: "AES256",
		Key:       ssecKey.Key,
		KeyMD5:    ssecKey.KeyMD5,
	}

	testData := "Hello, corruption test!"

	// Encrypt data
	encryptedReader, iv, err := CreateSSECEncryptedReader(strings.NewReader(testData), ssecCustomerKey)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader: %v", err)
	}

	encryptedData, err := io.ReadAll(encryptedReader)
	if err != nil {
		t.Fatalf("Failed to read encrypted data: %v", err)
	}

	// Corrupt the encrypted data
	corruptedData := make([]byte, len(encryptedData))
	copy(corruptedData, encryptedData)
	if len(corruptedData) > s3_constants.AESBlockSize {
		// Corrupt a byte after the IV
		corruptedData[s3_constants.AESBlockSize] ^= 0xFF
	}

	// Try to decrypt corrupted data
	decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(corruptedData), ssecCustomerKey, iv)
	if err != nil {
		t.Fatalf("Failed to create decrypted reader for corrupted data: %v", err)
	}

	decryptedData, err := io.ReadAll(decryptedReader)
	if err != nil {
		// This is okay - corrupted data might cause read errors
		t.Logf("Read error for corrupted data (expected): %v", err)
		return
	}

	// If we can read it, the data should be different from original
	if string(decryptedData) == testData {
		t.Error("Decrypted corrupted data should not match original")
	}
}

// TestSSEKMSCopyStrategy tests SSE-KMS copy strategy determination
func TestSSEKMSCopyStrategy(t *testing.T) {
	tests := []struct {
		name             string
		srcMetadata      map[string][]byte
		destKeyID        string
		expectedStrategy SSEKMSCopyStrategy
	}{
		{
			name:             "Unencrypted to unencrypted",
			srcMetadata:      map[string][]byte{},
			destKeyID:        "",
			expectedStrategy: SSEKMSCopyStrategyDirect,
		},
		{
			name: "Same KMS key",
			srcMetadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption:            []byte("aws:kms"),
				s3_constants.AmzServerSideEncryptionAwsKmsKeyId: []byte("test-key-123"),
			},
			destKeyID:        "test-key-123",
			expectedStrategy: SSEKMSCopyStrategyDirect,
		},
		{
			name: "Different KMS keys",
			srcMetadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption:            []byte("aws:kms"),
				s3_constants.AmzServerSideEncryptionAwsKmsKeyId: []byte("test-key-123"),
			},
			destKeyID:        "test-key-456",
			expectedStrategy: SSEKMSCopyStrategyDecryptEncrypt,
		},
		{
			name: "Encrypted to unencrypted",
			srcMetadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption:            []byte("aws:kms"),
				s3_constants.AmzServerSideEncryptionAwsKmsKeyId: []byte("test-key-123"),
			},
			destKeyID:        "",
			expectedStrategy: SSEKMSCopyStrategyDecryptEncrypt,
		},
		{
			name:             "Unencrypted to encrypted",
			srcMetadata:      map[string][]byte{},
			destKeyID:        "test-key-123",
			expectedStrategy: SSEKMSCopyStrategyDecryptEncrypt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy, err := DetermineSSEKMSCopyStrategy(tt.srcMetadata, tt.destKeyID)
			if err != nil {
				t.Fatalf("DetermineSSEKMSCopyStrategy failed: %v", err)
			}
			if strategy != tt.expectedStrategy {
				t.Errorf("Expected strategy %v, got %v", tt.expectedStrategy, strategy)
			}
		})
	}
}

// TestSSEKMSCopyHeaders tests SSE-KMS copy header parsing
func TestSSEKMSCopyHeaders(t *testing.T) {
	tests := []struct {
		name              string
		headers           map[string]string
		expectedKeyID     string
		expectedContext   map[string]string
		expectedBucketKey bool
		expectError       bool
	}{
		{
			name:              "No SSE-KMS headers",
			headers:           map[string]string{},
			expectedKeyID:     "",
			expectedContext:   nil,
			expectedBucketKey: false,
			expectError:       false,
		},
		{
			name: "SSE-KMS with key ID",
			headers: map[string]string{
				s3_constants.AmzServerSideEncryption:            "aws:kms",
				s3_constants.AmzServerSideEncryptionAwsKmsKeyId: "test-key-123",
			},
			expectedKeyID:     "test-key-123",
			expectedContext:   nil,
			expectedBucketKey: false,
			expectError:       false,
		},
		{
			name: "SSE-KMS with all options",
			headers: map[string]string{
				s3_constants.AmzServerSideEncryption:                 "aws:kms",
				s3_constants.AmzServerSideEncryptionAwsKmsKeyId:      "test-key-123",
				s3_constants.AmzServerSideEncryptionContext:          "eyJ0ZXN0IjoidmFsdWUifQ==", // base64 of {"test":"value"}
				s3_constants.AmzServerSideEncryptionBucketKeyEnabled: "true",
			},
			expectedKeyID:     "test-key-123",
			expectedContext:   map[string]string{"test": "value"},
			expectedBucketKey: true,
			expectError:       false,
		},
		{
			name: "Invalid key ID",
			headers: map[string]string{
				s3_constants.AmzServerSideEncryption:            "aws:kms",
				s3_constants.AmzServerSideEncryptionAwsKmsKeyId: "invalid key id",
			},
			expectError: true,
		},
		{
			name: "Invalid encryption context",
			headers: map[string]string{
				s3_constants.AmzServerSideEncryption:        "aws:kms",
				s3_constants.AmzServerSideEncryptionContext: "invalid-base64!",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("PUT", "/test", nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			keyID, context, bucketKey, err := ParseSSEKMSCopyHeaders(req)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if keyID != tt.expectedKeyID {
				t.Errorf("Expected keyID %s, got %s", tt.expectedKeyID, keyID)
			}

			if !mapsEqual(context, tt.expectedContext) {
				t.Errorf("Expected context %v, got %v", tt.expectedContext, context)
			}

			if bucketKey != tt.expectedBucketKey {
				t.Errorf("Expected bucketKey %v, got %v", tt.expectedBucketKey, bucketKey)
			}
		})
	}
}

// TestSSEKMSDirectCopy tests direct copy scenarios
func TestSSEKMSDirectCopy(t *testing.T) {
	tests := []struct {
		name        string
		srcMetadata map[string][]byte
		destKeyID   string
		canDirect   bool
	}{
		{
			name:        "Both unencrypted",
			srcMetadata: map[string][]byte{},
			destKeyID:   "",
			canDirect:   true,
		},
		{
			name: "Same key ID",
			srcMetadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption:            []byte("aws:kms"),
				s3_constants.AmzServerSideEncryptionAwsKmsKeyId: []byte("test-key-123"),
			},
			destKeyID: "test-key-123",
			canDirect: true,
		},
		{
			name: "Different key IDs",
			srcMetadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption:            []byte("aws:kms"),
				s3_constants.AmzServerSideEncryptionAwsKmsKeyId: []byte("test-key-123"),
			},
			destKeyID: "test-key-456",
			canDirect: false,
		},
		{
			name: "Source encrypted, dest unencrypted",
			srcMetadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption:            []byte("aws:kms"),
				s3_constants.AmzServerSideEncryptionAwsKmsKeyId: []byte("test-key-123"),
			},
			destKeyID: "",
			canDirect: false,
		},
		{
			name:        "Source unencrypted, dest encrypted",
			srcMetadata: map[string][]byte{},
			destKeyID:   "test-key-123",
			canDirect:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			canDirect := CanDirectCopySSEKMS(tt.srcMetadata, tt.destKeyID)
			if canDirect != tt.canDirect {
				t.Errorf("Expected canDirect %v, got %v", tt.canDirect, canDirect)
			}
		})
	}
}

// TestGetSourceSSEKMSInfo tests extraction of SSE-KMS info from metadata
func TestGetSourceSSEKMSInfo(t *testing.T) {
	tests := []struct {
		name              string
		metadata          map[string][]byte
		expectedKeyID     string
		expectedEncrypted bool
	}{
		{
			name:              "No encryption",
			metadata:          map[string][]byte{},
			expectedKeyID:     "",
			expectedEncrypted: false,
		},
		{
			name: "SSE-KMS with key ID",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption:            []byte("aws:kms"),
				s3_constants.AmzServerSideEncryptionAwsKmsKeyId: []byte("test-key-123"),
			},
			expectedKeyID:     "test-key-123",
			expectedEncrypted: true,
		},
		{
			name: "SSE-KMS without key ID (default key)",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("aws:kms"),
			},
			expectedKeyID:     "",
			expectedEncrypted: true,
		},
		{
			name: "Non-KMS encryption",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("AES256"),
			},
			expectedKeyID:     "",
			expectedEncrypted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyID, encrypted := GetSourceSSEKMSInfo(tt.metadata)
			if keyID != tt.expectedKeyID {
				t.Errorf("Expected keyID %s, got %s", tt.expectedKeyID, keyID)
			}
			if encrypted != tt.expectedEncrypted {
				t.Errorf("Expected encrypted %v, got %v", tt.expectedEncrypted, encrypted)
			}
		})
	}
}

// Helper function to compare maps
func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

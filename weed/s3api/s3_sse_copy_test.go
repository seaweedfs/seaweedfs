package s3api

import (
	"bytes"
	"io"
	"strings"
	"testing"
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
	encryptedReader, err := CreateSSECEncryptedReader(strings.NewReader(testData), sourceCustomerKey)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader: %v", err)
	}

	encryptedData, err := io.ReadAll(encryptedReader)
	if err != nil {
		t.Fatalf("Failed to read encrypted data: %v", err)
	}

	// Test copy strategy determination
	sourceMetadata := CreateTestMetadataWithSSEC(sourceKey)

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
		decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(encryptedData), sourceCustomerKey)
		if err != nil {
			t.Fatalf("Failed to create decrypted reader: %v", err)
		}

		// Re-encrypt with destination key
		reEncryptedReader, err := CreateSSECEncryptedReader(decryptedReader, destCustomerKey)
		if err != nil {
			t.Fatalf("Failed to create re-encrypted reader: %v", err)
		}

		reEncryptedData, err := io.ReadAll(reEncryptedReader)
		if err != nil {
			t.Fatalf("Failed to read re-encrypted data: %v", err)
		}

		// Verify we can decrypt with destination key
		finalDecryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(reEncryptedData), destCustomerKey)
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
	encryptedReader, err := CreateSSECEncryptedReader(strings.NewReader(testData), ssecCustomerKey)
	if err != nil {
		t.Fatalf("Failed to create SSE-C encrypted reader: %v", err)
	}

	encryptedData, err := io.ReadAll(encryptedReader)
	if err != nil {
		t.Fatalf("Failed to read SSE-C encrypted data: %v", err)
	}

	// Decrypt SSE-C data
	decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(encryptedData), ssecCustomerKey)
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
	reEncryptedReader, err := CreateSSECEncryptedReader(decryptedReader, ssecCustomerKey)
	if err != nil {
		t.Fatalf("Failed to create SSE-C encrypted reader: %v", err)
	}

	reEncryptedData, err := io.ReadAll(reEncryptedReader)
	if err != nil {
		t.Fatalf("Failed to read SSE-C encrypted data: %v", err)
	}

	// Decrypt with SSE-C
	finalDecryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(reEncryptedData), ssecCustomerKey)
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
	encryptedReader, err := CreateSSECEncryptedReader(strings.NewReader(testData), ssecCustomerKey)
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
	if len(corruptedData) > AESBlockSize {
		// Corrupt a byte after the IV
		corruptedData[AESBlockSize] ^= 0xFF
	}

	// Try to decrypt corrupted data
	decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(corruptedData), ssecCustomerKey)
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

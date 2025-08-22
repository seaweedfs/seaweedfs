package s3api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestSSECWrongKeyDecryption tests decryption with wrong SSE-C key
func TestSSECWrongKeyDecryption(t *testing.T) {
	// Setup original key and encrypt data
	originalKey := GenerateTestSSECKey(1)
	testData := "Hello, SSE-C world!"

	encryptedReader, iv, err := CreateSSECEncryptedReader(strings.NewReader(testData), &SSECustomerKey{
		Algorithm: "AES256",
		Key:       originalKey.Key,
		KeyMD5:    originalKey.KeyMD5,
	})
	if err != nil {
		t.Fatalf("Failed to create encrypted reader: %v", err)
	}

	// Read encrypted data
	encryptedData, err := io.ReadAll(encryptedReader)
	if err != nil {
		t.Fatalf("Failed to read encrypted data: %v", err)
	}

	// Try to decrypt with wrong key
	wrongKey := GenerateTestSSECKey(2) // Different seed = different key
	decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(encryptedData), &SSECustomerKey{
		Algorithm: "AES256",
		Key:       wrongKey.Key,
		KeyMD5:    wrongKey.KeyMD5,
	}, iv)
	if err != nil {
		t.Fatalf("Failed to create decrypted reader: %v", err)
	}

	// Read decrypted data - should be garbage/different from original
	decryptedData, err := io.ReadAll(decryptedReader)
	if err != nil {
		t.Fatalf("Failed to read decrypted data: %v", err)
	}

	// Verify the decrypted data is NOT the same as original (wrong key used)
	if string(decryptedData) == testData {
		t.Error("Decryption with wrong key should not produce original data")
	}
}

// TestSSEKMSKeyNotFound tests handling of missing KMS key
func TestSSEKMSKeyNotFound(t *testing.T) {
	// Note: The local KMS provider creates keys on-demand by design.
	// This test validates that when on-demand creation fails or is disabled,
	// appropriate errors are returned.

	// Test with an invalid key ID that would fail even on-demand creation
	invalidKeyID := "" // Empty key ID should fail
	encryptionContext := BuildEncryptionContext("test-bucket", "test-object", false)

	_, _, err := CreateSSEKMSEncryptedReader(strings.NewReader("test data"), invalidKeyID, encryptionContext)

	// Should get an error for invalid/empty key
	if err == nil {
		t.Error("Expected error for empty KMS key ID, got none")
	}

	// For local KMS with on-demand creation, we test what we can realistically test
	if err != nil {
		t.Logf("Got expected error for empty key ID: %v", err)
	}
}

// TestSSEHeadersWithoutEncryption tests inconsistent state where headers are present but no encryption
func TestSSEHeadersWithoutEncryption(t *testing.T) {
	testCases := []struct {
		name     string
		setupReq func() *http.Request
	}{
		{
			name: "SSE-C algorithm without key",
			setupReq: func() *http.Request {
				req := CreateTestHTTPRequest("PUT", "/bucket/object", nil)
				req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, "AES256")
				// Missing key and MD5
				return req
			},
		},
		{
			name: "SSE-C key without algorithm",
			setupReq: func() *http.Request {
				req := CreateTestHTTPRequest("PUT", "/bucket/object", nil)
				keyPair := GenerateTestSSECKey(1)
				req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKey, keyPair.KeyB64)
				// Missing algorithm
				return req
			},
		},
		{
			name: "SSE-KMS key ID without algorithm",
			setupReq: func() *http.Request {
				req := CreateTestHTTPRequest("PUT", "/bucket/object", nil)
				req.Header.Set(s3_constants.AmzServerSideEncryptionAwsKmsKeyId, "test-key-id")
				// Missing algorithm
				return req
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := tc.setupReq()

			// Validate headers - should catch incomplete configurations
			if strings.Contains(tc.name, "SSE-C") {
				err := ValidateSSECHeaders(req)
				if err == nil {
					t.Error("Expected validation error for incomplete SSE-C headers")
				}
			}
		})
	}
}

// TestSSECInvalidKeyFormats tests various invalid SSE-C key formats
func TestSSECInvalidKeyFormats(t *testing.T) {
	testCases := []struct {
		name      string
		algorithm string
		key       string
		keyMD5    string
		expectErr bool
	}{
		{
			name:      "Invalid algorithm",
			algorithm: "AES128",
			key:       "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3RrZXk=", // 32 bytes base64
			keyMD5:    "valid-md5-hash",
			expectErr: true,
		},
		{
			name:      "Invalid key length (too short)",
			algorithm: "AES256",
			key:       "c2hvcnRrZXk=", // "shortkey" base64 - too short
			keyMD5:    "valid-md5-hash",
			expectErr: true,
		},
		{
			name:      "Invalid key length (too long)",
			algorithm: "AES256",
			key:       "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleQ==", // too long
			keyMD5:    "valid-md5-hash",
			expectErr: true,
		},
		{
			name:      "Invalid base64 key",
			algorithm: "AES256",
			key:       "invalid-base64!",
			keyMD5:    "valid-md5-hash",
			expectErr: true,
		},
		{
			name:      "Invalid base64 MD5",
			algorithm: "AES256",
			key:       "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3RrZXk=",
			keyMD5:    "invalid-base64!",
			expectErr: true,
		},
		{
			name:      "Mismatched MD5",
			algorithm: "AES256",
			key:       "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3RrZXk=",
			keyMD5:    "d29uZy1tZDUtaGFzaA==", // "wrong-md5-hash" base64
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := CreateTestHTTPRequest("PUT", "/bucket/object", nil)
			req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, tc.algorithm)
			req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKey, tc.key)
			req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, tc.keyMD5)

			err := ValidateSSECHeaders(req)
			if tc.expectErr && err == nil {
				t.Errorf("Expected error for %s, but got none", tc.name)
			}
			if !tc.expectErr && err != nil {
				t.Errorf("Expected no error for %s, but got: %v", tc.name, err)
			}
		})
	}
}

// TestSSEKMSInvalidConfigurations tests various invalid SSE-KMS configurations
func TestSSEKMSInvalidConfigurations(t *testing.T) {
	testCases := []struct {
		name         string
		setupRequest func() *http.Request
		expectError  bool
	}{
		{
			name: "Invalid algorithm",
			setupRequest: func() *http.Request {
				req := CreateTestHTTPRequest("PUT", "/bucket/object", nil)
				req.Header.Set(s3_constants.AmzServerSideEncryption, "invalid-algorithm")
				return req
			},
			expectError: true,
		},
		{
			name: "Empty key ID",
			setupRequest: func() *http.Request {
				req := CreateTestHTTPRequest("PUT", "/bucket/object", nil)
				req.Header.Set(s3_constants.AmzServerSideEncryption, "aws:kms")
				req.Header.Set(s3_constants.AmzServerSideEncryptionAwsKmsKeyId, "")
				return req
			},
			expectError: false, // Empty key ID might be valid (use default)
		},
		{
			name: "Invalid key ID format",
			setupRequest: func() *http.Request {
				req := CreateTestHTTPRequest("PUT", "/bucket/object", nil)
				req.Header.Set(s3_constants.AmzServerSideEncryption, "aws:kms")
				req.Header.Set(s3_constants.AmzServerSideEncryptionAwsKmsKeyId, "invalid key id with spaces")
				return req
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := tc.setupRequest()

			_, err := ParseSSEKMSHeaders(req)
			if tc.expectError && err == nil {
				t.Errorf("Expected error for %s, but got none", tc.name)
			}
			if !tc.expectError && err != nil {
				t.Errorf("Expected no error for %s, but got: %v", tc.name, err)
			}
		})
	}
}

// TestSSEEmptyDataHandling tests handling of empty data with SSE
func TestSSEEmptyDataHandling(t *testing.T) {
	t.Run("SSE-C with empty data", func(t *testing.T) {
		keyPair := GenerateTestSSECKey(1)
		customerKey := &SSECustomerKey{
			Algorithm: "AES256",
			Key:       keyPair.Key,
			KeyMD5:    keyPair.KeyMD5,
		}

		// Encrypt empty data
		encryptedReader, iv, err := CreateSSECEncryptedReader(strings.NewReader(""), customerKey)
		if err != nil {
			t.Fatalf("Failed to create encrypted reader for empty data: %v", err)
		}

		encryptedData, err := io.ReadAll(encryptedReader)
		if err != nil {
			t.Fatalf("Failed to read encrypted empty data: %v", err)
		}

		// Should have IV for empty data
		if len(iv) != s3_constants.AESBlockSize {
			t.Error("IV should be present even for empty data")
		}

		// Decrypt and verify
		decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(encryptedData), customerKey, iv)
		if err != nil {
			t.Fatalf("Failed to create decrypted reader for empty data: %v", err)
		}

		decryptedData, err := io.ReadAll(decryptedReader)
		if err != nil {
			t.Fatalf("Failed to read decrypted empty data: %v", err)
		}

		if len(decryptedData) != 0 {
			t.Errorf("Expected empty decrypted data, got %d bytes", len(decryptedData))
		}
	})

	t.Run("SSE-KMS with empty data", func(t *testing.T) {
		kmsKey := SetupTestKMS(t)
		defer kmsKey.Cleanup()

		encryptionContext := BuildEncryptionContext("test-bucket", "test-object", false)

		// Encrypt empty data
		encryptedReader, sseKey, err := CreateSSEKMSEncryptedReader(strings.NewReader(""), kmsKey.KeyID, encryptionContext)
		if err != nil {
			t.Fatalf("Failed to create encrypted reader for empty data: %v", err)
		}

		encryptedData, err := io.ReadAll(encryptedReader)
		if err != nil {
			t.Fatalf("Failed to read encrypted empty data: %v", err)
		}

		// Empty data should produce empty encrypted data (IV is stored in metadata)
		if len(encryptedData) != 0 {
			t.Errorf("Encrypted empty data should be empty, got %d bytes", len(encryptedData))
		}

		// Decrypt and verify
		decryptedReader, err := CreateSSEKMSDecryptedReader(bytes.NewReader(encryptedData), sseKey)
		if err != nil {
			t.Fatalf("Failed to create decrypted reader for empty data: %v", err)
		}

		decryptedData, err := io.ReadAll(decryptedReader)
		if err != nil {
			t.Fatalf("Failed to read decrypted empty data: %v", err)
		}

		if len(decryptedData) != 0 {
			t.Errorf("Expected empty decrypted data, got %d bytes", len(decryptedData))
		}
	})
}

// TestSSEConcurrentAccess tests SSE operations under concurrent access
func TestSSEConcurrentAccess(t *testing.T) {
	keyPair := GenerateTestSSECKey(1)
	customerKey := &SSECustomerKey{
		Algorithm: "AES256",
		Key:       keyPair.Key,
		KeyMD5:    keyPair.KeyMD5,
	}

	const numGoroutines = 10
	done := make(chan bool, numGoroutines)
	errors := make(chan error, numGoroutines)

	// Run multiple encryption/decryption operations concurrently
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			testData := fmt.Sprintf("test data %d", id)

			// Encrypt
			encryptedReader, iv, err := CreateSSECEncryptedReader(strings.NewReader(testData), customerKey)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d encrypt error: %v", id, err)
				return
			}

			encryptedData, err := io.ReadAll(encryptedReader)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d read encrypted error: %v", id, err)
				return
			}

			// Decrypt
			decryptedReader, err := CreateSSECDecryptedReader(bytes.NewReader(encryptedData), customerKey, iv)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d decrypt error: %v", id, err)
				return
			}

			decryptedData, err := io.ReadAll(decryptedReader)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d read decrypted error: %v", id, err)
				return
			}

			if string(decryptedData) != testData {
				errors <- fmt.Errorf("goroutine %d data mismatch: expected %s, got %s", id, testData, string(decryptedData))
				return
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Check for errors
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

package s3api

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/kms/local"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// setupTestKMS initializes a local KMS provider for testing
func setupTestKMS(t *testing.T) (string, func()) {
	// Initialize local KMS provider directly
	provider, err := local.NewLocalKMSProvider(nil)
	if err != nil {
		t.Fatalf("Failed to create local KMS provider: %v", err)
	}

	// Set it as the global provider (simulate InitializeGlobalKMS)
	kms.SetGlobalKMSForTesting(provider)

	// Create a test key
	localProvider := provider.(*local.LocalKMSProvider)
	testKey, err := localProvider.CreateKey("Test key for SSE-KMS", []string{"test-key"})
	if err != nil {
		t.Fatalf("Failed to create test key: %v", err)
	}

	// Cleanup function
	cleanup := func() {
		kms.SetGlobalKMSForTesting(nil) // Clear global KMS
		if err := provider.Close(); err != nil {
			t.Logf("Warning: Failed to close KMS provider: %v", err)
		}
	}

	return testKey.KeyID, cleanup
}

func TestSSEKMSEncryptionDecryption(t *testing.T) {
	keyID, cleanup := setupTestKMS(t)
	defer cleanup()

	// Test data
	testData := "Hello, SSE-KMS world! This is a test of envelope encryption."
	testReader := strings.NewReader(testData)

	// Create encryption context
	encryptionContext := BuildEncryptionContext("test-bucket", "test-object", false)

	// Encrypt the data
	encryptedReader, sseKey, err := CreateSSEKMSEncryptedReader(testReader, keyID, encryptionContext)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader: %v", err)
	}

	// Verify SSE key metadata
	if sseKey.KeyID != keyID {
		t.Errorf("Expected key ID %s, got %s", keyID, sseKey.KeyID)
	}

	if len(sseKey.EncryptedDataKey) == 0 {
		t.Error("Encrypted data key should not be empty")
	}

	if sseKey.EncryptionContext == nil {
		t.Error("Encryption context should not be nil")
	}

	// Read the encrypted data
	encryptedData, err := io.ReadAll(encryptedReader)
	if err != nil {
		t.Fatalf("Failed to read encrypted data: %v", err)
	}

	// Verify the encrypted data is different from original
	if string(encryptedData) == testData {
		t.Error("Encrypted data should be different from original data")
	}

	// The encrypted data should be longer (IV + encrypted content)
	if len(encryptedData) <= len(testData) {
		t.Error("Encrypted data should be longer than original data (includes IV)")
	}

	// Decrypt the data
	decryptedReader, err := CreateSSEKMSDecryptedReader(bytes.NewReader(encryptedData), sseKey)
	if err != nil {
		t.Fatalf("Failed to create decrypted reader: %v", err)
	}

	// Read the decrypted data
	decryptedData, err := io.ReadAll(decryptedReader)
	if err != nil {
		t.Fatalf("Failed to read decrypted data: %v", err)
	}

	// Verify the decrypted data matches the original
	if string(decryptedData) != testData {
		t.Errorf("Decrypted data does not match original.\nExpected: %s\nGot: %s", testData, string(decryptedData))
	}
}

func TestSSEKMSKeyValidation(t *testing.T) {
	tests := []struct {
		name      string
		keyID     string
		wantValid bool
	}{
		{
			name:      "Valid UUID key ID",
			keyID:     "12345678-1234-1234-1234-123456789012",
			wantValid: true,
		},
		{
			name:      "Valid alias",
			keyID:     "alias/my-test-key",
			wantValid: true,
		},
		{
			name:      "Valid ARN",
			keyID:     "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
			wantValid: true,
		},
		{
			name:      "Valid alias ARN",
			keyID:     "arn:aws:kms:us-east-1:123456789012:alias/my-test-key",
			wantValid: true,
		},
		{
			name:      "Empty key ID",
			keyID:     "",
			wantValid: false,
		},
		{
			name:      "Invalid format",
			keyID:     "invalid-key-format",
			wantValid: false,
		},
		{
			name:      "Short UUID",
			keyID:     "12345678-1234",
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := isValidKMSKeyID(tt.keyID)
			if valid != tt.wantValid {
				t.Errorf("isValidKMSKeyID(%s) = %v, want %v", tt.keyID, valid, tt.wantValid)
			}
		})
	}
}

func TestSSEKMSMetadataSerialization(t *testing.T) {
	// Create test SSE key
	sseKey := &SSEKMSKey{
		KeyID:            "test-key-id",
		EncryptedDataKey: []byte("encrypted-data-key"),
		EncryptionContext: map[string]string{
			"aws:s3:arn": "arn:aws:s3:::test-bucket/test-object",
		},
		BucketKeyEnabled: true,
	}

	// Serialize metadata
	serialized, err := SerializeSSEKMSMetadata(sseKey)
	if err != nil {
		t.Fatalf("Failed to serialize SSE-KMS metadata: %v", err)
	}

	// Verify it's valid JSON
	var jsonData map[string]interface{}
	if err := json.Unmarshal(serialized, &jsonData); err != nil {
		t.Fatalf("Serialized data is not valid JSON: %v", err)
	}

	// Deserialize metadata
	deserializedKey, err := DeserializeSSEKMSMetadata(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize SSE-KMS metadata: %v", err)
	}

	// Verify the deserialized data matches original
	if deserializedKey.KeyID != sseKey.KeyID {
		t.Errorf("KeyID mismatch: expected %s, got %s", sseKey.KeyID, deserializedKey.KeyID)
	}

	if !bytes.Equal(deserializedKey.EncryptedDataKey, sseKey.EncryptedDataKey) {
		t.Error("EncryptedDataKey mismatch")
	}

	if len(deserializedKey.EncryptionContext) != len(sseKey.EncryptionContext) {
		t.Error("EncryptionContext length mismatch")
	}

	for k, v := range sseKey.EncryptionContext {
		if deserializedKey.EncryptionContext[k] != v {
			t.Errorf("EncryptionContext mismatch for key %s: expected %s, got %s", k, v, deserializedKey.EncryptionContext[k])
		}
	}

	if deserializedKey.BucketKeyEnabled != sseKey.BucketKeyEnabled {
		t.Errorf("BucketKeyEnabled mismatch: expected %v, got %v", sseKey.BucketKeyEnabled, deserializedKey.BucketKeyEnabled)
	}
}

func TestBuildEncryptionContext(t *testing.T) {
	tests := []struct {
		name         string
		bucket       string
		object       string
		useBucketKey bool
		expectedARN  string
	}{
		{
			name:         "Object-level encryption",
			bucket:       "test-bucket",
			object:       "test-object",
			useBucketKey: false,
			expectedARN:  "arn:aws:s3:::test-bucket/test-object",
		},
		{
			name:         "Bucket-level encryption",
			bucket:       "test-bucket",
			object:       "test-object",
			useBucketKey: true,
			expectedARN:  "arn:aws:s3:::test-bucket",
		},
		{
			name:         "Nested object path",
			bucket:       "my-bucket",
			object:       "folder/subfolder/file.txt",
			useBucketKey: false,
			expectedARN:  "arn:aws:s3:::my-bucket/folder/subfolder/file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			context := BuildEncryptionContext(tt.bucket, tt.object, tt.useBucketKey)

			if context == nil {
				t.Fatal("Encryption context should not be nil")
			}

			arn, exists := context[kms.EncryptionContextS3ARN]
			if !exists {
				t.Error("Encryption context should contain S3 ARN")
			}

			if arn != tt.expectedARN {
				t.Errorf("Expected ARN %s, got %s", tt.expectedARN, arn)
			}
		})
	}
}

func TestKMSErrorMapping(t *testing.T) {
	tests := []struct {
		name        string
		kmsError    *kms.KMSError
		expectedErr string
	}{
		{
			name: "Key not found",
			kmsError: &kms.KMSError{
				Code:    kms.ErrCodeNotFoundException,
				Message: "Key not found",
			},
			expectedErr: "KMSKeyNotFoundException",
		},
		{
			name: "Access denied",
			kmsError: &kms.KMSError{
				Code:    kms.ErrCodeAccessDenied,
				Message: "Access denied",
			},
			expectedErr: "KMSAccessDeniedException",
		},
		{
			name: "Key unavailable",
			kmsError: &kms.KMSError{
				Code:    kms.ErrCodeKeyUnavailable,
				Message: "Key is disabled",
			},
			expectedErr: "KMSKeyDisabledException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errorCode := MapKMSErrorToS3Error(tt.kmsError)

			// Get the actual error description
			apiError := s3err.GetAPIError(errorCode)
			if apiError.Code != tt.expectedErr {
				t.Errorf("Expected error code %s, got %s", tt.expectedErr, apiError.Code)
			}
		})
	}
}

// TestLargeDataEncryption tests encryption/decryption of larger data streams
func TestSSEKMSLargeDataEncryption(t *testing.T) {
	keyID, cleanup := setupTestKMS(t)
	defer cleanup()

	// Create a larger test dataset (1MB)
	testData := strings.Repeat("This is a test of SSE-KMS with larger data streams. ", 20000)
	testReader := strings.NewReader(testData)

	// Create encryption context
	encryptionContext := BuildEncryptionContext("large-bucket", "large-object", false)

	// Encrypt the data
	encryptedReader, sseKey, err := CreateSSEKMSEncryptedReader(testReader, keyID, encryptionContext)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader: %v", err)
	}

	// Read the encrypted data
	encryptedData, err := io.ReadAll(encryptedReader)
	if err != nil {
		t.Fatalf("Failed to read encrypted data: %v", err)
	}

	// Decrypt the data
	decryptedReader, err := CreateSSEKMSDecryptedReader(bytes.NewReader(encryptedData), sseKey)
	if err != nil {
		t.Fatalf("Failed to create decrypted reader: %v", err)
	}

	// Read the decrypted data
	decryptedData, err := io.ReadAll(decryptedReader)
	if err != nil {
		t.Fatalf("Failed to read decrypted data: %v", err)
	}

	// Verify the decrypted data matches the original
	if string(decryptedData) != testData {
		t.Errorf("Decrypted data length: %d, original data length: %d", len(decryptedData), len(testData))
		t.Error("Decrypted large data does not match original")
	}

	t.Logf("Successfully encrypted/decrypted %d bytes of data", len(testData))
}

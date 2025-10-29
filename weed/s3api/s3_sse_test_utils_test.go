package s3api

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/kms/local"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestKeyPair represents a test SSE-C key pair
type TestKeyPair struct {
	Key    []byte
	KeyB64 string
	KeyMD5 string
}

// TestSSEKMSKey represents a test SSE-KMS key
type TestSSEKMSKey struct {
	KeyID   string
	Cleanup func()
}

// GenerateTestSSECKey creates a test SSE-C key pair
func GenerateTestSSECKey(seed byte) *TestKeyPair {
	key := make([]byte, 32) // 256-bit key
	for i := range key {
		key[i] = seed + byte(i)
	}

	keyB64 := base64.StdEncoding.EncodeToString(key)
	md5sum := md5.Sum(key)
	keyMD5 := base64.StdEncoding.EncodeToString(md5sum[:])

	return &TestKeyPair{
		Key:    key,
		KeyB64: keyB64,
		KeyMD5: keyMD5,
	}
}

// SetupTestSSECHeaders sets SSE-C headers on an HTTP request
func SetupTestSSECHeaders(req *http.Request, keyPair *TestKeyPair) {
	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, "AES256")
	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKey, keyPair.KeyB64)
	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, keyPair.KeyMD5)
}

// SetupTestSSECCopyHeaders sets SSE-C copy source headers on an HTTP request
func SetupTestSSECCopyHeaders(req *http.Request, keyPair *TestKeyPair) {
	req.Header.Set(s3_constants.AmzCopySourceServerSideEncryptionCustomerAlgorithm, "AES256")
	req.Header.Set(s3_constants.AmzCopySourceServerSideEncryptionCustomerKey, keyPair.KeyB64)
	req.Header.Set(s3_constants.AmzCopySourceServerSideEncryptionCustomerKeyMD5, keyPair.KeyMD5)
}

// SetupTestKMS initializes a local KMS provider for testing
func SetupTestKMS(t *testing.T) *TestSSEKMSKey {
	// Initialize local KMS provider directly
	provider, err := local.NewLocalKMSProvider(nil)
	if err != nil {
		t.Fatalf("Failed to create local KMS provider: %v", err)
	}

	// Set it as the global provider
	kms.SetGlobalKMSProvider(provider)

	// Create a test key
	localProvider := provider.(*local.LocalKMSProvider)
	testKey, err := localProvider.CreateKey("Test key for SSE-KMS", []string{"test-key"})
	if err != nil {
		t.Fatalf("Failed to create test key: %v", err)
	}

	// Cleanup function
	cleanup := func() {
		kms.SetGlobalKMSProvider(nil) // Clear global KMS
		if err := provider.Close(); err != nil {
			t.Logf("Warning: Failed to close KMS provider: %v", err)
		}
	}

	return &TestSSEKMSKey{
		KeyID:   testKey.KeyID,
		Cleanup: cleanup,
	}
}

// SetupTestSSEKMSHeaders sets SSE-KMS headers on an HTTP request
func SetupTestSSEKMSHeaders(req *http.Request, keyID string) {
	req.Header.Set(s3_constants.AmzServerSideEncryption, "aws:kms")
	if keyID != "" {
		req.Header.Set(s3_constants.AmzServerSideEncryptionAwsKmsKeyId, keyID)
	}
}

// CreateTestMetadata creates test metadata with SSE information
func CreateTestMetadata() map[string][]byte {
	return make(map[string][]byte)
}

// CreateTestMetadataWithSSEC creates test metadata containing SSE-C information
func CreateTestMetadataWithSSEC(keyPair *TestKeyPair) map[string][]byte {
	metadata := CreateTestMetadata()
	metadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] = []byte("AES256")
	metadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5] = []byte(keyPair.KeyMD5)
	// Add encryption IV and other encrypted data that would be stored
	iv := make([]byte, 16)
	for i := range iv {
		iv[i] = byte(i)
	}
	StoreSSECIVInMetadata(metadata, iv)
	return metadata
}

// CreateTestMetadataWithSSEKMS creates test metadata containing SSE-KMS information
func CreateTestMetadataWithSSEKMS(sseKey *SSEKMSKey) map[string][]byte {
	metadata := CreateTestMetadata()
	metadata[s3_constants.AmzServerSideEncryption] = []byte("aws:kms")
	if sseKey != nil {
		serialized, _ := SerializeSSEKMSMetadata(sseKey)
		metadata[s3_constants.AmzEncryptedDataKey] = sseKey.EncryptedDataKey
		metadata[s3_constants.AmzEncryptionContextMeta] = serialized
	}
	return metadata
}

// CreateTestHTTPRequest creates a test HTTP request with optional SSE headers
func CreateTestHTTPRequest(method, path string, body []byte) *http.Request {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req := httptest.NewRequest(method, path, bodyReader)
	return req
}

// CreateTestHTTPResponse creates a test HTTP response recorder
func CreateTestHTTPResponse() *httptest.ResponseRecorder {
	return httptest.NewRecorder()
}

// SetupTestMuxVars sets up mux variables for testing
func SetupTestMuxVars(req *http.Request, vars map[string]string) {
	mux.SetURLVars(req, vars)
}

// AssertSSECHeaders verifies that SSE-C response headers are set correctly
func AssertSSECHeaders(t *testing.T, w *httptest.ResponseRecorder, keyPair *TestKeyPair) {
	algorithm := w.Header().Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm)
	if algorithm != "AES256" {
		t.Errorf("Expected algorithm AES256, got %s", algorithm)
	}

	keyMD5 := w.Header().Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5)
	if keyMD5 != keyPair.KeyMD5 {
		t.Errorf("Expected key MD5 %s, got %s", keyPair.KeyMD5, keyMD5)
	}
}

// AssertSSEKMSHeaders verifies that SSE-KMS response headers are set correctly
func AssertSSEKMSHeaders(t *testing.T, w *httptest.ResponseRecorder, keyID string) {
	algorithm := w.Header().Get(s3_constants.AmzServerSideEncryption)
	if algorithm != "aws:kms" {
		t.Errorf("Expected algorithm aws:kms, got %s", algorithm)
	}

	if keyID != "" {
		responseKeyID := w.Header().Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId)
		if responseKeyID != keyID {
			t.Errorf("Expected key ID %s, got %s", keyID, responseKeyID)
		}
	}
}

// CreateCorruptedSSECMetadata creates intentionally corrupted SSE-C metadata for testing
func CreateCorruptedSSECMetadata() map[string][]byte {
	metadata := CreateTestMetadata()
	// Missing algorithm
	metadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5] = []byte("invalid-md5")
	return metadata
}

// CreateCorruptedSSEKMSMetadata creates intentionally corrupted SSE-KMS metadata for testing
func CreateCorruptedSSEKMSMetadata() map[string][]byte {
	metadata := CreateTestMetadata()
	metadata[s3_constants.AmzServerSideEncryption] = []byte("aws:kms")
	// Invalid encrypted data key
	metadata[s3_constants.AmzEncryptedDataKey] = []byte("invalid-base64!")
	return metadata
}

// TestDataSizes provides various data sizes for testing
var TestDataSizes = []int{
	0,       // Empty
	1,       // Single byte
	15,      // Less than AES block size
	16,      // Exactly AES block size
	17,      // More than AES block size
	1024,    // 1KB
	65536,   // 64KB
	1048576, // 1MB
}

// GenerateTestData creates test data of specified size
func GenerateTestData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

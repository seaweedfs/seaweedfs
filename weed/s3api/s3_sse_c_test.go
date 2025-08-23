package s3api

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func base64MD5(b []byte) string {
	s := md5.Sum(b)
	return base64.StdEncoding.EncodeToString(s[:])
}

func TestSSECHeaderValidation(t *testing.T) {
	// Test valid SSE-C headers
	req := &http.Request{Header: make(http.Header)}

	key := make([]byte, 32) // 256-bit key
	for i := range key {
		key[i] = byte(i)
	}

	keyBase64 := base64.StdEncoding.EncodeToString(key)
	md5sum := md5.Sum(key)
	keyMD5 := base64.StdEncoding.EncodeToString(md5sum[:])

	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, "AES256")
	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKey, keyBase64)
	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, keyMD5)

	// Test validation
	err := ValidateSSECHeaders(req)
	if err != nil {
		t.Errorf("Expected valid headers, got error: %v", err)
	}

	// Test parsing
	customerKey, err := ParseSSECHeaders(req)
	if err != nil {
		t.Errorf("Expected successful parsing, got error: %v", err)
	}

	if customerKey == nil {
		t.Error("Expected customer key, got nil")
	}

	if customerKey.Algorithm != "AES256" {
		t.Errorf("Expected algorithm AES256, got %s", customerKey.Algorithm)
	}

	if !bytes.Equal(customerKey.Key, key) {
		t.Error("Key doesn't match original")
	}

	if customerKey.KeyMD5 != keyMD5 {
		t.Errorf("Expected key MD5 %s, got %s", keyMD5, customerKey.KeyMD5)
	}
}

func TestSSECCopySourceHeaders(t *testing.T) {
	// Test valid SSE-C copy source headers
	req := &http.Request{Header: make(http.Header)}

	key := make([]byte, 32) // 256-bit key
	for i := range key {
		key[i] = byte(i) + 1 // Different from regular test
	}

	keyBase64 := base64.StdEncoding.EncodeToString(key)
	md5sum2 := md5.Sum(key)
	keyMD5 := base64.StdEncoding.EncodeToString(md5sum2[:])

	req.Header.Set(s3_constants.AmzCopySourceServerSideEncryptionCustomerAlgorithm, "AES256")
	req.Header.Set(s3_constants.AmzCopySourceServerSideEncryptionCustomerKey, keyBase64)
	req.Header.Set(s3_constants.AmzCopySourceServerSideEncryptionCustomerKeyMD5, keyMD5)

	// Test parsing copy source headers
	customerKey, err := ParseSSECCopySourceHeaders(req)
	if err != nil {
		t.Errorf("Expected successful copy source parsing, got error: %v", err)
	}

	if customerKey == nil {
		t.Error("Expected customer key from copy source headers, got nil")
	}

	if customerKey.Algorithm != "AES256" {
		t.Errorf("Expected algorithm AES256, got %s", customerKey.Algorithm)
	}

	if !bytes.Equal(customerKey.Key, key) {
		t.Error("Copy source key doesn't match original")
	}

	// Test that regular headers don't interfere with copy source headers
	regularKey, err := ParseSSECHeaders(req)
	if err != nil {
		t.Errorf("Regular header parsing should not fail: %v", err)
	}

	if regularKey != nil {
		t.Error("Expected nil for regular headers when only copy source headers are present")
	}
}

func TestSSECHeaderValidationErrors(t *testing.T) {
	tests := []struct {
		name      string
		algorithm string
		key       string
		keyMD5    string
		wantErr   error
	}{
		{
			name:      "invalid algorithm",
			algorithm: "AES128",
			key:       base64.StdEncoding.EncodeToString(make([]byte, 32)),
			keyMD5:    base64MD5(make([]byte, 32)),
			wantErr:   ErrInvalidEncryptionAlgorithm,
		},
		{
			name:      "invalid key length",
			algorithm: "AES256",
			key:       base64.StdEncoding.EncodeToString(make([]byte, 16)),
			keyMD5:    base64MD5(make([]byte, 16)),
			wantErr:   ErrInvalidEncryptionKey,
		},
		{
			name:      "mismatched MD5",
			algorithm: "AES256",
			key:       base64.StdEncoding.EncodeToString(make([]byte, 32)),
			keyMD5:    "wrong==md5",
			wantErr:   ErrSSECustomerKeyMD5Mismatch,
		},
		{
			name:      "incomplete headers",
			algorithm: "AES256",
			key:       "",
			keyMD5:    "",
			wantErr:   ErrInvalidRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &http.Request{Header: make(http.Header)}

			if tt.algorithm != "" {
				req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, tt.algorithm)
			}
			if tt.key != "" {
				req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKey, tt.key)
			}
			if tt.keyMD5 != "" {
				req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, tt.keyMD5)
			}

			err := ValidateSSECHeaders(req)
			if err != tt.wantErr {
				t.Errorf("Expected error %v, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestSSECEncryptionDecryption(t *testing.T) {
	// Create customer key
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	md5sumKey := md5.Sum(key)
	customerKey := &SSECustomerKey{
		Algorithm: "AES256",
		Key:       key,
		KeyMD5:    base64.StdEncoding.EncodeToString(md5sumKey[:]),
	}

	// Test data
	testData := []byte("Hello, World! This is a test of SSE-C encryption.")

	// Create encrypted reader
	dataReader := bytes.NewReader(testData)
	encryptedReader, iv, err := CreateSSECEncryptedReader(dataReader, customerKey)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader: %v", err)
	}

	// Read encrypted data
	encryptedData, err := io.ReadAll(encryptedReader)
	if err != nil {
		t.Fatalf("Failed to read encrypted data: %v", err)
	}

	// Verify data is actually encrypted (different from original)
	if bytes.Equal(encryptedData[16:], testData) { // Skip IV
		t.Error("Data doesn't appear to be encrypted")
	}

	// Create decrypted reader
	encryptedReader2 := bytes.NewReader(encryptedData)
	decryptedReader, err := CreateSSECDecryptedReader(encryptedReader2, customerKey, iv)
	if err != nil {
		t.Fatalf("Failed to create decrypted reader: %v", err)
	}

	// Read decrypted data
	decryptedData, err := io.ReadAll(decryptedReader)
	if err != nil {
		t.Fatalf("Failed to read decrypted data: %v", err)
	}

	// Verify decrypted data matches original
	if !bytes.Equal(decryptedData, testData) {
		t.Errorf("Decrypted data doesn't match original.\nOriginal: %s\nDecrypted: %s", testData, decryptedData)
	}
}

func TestSSECIsSSECRequest(t *testing.T) {
	// Test with SSE-C headers
	req := &http.Request{Header: make(http.Header)}
	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, "AES256")

	if !IsSSECRequest(req) {
		t.Error("Expected IsSSECRequest to return true when SSE-C headers are present")
	}

	// Test without SSE-C headers
	req2 := &http.Request{Header: make(http.Header)}
	if IsSSECRequest(req2) {
		t.Error("Expected IsSSECRequest to return false when no SSE-C headers are present")
	}
}

// Test encryption with different data sizes (similar to s3tests)
func TestSSECEncryptionVariousSizes(t *testing.T) {
	sizes := []int{1, 13, 1024, 1024 * 1024} // 1B, 13B, 1KB, 1MB

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			// Create customer key
			key := make([]byte, 32)
			for i := range key {
				key[i] = byte(i + size) // Make key unique per test
			}

			md5sumDyn := md5.Sum(key)
			customerKey := &SSECustomerKey{
				Algorithm: "AES256",
				Key:       key,
				KeyMD5:    base64.StdEncoding.EncodeToString(md5sumDyn[:]),
			}

			// Create test data of specified size
			testData := make([]byte, size)
			for i := range testData {
				testData[i] = byte('A' + (i % 26)) // Pattern of A-Z
			}

			// Encrypt
			dataReader := bytes.NewReader(testData)
			encryptedReader, iv, err := CreateSSECEncryptedReader(dataReader, customerKey)
			if err != nil {
				t.Fatalf("Failed to create encrypted reader: %v", err)
			}

			encryptedData, err := io.ReadAll(encryptedReader)
			if err != nil {
				t.Fatalf("Failed to read encrypted data: %v", err)
			}

			// Verify encrypted data has same size as original (IV is stored in metadata, not in stream)
			if len(encryptedData) != size {
				t.Errorf("Expected encrypted data length %d (same as original), got %d", size, len(encryptedData))
			}

			// Decrypt
			encryptedReader2 := bytes.NewReader(encryptedData)
			decryptedReader, err := CreateSSECDecryptedReader(encryptedReader2, customerKey, iv)
			if err != nil {
				t.Fatalf("Failed to create decrypted reader: %v", err)
			}

			decryptedData, err := io.ReadAll(decryptedReader)
			if err != nil {
				t.Fatalf("Failed to read decrypted data: %v", err)
			}

			// Verify decrypted data matches original
			if !bytes.Equal(decryptedData, testData) {
				t.Errorf("Decrypted data doesn't match original for size %d", size)
			}
		})
	}
}

func TestSSECEncryptionWithNilKey(t *testing.T) {
	testData := []byte("test data")
	dataReader := bytes.NewReader(testData)

	// Test encryption with nil key (should pass through)
	encryptedReader, iv, err := CreateSSECEncryptedReader(dataReader, nil)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader with nil key: %v", err)
	}

	result, err := io.ReadAll(encryptedReader)
	if err != nil {
		t.Fatalf("Failed to read from pass-through reader: %v", err)
	}

	if !bytes.Equal(result, testData) {
		t.Error("Data should pass through unchanged when key is nil")
	}

	// Test decryption with nil key (should pass through)
	dataReader2 := bytes.NewReader(testData)
	decryptedReader, err := CreateSSECDecryptedReader(dataReader2, nil, iv)
	if err != nil {
		t.Fatalf("Failed to create decrypted reader with nil key: %v", err)
	}

	result2, err := io.ReadAll(decryptedReader)
	if err != nil {
		t.Fatalf("Failed to read from pass-through reader: %v", err)
	}

	if !bytes.Equal(result2, testData) {
		t.Error("Data should pass through unchanged when key is nil")
	}
}

// TestSSECEncryptionSmallBuffers tests the fix for the critical bug where small buffers
// could corrupt the data stream when reading in chunks smaller than the IV size
func TestSSECEncryptionSmallBuffers(t *testing.T) {
	testData := []byte("This is a test message for small buffer reads")

	// Create customer key
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	md5sumKey3 := md5.Sum(key)
	customerKey := &SSECustomerKey{
		Algorithm: "AES256",
		Key:       key,
		KeyMD5:    base64.StdEncoding.EncodeToString(md5sumKey3[:]),
	}

	// Create encrypted reader
	dataReader := bytes.NewReader(testData)
	encryptedReader, iv, err := CreateSSECEncryptedReader(dataReader, customerKey)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader: %v", err)
	}

	// Read with very small buffers (smaller than IV size of 16 bytes)
	var encryptedData []byte
	smallBuffer := make([]byte, 5) // Much smaller than 16-byte IV

	for {
		n, err := encryptedReader.Read(smallBuffer)
		if n > 0 {
			encryptedData = append(encryptedData, smallBuffer[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Error reading encrypted data: %v", err)
		}
	}

	// Verify we have some encrypted data (IV is in metadata, not in stream)
	if len(encryptedData) == 0 && len(testData) > 0 {
		t.Fatal("Expected encrypted data but got none")
	}

	// Expected size: same as original data (IV is stored in metadata, not in stream)
	if len(encryptedData) != len(testData) {
		t.Errorf("Expected encrypted data size %d (same as original), got %d", len(testData), len(encryptedData))
	}

	// Decrypt and verify
	encryptedReader2 := bytes.NewReader(encryptedData)
	decryptedReader, err := CreateSSECDecryptedReader(encryptedReader2, customerKey, iv)
	if err != nil {
		t.Fatalf("Failed to create decrypted reader: %v", err)
	}

	decryptedData, err := io.ReadAll(decryptedReader)
	if err != nil {
		t.Fatalf("Failed to read decrypted data: %v", err)
	}

	if !bytes.Equal(decryptedData, testData) {
		t.Errorf("Decrypted data doesn't match original.\nOriginal: %s\nDecrypted: %s", testData, decryptedData)
	}
}

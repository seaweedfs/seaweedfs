package s3api

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestSSECHeaderValidation(t *testing.T) {
	// Test valid SSE-C headers
	req := &http.Request{Header: make(http.Header)}

	key := make([]byte, 32) // 256-bit key
	for i := range key {
		key[i] = byte(i)
	}

	keyBase64 := base64.StdEncoding.EncodeToString(key)
	keyMD5 := fmt.Sprintf("%x", md5.Sum(key))

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

	if customerKey.KeyMD5 != strings.ToLower(keyMD5) {
		t.Errorf("Expected key MD5 %s, got %s", strings.ToLower(keyMD5), customerKey.KeyMD5)
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
	keyMD5 := fmt.Sprintf("%x", md5.Sum(key))

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
			keyMD5:    fmt.Sprintf("%x", md5.Sum(make([]byte, 32))),
			wantErr:   ErrInvalidEncryptionAlgorithm,
		},
		{
			name:      "invalid key length",
			algorithm: "AES256",
			key:       base64.StdEncoding.EncodeToString(make([]byte, 16)),
			keyMD5:    fmt.Sprintf("%x", md5.Sum(make([]byte, 16))),
			wantErr:   ErrInvalidEncryptionKey,
		},
		{
			name:      "mismatched MD5",
			algorithm: "AES256",
			key:       base64.StdEncoding.EncodeToString(make([]byte, 32)),
			keyMD5:    "wrongmd5",
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

	customerKey := &SSECustomerKey{
		Algorithm: "AES256",
		Key:       key,
		KeyMD5:    fmt.Sprintf("%x", md5.Sum(key)),
	}

	// Test data
	testData := []byte("Hello, World! This is a test of SSE-C encryption.")

	// Create encrypted reader
	dataReader := bytes.NewReader(testData)
	encryptedReader, err := CreateSSECEncryptedReader(dataReader, customerKey)
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
	decryptedReader, err := CreateSSECDecryptedReader(encryptedReader2, customerKey)
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

func TestSSECMetadataExtraction(t *testing.T) {
	req := &http.Request{Header: make(http.Header)}

	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, "AES256")
	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, "somemd5hash")

	metadata := GetSSECMetadataFromHeaders(req)

	if len(metadata) != 2 {
		t.Errorf("Expected 2 metadata entries, got %d", len(metadata))
	}

	if string(metadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm]) != "AES256" {
		t.Error("Algorithm metadata not extracted correctly")
	}

	if string(metadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5]) != "somemd5hash" {
		t.Error("Key MD5 metadata not extracted correctly")
	}
}

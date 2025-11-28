package s3api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestSSES3EncryptionDecryption tests basic SSE-S3 encryption and decryption
func TestSSES3EncryptionDecryption(t *testing.T) {
	// Generate SSE-S3 key
	sseS3Key, err := GenerateSSES3Key()
	if err != nil {
		t.Fatalf("Failed to generate SSE-S3 key: %v", err)
	}

	// Test data
	testData := []byte("Hello, World! This is a test of SSE-S3 encryption.")

	// Create encrypted reader
	dataReader := bytes.NewReader(testData)
	encryptedReader, iv, err := CreateSSES3EncryptedReader(dataReader, sseS3Key)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader: %v", err)
	}

	// Read encrypted data
	encryptedData, err := io.ReadAll(encryptedReader)
	if err != nil {
		t.Fatalf("Failed to read encrypted data: %v", err)
	}

	// Verify data is actually encrypted (different from original)
	if bytes.Equal(encryptedData, testData) {
		t.Error("Data doesn't appear to be encrypted")
	}

	// Create decrypted reader
	encryptedReader2 := bytes.NewReader(encryptedData)
	decryptedReader, err := CreateSSES3DecryptedReader(encryptedReader2, sseS3Key, iv)
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

// TestSSES3IsRequestInternal tests detection of SSE-S3 requests
func TestSSES3IsRequestInternal(t *testing.T) {
	testCases := []struct {
		name     string
		headers  map[string]string
		expected bool
	}{
		{
			name: "Valid SSE-S3 request",
			headers: map[string]string{
				s3_constants.AmzServerSideEncryption: "AES256",
			},
			expected: true,
		},
		{
			name:     "No SSE headers",
			headers:  map[string]string{},
			expected: false,
		},
		{
			name: "SSE-KMS request",
			headers: map[string]string{
				s3_constants.AmzServerSideEncryption: "aws:kms",
			},
			expected: false,
		},
		{
			name: "SSE-C request",
			headers: map[string]string{
				s3_constants.AmzServerSideEncryptionCustomerAlgorithm: "AES256",
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &http.Request{Header: make(http.Header)}
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}

			result := IsSSES3RequestInternal(req)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

// TestSSES3MetadataSerialization tests SSE-S3 metadata serialization and deserialization
func TestSSES3MetadataSerialization(t *testing.T) {
	// Initialize global key manager
	globalSSES3KeyManager = NewSSES3KeyManager()
	defer func() {
		globalSSES3KeyManager = NewSSES3KeyManager()
	}()

	// Set up the key manager with a super key for testing
	keyManager := GetSSES3KeyManager()
	keyManager.superKey = make([]byte, 32)
	for i := range keyManager.superKey {
		keyManager.superKey[i] = byte(i)
	}

	// Generate SSE-S3 key
	sseS3Key, err := GenerateSSES3Key()
	if err != nil {
		t.Fatalf("Failed to generate SSE-S3 key: %v", err)
	}

	// Add IV to the key
	sseS3Key.IV = make([]byte, 16)
	for i := range sseS3Key.IV {
		sseS3Key.IV[i] = byte(i * 2)
	}

	// Serialize metadata
	serialized, err := SerializeSSES3Metadata(sseS3Key)
	if err != nil {
		t.Fatalf("Failed to serialize SSE-S3 metadata: %v", err)
	}

	if len(serialized) == 0 {
		t.Error("Serialized metadata is empty")
	}

	// Deserialize metadata
	deserializedKey, err := DeserializeSSES3Metadata(serialized, keyManager)
	if err != nil {
		t.Fatalf("Failed to deserialize SSE-S3 metadata: %v", err)
	}

	// Verify key matches
	if !bytes.Equal(deserializedKey.Key, sseS3Key.Key) {
		t.Error("Deserialized key doesn't match original key")
	}

	// Verify IV matches
	if !bytes.Equal(deserializedKey.IV, sseS3Key.IV) {
		t.Error("Deserialized IV doesn't match original IV")
	}

	// Verify algorithm matches
	if deserializedKey.Algorithm != sseS3Key.Algorithm {
		t.Errorf("Algorithm mismatch: expected %s, got %s", sseS3Key.Algorithm, deserializedKey.Algorithm)
	}

	// Verify key ID matches
	if deserializedKey.KeyID != sseS3Key.KeyID {
		t.Errorf("Key ID mismatch: expected %s, got %s", sseS3Key.KeyID, deserializedKey.KeyID)
	}
}

// TestDetectPrimarySSETypeS3 tests detection of SSE-S3 as primary encryption type
func TestDetectPrimarySSETypeS3(t *testing.T) {
	s3a := &S3ApiServer{}

	testCases := []struct {
		name     string
		entry    *filer_pb.Entry
		expected string
	}{
		{
			name: "Single SSE-S3 chunk",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.AmzServerSideEncryption: []byte("AES256"),
				},
				Attributes: &filer_pb.FuseAttributes{},
				Chunks: []*filer_pb.FileChunk{
					{
						FileId:      "1,123",
						Offset:      0,
						Size:        1024,
						SseType:     filer_pb.SSEType_SSE_S3,
						SseMetadata: []byte("metadata"),
					},
				},
			},
			expected: s3_constants.SSETypeS3,
		},
		{
			name: "Multiple SSE-S3 chunks",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.AmzServerSideEncryption: []byte("AES256"),
				},
				Attributes: &filer_pb.FuseAttributes{},
				Chunks: []*filer_pb.FileChunk{
					{
						FileId:      "1,123",
						Offset:      0,
						Size:        1024,
						SseType:     filer_pb.SSEType_SSE_S3,
						SseMetadata: []byte("metadata1"),
					},
					{
						FileId:      "2,456",
						Offset:      1024,
						Size:        1024,
						SseType:     filer_pb.SSEType_SSE_S3,
						SseMetadata: []byte("metadata2"),
					},
				},
			},
			expected: s3_constants.SSETypeS3,
		},
		{
			name: "Mixed SSE-S3 and SSE-KMS chunks (SSE-S3 majority)",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.AmzServerSideEncryption: []byte("AES256"),
				},
				Attributes: &filer_pb.FuseAttributes{},
				Chunks: []*filer_pb.FileChunk{
					{
						FileId:      "1,123",
						Offset:      0,
						Size:        1024,
						SseType:     filer_pb.SSEType_SSE_S3,
						SseMetadata: []byte("metadata1"),
					},
					{
						FileId:      "2,456",
						Offset:      1024,
						Size:        1024,
						SseType:     filer_pb.SSEType_SSE_S3,
						SseMetadata: []byte("metadata2"),
					},
					{
						FileId:      "3,789",
						Offset:      2048,
						Size:        1024,
						SseType:     filer_pb.SSEType_SSE_KMS,
						SseMetadata: []byte("metadata3"),
					},
				},
			},
			expected: s3_constants.SSETypeS3,
		},
		{
			name: "No chunks, SSE-S3 metadata without KMS key ID",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.AmzServerSideEncryption: []byte("AES256"),
				},
				Attributes: &filer_pb.FuseAttributes{},
				Chunks:     []*filer_pb.FileChunk{},
			},
			expected: s3_constants.SSETypeS3,
		},
		{
			name: "No chunks, SSE-KMS metadata with KMS key ID",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.AmzServerSideEncryption:            []byte("AES256"),
					s3_constants.AmzServerSideEncryptionAwsKmsKeyId: []byte("test-key-id"),
				},
				Attributes: &filer_pb.FuseAttributes{},
				Chunks:     []*filer_pb.FileChunk{},
			},
			expected: s3_constants.SSETypeKMS,
		},
		{
			name: "SSE-C chunks",
			entry: &filer_pb.Entry{
				Extended: map[string][]byte{
					s3_constants.AmzServerSideEncryptionCustomerAlgorithm: []byte("AES256"),
				},
				Attributes: &filer_pb.FuseAttributes{},
				Chunks: []*filer_pb.FileChunk{
					{
						FileId:      "1,123",
						Offset:      0,
						Size:        1024,
						SseType:     filer_pb.SSEType_SSE_C,
						SseMetadata: []byte("metadata"),
					},
				},
			},
			expected: s3_constants.SSETypeC,
		},
		{
			name: "Unencrypted",
			entry: &filer_pb.Entry{
				Extended:   map[string][]byte{},
				Attributes: &filer_pb.FuseAttributes{},
				Chunks: []*filer_pb.FileChunk{
					{
						FileId: "1,123",
						Offset: 0,
						Size:   1024,
					},
				},
			},
			expected: "None",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := s3a.detectPrimarySSEType(tc.entry)
			if result != tc.expected {
				t.Errorf("Expected %s, got %s", tc.expected, result)
			}
		})
	}
}

// TestAddSSES3HeadersToResponse tests that SSE-S3 headers are added to responses
func TestAddSSES3HeadersToResponse(t *testing.T) {
	s3a := &S3ApiServer{}

	entry := &filer_pb.Entry{
		Extended: map[string][]byte{
			s3_constants.AmzServerSideEncryption: []byte("AES256"),
		},
		Attributes: &filer_pb.FuseAttributes{},
		Chunks: []*filer_pb.FileChunk{
			{
				FileId:      "1,123",
				Offset:      0,
				Size:        1024,
				SseType:     filer_pb.SSEType_SSE_S3,
				SseMetadata: []byte("metadata"),
			},
		},
	}

	proxyResponse := &http.Response{
		Header: make(http.Header),
	}

	s3a.addSSEHeadersToResponse(proxyResponse, entry)

	algorithm := proxyResponse.Header.Get(s3_constants.AmzServerSideEncryption)
	if algorithm != "AES256" {
		t.Errorf("Expected SSE algorithm AES256, got %s", algorithm)
	}

	// Should NOT have SSE-C or SSE-KMS specific headers
	if proxyResponse.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm) != "" {
		t.Error("Should not have SSE-C customer algorithm header")
	}

	if proxyResponse.Header.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId) != "" {
		t.Error("Should not have SSE-KMS key ID header")
	}
}

// TestSSES3EncryptionWithBaseIV tests multipart encryption with base IV
func TestSSES3EncryptionWithBaseIV(t *testing.T) {
	// Generate SSE-S3 key
	sseS3Key, err := GenerateSSES3Key()
	if err != nil {
		t.Fatalf("Failed to generate SSE-S3 key: %v", err)
	}

	// Generate base IV
	baseIV := make([]byte, 16)
	for i := range baseIV {
		baseIV[i] = byte(i)
	}

	// Test data for two parts
	testData1 := []byte("Part 1 of multipart upload test.")
	testData2 := []byte("Part 2 of multipart upload test.")

	// Encrypt part 1 at offset 0
	dataReader1 := bytes.NewReader(testData1)
	encryptedReader1, iv1, err := CreateSSES3EncryptedReaderWithBaseIV(dataReader1, sseS3Key, baseIV, 0)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader for part 1: %v", err)
	}

	encryptedData1, err := io.ReadAll(encryptedReader1)
	if err != nil {
		t.Fatalf("Failed to read encrypted data for part 1: %v", err)
	}

	// Encrypt part 2 at offset (simulating second part)
	dataReader2 := bytes.NewReader(testData2)
	offset2 := int64(len(testData1))
	encryptedReader2, iv2, err := CreateSSES3EncryptedReaderWithBaseIV(dataReader2, sseS3Key, baseIV, offset2)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader for part 2: %v", err)
	}

	encryptedData2, err := io.ReadAll(encryptedReader2)
	if err != nil {
		t.Fatalf("Failed to read encrypted data for part 2: %v", err)
	}

	// IVs should be different (offset-based)
	if bytes.Equal(iv1, iv2) {
		t.Error("IVs should be different for different offsets")
	}

	// Decrypt part 1
	decryptedReader1, err := CreateSSES3DecryptedReader(bytes.NewReader(encryptedData1), sseS3Key, iv1)
	if err != nil {
		t.Fatalf("Failed to create decrypted reader for part 1: %v", err)
	}

	decryptedData1, err := io.ReadAll(decryptedReader1)
	if err != nil {
		t.Fatalf("Failed to read decrypted data for part 1: %v", err)
	}

	// Decrypt part 2
	decryptedReader2, err := CreateSSES3DecryptedReader(bytes.NewReader(encryptedData2), sseS3Key, iv2)
	if err != nil {
		t.Fatalf("Failed to create decrypted reader for part 2: %v", err)
	}

	decryptedData2, err := io.ReadAll(decryptedReader2)
	if err != nil {
		t.Fatalf("Failed to read decrypted data for part 2: %v", err)
	}

	// Verify decrypted data matches original
	if !bytes.Equal(decryptedData1, testData1) {
		t.Errorf("Decrypted part 1 doesn't match original.\nOriginal: %s\nDecrypted: %s", testData1, decryptedData1)
	}

	if !bytes.Equal(decryptedData2, testData2) {
		t.Errorf("Decrypted part 2 doesn't match original.\nOriginal: %s\nDecrypted: %s", testData2, decryptedData2)
	}
}

// TestSSES3WrongKeyDecryption tests that wrong key fails decryption
func TestSSES3WrongKeyDecryption(t *testing.T) {
	// Generate two different keys
	sseS3Key1, err := GenerateSSES3Key()
	if err != nil {
		t.Fatalf("Failed to generate SSE-S3 key 1: %v", err)
	}

	sseS3Key2, err := GenerateSSES3Key()
	if err != nil {
		t.Fatalf("Failed to generate SSE-S3 key 2: %v", err)
	}

	// Test data
	testData := []byte("Secret data encrypted with key 1")

	// Encrypt with key 1
	dataReader := bytes.NewReader(testData)
	encryptedReader, iv, err := CreateSSES3EncryptedReader(dataReader, sseS3Key1)
	if err != nil {
		t.Fatalf("Failed to create encrypted reader: %v", err)
	}

	encryptedData, err := io.ReadAll(encryptedReader)
	if err != nil {
		t.Fatalf("Failed to read encrypted data: %v", err)
	}

	// Try to decrypt with key 2 (wrong key)
	decryptedReader, err := CreateSSES3DecryptedReader(bytes.NewReader(encryptedData), sseS3Key2, iv)
	if err != nil {
		t.Fatalf("Failed to create decrypted reader: %v", err)
	}

	decryptedData, err := io.ReadAll(decryptedReader)
	if err != nil {
		t.Fatalf("Failed to read decrypted data: %v", err)
	}

	// Decrypted data should NOT match original (wrong key produces garbage)
	if bytes.Equal(decryptedData, testData) {
		t.Error("Decryption with wrong key should not produce correct plaintext")
	}
}

// TestSSES3KeyGeneration tests SSE-S3 key generation
func TestSSES3KeyGeneration(t *testing.T) {
	// Generate multiple keys
	keys := make([]*SSES3Key, 10)
	for i := range keys {
		key, err := GenerateSSES3Key()
		if err != nil {
			t.Fatalf("Failed to generate SSE-S3 key %d: %v", i, err)
		}
		keys[i] = key

		// Verify key properties
		if len(key.Key) != SSES3KeySize {
			t.Errorf("Key %d has wrong size: expected %d, got %d", i, SSES3KeySize, len(key.Key))
		}

		if key.Algorithm != SSES3Algorithm {
			t.Errorf("Key %d has wrong algorithm: expected %s, got %s", i, SSES3Algorithm, key.Algorithm)
		}

		if key.KeyID == "" {
			t.Errorf("Key %d has empty key ID", i)
		}
	}

	// Verify keys are unique
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if bytes.Equal(keys[i].Key, keys[j].Key) {
				t.Errorf("Keys %d and %d are identical (should be unique)", i, j)
			}
			if keys[i].KeyID == keys[j].KeyID {
				t.Errorf("Key IDs %d and %d are identical (should be unique)", i, j)
			}
		}
	}
}

// TestSSES3VariousSizes tests SSE-S3 encryption/decryption with various data sizes
func TestSSES3VariousSizes(t *testing.T) {
	sizes := []int{1, 15, 16, 17, 100, 1024, 4096, 1048576}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			// Generate test data
			testData := make([]byte, size)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			// Generate key
			sseS3Key, err := GenerateSSES3Key()
			if err != nil {
				t.Fatalf("Failed to generate SSE-S3 key: %v", err)
			}

			// Encrypt
			dataReader := bytes.NewReader(testData)
			encryptedReader, iv, err := CreateSSES3EncryptedReader(dataReader, sseS3Key)
			if err != nil {
				t.Fatalf("Failed to create encrypted reader: %v", err)
			}

			encryptedData, err := io.ReadAll(encryptedReader)
			if err != nil {
				t.Fatalf("Failed to read encrypted data: %v", err)
			}

			// Verify encrypted size matches original
			if len(encryptedData) != size {
				t.Errorf("Encrypted size mismatch: expected %d, got %d", size, len(encryptedData))
			}

			// Decrypt
			decryptedReader, err := CreateSSES3DecryptedReader(bytes.NewReader(encryptedData), sseS3Key, iv)
			if err != nil {
				t.Fatalf("Failed to create decrypted reader: %v", err)
			}

			decryptedData, err := io.ReadAll(decryptedReader)
			if err != nil {
				t.Fatalf("Failed to read decrypted data: %v", err)
			}

			// Verify
			if !bytes.Equal(decryptedData, testData) {
				t.Errorf("Decrypted data doesn't match original for size %d", size)
			}
		})
	}
}

// TestSSES3ResponseHeaders tests that SSE-S3 response headers are set correctly
func TestSSES3ResponseHeaders(t *testing.T) {
	w := httptest.NewRecorder()

	// Simulate setting SSE-S3 response headers
	w.Header().Set(s3_constants.AmzServerSideEncryption, SSES3Algorithm)

	// Verify headers
	algorithm := w.Header().Get(s3_constants.AmzServerSideEncryption)
	if algorithm != "AES256" {
		t.Errorf("Expected algorithm AES256, got %s", algorithm)
	}

	// Should NOT have customer key headers
	if w.Header().Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm) != "" {
		t.Error("Should not have SSE-C customer algorithm header")
	}

	if w.Header().Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5) != "" {
		t.Error("Should not have SSE-C customer key MD5 header")
	}

	// Should NOT have KMS key ID
	if w.Header().Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId) != "" {
		t.Error("Should not have SSE-KMS key ID header")
	}
}

// TestSSES3IsEncryptedInternal tests detection of SSE-S3 encryption from metadata
func TestSSES3IsEncryptedInternal(t *testing.T) {
	testCases := []struct {
		name     string
		metadata map[string][]byte
		expected bool
	}{
		{
			name:     "Empty metadata",
			metadata: map[string][]byte{},
			expected: false,
		},
		{
			name: "Valid SSE-S3 metadata with key",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("AES256"),
				s3_constants.SeaweedFSSSES3Key:       []byte("test-key-data"),
			},
			expected: true,
		},
		{
			name: "SSE-S3 header without key (orphaned header - GitHub #7562)",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("AES256"),
			},
			expected: false, // Should not be considered encrypted without the key
		},
		{
			name: "SSE-KMS metadata",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("aws:kms"),
			},
			expected: false,
		},
		{
			name: "SSE-C metadata",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryptionCustomerAlgorithm: []byte("AES256"),
			},
			expected: false,
		},
		{
			name: "Key without header",
			metadata: map[string][]byte{
				s3_constants.SeaweedFSSSES3Key: []byte("test-key-data"),
			},
			expected: false, // Need both header and key
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsSSES3EncryptedInternal(tc.metadata)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

// TestSSES3InvalidMetadataDeserialization tests error handling for invalid metadata
func TestSSES3InvalidMetadataDeserialization(t *testing.T) {
	keyManager := NewSSES3KeyManager()
	keyManager.superKey = make([]byte, 32)

	testCases := []struct {
		name        string
		metadata    []byte
		shouldError bool
	}{
		{
			name:        "Empty metadata",
			metadata:    []byte{},
			shouldError: true,
		},
		{
			name:        "Invalid JSON",
			metadata:    []byte("not valid json"),
			shouldError: true,
		},
		{
			name:        "Missing keyId",
			metadata:    []byte(`{"algorithm":"AES256"}`),
			shouldError: true,
		},
		{
			name:        "Invalid base64 encrypted DEK",
			metadata:    []byte(`{"keyId":"test","algorithm":"AES256","encryptedDEK":"not-valid-base64!","nonce":"dGVzdA=="}`),
			shouldError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DeserializeSSES3Metadata(tc.metadata, keyManager)
			if tc.shouldError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tc.shouldError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// TestGetSSES3Headers tests SSE-S3 header generation
func TestGetSSES3Headers(t *testing.T) {
	headers := GetSSES3Headers()

	if len(headers) == 0 {
		t.Error("Expected headers to be non-empty")
	}

	algorithm, exists := headers[s3_constants.AmzServerSideEncryption]
	if !exists {
		t.Error("Expected AmzServerSideEncryption header to exist")
	}

	if algorithm != "AES256" {
		t.Errorf("Expected algorithm AES256, got %s", algorithm)
	}
}

// TestProcessSSES3Request tests processing of SSE-S3 requests
func TestProcessSSES3Request(t *testing.T) {
	// Initialize global key manager
	globalSSES3KeyManager = NewSSES3KeyManager()
	defer func() {
		globalSSES3KeyManager = NewSSES3KeyManager()
	}()

	// Set up the key manager with a super key for testing
	keyManager := GetSSES3KeyManager()
	keyManager.superKey = make([]byte, 32)
	for i := range keyManager.superKey {
		keyManager.superKey[i] = byte(i)
	}

	// Create SSE-S3 request
	req := httptest.NewRequest("PUT", "/bucket/object", nil)
	req.Header.Set(s3_constants.AmzServerSideEncryption, "AES256")

	// Process request
	metadata, err := ProcessSSES3Request(req)
	if err != nil {
		t.Fatalf("Failed to process SSE-S3 request: %v", err)
	}

	if metadata == nil {
		t.Fatal("Expected metadata to be non-nil")
	}

	// Verify metadata contains SSE algorithm
	if sseAlgo, exists := metadata[s3_constants.AmzServerSideEncryption]; !exists {
		t.Error("Expected SSE algorithm in metadata")
	} else if string(sseAlgo) != "AES256" {
		t.Errorf("Expected AES256, got %s", string(sseAlgo))
	}

	// Verify metadata contains key data
	if _, exists := metadata[s3_constants.SeaweedFSSSES3Key]; !exists {
		t.Error("Expected SSE-S3 key data in metadata")
	}
}

// TestGetSSES3KeyFromMetadata tests extraction of SSE-S3 key from metadata
func TestGetSSES3KeyFromMetadata(t *testing.T) {
	// Initialize global key manager
	globalSSES3KeyManager = NewSSES3KeyManager()
	defer func() {
		globalSSES3KeyManager = NewSSES3KeyManager()
	}()

	// Set up the key manager with a super key for testing
	keyManager := GetSSES3KeyManager()
	keyManager.superKey = make([]byte, 32)
	for i := range keyManager.superKey {
		keyManager.superKey[i] = byte(i)
	}

	// Generate and serialize key
	sseS3Key, err := GenerateSSES3Key()
	if err != nil {
		t.Fatalf("Failed to generate SSE-S3 key: %v", err)
	}

	sseS3Key.IV = make([]byte, 16)
	for i := range sseS3Key.IV {
		sseS3Key.IV[i] = byte(i)
	}

	serialized, err := SerializeSSES3Metadata(sseS3Key)
	if err != nil {
		t.Fatalf("Failed to serialize SSE-S3 metadata: %v", err)
	}

	metadata := map[string][]byte{
		s3_constants.SeaweedFSSSES3Key: serialized,
	}

	// Extract key
	extractedKey, err := GetSSES3KeyFromMetadata(metadata, keyManager)
	if err != nil {
		t.Fatalf("Failed to get SSE-S3 key from metadata: %v", err)
	}

	// Verify key matches
	if !bytes.Equal(extractedKey.Key, sseS3Key.Key) {
		t.Error("Extracted key doesn't match original key")
	}

	if !bytes.Equal(extractedKey.IV, sseS3Key.IV) {
		t.Error("Extracted IV doesn't match original IV")
	}
}

// TestSSES3EnvelopeEncryption tests that envelope encryption works correctly
func TestSSES3EnvelopeEncryption(t *testing.T) {
	// Initialize key manager with a super key
	keyManager := NewSSES3KeyManager()
	keyManager.superKey = make([]byte, 32)
	for i := range keyManager.superKey {
		keyManager.superKey[i] = byte(i + 100)
	}

	// Generate a DEK
	dek := make([]byte, 32)
	for i := range dek {
		dek[i] = byte(i)
	}

	// Encrypt DEK with super key
	encryptedDEK, nonce, err := keyManager.encryptKeyWithSuperKey(dek)
	if err != nil {
		t.Fatalf("Failed to encrypt DEK: %v", err)
	}

	if len(encryptedDEK) == 0 {
		t.Error("Encrypted DEK is empty")
	}

	if len(nonce) == 0 {
		t.Error("Nonce is empty")
	}

	// Decrypt DEK with super key
	decryptedDEK, err := keyManager.decryptKeyWithSuperKey(encryptedDEK, nonce)
	if err != nil {
		t.Fatalf("Failed to decrypt DEK: %v", err)
	}

	// Verify DEK matches
	if !bytes.Equal(decryptedDEK, dek) {
		t.Error("Decrypted DEK doesn't match original DEK")
	}
}

// TestValidateSSES3Key tests SSE-S3 key validation
func TestValidateSSES3Key(t *testing.T) {
	testCases := []struct {
		name        string
		key         *SSES3Key
		shouldError bool
		errorMsg    string
	}{
		{
			name:        "Nil key",
			key:         nil,
			shouldError: true,
			errorMsg:    "SSE-S3 key cannot be nil",
		},
		{
			name: "Valid key",
			key: &SSES3Key{
				Key:       make([]byte, 32),
				KeyID:     "test-key",
				Algorithm: "AES256",
			},
			shouldError: false,
		},
		{
			name: "Valid key with IV",
			key: &SSES3Key{
				Key:       make([]byte, 32),
				KeyID:     "test-key",
				Algorithm: "AES256",
				IV:        make([]byte, 16),
			},
			shouldError: false,
		},
		{
			name: "Invalid key size (too small)",
			key: &SSES3Key{
				Key:       make([]byte, 16),
				KeyID:     "test-key",
				Algorithm: "AES256",
			},
			shouldError: true,
			errorMsg:    "invalid SSE-S3 key size",
		},
		{
			name: "Invalid key size (too large)",
			key: &SSES3Key{
				Key:       make([]byte, 64),
				KeyID:     "test-key",
				Algorithm: "AES256",
			},
			shouldError: true,
			errorMsg:    "invalid SSE-S3 key size",
		},
		{
			name: "Nil key bytes",
			key: &SSES3Key{
				Key:       nil,
				KeyID:     "test-key",
				Algorithm: "AES256",
			},
			shouldError: true,
			errorMsg:    "SSE-S3 key bytes cannot be nil",
		},
		{
			name: "Empty key ID",
			key: &SSES3Key{
				Key:       make([]byte, 32),
				KeyID:     "",
				Algorithm: "AES256",
			},
			shouldError: true,
			errorMsg:    "SSE-S3 key ID cannot be empty",
		},
		{
			name: "Invalid algorithm",
			key: &SSES3Key{
				Key:       make([]byte, 32),
				KeyID:     "test-key",
				Algorithm: "INVALID",
			},
			shouldError: true,
			errorMsg:    "invalid SSE-S3 algorithm",
		},
		{
			name: "Invalid IV length",
			key: &SSES3Key{
				Key:       make([]byte, 32),
				KeyID:     "test-key",
				Algorithm: "AES256",
				IV:        make([]byte, 8), // Wrong size
			},
			shouldError: true,
			errorMsg:    "invalid SSE-S3 IV length",
		},
		{
			name: "Empty IV is allowed (set during encryption)",
			key: &SSES3Key{
				Key:       make([]byte, 32),
				KeyID:     "test-key",
				Algorithm: "AES256",
				IV:        []byte{}, // Empty is OK
			},
			shouldError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSSES3Key(tc.key)
			if tc.shouldError {
				if err == nil {
					t.Error("Expected error but got none")
				} else if tc.errorMsg != "" && !strings.Contains(err.Error(), tc.errorMsg) {
					t.Errorf("Expected error containing %q, got: %v", tc.errorMsg, err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

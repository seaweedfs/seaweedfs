package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// TestSSECIsEncrypted tests detection of SSE-C encryption from metadata
func TestSSECIsEncrypted(t *testing.T) {
	testCases := []struct {
		name     string
		metadata map[string][]byte
		expected bool
	}{
		{
			name:     "Empty metadata",
			metadata: CreateTestMetadata(),
			expected: false,
		},
		{
			name:     "Valid SSE-C metadata",
			metadata: CreateTestMetadataWithSSEC(GenerateTestSSECKey(1)),
			expected: true,
		},
		{
			name: "SSE-C algorithm only",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryptionCustomerAlgorithm: []byte("AES256"),
			},
			expected: true,
		},
		{
			name: "SSE-C key MD5 only",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryptionCustomerKeyMD5: []byte("somemd5"),
			},
			expected: true,
		},
		{
			name: "Other encryption type (SSE-KMS)",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("aws:kms"),
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsSSECEncrypted(tc.metadata)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

// TestSSEKMSIsEncrypted tests detection of SSE-KMS encryption from metadata
func TestSSEKMSIsEncrypted(t *testing.T) {
	testCases := []struct {
		name     string
		metadata map[string][]byte
		expected bool
	}{
		{
			name:     "Empty metadata",
			metadata: CreateTestMetadata(),
			expected: false,
		},
		{
			name: "Valid SSE-KMS metadata",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("aws:kms"),
				s3_constants.AmzEncryptedDataKey:     []byte("encrypted-key"),
			},
			expected: true,
		},
		{
			name: "SSE-KMS algorithm only",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("aws:kms"),
			},
			expected: true,
		},
		{
			name: "SSE-KMS encrypted data key only",
			metadata: map[string][]byte{
				s3_constants.AmzEncryptedDataKey: []byte("encrypted-key"),
			},
			expected: false, // Only encrypted data key without algorithm header should not be considered SSE-KMS
		},
		{
			name: "Other encryption type (SSE-C)",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryptionCustomerAlgorithm: []byte("AES256"),
			},
			expected: false,
		},
		{
			name: "SSE-S3 (AES256)",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("AES256"),
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsSSEKMSEncrypted(tc.metadata)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

// TestSSETypeDiscrimination tests that SSE types don't interfere with each other
func TestSSETypeDiscrimination(t *testing.T) {
	// Test SSE-C headers don't trigger SSE-KMS detection
	t.Run("SSE-C headers don't trigger SSE-KMS", func(t *testing.T) {
		req := CreateTestHTTPRequest("PUT", "/bucket/object", nil)
		keyPair := GenerateTestSSECKey(1)
		SetupTestSSECHeaders(req, keyPair)

		// Should detect SSE-C, not SSE-KMS
		if !IsSSECRequest(req) {
			t.Error("Should detect SSE-C request")
		}
		if IsSSEKMSRequest(req) {
			t.Error("Should not detect SSE-KMS request for SSE-C headers")
		}
	})

	// Test SSE-KMS headers don't trigger SSE-C detection
	t.Run("SSE-KMS headers don't trigger SSE-C", func(t *testing.T) {
		req := CreateTestHTTPRequest("PUT", "/bucket/object", nil)
		SetupTestSSEKMSHeaders(req, "test-key-id")

		// Should detect SSE-KMS, not SSE-C
		if IsSSECRequest(req) {
			t.Error("Should not detect SSE-C request for SSE-KMS headers")
		}
		if !IsSSEKMSRequest(req) {
			t.Error("Should detect SSE-KMS request")
		}
	})

	// Test metadata discrimination
	t.Run("Metadata type discrimination", func(t *testing.T) {
		ssecMetadata := CreateTestMetadataWithSSEC(GenerateTestSSECKey(1))

		// Should detect as SSE-C, not SSE-KMS
		if !IsSSECEncrypted(ssecMetadata) {
			t.Error("Should detect SSE-C encrypted metadata")
		}
		if IsSSEKMSEncrypted(ssecMetadata) {
			t.Error("Should not detect SSE-KMS for SSE-C metadata")
		}
	})
}

// TestSSECParseCorruptedMetadata tests handling of corrupted SSE-C metadata
func TestSSECParseCorruptedMetadata(t *testing.T) {
	testCases := []struct {
		name         string
		metadata     map[string][]byte
		expectError  bool
		errorMessage string
	}{
		{
			name: "Missing algorithm",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryptionCustomerKeyMD5: []byte("valid-md5"),
			},
			expectError: false, // Detection should still work with partial metadata
		},
		{
			name: "Invalid key MD5 format",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryptionCustomerAlgorithm: []byte("AES256"),
				s3_constants.AmzServerSideEncryptionCustomerKeyMD5:    []byte("invalid-base64!"),
			},
			expectError: false, // Detection should work, validation happens later
		},
		{
			name: "Empty values",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryptionCustomerAlgorithm: []byte(""),
				s3_constants.AmzServerSideEncryptionCustomerKeyMD5:    []byte(""),
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test that detection doesn't panic on corrupted metadata
			result := IsSSECEncrypted(tc.metadata)
			// The detection should be robust and not crash
			t.Logf("Detection result for %s: %v", tc.name, result)
		})
	}
}

// TestSSEKMSParseCorruptedMetadata tests handling of corrupted SSE-KMS metadata
func TestSSEKMSParseCorruptedMetadata(t *testing.T) {
	testCases := []struct {
		name     string
		metadata map[string][]byte
	}{
		{
			name: "Invalid encrypted data key",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("aws:kms"),
				s3_constants.AmzEncryptedDataKey:     []byte("invalid-base64!"),
			},
		},
		{
			name: "Invalid encryption context",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption:  []byte("aws:kms"),
				s3_constants.AmzEncryptionContextMeta: []byte("invalid-json"),
			},
		},
		{
			name: "Empty values",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte(""),
				s3_constants.AmzEncryptedDataKey:     []byte(""),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test that detection doesn't panic on corrupted metadata
			result := IsSSEKMSEncrypted(tc.metadata)
			t.Logf("Detection result for %s: %v", tc.name, result)
		})
	}
}

// TestSSEMetadataDeserialization tests SSE-KMS metadata deserialization with various inputs
func TestSSEMetadataDeserialization(t *testing.T) {
	testCases := []struct {
		name        string
		data        []byte
		expectError bool
	}{
		{
			name:        "Empty data",
			data:        []byte{},
			expectError: true,
		},
		{
			name:        "Invalid JSON",
			data:        []byte("invalid-json"),
			expectError: true,
		},
		{
			name:        "Valid JSON but wrong structure",
			data:        []byte(`{"wrong": "structure"}`),
			expectError: false, // Our deserialization might be lenient
		},
		{
			name:        "Null data",
			data:        nil,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DeserializeSSEKMSMetadata(tc.data)
			if tc.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

// TestGeneralSSEDetection tests the general SSE detection that works across types
func TestGeneralSSEDetection(t *testing.T) {
	testCases := []struct {
		name     string
		metadata map[string][]byte
		expected bool
	}{
		{
			name:     "No encryption",
			metadata: CreateTestMetadata(),
			expected: false,
		},
		{
			name:     "SSE-C encrypted",
			metadata: CreateTestMetadataWithSSEC(GenerateTestSSECKey(1)),
			expected: true,
		},
		{
			name: "SSE-KMS encrypted",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("aws:kms"),
			},
			expected: true,
		},
		{
			name: "SSE-S3 encrypted",
			metadata: map[string][]byte{
				s3_constants.AmzServerSideEncryption: []byte("AES256"),
			},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsAnySSEEncrypted(tc.metadata)
			if result != tc.expected {
				t.Errorf("Expected %v, got %v", tc.expected, result)
			}
		})
	}
}

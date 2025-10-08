package s3api

import (
	"fmt"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
)

// TestBucketDefaultSSEKMSEnforcement tests bucket default encryption enforcement
func TestBucketDefaultSSEKMSEnforcement(t *testing.T) {
	kmsKey := SetupTestKMS(t)
	defer kmsKey.Cleanup()

	// Create bucket encryption configuration
	config := &s3_pb.EncryptionConfiguration{
		SseAlgorithm:     "aws:kms",
		KmsKeyId:         kmsKey.KeyID,
		BucketKeyEnabled: false,
	}

	t.Run("Bucket with SSE-KMS default encryption", func(t *testing.T) {
		// Test that default encryption config is properly stored and retrieved
		if config.SseAlgorithm != "aws:kms" {
			t.Errorf("Expected SSE algorithm aws:kms, got %s", config.SseAlgorithm)
		}

		if config.KmsKeyId != kmsKey.KeyID {
			t.Errorf("Expected KMS key ID %s, got %s", kmsKey.KeyID, config.KmsKeyId)
		}
	})

	t.Run("Default encryption headers generation", func(t *testing.T) {
		// Test generating default encryption headers for objects
		headers := GetDefaultEncryptionHeaders(config)

		if headers == nil {
			t.Fatal("Expected default headers, got nil")
		}

		expectedAlgorithm := headers["X-Amz-Server-Side-Encryption"]
		if expectedAlgorithm != "aws:kms" {
			t.Errorf("Expected X-Amz-Server-Side-Encryption header aws:kms, got %s", expectedAlgorithm)
		}

		expectedKeyID := headers["X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"]
		if expectedKeyID != kmsKey.KeyID {
			t.Errorf("Expected X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id header %s, got %s", kmsKey.KeyID, expectedKeyID)
		}
	})

	t.Run("Default encryption detection", func(t *testing.T) {
		// Test IsDefaultEncryptionEnabled
		enabled := IsDefaultEncryptionEnabled(config)
		if !enabled {
			t.Error("Should detect default encryption as enabled")
		}

		// Test with nil config
		enabled = IsDefaultEncryptionEnabled(nil)
		if enabled {
			t.Error("Should detect default encryption as disabled for nil config")
		}

		// Test with empty config
		emptyConfig := &s3_pb.EncryptionConfiguration{}
		enabled = IsDefaultEncryptionEnabled(emptyConfig)
		if enabled {
			t.Error("Should detect default encryption as disabled for empty config")
		}
	})
}

// TestBucketEncryptionConfigValidation tests XML validation of bucket encryption configurations
func TestBucketEncryptionConfigValidation(t *testing.T) {
	testCases := []struct {
		name        string
		xml         string
		expectError bool
		description string
	}{
		{
			name: "Valid SSE-S3 configuration",
			xml: `<ServerSideEncryptionConfiguration>
				<Rule>
					<ApplyServerSideEncryptionByDefault>
						<SSEAlgorithm>AES256</SSEAlgorithm>
					</ApplyServerSideEncryptionByDefault>
				</Rule>
			</ServerSideEncryptionConfiguration>`,
			expectError: false,
			description: "Basic SSE-S3 configuration should be valid",
		},
		{
			name: "Valid SSE-KMS configuration",
			xml: `<ServerSideEncryptionConfiguration>
				<Rule>
					<ApplyServerSideEncryptionByDefault>
						<SSEAlgorithm>aws:kms</SSEAlgorithm>
						<KMSMasterKeyID>test-key-id</KMSMasterKeyID>
					</ApplyServerSideEncryptionByDefault>
				</Rule>
			</ServerSideEncryptionConfiguration>`,
			expectError: false,
			description: "SSE-KMS configuration with key ID should be valid",
		},
		{
			name: "Valid SSE-KMS without key ID",
			xml: `<ServerSideEncryptionConfiguration>
				<Rule>
					<ApplyServerSideEncryptionByDefault>
						<SSEAlgorithm>aws:kms</SSEAlgorithm>
					</ApplyServerSideEncryptionByDefault>
				</Rule>
			</ServerSideEncryptionConfiguration>`,
			expectError: false,
			description: "SSE-KMS without key ID should use default key",
		},
		{
			name: "Invalid XML structure",
			xml: `<ServerSideEncryptionConfiguration>
				<InvalidRule>
					<SSEAlgorithm>AES256</SSEAlgorithm>
				</InvalidRule>
			</ServerSideEncryptionConfiguration>`,
			expectError: true,
			description: "Invalid XML structure should be rejected",
		},
		{
			name: "Empty configuration",
			xml: `<ServerSideEncryptionConfiguration>
			</ServerSideEncryptionConfiguration>`,
			expectError: true,
			description: "Empty configuration should be rejected",
		},
		{
			name: "Invalid algorithm",
			xml: `<ServerSideEncryptionConfiguration>
				<Rule>
					<ApplyServerSideEncryptionByDefault>
						<SSEAlgorithm>INVALID</SSEAlgorithm>
					</ApplyServerSideEncryptionByDefault>
				</Rule>
			</ServerSideEncryptionConfiguration>`,
			expectError: true,
			description: "Invalid algorithm should be rejected",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config, err := encryptionConfigFromXMLBytes([]byte(tc.xml))

			if tc.expectError && err == nil {
				t.Errorf("Expected error for %s, but got none. %s", tc.name, tc.description)
			}

			if !tc.expectError && err != nil {
				t.Errorf("Expected no error for %s, but got: %v. %s", tc.name, err, tc.description)
			}

			if !tc.expectError && config != nil {
				// Validate the parsed configuration
				t.Logf("Successfully parsed config: Algorithm=%s, KeyID=%s",
					config.SseAlgorithm, config.KmsKeyId)
			}
		})
	}
}

// TestBucketEncryptionAPIOperations tests the bucket encryption API operations
func TestBucketEncryptionAPIOperations(t *testing.T) {
	// Note: These tests would normally require a full S3 API server setup
	// For now, we test the individual components

	t.Run("PUT bucket encryption", func(t *testing.T) {
		xml := `<ServerSideEncryptionConfiguration>
			<Rule>
				<ApplyServerSideEncryptionByDefault>
					<SSEAlgorithm>aws:kms</SSEAlgorithm>
					<KMSMasterKeyID>test-key-id</KMSMasterKeyID>
				</ApplyServerSideEncryptionByDefault>
			</Rule>
		</ServerSideEncryptionConfiguration>`

		// Parse the XML to protobuf
		config, err := encryptionConfigFromXMLBytes([]byte(xml))
		if err != nil {
			t.Fatalf("Failed to parse encryption config: %v", err)
		}

		// Verify the parsed configuration
		if config.SseAlgorithm != "aws:kms" {
			t.Errorf("Expected algorithm aws:kms, got %s", config.SseAlgorithm)
		}

		if config.KmsKeyId != "test-key-id" {
			t.Errorf("Expected key ID test-key-id, got %s", config.KmsKeyId)
		}

		// Convert back to XML
		xmlBytes, err := encryptionConfigToXMLBytes(config)
		if err != nil {
			t.Fatalf("Failed to convert config to XML: %v", err)
		}

		// Verify round-trip
		if len(xmlBytes) == 0 {
			t.Error("Generated XML should not be empty")
		}

		// Parse again to verify
		roundTripConfig, err := encryptionConfigFromXMLBytes(xmlBytes)
		if err != nil {
			t.Fatalf("Failed to parse round-trip XML: %v", err)
		}

		if roundTripConfig.SseAlgorithm != config.SseAlgorithm {
			t.Error("Round-trip algorithm doesn't match")
		}

		if roundTripConfig.KmsKeyId != config.KmsKeyId {
			t.Error("Round-trip key ID doesn't match")
		}
	})

	t.Run("GET bucket encryption", func(t *testing.T) {
		// Test getting encryption configuration
		config := &s3_pb.EncryptionConfiguration{
			SseAlgorithm:     "AES256",
			KmsKeyId:         "",
			BucketKeyEnabled: false,
		}

		// Convert to XML for GET response
		xmlBytes, err := encryptionConfigToXMLBytes(config)
		if err != nil {
			t.Fatalf("Failed to convert config to XML: %v", err)
		}

		if len(xmlBytes) == 0 {
			t.Error("Generated XML should not be empty")
		}

		// Verify XML contains expected elements
		xmlStr := string(xmlBytes)
		if !strings.Contains(xmlStr, "AES256") {
			t.Error("XML should contain AES256 algorithm")
		}
	})

	t.Run("DELETE bucket encryption", func(t *testing.T) {
		// Test deleting encryption configuration
		// This would typically involve removing the configuration from metadata

		// Simulate checking if encryption is enabled after deletion
		enabled := IsDefaultEncryptionEnabled(nil)
		if enabled {
			t.Error("Encryption should be disabled after deletion")
		}
	})
}

// TestBucketEncryptionEdgeCases tests edge cases in bucket encryption
func TestBucketEncryptionEdgeCases(t *testing.T) {
	t.Run("Large XML configuration", func(t *testing.T) {
		// Test with a large but valid XML
		largeXML := `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<Rule>
				<ApplyServerSideEncryptionByDefault>
					<SSEAlgorithm>aws:kms</SSEAlgorithm>
					<KMSMasterKeyID>arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012</KMSMasterKeyID>
				</ApplyServerSideEncryptionByDefault>
				<BucketKeyEnabled>true</BucketKeyEnabled>
			</Rule>
		</ServerSideEncryptionConfiguration>`

		config, err := encryptionConfigFromXMLBytes([]byte(largeXML))
		if err != nil {
			t.Fatalf("Failed to parse large XML: %v", err)
		}

		if config.SseAlgorithm != "aws:kms" {
			t.Error("Should parse large XML correctly")
		}
	})

	t.Run("XML with namespaces", func(t *testing.T) {
		// Test XML with namespaces
		namespacedXML := `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<Rule>
				<ApplyServerSideEncryptionByDefault>
					<SSEAlgorithm>AES256</SSEAlgorithm>
				</ApplyServerSideEncryptionByDefault>
			</Rule>
		</ServerSideEncryptionConfiguration>`

		config, err := encryptionConfigFromXMLBytes([]byte(namespacedXML))
		if err != nil {
			t.Fatalf("Failed to parse namespaced XML: %v", err)
		}

		if config.SseAlgorithm != "AES256" {
			t.Error("Should parse namespaced XML correctly")
		}
	})

	t.Run("Malformed XML", func(t *testing.T) {
		malformedXMLs := []string{
			`<ServerSideEncryptionConfiguration><Rule><SSEAlgorithm>AES256</Rule>`,                 // Unclosed tags
			`<ServerSideEncryptionConfiguration><Rule></Rule></ServerSideEncryptionConfiguration>`, // Empty rule
			`not-xml-at-all`, // Not XML
			`<ServerSideEncryptionConfiguration xmlns="invalid-namespace"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`, // Invalid namespace
		}

		for i, malformedXML := range malformedXMLs {
			t.Run(fmt.Sprintf("Malformed XML %d", i), func(t *testing.T) {
				_, err := encryptionConfigFromXMLBytes([]byte(malformedXML))
				if err == nil {
					t.Errorf("Expected error for malformed XML %d, but got none", i)
				}
			})
		}
	})
}

// TestGetDefaultEncryptionHeaders tests generation of default encryption headers
func TestGetDefaultEncryptionHeaders(t *testing.T) {
	testCases := []struct {
		name            string
		config          *s3_pb.EncryptionConfiguration
		expectedHeaders map[string]string
	}{
		{
			name:            "Nil configuration",
			config:          nil,
			expectedHeaders: nil,
		},
		{
			name: "SSE-S3 configuration",
			config: &s3_pb.EncryptionConfiguration{
				SseAlgorithm: "AES256",
			},
			expectedHeaders: map[string]string{
				"X-Amz-Server-Side-Encryption": "AES256",
			},
		},
		{
			name: "SSE-KMS configuration with key",
			config: &s3_pb.EncryptionConfiguration{
				SseAlgorithm: "aws:kms",
				KmsKeyId:     "test-key-id",
			},
			expectedHeaders: map[string]string{
				"X-Amz-Server-Side-Encryption":                "aws:kms",
				"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": "test-key-id",
			},
		},
		{
			name: "SSE-KMS configuration without key",
			config: &s3_pb.EncryptionConfiguration{
				SseAlgorithm: "aws:kms",
			},
			expectedHeaders: map[string]string{
				"X-Amz-Server-Side-Encryption": "aws:kms",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			headers := GetDefaultEncryptionHeaders(tc.config)

			if tc.expectedHeaders == nil && headers != nil {
				t.Error("Expected nil headers but got some")
			}

			if tc.expectedHeaders != nil && headers == nil {
				t.Error("Expected headers but got nil")
			}

			if tc.expectedHeaders != nil && headers != nil {
				for key, expectedValue := range tc.expectedHeaders {
					if actualValue, exists := headers[key]; !exists {
						t.Errorf("Expected header %s not found", key)
					} else if actualValue != expectedValue {
						t.Errorf("Header %s: expected %s, got %s", key, expectedValue, actualValue)
					}
				}

				// Check for unexpected headers
				for key := range headers {
					if _, expected := tc.expectedHeaders[key]; !expected {
						t.Errorf("Unexpected header found: %s", key)
					}
				}
			}
		})
	}
}

package kms

import (
	"encoding/json"
	"testing"
)

func TestCiphertextEnvelope_CreateAndParse(t *testing.T) {
	// Test basic envelope creation and parsing
	provider := "openbao"
	keyID := "test-key-123"
	ciphertext := "vault:v1:abcd1234encrypted"
	providerSpecific := map[string]interface{}{
		"transit_path": "transit",
		"version":      1,
	}

	// Create envelope
	envelopeBlob, err := CreateEnvelope(provider, keyID, ciphertext, providerSpecific)
	if err != nil {
		t.Fatalf("CreateEnvelope failed: %v", err)
	}

	// Verify it's valid JSON
	var jsonCheck map[string]interface{}
	if err := json.Unmarshal(envelopeBlob, &jsonCheck); err != nil {
		t.Fatalf("Envelope is not valid JSON: %v", err)
	}

	// Parse envelope back
	envelope, err := ParseEnvelope(envelopeBlob)
	if err != nil {
		t.Fatalf("ParseEnvelope failed: %v", err)
	}

	// Verify fields
	if envelope.Provider != provider {
		t.Errorf("Provider mismatch: expected %s, got %s", provider, envelope.Provider)
	}
	if envelope.KeyID != keyID {
		t.Errorf("KeyID mismatch: expected %s, got %s", keyID, envelope.KeyID)
	}
	if envelope.Ciphertext != ciphertext {
		t.Errorf("Ciphertext mismatch: expected %s, got %s", ciphertext, envelope.Ciphertext)
	}
	if envelope.Version != 1 {
		t.Errorf("Version mismatch: expected 1, got %d", envelope.Version)
	}
	if envelope.ProviderSpecific == nil {
		t.Error("ProviderSpecific is nil")
	}
}

func TestCiphertextEnvelope_InvalidFormat(t *testing.T) {
	// Test parsing invalid (non-envelope) ciphertext should fail
	rawCiphertext := []byte("some-raw-data-not-json")

	_, err := ParseEnvelope(rawCiphertext)
	if err == nil {
		t.Fatal("Expected error for invalid format, got none")
	}
}

func TestCiphertextEnvelope_ValidationErrors(t *testing.T) {
	// Test validation errors
	testCases := []struct {
		name        string
		provider    string
		keyID       string
		ciphertext  string
		expectError bool
	}{
		{"Valid", "openbao", "key1", "cipher1", false},
		{"Empty provider", "", "key1", "cipher1", true},
		{"Empty keyID", "openbao", "", "cipher1", true},
		{"Empty ciphertext", "openbao", "key1", "", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			envelopeBlob, err := CreateEnvelope(tc.provider, tc.keyID, tc.ciphertext, nil)
			if err != nil && !tc.expectError {
				t.Fatalf("Unexpected error in CreateEnvelope: %v", err)
			}
			if err == nil && tc.expectError {
				t.Fatal("Expected error in CreateEnvelope but got none")
			}

			if !tc.expectError {
				// Test parsing as well
				_, err = ParseEnvelope(envelopeBlob)
				if err != nil {
					t.Fatalf("ParseEnvelope failed: %v", err)
				}
			}
		})
	}
}

func TestCiphertextEnvelope_MultipleProviders(t *testing.T) {
	// Test with different providers to ensure API consistency
	providers := []struct {
		name       string
		keyID      string
		ciphertext string
	}{
		{"openbao", "transit/test-key", "vault:v1:encrypted123"},
		{"gcp", "projects/test/locations/us/keyRings/ring/cryptoKeys/key", "gcp-encrypted-data"},
		{"azure", "https://vault.vault.azure.net/keys/test/123", "azure-encrypted-bytes"},
		{"aws", "arn:aws:kms:us-east-1:123:key/abc", "aws-encrypted-blob"},
	}

	for _, provider := range providers {
		t.Run(provider.name, func(t *testing.T) {
			// Create envelope
			envelopeBlob, err := CreateEnvelope(provider.name, provider.keyID, provider.ciphertext, nil)
			if err != nil {
				t.Fatalf("CreateEnvelope failed for %s: %v", provider.name, err)
			}

			// Parse envelope
			envelope, err := ParseEnvelope(envelopeBlob)
			if err != nil {
				t.Fatalf("ParseEnvelope failed for %s: %v", provider.name, err)
			}

			// Verify consistency
			if envelope.Provider != provider.name {
				t.Errorf("Provider mismatch for %s: expected %s, got %s",
					provider.name, provider.name, envelope.Provider)
			}
			if envelope.KeyID != provider.keyID {
				t.Errorf("KeyID mismatch for %s: expected %s, got %s",
					provider.name, provider.keyID, envelope.KeyID)
			}
		})
	}
}

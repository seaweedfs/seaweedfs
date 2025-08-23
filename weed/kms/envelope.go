package kms

import (
	"encoding/json"
	"fmt"
)

// CiphertextEnvelope represents a standardized format for storing encrypted data
// along with the metadata needed for decryption. This ensures consistent API
// behavior across all KMS providers.
type CiphertextEnvelope struct {
	// Provider identifies which KMS provider was used
	Provider string `json:"provider"`

	// KeyID is the identifier of the key used for encryption
	KeyID string `json:"key_id"`

	// Ciphertext is the encrypted data (base64 encoded for JSON compatibility)
	Ciphertext string `json:"ciphertext"`

	// Version allows for future format changes
	Version int `json:"version"`

	// ProviderSpecific contains provider-specific metadata if needed
	ProviderSpecific map[string]interface{} `json:"provider_specific,omitempty"`
}

// CreateEnvelope creates a ciphertext envelope for consistent KMS provider behavior
func CreateEnvelope(provider, keyID, ciphertext string, providerSpecific map[string]interface{}) ([]byte, error) {
	// Validate required fields
	if provider == "" {
		return nil, fmt.Errorf("provider cannot be empty")
	}
	if keyID == "" {
		return nil, fmt.Errorf("keyID cannot be empty")
	}
	if ciphertext == "" {
		return nil, fmt.Errorf("ciphertext cannot be empty")
	}

	envelope := CiphertextEnvelope{
		Provider:         provider,
		KeyID:            keyID,
		Ciphertext:       ciphertext,
		Version:          1,
		ProviderSpecific: providerSpecific,
	}

	return json.Marshal(envelope)
}

// ParseEnvelope parses a ciphertext envelope to extract key information
func ParseEnvelope(ciphertextBlob []byte) (*CiphertextEnvelope, error) {
	if len(ciphertextBlob) == 0 {
		return nil, fmt.Errorf("ciphertext blob cannot be empty")
	}

	// Parse as envelope format
	var envelope CiphertextEnvelope
	if err := json.Unmarshal(ciphertextBlob, &envelope); err != nil {
		return nil, fmt.Errorf("failed to parse ciphertext envelope: %w", err)
	}

	// Validate required fields
	if envelope.Provider == "" {
		return nil, fmt.Errorf("envelope missing provider field")
	}
	if envelope.KeyID == "" {
		return nil, fmt.Errorf("envelope missing key_id field")
	}
	if envelope.Ciphertext == "" {
		return nil, fmt.Errorf("envelope missing ciphertext field")
	}
	if envelope.Version == 0 {
		envelope.Version = 1 // Default to version 1
	}

	return &envelope, nil
}

package s3api

import (
	"encoding/base64"
	"fmt"
)

// SSE-C metadata keys for storing encryption information in entry metadata
const (
	// MetaSSECIV is the initialization vector used for SSE-C encryption
	// Used by SSE-C to store IV in entry metadata
	MetaSSECIV = "X-SeaweedFS-Server-Side-Encryption-Iv"
)

// StoreSSECIVInMetadata stores the SSE-C IV in entry metadata as base64 encoded string
// Used by SSE-C for storing IV in entry.Extended
func StoreSSECIVInMetadata(metadata map[string][]byte, iv []byte) {
	if len(iv) > 0 {
		metadata[MetaSSECIV] = []byte(base64.StdEncoding.EncodeToString(iv))
	}
}

// GetSSECIVFromMetadata retrieves the SSE-C IV from entry metadata
// Used by SSE-C for retrieving IV from entry.Extended
func GetSSECIVFromMetadata(metadata map[string][]byte) ([]byte, error) {
	if ivBase64, exists := metadata[MetaSSECIV]; exists {
		iv, err := base64.StdEncoding.DecodeString(string(ivBase64))
		if err != nil {
			return nil, fmt.Errorf("failed to decode SSE-C IV from metadata: %w", err)
		}
		return iv, nil
	}
	return nil, fmt.Errorf("SSE-C IV not found in metadata")
}

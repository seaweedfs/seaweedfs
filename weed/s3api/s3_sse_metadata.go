package s3api

import (
	"encoding/base64"
	"fmt"
)

// SSE metadata keys for storing encryption information in entry metadata
const (
	// MetaSSEIV is the initialization vector used for encryption
	// Used by SSE-C to store IV in entry metadata
	MetaSSEIV = "X-SeaweedFS-Server-Side-Encryption-Iv"
)

// StoreIVInMetadata stores the IV in entry metadata as base64 encoded string
// Used by SSE-C for storing IV in entry.Extended
func StoreIVInMetadata(metadata map[string][]byte, iv []byte) {
	if len(iv) > 0 {
		metadata[MetaSSEIV] = []byte(base64.StdEncoding.EncodeToString(iv))
	}
}

// GetIVFromMetadata retrieves the IV from entry metadata
// Used by SSE-C for retrieving IV from entry.Extended
func GetIVFromMetadata(metadata map[string][]byte) ([]byte, error) {
	if ivBase64, exists := metadata[MetaSSEIV]; exists {
		iv, err := base64.StdEncoding.DecodeString(string(ivBase64))
		if err != nil {
			return nil, fmt.Errorf("failed to decode IV from metadata: %w", err)
		}
		return iv, nil
	}
	return nil, fmt.Errorf("IV not found in metadata")
}

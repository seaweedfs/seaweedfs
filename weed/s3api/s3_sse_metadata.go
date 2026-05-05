package s3api

import (
	"encoding/base64"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// StoreSSECIVInMetadata stores the SSE-C IV in entry metadata as raw bytes.
//
// The SSE-C IV is stored as raw bytes (not base64-encoded) in entry.Extended.
// This matches the storage format used by putToFiler() for the same key, and
// what the GET handler reads back at s3api_object_handlers.go for SSE-C
// decryption. Earlier this helper base64-encoded the IV, while putToFiler
// stored raw bytes — leaving CopyObject and GET in disagreement and causing
// 500s on copy and "invalid IV length: expected 16 bytes, got 24" on read of
// copied SSE-C objects (issue #9281).
func StoreSSECIVInMetadata(metadata map[string][]byte, iv []byte) {
	if len(iv) > 0 {
		metadata[s3_constants.SeaweedFSSSEIV] = iv
	}
}

// GetSSECIVFromMetadata retrieves the SSE-C IV from entry metadata.
//
// Reads the IV as raw bytes — the format StoreSSECIVInMetadata and putToFiler
// both write. For backward compatibility with any objects whose IV was stored
// in the legacy base64 form (24 bytes, matching base64 of a 16-byte IV), this
// also accepts and decodes that form.
func GetSSECIVFromMetadata(metadata map[string][]byte) ([]byte, error) {
	stored, exists := metadata[s3_constants.SeaweedFSSSEIV]
	if !exists {
		return nil, fmt.Errorf("SSE-C IV not found in metadata")
	}
	// Raw 16-byte IV — the canonical format.
	if len(stored) == s3_constants.AESBlockSize {
		return stored, nil
	}
	// Legacy base64-encoded form: 24 bytes matches base64 of a 16-byte IV
	// (with two '=' padding chars). Try a base64 decode and validate length.
	if iv, err := base64.StdEncoding.DecodeString(string(stored)); err == nil && len(iv) == s3_constants.AESBlockSize {
		return iv, nil
	}
	return nil, fmt.Errorf("SSE-C IV in metadata has unexpected length %d (expected %d raw or %d base64)",
		len(stored), s3_constants.AESBlockSize, base64.StdEncoding.EncodedLen(s3_constants.AESBlockSize))
}

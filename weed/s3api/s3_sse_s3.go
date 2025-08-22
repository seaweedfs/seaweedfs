package s3api

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	mathrand "math/rand"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// SSE-S3 uses AES-256 encryption with server-managed keys
const (
	SSES3Algorithm = s3_constants.SSEAlgorithmAES256
	SSES3KeySize   = 32 // 256 bits
)

// SSES3Key represents a server-managed encryption key for SSE-S3
type SSES3Key struct {
	Key       []byte
	KeyID     string
	Algorithm string
	IV        []byte // Initialization Vector for this key
}

// IsSSES3RequestInternal checks if the request specifies SSE-S3 encryption
func IsSSES3RequestInternal(r *http.Request) bool {
	sseHeader := r.Header.Get(s3_constants.AmzServerSideEncryption)
	result := sseHeader == SSES3Algorithm

	// Debug: log header detection for SSE-S3 requests
	if result {
		glog.V(4).Infof("SSE-S3 detection: method=%s, header=%q, expected=%q, result=%t, copySource=%q", r.Method, sseHeader, SSES3Algorithm, result, r.Header.Get("X-Amz-Copy-Source"))
	}

	return result
}

// IsSSES3EncryptedInternal checks if the object metadata indicates SSE-S3 encryption
func IsSSES3EncryptedInternal(metadata map[string][]byte) bool {
	if sseAlgorithm, exists := metadata[s3_constants.AmzServerSideEncryption]; exists {
		return string(sseAlgorithm) == SSES3Algorithm
	}
	return false
}

// GenerateSSES3Key generates a new SSE-S3 encryption key
func GenerateSSES3Key() (*SSES3Key, error) {
	key := make([]byte, SSES3KeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("failed to generate SSE-S3 key: %w", err)
	}

	// Generate a key ID for tracking
	keyID := fmt.Sprintf("sse-s3-key-%d", mathrand.Int63())

	return &SSES3Key{
		Key:       key,
		KeyID:     keyID,
		Algorithm: SSES3Algorithm,
	}, nil
}

// CreateSSES3EncryptedReader creates an encrypted reader for SSE-S3
// Returns the encrypted reader and the IV for metadata storage
func CreateSSES3EncryptedReader(reader io.Reader, key *SSES3Key) (io.Reader, []byte, error) {
	// Create AES cipher
	block, err := aes.NewCipher(key.Key)
	if err != nil {
		return nil, nil, fmt.Errorf("create AES cipher: %w", err)
	}

	// Generate random IV
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, nil, fmt.Errorf("generate IV: %w", err)
	}

	// Create CTR mode cipher
	stream := cipher.NewCTR(block, iv)

	// Return encrypted reader and IV separately for metadata storage
	encryptedReader := &cipher.StreamReader{S: stream, R: reader}

	return encryptedReader, iv, nil
}

// CreateSSES3DecryptedReader creates a decrypted reader for SSE-S3 using IV from metadata
func CreateSSES3DecryptedReader(reader io.Reader, key *SSES3Key, iv []byte) (io.Reader, error) {
	// Create AES cipher
	block, err := aes.NewCipher(key.Key)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}

	// Create CTR mode cipher with the provided IV
	stream := cipher.NewCTR(block, iv)

	return &cipher.StreamReader{S: stream, R: reader}, nil
}

// GetSSES3Headers returns the headers for SSE-S3 encrypted objects
func GetSSES3Headers() map[string]string {
	return map[string]string{
		s3_constants.AmzServerSideEncryption: SSES3Algorithm,
	}
}

// SerializeSSES3Metadata serializes SSE-S3 metadata for storage
func SerializeSSES3Metadata(key *SSES3Key) ([]byte, error) {
	if err := ValidateSSES3Key(key); err != nil {
		return nil, err
	}

	// For SSE-S3, we typically don't store the actual key in metadata
	// Instead, we store a key ID or reference that can be used to retrieve the key
	// from a secure key management system

	metadata := map[string]string{
		"algorithm": key.Algorithm,
		"keyId":     key.KeyID,
	}

	// Include IV if present (needed for chunk-level decryption)
	if key.IV != nil {
		metadata["iv"] = base64.StdEncoding.EncodeToString(key.IV)
	}

	// Use JSON for proper serialization
	data, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("marshal SSE-S3 metadata: %w", err)
	}

	return data, nil
}

// DeserializeSSES3Metadata deserializes SSE-S3 metadata from storage and retrieves the actual key
func DeserializeSSES3Metadata(data []byte, keyManager *SSES3KeyManager) (*SSES3Key, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty SSE-S3 metadata")
	}

	// Parse the JSON metadata to extract keyId
	var metadata map[string]string
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse SSE-S3 metadata: %w", err)
	}

	keyID, exists := metadata["keyId"]
	if !exists {
		return nil, fmt.Errorf("keyId not found in SSE-S3 metadata")
	}

	algorithm, exists := metadata["algorithm"]
	if !exists {
		algorithm = s3_constants.SSEAlgorithmAES256 // Default algorithm
	}

	// Retrieve the actual key using the keyId
	if keyManager == nil {
		return nil, fmt.Errorf("key manager is required for SSE-S3 key retrieval")
	}

	key, err := keyManager.GetOrCreateKey(keyID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve SSE-S3 key with ID %s: %w", keyID, err)
	}

	// Verify the algorithm matches
	if key.Algorithm != algorithm {
		return nil, fmt.Errorf("algorithm mismatch: expected %s, got %s", algorithm, key.Algorithm)
	}

	// Restore IV if present in metadata (for chunk-level decryption)
	if ivStr, exists := metadata["iv"]; exists {
		iv, err := base64.StdEncoding.DecodeString(ivStr)
		if err != nil {
			return nil, fmt.Errorf("failed to decode IV: %w", err)
		}
		key.IV = iv
	}

	return key, nil
}

// SSES3KeyManager manages SSE-S3 encryption keys
type SSES3KeyManager struct {
	// In a production system, this would interface with a secure key management system
	keys map[string]*SSES3Key
}

// NewSSES3KeyManager creates a new SSE-S3 key manager
func NewSSES3KeyManager() *SSES3KeyManager {
	return &SSES3KeyManager{
		keys: make(map[string]*SSES3Key),
	}
}

// GetOrCreateKey gets an existing key or creates a new one
func (km *SSES3KeyManager) GetOrCreateKey(keyID string) (*SSES3Key, error) {
	if keyID == "" {
		// Generate new key
		return GenerateSSES3Key()
	}

	// Check if key exists
	if key, exists := km.keys[keyID]; exists {
		return key, nil
	}

	// Create new key
	key, err := GenerateSSES3Key()
	if err != nil {
		return nil, err
	}

	key.KeyID = keyID
	km.keys[keyID] = key

	return key, nil
}

// StoreKey stores a key in the manager
func (km *SSES3KeyManager) StoreKey(key *SSES3Key) {
	km.keys[key.KeyID] = key
}

// GetKey retrieves a key by ID
func (km *SSES3KeyManager) GetKey(keyID string) (*SSES3Key, bool) {
	key, exists := km.keys[keyID]
	return key, exists
}

// Global SSE-S3 key manager instance
var globalSSES3KeyManager = NewSSES3KeyManager()

// GetSSES3KeyManager returns the global SSE-S3 key manager
func GetSSES3KeyManager() *SSES3KeyManager {
	return globalSSES3KeyManager
}

// ProcessSSES3Request processes an SSE-S3 request and returns encryption metadata
func ProcessSSES3Request(r *http.Request) (map[string][]byte, error) {
	if !IsSSES3RequestInternal(r) {
		return nil, nil
	}

	// Generate or retrieve encryption key
	keyManager := GetSSES3KeyManager()
	key, err := keyManager.GetOrCreateKey("")
	if err != nil {
		return nil, fmt.Errorf("get SSE-S3 key: %w", err)
	}

	// Serialize key metadata
	keyData, err := SerializeSSES3Metadata(key)
	if err != nil {
		return nil, fmt.Errorf("serialize SSE-S3 metadata: %w", err)
	}

	// Store key in manager
	keyManager.StoreKey(key)

	// Return metadata
	metadata := map[string][]byte{
		s3_constants.AmzServerSideEncryption: []byte(SSES3Algorithm),
		s3_constants.SeaweedFSSSES3Key:       keyData,
	}

	return metadata, nil
}

// GetSSES3KeyFromMetadata extracts SSE-S3 key from object metadata
func GetSSES3KeyFromMetadata(metadata map[string][]byte, keyManager *SSES3KeyManager) (*SSES3Key, error) {
	keyData, exists := metadata[s3_constants.SeaweedFSSSES3Key]
	if !exists {
		return nil, fmt.Errorf("SSE-S3 key not found in metadata")
	}

	return DeserializeSSES3Metadata(keyData, keyManager)
}

// CreateSSES3EncryptedReaderWithBaseIV creates an encrypted reader using a base IV for multipart upload consistency.
// The returned IV is the offset-derived IV, calculated from the input baseIV and offset.
func CreateSSES3EncryptedReaderWithBaseIV(reader io.Reader, key *SSES3Key, baseIV []byte, offset int64) (io.Reader, []byte /* derivedIV */, error) {
	// Validate key to prevent panics and security issues
	if key == nil {
		return nil, nil, fmt.Errorf("SSES3Key is nil")
	}
	if key.Key == nil || len(key.Key) != SSES3KeySize {
		return nil, nil, fmt.Errorf("invalid SSES3Key: must be %d bytes, got %d", SSES3KeySize, len(key.Key))
	}
	if err := ValidateSSES3Key(key); err != nil {
		return nil, nil, err
	}

	block, err := aes.NewCipher(key.Key)
	if err != nil {
		return nil, nil, fmt.Errorf("create AES cipher: %w", err)
	}

	// Calculate the proper IV with offset to ensure unique IV per chunk/part
	// This prevents the severe security vulnerability of IV reuse in CTR mode
	iv := calculateIVWithOffset(baseIV, offset)

	stream := cipher.NewCTR(block, iv)
	encryptedReader := &cipher.StreamReader{S: stream, R: reader}
	return encryptedReader, iv, nil
}

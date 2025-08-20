package s3api

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
	mathrand "math/rand"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// SSE-S3 uses AES-256 encryption with server-managed keys
const (
	SSES3Algorithm = "AES256"
	SSES3KeySize   = 32 // 256 bits
)

// SSES3Key represents a server-managed encryption key for SSE-S3
type SSES3Key struct {
	Key       []byte
	KeyID     string
	Algorithm string
}

// IsSSES3RequestInternal checks if the request specifies SSE-S3 encryption
func IsSSES3RequestInternal(r *http.Request) bool {
	return r.Header.Get(s3_constants.AmzServerSideEncryption) == SSES3Algorithm
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
func CreateSSES3EncryptedReader(reader io.Reader, key *SSES3Key) (io.Reader, error) {
	// Create AES cipher
	block, err := aes.NewCipher(key.Key)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}

	// Generate random IV
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, fmt.Errorf("generate IV: %w", err)
	}

	// Create CTR mode cipher
	stream := cipher.NewCTR(block, iv)

	// Create encrypted reader that includes IV at the beginning
	return &sses3EncryptedReader{
		reader: reader,
		stream: stream,
		iv:     iv,
		ivSent: false,
	}, nil
}

// CreateSSES3DecryptedReader creates a decrypted reader for SSE-S3
func CreateSSES3DecryptedReader(reader io.Reader, key *SSES3Key) (io.Reader, error) {
	// Create AES cipher
	block, err := aes.NewCipher(key.Key)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}

	return &sses3DecryptedReader{
		reader: reader,
		block:  block,
		ivRead: false,
	}, nil
}

// sses3EncryptedReader implements io.Reader for SSE-S3 encryption
type sses3EncryptedReader struct {
	reader io.Reader
	stream cipher.Stream
	iv     []byte
	ivSent bool
}

func (r *sses3EncryptedReader) Read(p []byte) (int, error) {
	// First, send the IV if not already sent
	if !r.ivSent {
		if len(p) < len(r.iv) {
			return 0, fmt.Errorf("buffer too small for IV")
		}
		copy(p, r.iv)
		r.ivSent = true
		return len(r.iv), nil
	}

	// Read from source
	n, err := r.reader.Read(p)
	if n > 0 {
		// Encrypt the data in place
		r.stream.XORKeyStream(p[:n], p[:n])
	}

	return n, err
}

// sses3DecryptedReader implements io.Reader for SSE-S3 decryption
type sses3DecryptedReader struct {
	reader io.Reader
	block  cipher.Block
	stream cipher.Stream
	ivRead bool
}

func (r *sses3DecryptedReader) Read(p []byte) (int, error) {
	// First, read the IV if not already read
	if !r.ivRead {
		iv := make([]byte, aes.BlockSize)
		if _, err := io.ReadFull(r.reader, iv); err != nil {
			return 0, fmt.Errorf("read IV: %w", err)
		}

		// Create CTR mode cipher with the IV
		r.stream = cipher.NewCTR(r.block, iv)
		r.ivRead = true
	}

	// Read encrypted data
	n, err := r.reader.Read(p)
	if n > 0 {
		// Decrypt the data in place
		r.stream.XORKeyStream(p[:n], p[:n])
	}

	return n, err
}

// GetSSES3Headers returns the headers for SSE-S3 encrypted objects
func GetSSES3Headers() map[string]string {
	return map[string]string{
		s3_constants.AmzServerSideEncryption: SSES3Algorithm,
	}
}

// SerializeSSES3Metadata serializes SSE-S3 metadata for storage
func SerializeSSES3Metadata(key *SSES3Key) ([]byte, error) {
	// For SSE-S3, we typically don't store the actual key in metadata
	// Instead, we store a key ID or reference that can be used to retrieve the key
	// from a secure key management system

	metadata := map[string]string{
		"algorithm": key.Algorithm,
		"keyId":     key.KeyID,
	}

	// In a production system, this would be more sophisticated
	// For now, we'll use a simple JSON-like format
	serialized := fmt.Sprintf(`{"algorithm":"%s","keyId":"%s"}`,
		metadata["algorithm"], metadata["keyId"])

	return []byte(serialized), nil
}

// DeserializeSSES3Metadata deserializes SSE-S3 metadata from storage
func DeserializeSSES3Metadata(data []byte) (*SSES3Key, error) {
	// This is a simplified deserialization
	// In a production system, this would properly parse JSON and retrieve
	// the actual encryption key from a secure key management system

	// For now, we'll generate a new key (this is not correct for production)
	// In reality, we'd use the keyId to retrieve the actual key
	key, err := GenerateSSES3Key()
	if err != nil {
		return nil, fmt.Errorf("generate SSE-S3 key: %w", err)
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
		"sse-s3-key":                         keyData,
	}

	return metadata, nil
}

// GetSSES3KeyFromMetadata extracts SSE-S3 key from object metadata
func GetSSES3KeyFromMetadata(metadata map[string][]byte) (*SSES3Key, error) {
	keyData, exists := metadata["sse-s3-key"]
	if !exists {
		return nil, fmt.Errorf("SSE-S3 key not found in metadata")
	}

	return DeserializeSSES3Metadata(keyData)
}

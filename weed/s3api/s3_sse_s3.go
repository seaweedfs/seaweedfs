package s3api

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

// SerializeSSES3Metadata serializes SSE-S3 metadata for storage using envelope encryption
func SerializeSSES3Metadata(key *SSES3Key) ([]byte, error) {
	if err := ValidateSSES3Key(key); err != nil {
		return nil, err
	}

	// Encrypt the DEK using the global key manager's super key
	keyManager := GetSSES3KeyManager()
	encryptedDEK, nonce, err := keyManager.encryptKeyWithSuperKey(key.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt DEK: %w", err)
	}

	metadata := map[string]string{
		"algorithm":    key.Algorithm,
		"keyId":        key.KeyID,
		"encryptedDEK": base64.StdEncoding.EncodeToString(encryptedDEK),
		"nonce":        base64.StdEncoding.EncodeToString(nonce),
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

// DeserializeSSES3Metadata deserializes SSE-S3 metadata from storage and decrypts the DEK
func DeserializeSSES3Metadata(data []byte, keyManager *SSES3KeyManager) (*SSES3Key, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty SSE-S3 metadata")
	}

	// Parse the JSON metadata
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

	// Decode the encrypted DEK and nonce
	encryptedDEKStr, exists := metadata["encryptedDEK"]
	if !exists {
		return nil, fmt.Errorf("encryptedDEK not found in SSE-S3 metadata")
	}
	encryptedDEK, err := base64.StdEncoding.DecodeString(encryptedDEKStr)
	if err != nil {
		return nil, fmt.Errorf("failed to decode encrypted DEK: %w", err)
	}

	nonceStr, exists := metadata["nonce"]
	if !exists {
		return nil, fmt.Errorf("nonce not found in SSE-S3 metadata")
	}
	nonce, err := base64.StdEncoding.DecodeString(nonceStr)
	if err != nil {
		return nil, fmt.Errorf("failed to decode nonce: %w", err)
	}

	// Decrypt the DEK using the key manager
	if keyManager == nil {
		return nil, fmt.Errorf("key manager is required for SSE-S3 key retrieval")
	}

	dekBytes, err := keyManager.decryptKeyWithSuperKey(encryptedDEK, nonce)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt DEK: %w", err)
	}

	// Reconstruct the key
	key := &SSES3Key{
		Key:       dekBytes,
		KeyID:     keyID,
		Algorithm: algorithm,
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

// SSES3KeyManager manages SSE-S3 encryption keys using envelope encryption
// Instead of storing keys in memory, it uses a super key (KEK) to encrypt/decrypt DEKs
type SSES3KeyManager struct {
	mu          sync.RWMutex
	superKey    []byte               // 256-bit master key (KEK - Key Encryption Key)
	filerClient filer_pb.FilerClient // Filer client for KEK persistence
	kekPath     string               // Path in filer where KEK is stored (e.g., /etc/s3/sse_kek)
}

const (
	// KEK storage directory and file name in filer
	SSES3KEKDirectory = "/etc/s3"
	SSES3KEKParentDir = "/etc"
	SSES3KEKDirName   = "s3"
	SSES3KEKFileName  = "sse_kek"

	// Full KEK path in filer
	defaultKEKPath = SSES3KEKDirectory + "/" + SSES3KEKFileName
)

// NewSSES3KeyManager creates a new SSE-S3 key manager with envelope encryption
func NewSSES3KeyManager() *SSES3KeyManager {
	// This will be initialized properly when attached to an S3ApiServer
	return &SSES3KeyManager{
		kekPath: defaultKEKPath,
	}
}

// InitializeWithFiler initializes the key manager with a filer client
func (km *SSES3KeyManager) InitializeWithFiler(filerClient filer_pb.FilerClient) error {
	km.mu.Lock()
	defer km.mu.Unlock()

	km.filerClient = filerClient

	// Try to load existing KEK from filer
	if err := km.loadSuperKeyFromFiler(); err != nil {
		// Only generate a new key if it does not exist.
		// For other errors (e.g. connectivity), we should fail fast to prevent creating a new key
		// and making existing data undecryptable.
		if errors.Is(err, filer_pb.ErrNotFound) {
			glog.V(1).Infof("SSE-S3 KeyManager: KEK not found, generating new KEK (load from filer %s: %v)", km.kekPath, err)
			if genErr := km.generateAndSaveSuperKeyToFiler(); genErr != nil {
				return fmt.Errorf("failed to generate and save SSE-S3 super key: %w", genErr)
			}
		} else {
			// A different error occurred (e.g., network issue, permission denied).
			// Return the error to prevent starting with a broken state.
			return fmt.Errorf("failed to load SSE-S3 super key from %s: %w", km.kekPath, err)
		}
	} else {
		glog.V(1).Infof("SSE-S3 KeyManager: Loaded KEK from filer %s", km.kekPath)
	}

	return nil
}

// loadSuperKeyFromFiler loads the KEK from the filer
func (km *SSES3KeyManager) loadSuperKeyFromFiler() error {
	if km.filerClient == nil {
		return fmt.Errorf("filer client not initialized")
	}

	// Get the entry from filer
	entry, err := filer_pb.GetEntry(context.Background(), km.filerClient, util.FullPath(km.kekPath))
	if err != nil {
		return fmt.Errorf("failed to get KEK entry from filer: %w", err)
	}

	// Read the content
	if len(entry.Content) == 0 {
		return fmt.Errorf("KEK entry is empty")
	}

	// Decode hex-encoded key
	key, err := hex.DecodeString(string(entry.Content))
	if err != nil {
		return fmt.Errorf("failed to decode KEK: %w", err)
	}

	if len(key) != SSES3KeySize {
		return fmt.Errorf("invalid KEK size: expected %d bytes, got %d", SSES3KeySize, len(key))
	}

	km.superKey = key
	return nil
}

// generateAndSaveSuperKeyToFiler generates a new KEK and saves it to the filer
func (km *SSES3KeyManager) generateAndSaveSuperKeyToFiler() error {
	if km.filerClient == nil {
		return fmt.Errorf("filer client not initialized")
	}

	// Generate a random 256-bit super key (KEK)
	superKey := make([]byte, SSES3KeySize)
	if _, err := io.ReadFull(rand.Reader, superKey); err != nil {
		return fmt.Errorf("failed to generate KEK: %w", err)
	}

	// Encode as hex for storage
	encodedKey := []byte(hex.EncodeToString(superKey))

	// Create the entry in filer
	// First ensure the parent directory exists
	if err := filer_pb.Mkdir(context.Background(), km.filerClient, SSES3KEKParentDir, SSES3KEKDirName, func(entry *filer_pb.Entry) {
		// Set appropriate permissions for the directory
		entry.Attributes.FileMode = uint32(0700 | os.ModeDir)
	}); err != nil {
		// Only ignore "file exists" errors.
		if !strings.Contains(err.Error(), "file exists") {
			return fmt.Errorf("failed to create KEK directory %s: %w", SSES3KEKDirectory, err)
		}
		glog.V(3).Infof("Parent directory %s already exists, continuing.", SSES3KEKDirectory)
	}

	// Create the KEK file
	if err := filer_pb.MkFile(context.Background(), km.filerClient, SSES3KEKDirectory, SSES3KEKFileName, nil, func(entry *filer_pb.Entry) {
		entry.Content = encodedKey
		entry.Attributes.FileMode = 0600 // Read/write for owner only
		entry.Attributes.FileSize = uint64(len(encodedKey))
	}); err != nil {
		return fmt.Errorf("failed to create KEK file in filer: %w", err)
	}

	km.superKey = superKey
	glog.Infof("SSE-S3 KeyManager: Generated and saved new KEK to filer %s", km.kekPath)
	return nil
}

// GetOrCreateKey gets an existing key or creates a new one
// With envelope encryption, we always generate a new DEK since we don't store them
func (km *SSES3KeyManager) GetOrCreateKey(keyID string) (*SSES3Key, error) {
	// Always generate a new key - we use envelope encryption so no need to cache DEKs
	return GenerateSSES3Key()
}

// encryptKeyWithSuperKey encrypts a DEK using the super key (KEK) with AES-GCM
func (km *SSES3KeyManager) encryptKeyWithSuperKey(dek []byte) ([]byte, []byte, error) {
	km.mu.RLock()
	defer km.mu.RUnlock()

	block, err := aes.NewCipher(km.superKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Generate random nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt the DEK
	encryptedDEK := gcm.Seal(nil, nonce, dek, nil)

	return encryptedDEK, nonce, nil
}

// decryptKeyWithSuperKey decrypts a DEK using the super key (KEK) with AES-GCM
func (km *SSES3KeyManager) decryptKeyWithSuperKey(encryptedDEK, nonce []byte) ([]byte, error) {
	km.mu.RLock()
	defer km.mu.RUnlock()

	block, err := aes.NewCipher(km.superKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	if len(nonce) != gcm.NonceSize() {
		return nil, fmt.Errorf("invalid nonce size: expected %d, got %d", gcm.NonceSize(), len(nonce))
	}

	// Decrypt the DEK
	dek, err := gcm.Open(nil, nonce, encryptedDEK, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt DEK: %w", err)
	}

	return dek, nil
}

// StoreKey is now a no-op since we use envelope encryption and don't cache DEKs
// The encrypted DEK is stored in the object metadata, not in the key manager
func (km *SSES3KeyManager) StoreKey(key *SSES3Key) {
	// No-op: With envelope encryption, we don't need to store keys in memory
	// The DEK is encrypted with the super key and stored in object metadata
}

// GetKey is now a no-op since we don't cache keys
// Keys are retrieved by decrypting the encrypted DEK from object metadata
func (km *SSES3KeyManager) GetKey(keyID string) (*SSES3Key, bool) {
	// No-op: With envelope encryption, keys are not cached
	// Each object's metadata contains the encrypted DEK
	return nil, false
}

// Global SSE-S3 key manager instance
var globalSSES3KeyManager = NewSSES3KeyManager()

// GetSSES3KeyManager returns the global SSE-S3 key manager
func GetSSES3KeyManager() *SSES3KeyManager {
	return globalSSES3KeyManager
}

// InitializeGlobalSSES3KeyManager initializes the global key manager with filer access
func InitializeGlobalSSES3KeyManager(s3ApiServer *S3ApiServer) error {
	return globalSSES3KeyManager.InitializeWithFiler(s3ApiServer)
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

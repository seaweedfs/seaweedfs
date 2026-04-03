package s3api

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"golang.org/x/crypto/hkdf"
	"google.golang.org/grpc"
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
// An object is considered SSE-S3 encrypted only if it has BOTH the encryption header
// AND the actual encryption key metadata. This prevents false positives when an object
// has leftover headers from a previous encryption state (e.g., after being decrypted
// during a copy operation). Fixes GitHub issue #7562.
func IsSSES3EncryptedInternal(metadata map[string][]byte) bool {
	// Check for SSE-S3 algorithm header
	sseAlgorithm, hasHeader := metadata[s3_constants.AmzServerSideEncryption]
	if !hasHeader || string(sseAlgorithm) != SSES3Algorithm {
		return false
	}

	// Must also have the actual encryption key to be considered encrypted
	// Without the key, the object cannot be decrypted and should be treated as unencrypted
	_, hasKey := metadata[s3_constants.SeaweedFSSSES3Key]
	return hasKey
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
	decryptReader := &cipher.StreamReader{S: stream, R: reader}

	// Wrap with closer if the underlying reader implements io.Closer
	if closer, ok := reader.(io.Closer); ok {
		return &decryptReaderCloser{
			Reader:           decryptReader,
			underlyingCloser: closer,
		}, nil
	}

	return decryptReader, nil
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

	// security.toml keys (also settable via env vars WEED_SSE_S3_KEK / WEED_SSE_S3_KEY):
	//
	// sse_s3.kek: hex-encoded 256-bit key, same format as /etc/s3/sse_kek.
	//   Drop-in replacement for the filer-stored KEK. If /etc/s3/sse_kek also
	//   exists, the values must match or the server refuses to start.
	//
	// sse_s3.key: any secret string; a 256-bit key is derived via HKDF-SHA256.
	//   Cannot be used while /etc/s3/sse_kek exists — the filer file must be
	//   deleted first (to avoid silently orphaning old data).
	sseS3KEKConfigKey = "sse_s3.kek"
	sseS3KeyConfigKey = "sse_s3.key"
)

// NewSSES3KeyManager creates a new SSE-S3 key manager with envelope encryption
func NewSSES3KeyManager() *SSES3KeyManager {
	// This will be initialized properly when attached to an S3ApiServer
	return &SSES3KeyManager{
		kekPath: defaultKEKPath,
	}
}

// deriveKeyFromSecret derives a 256-bit key from an arbitrary secret string
// using HKDF-SHA256. The derivation is deterministic: the same secret always
// produces the same key.
func deriveKeyFromSecret(secret string) []byte {
	hkdfReader := hkdf.New(sha256.New, []byte(secret), nil, []byte("seaweedfs-sse-s3-kek"))
	key := make([]byte, SSES3KeySize)
	// hkdf.New with SHA-256 and 32-byte output never fails
	io.ReadFull(hkdfReader, key)
	return key
}

// loadFilerKEK tries to load the KEK from /etc/s3/sse_kek on the filer.
// Returns the key bytes on success, nil if the file does not exist or filer
// is not configured, or an error on transient failures (retries internally).
func (km *SSES3KeyManager) loadFilerKEK() ([]byte, error) {
	if km.filerClient == nil {
		return nil, nil // no filer configured
	}
	var lastErr error
	for i := 0; i < 10; i++ {
		err := km.loadSuperKeyFromFiler()
		if err == nil {
			// loadSuperKeyFromFiler sets km.superKey; grab a copy
			key := make([]byte, len(km.superKey))
			copy(key, km.superKey)
			km.superKey = nil // will be set by caller
			return key, nil
		}
		if errors.Is(err, filer_pb.ErrNotFound) {
			return nil, nil // file does not exist
		}
		lastErr = err
		glog.Warningf("SSE-S3 KeyManager: failed to load KEK (attempt %d/10): %v", i+1, err)
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("failed to load KEK from %s after 10 attempts: %w", km.kekPath, lastErr)
}

// InitializeWithFiler initializes the key manager with a filer client.
//
// Key source priority (via security.toml or WEED_ env vars):
//  1. sse_s3.kek (env: WEED_SSE_S3_KEK) — hex-encoded, same format as /etc/s3/sse_kek.
//     If the filer file also exists, they must match.
//  2. sse_s3.key (env: WEED_SSE_S3_KEY) — any string; 256-bit key derived via HKDF.
//     Refused if /etc/s3/sse_kek exists — delete the filer file first.
//  3. Existing /etc/s3/sse_kek on the filer (backward compat).
//  4. SSE-S3 disabled (fail on use, not on startup).
func (km *SSES3KeyManager) InitializeWithFiler(filerClient filer_pb.FilerClient) error {
	km.mu.Lock()
	defer km.mu.Unlock()

	km.filerClient = filerClient

	v := util.GetViper()
	cfgKEK := v.GetString(sseS3KEKConfigKey) // hex-encoded, drop-in for filer file
	cfgKey := v.GetString(sseS3KeyConfigKey)  // any string, HKDF-derived

	if cfgKEK != "" && cfgKey != "" {
		return fmt.Errorf("only one of %s and %s may be set, not both", sseS3KEKConfigKey, sseS3KeyConfigKey)
	}

	// --- Case 1: sse_s3.kek (hex, same format as filer file) ---
	if cfgKEK != "" {
		key, err := hex.DecodeString(cfgKEK)
		if err != nil {
			return fmt.Errorf("invalid %s: must be hex-encoded: %w", sseS3KEKConfigKey, err)
		}
		if len(key) != SSES3KeySize {
			return fmt.Errorf("invalid %s: must be %d bytes (%d hex chars), got %d bytes",
				sseS3KEKConfigKey, SSES3KeySize, SSES3KeySize*2, len(key))
		}

		// If filer file exists, verify it matches
		filerKey, err := km.loadFilerKEK()
		if err != nil {
			return err
		}
		if filerKey != nil && !bytes.Equal(filerKey, key) {
			return fmt.Errorf("%s does not match existing %s — they must be identical or remove the filer file first",
				sseS3KEKConfigKey, km.kekPath)
		}

		km.superKey = key
		glog.V(0).Infof("SSE-S3 KeyManager: Loaded KEK from %s config", sseS3KEKConfigKey)
		return nil
	}

	// --- Case 2: sse_s3.key (any string, HKDF-derived) ---
	if cfgKey != "" {
		// Refuse if filer file exists — user must delete it first
		filerKey, err := km.loadFilerKEK()
		if err != nil {
			return err
		}
		if filerKey != nil {
			return fmt.Errorf("%s cannot be used while %s exists on the filer — "+
				"delete the filer file first (weed shell: fs.rm %s) to avoid orphaning data encrypted with the old KEK",
				sseS3KeyConfigKey, km.kekPath, km.kekPath)
		}

		km.superKey = deriveKeyFromSecret(cfgKey)
		glog.V(0).Infof("SSE-S3 KeyManager: Derived KEK from %s config", sseS3KeyConfigKey)
		return nil
	}

	// --- Case 3: Load existing filer KEK (backward compatibility) ---
	filerKey, err := km.loadFilerKEK()
	if err != nil {
		return err
	}
	if filerKey != nil {
		km.superKey = filerKey
		glog.V(1).Infof("SSE-S3 KeyManager: Loaded KEK from filer %s", km.kekPath)
		glog.V(0).Infof("SSE-S3 KeyManager: Consider setting %s in security.toml (or WEED_SSE_S3_KEK env var) instead of storing KEK on filer", sseS3KEKConfigKey)
		return nil
	}

	// --- Case 4: No env var, no filer file — SSE-S3 disabled ---
	glog.V(0).Infof("SSE-S3 KeyManager: No KEK configured. SSE-S3 encryption is disabled. "+
		"Set %s or %s in security.toml to enable it.", sseS3KEKConfigKey, sseS3KeyConfigKey)
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
		// Only ignore "already exists" errors.
		if !errors.Is(err, filer_pb.ErrEntryAlreadyExists) {
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

	if len(km.superKey) == 0 {
		return nil, nil, fmt.Errorf("SSE-S3 encryption is not configured — set %s or %s in security.toml", sseS3KEKConfigKey, sseS3KeyConfigKey)
	}

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

	if len(km.superKey) == 0 {
		return nil, fmt.Errorf("SSE-S3 decryption is not configured — set %s or %s in security.toml", sseS3KEKConfigKey, sseS3KeyConfigKey)
	}

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

// GetMasterKey returns a derived key from the master KEK for STS signing
// This uses HKDF to isolate the STS security domain from the SSE-S3 domain
func (km *SSES3KeyManager) GetMasterKey() []byte {
	km.mu.RLock()
	defer km.mu.RUnlock()

	if len(km.superKey) == 0 {
		return nil
	}

	// Derive a separate key for STS to isolate security domains
	// We use the KEK as the secret, and "seaweedfs-sts-signing-key" as the info
	hkdfReader := hkdf.New(sha256.New, km.superKey, nil, []byte("seaweedfs-sts-signing-key"))
	derived := make([]byte, 32) // 256-bit derived key
	if _, err := io.ReadFull(hkdfReader, derived); err != nil {
		glog.Errorf("Failed to derive STS key: %v", err)
		return nil
	}
	return derived
}

// Global SSE-S3 key manager instance
var globalSSES3KeyManager = NewSSES3KeyManager()

// GetSSES3KeyManager returns the global SSE-S3 key manager
func GetSSES3KeyManager() *SSES3KeyManager {
	return globalSSES3KeyManager
}

// KeyManagerFilerClient wraps wdclient.FilerClient to satisfy filer_pb.FilerClient interface
type KeyManagerFilerClient struct {
	*wdclient.FilerClient
	grpcDialOption grpc.DialOption
}

func (k *KeyManagerFilerClient) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (k *KeyManagerFilerClient) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	filerAddress := k.GetCurrentFiler()
	if filerAddress == "" {
		return fmt.Errorf("no filer available")
	}
	return pb.WithGrpcFilerClient(streamingMode, 0, filerAddress, k.grpcDialOption, fn)
}

// InitializeGlobalSSES3KeyManager initializes the global key manager with filer access
func InitializeGlobalSSES3KeyManager(filerClient *wdclient.FilerClient, grpcDialOption grpc.DialOption) error {
	wrapper := &KeyManagerFilerClient{
		FilerClient:    filerClient,
		grpcDialOption: grpcDialOption,
	}
	return globalSSES3KeyManager.InitializeWithFiler(wrapper)
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

// GetSSES3IV extracts the IV for single-part SSE-S3 objects
// Priority: 1) object-level metadata (for inline/small files), 2) first chunk metadata
func GetSSES3IV(entry *filer_pb.Entry, sseS3Key *SSES3Key, keyManager *SSES3KeyManager) ([]byte, error) {
	// First check if IV is in the object-level key (for small/inline files)
	if len(sseS3Key.IV) > 0 {
		return sseS3Key.IV, nil
	}

	// Fallback: Get IV from first chunk's metadata (for chunked files)
	if len(entry.GetChunks()) > 0 {
		chunk := entry.GetChunks()[0]
		if len(chunk.GetSseMetadata()) > 0 {
			chunkKey, err := DeserializeSSES3Metadata(chunk.GetSseMetadata(), keyManager)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize chunk SSE-S3 metadata: %w", err)
			}
			if len(chunkKey.IV) > 0 {
				return chunkKey.IV, nil
			}
		}
	}

	return nil, fmt.Errorf("SSE-S3 IV not found in object or chunk metadata")
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
	// Skip is not used here because we're encrypting from the start (not reading a range)
	iv, _ := calculateIVWithOffset(baseIV, offset)

	stream := cipher.NewCTR(block, iv)
	encryptedReader := &cipher.StreamReader{S: stream, R: reader}
	return encryptedReader, iv, nil
}

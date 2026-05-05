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
	"strings"
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
	Key           []byte
	KeyID         string
	Algorithm     string
	IV            []byte // Initialization Vector for this key
	KeyCommitment []byte // HMAC-SHA256 commitment binding key to IV+algorithm
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
	// IV comes from object metadata, which is mutable. Validate before passing
	// to cipher.NewCTR so a tampered length produces an error rather than the
	// crypto/cipher panic the documentation specifies.
	if err := ValidateIV(iv, "SSE-S3 IV"); err != nil {
		return nil, err
	}

	// Verify key commitment before decryption if one exists in metadata
	if err := VerifyKeyCommitment(key.Key, iv, key.Algorithm, key.KeyCommitment); err != nil {
		return nil, err
	}

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
		// Compute and store key commitment binding key ↔ IV + algorithm
		commitment := ComputeKeyCommitment(key.Key, key.IV, key.Algorithm)
		metadata["keyCommitment"] = base64.StdEncoding.EncodeToString(commitment)
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

	// Restore key commitment if present (for tamper detection)
	if commitStr, exists := metadata["keyCommitment"]; exists {
		commitment, err := base64.StdEncoding.DecodeString(commitStr)
		if err != nil {
			return nil, fmt.Errorf("failed to decode key commitment: %w", err)
		}
		key.KeyCommitment = commitment
	}

	return key, nil
}

// SSES3KeyManager manages SSE-S3 encryption keys using envelope encryption
// Instead of storing keys in memory, it uses a super key (KEK) to encrypt/decrypt DEKs
type SSES3KeyManager struct {
	mu             sync.RWMutex
	superKey       []byte               // 256-bit master key (KEK - Key Encryption Key)
	filerClient    filer_pb.FilerClient // Filer client for KEK persistence
	kekPath        string               // Path in filer where KEK is stored (e.g., /etc/s3/sse_kek)
	kekPassphrase  string               // If set, KEK is encrypted at rest using a key derived from this passphrase
}

const (
	// KEK storage layout on the filer. The migration code paths
	// updateKEKContent / generateAndSaveSuperKeyToFiler rely on the directory
	// + filename split; defaultKEKPath is the joined form kept for the
	// existing reader code.
	SSES3KEKDirectory = "/etc/s3"
	SSES3KEKParentDir = "/etc"
	SSES3KEKDirName   = "s3"
	SSES3KEKFileName  = "sse_kek"

	// Legacy KEK path on the filer (backward compatibility)
	defaultKEKPath = SSES3KEKDirectory + "/" + SSES3KEKFileName

	// security.toml keys (also settable via env vars WEED_S3_SSE_KEK / WEED_S3_SSE_KEY):
	//
	// s3.sse.kek: hex-encoded 256-bit key, same format as /etc/s3/sse_kek.
	//   Drop-in replacement for the filer-stored KEK. If /etc/s3/sse_kek also
	//   exists, the values must match or the server refuses to start.
	//
	// s3.sse.key: any secret string; a 256-bit key is derived via HKDF-SHA256.
	//   Cannot be used while /etc/s3/sse_kek exists — the filer file must be
	//   deleted first (to avoid silently orphaning old data).
	sseS3KEKConfigKey           = "s3.sse.kek"
	sseS3KeyConfigKey           = "s3.sse.key"
	sseS3KEKPassphraseConfigKey = "s3.sse.kek.passphrase"
)

// legacyKEKWrappingSalt is the fixed salt the original implementation used
// for HKDF derivation. It is retained for backward compatibility — KEKs
// wrapped before per-installation salts shipped (the v1 format below) are
// still unwrappable. New writes always use a random salt.
var legacyKEKWrappingSalt = []byte("seaweedfs-sse-s3-kek-wrapping-v1")

// kekWrappedV2Magic identifies the new on-disk format that prefixes the
// wrapped KEK with a random salt. Seeing this magic at byte 0 of the
// decoded payload tells unwrapKEK to read the per-installation salt
// instead of falling back to legacyKEKWrappingSalt.
var kekWrappedV2Magic = []byte{0x53, 0x57, 0x76, 0x32} // "SWv2"

// kekRandomSaltSize is the per-installation salt length in bytes for HKDF.
// 32 bytes matches the SHA-256 output and is the standard recommendation.
const kekRandomSaltSize = 32


// NewSSES3KeyManager creates a new SSE-S3 key manager with envelope encryption.
// If kekPassphrase is non-empty, the KEK is encrypted at rest using a key derived from it.
func NewSSES3KeyManager(kekPassphrase ...string) *SSES3KeyManager {
	km := &SSES3KeyManager{
		kekPath: defaultKEKPath,
	}
	if len(kekPassphrase) > 0 {
		km.kekPassphrase = kekPassphrase[0]
	}
	return km
}

// deriveWrappingKey derives a 256-bit AES key from the configured passphrase
// using HKDF-SHA256 with the supplied salt. Per-installation random salts
// land in the v2 format; the legacy fixed salt is still accepted for KEKs
// that were wrapped before random salts shipped.
func (km *SSES3KeyManager) deriveWrappingKey(salt []byte) ([]byte, error) {
	if km.kekPassphrase == "" {
		return nil, fmt.Errorf("no KEK passphrase configured")
	}
	hkdfReader := hkdf.New(sha256.New, []byte(km.kekPassphrase), salt, []byte("kek-wrapping"))
	wrappingKey := make([]byte, SSES3KeySize)
	if _, err := io.ReadFull(hkdfReader, wrappingKey); err != nil {
		return nil, fmt.Errorf("HKDF derive wrapping key: %w", err)
	}
	return wrappingKey, nil
}

// wrapKEK encrypts the KEK using AES-GCM with a freshly-derived wrapping
// key. Output is base64(magic || salt || nonce || ciphertext+tag) — the
// random salt is the defence against rainbow-table precomputation against a
// shared passphrase, and storing it next to the ciphertext means the
// installation can rotate the passphrase without having to migrate the salt
// separately.
func (km *SSES3KeyManager) wrapKEK(kek []byte) ([]byte, error) {
	salt := make([]byte, kekRandomSaltSize)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, fmt.Errorf("generate KEK salt: %w", err)
	}
	wrappingKey, err := km.deriveWrappingKey(salt)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(wrappingKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	header := make([]byte, 0, len(kekWrappedV2Magic)+len(salt))
	header = append(header, kekWrappedV2Magic...)
	header = append(header, salt...)
	sealed := gcm.Seal(append(header, nonce...), nonce, kek, nil) // magic || salt || nonce || ciphertext+tag
	return []byte(base64.StdEncoding.EncodeToString(sealed)), nil
}

// unwrapKEK decrypts a wrapped KEK produced by wrapKEK. Two on-disk formats
// are accepted:
//
//	v2 (preferred): magic("SWv2") || salt || nonce || ciphertext+tag — the
//	    salt is read from the payload before HKDF runs.
//	v1 (legacy):    nonce || ciphertext+tag — falls back to the fixed
//	    legacyKEKWrappingSalt; rewrapping into v2 happens via the migration
//	    path in loadSuperKeyFromFiler.
//
// The returned `isV2` flag tells the caller which format was on disk, so
// the migration path can rewrap legacy entries without re-decoding the
// base64 payload a second time.
func (km *SSES3KeyManager) unwrapKEK(wrapped []byte) (kek []byte, isV2 bool, err error) {
	raw, err := base64.StdEncoding.DecodeString(string(wrapped))
	if err != nil {
		return nil, false, fmt.Errorf("base64 decode wrapped KEK: %w", err)
	}

	salt := legacyKEKWrappingSalt
	payload := raw
	if len(raw) > len(kekWrappedV2Magic)+kekRandomSaltSize && bytes.Equal(raw[:len(kekWrappedV2Magic)], kekWrappedV2Magic) {
		salt = raw[len(kekWrappedV2Magic) : len(kekWrappedV2Magic)+kekRandomSaltSize]
		payload = raw[len(kekWrappedV2Magic)+kekRandomSaltSize:]
		isV2 = true
	}

	wrappingKey, err := km.deriveWrappingKey(salt)
	if err != nil {
		return nil, false, err
	}
	block, err := aes.NewCipher(wrappingKey)
	if err != nil {
		return nil, false, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, false, err
	}
	if len(payload) < gcm.NonceSize() {
		return nil, false, fmt.Errorf("wrapped KEK too short")
	}
	nonce := payload[:gcm.NonceSize()]
	ciphertext := payload[gcm.NonceSize():]
	out, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, false, err
	}
	return out, isV2, nil
}

// deriveKeyFromSecret derives a 256-bit key from an arbitrary secret string
// using HKDF-SHA256. The derivation is deterministic: the same secret always
// produces the same key.
func deriveKeyFromSecret(secret string) ([]byte, error) {
	hkdfReader := hkdf.New(sha256.New, []byte(secret), nil, []byte("seaweedfs-sse-s3-kek"))
	key := make([]byte, SSES3KeySize)
	if _, err := io.ReadFull(hkdfReader, key); err != nil {
		return nil, fmt.Errorf("failed to derive key: %w", err)
	}
	return key, nil
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
//  1. s3.sse.kek (env: WEED_S3_SSE_KEK) — hex-encoded, same format as /etc/s3/sse_kek.
//     If the filer file also exists, they must match.
//  2. s3.sse.key (env: WEED_S3_SSE_KEY) — any string; 256-bit key derived via HKDF.
//     Refused if /etc/s3/sse_kek exists — delete the filer file first.
//  3. Existing /etc/s3/sse_kek on the filer (backward compat).
//  4. SSE-S3 disabled (fail on first encrypt/decrypt attempt).
func (km *SSES3KeyManager) InitializeWithFiler(filerClient filer_pb.FilerClient) error {
	// Set filerClient under lock, then release — the rest may do slow I/O
	// (filer retries with sleep) and must not block encrypt/decrypt callers.
	km.mu.Lock()
	km.filerClient = filerClient
	km.mu.Unlock()

	v := util.GetViper()
	cfgKEK := v.GetString(sseS3KEKConfigKey) // hex-encoded, drop-in for filer file
	cfgKey := v.GetString(sseS3KeyConfigKey) // any string, HKDF-derived

	if cfgKEK != "" && cfgKey != "" {
		return fmt.Errorf("only one of %s and %s may be set, not both", sseS3KEKConfigKey, sseS3KeyConfigKey)
	}

	var resolvedKey []byte

	switch {
	// --- Case 1: s3.sse.kek (hex, same format as filer file) ---
	case cfgKEK != "":
		key, err := hex.DecodeString(cfgKEK)
		if err != nil {
			return fmt.Errorf("invalid %s: must be hex-encoded: %w", sseS3KEKConfigKey, err)
		}
		if len(key) != SSES3KeySize {
			return fmt.Errorf("invalid %s: must be %d bytes (%d hex chars), got %d bytes",
				sseS3KEKConfigKey, SSES3KeySize, SSES3KeySize*2, len(key))
		}

		// Best-effort consistency check: if the filer file exists, warn on
		// mismatch. A temporarily unreachable filer must not block startup
		// when the operator has explicitly provided a KEK.
		filerKey, err := km.loadFilerKEK()
		if err != nil {
			glog.Warningf("SSE-S3 KeyManager: could not reach filer to verify %s against %s: %v (proceeding with configured KEK)",
				sseS3KEKConfigKey, km.kekPath, err)
		} else if filerKey != nil && !bytes.Equal(filerKey, key) {
			return fmt.Errorf("%s does not match existing %s — "+
				"use the same key value as the filer file, or migrate existing data to the new key. "+
				"See the Server-Side-Encryption wiki for migration steps",
				sseS3KEKConfigKey, km.kekPath)
		}

		resolvedKey = key
		glog.V(0).Infof("SSE-S3 KeyManager: Loaded KEK from %s config", sseS3KEKConfigKey)

	// --- Case 2: s3.sse.key (any string, HKDF-derived) ---
	case cfgKey != "":
		// If the filer still has a legacy KEK file, the operator must migrate
		// existing data first — using a derived key would silently orphan
		// objects encrypted with the old KEK.
		filerKey, err := km.loadFilerKEK()
		if err != nil {
			glog.Warningf("SSE-S3 KeyManager: could not reach filer to check for legacy %s: %v (proceeding with configured key)",
				km.kekPath, err)
		} else if filerKey != nil {
			return fmt.Errorf("%s cannot be used while %s exists on the filer — "+
				"existing objects are encrypted with the filer KEK. "+
				"Migrate to %s first (copy the filer KEK value) or follow the key-rotation steps in the Server-Side-Encryption wiki",
				sseS3KeyConfigKey, km.kekPath, sseS3KEKConfigKey)
		}

		derived, err := deriveKeyFromSecret(cfgKey)
		if err != nil {
			return err
		}
		resolvedKey = derived
		glog.V(0).Infof("SSE-S3 KeyManager: Derived KEK from %s config", sseS3KeyConfigKey)

	// --- Case 3: Load existing filer KEK (backward compatibility) ---
	default:
		filerKey, err := km.loadFilerKEK()
		if err != nil {
			return err
		}
		if filerKey != nil {
			resolvedKey = filerKey
			glog.V(1).Infof("SSE-S3 KeyManager: Loaded KEK from filer %s", km.kekPath)
			glog.V(0).Infof("SSE-S3 KeyManager: Consider setting %s in security.toml instead of storing KEK on filer", sseS3KEKConfigKey)
		} else {
			// --- Case 4: Nothing configured — SSE-S3 disabled ---
			glog.V(0).Infof("SSE-S3 KeyManager: No KEK configured. SSE-S3 encryption is disabled. "+
				"Set %s or %s in security.toml to enable it.", sseS3KEKConfigKey, sseS3KeyConfigKey)
		}
	}

	// Only hold the lock to write the final state.
	km.mu.Lock()
	km.superKey = resolvedKey
	km.mu.Unlock()
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

	var key []byte
	if km.kekPassphrase != "" {
		// Try to unwrap encrypted KEK first
		var wasV2 bool
		key, wasV2, err = km.unwrapKEK(entry.Content)
		if err == nil {
			// Successful unwrap: if the payload was the legacy fixed-salt
			// format, opportunistically rewrap it under a fresh per-installation
			// salt so the next restart picks up the stronger format. The
			// version flag comes straight out of unwrapKEK, avoiding a second
			// base64 decode pass over the same content.
			if !wasV2 {
				if rewrapped, wrapErr := km.wrapKEK(key); wrapErr != nil {
					glog.Warningf("SSE-S3 KeyManager: failed to rewrap legacy fixed-salt KEK to v2: %v", wrapErr)
				} else if updErr := km.updateKEKContent(rewrapped); updErr != nil {
					glog.Warningf("SSE-S3 KeyManager: failed to persist v2-rewrapped KEK: %v", updErr)
				} else {
					glog.V(1).Infof("SSE-S3 KeyManager: migrated KEK from fixed-salt v1 to per-installation salt v2")
				}
			}
		} else {
			// Fall back: maybe this is a legacy plaintext hex KEK — try to decode and re-wrap
			legacyKey, hexErr := hex.DecodeString(string(entry.Content))
			if hexErr != nil || len(legacyKey) != SSES3KeySize {
				return fmt.Errorf("failed to unwrap KEK: %w", err)
			}
			glog.Warningf("SSE-S3 KeyManager: migrating plaintext KEK to encrypted storage")
			key = legacyKey
			// Re-save in encrypted form. Both failure modes used to be swallowed,
			// which left the KEK on disk in plaintext while startup proceeded —
			// an operator setting a passphrase saw a silent no-op and no signal
			// that the migration had failed. Log loudly so the next restart
			// makes the unmigrated state obvious; we still load the in-memory
			// key so the server stays up.
			wrapped, wrapErr := km.wrapKEK(key)
			if wrapErr != nil {
				glog.Errorf("SSE-S3 KeyManager: failed to wrap legacy KEK during migration; KEK remains plaintext on filer: %v", wrapErr)
			} else if updErr := km.updateKEKContent(wrapped); updErr != nil {
				glog.Errorf("SSE-S3 KeyManager: failed to persist wrapped KEK during migration; KEK remains plaintext on filer: %v", updErr)
			}
		}
	} else {
		// Legacy plaintext hex mode
		glog.Warningf("SSE-S3 KeyManager: KEK stored in plaintext — set a KEK passphrase for encrypted storage")
		key, err = hex.DecodeString(string(entry.Content))
		if err != nil {
			return fmt.Errorf("failed to decode KEK: %w", err)
		}
	}

	if len(key) != SSES3KeySize {
		return fmt.Errorf("invalid KEK size: expected %d bytes, got %d", SSES3KeySize, len(key))
	}

	km.superKey = key
	return nil
}

// updateKEKContent overwrites the existing KEK file content in the filer.
// Used by the plaintext→encrypted migration path and by the v1→v2 salt
// rewrap; both run after a successful read of the current KEK, so the
// entry is guaranteed to exist. MkFile uses CreateEntry which fails with
// ErrEntryAlreadyExists when the file is already there — we need
// UpdateEntry instead so the migration actually persists.
//
// Splits km.kekPath at the last "/" so an operator-overridden path is
// honoured. Defaults match defaultKEKPath when km.kekPath is unset.
func (km *SSES3KeyManager) updateKEKContent(content []byte) error {
	dir, name := splitKEKPath(km.kekPath)
	ctx := context.Background()
	return km.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if err != nil {
			return fmt.Errorf("lookup KEK entry: %w", err)
		}
		entry := resp.Entry
		if entry == nil {
			return fmt.Errorf("KEK entry not found at %s/%s", dir, name)
		}
		entry.Content = content
		if entry.Attributes == nil {
			entry.Attributes = &filer_pb.FuseAttributes{}
		}
		entry.Attributes.FileMode = 0600
		entry.Attributes.FileSize = uint64(len(content))
		entry.Attributes.Mtime = time.Now().Unix()
		return filer_pb.UpdateEntry(ctx, client, &filer_pb.UpdateEntryRequest{
			Directory: dir,
			Entry:     entry,
		})
	})
}

// splitKEKPath splits an absolute KEK file path into (directory, name).
// Falls back to the default location if the path is empty or has no slash.
func splitKEKPath(p string) (dir, name string) {
	if p == "" {
		return SSES3KEKDirectory, SSES3KEKFileName
	}
	idx := strings.LastIndex(p, "/")
	if idx <= 0 {
		return SSES3KEKDirectory, SSES3KEKFileName
	}
	return p[:idx], p[idx+1:]
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

// SSES3KEKPassphraseEnv is the legacy environment variable from which the
// global SSE-S3 key manager picks up its KEK-wrapping passphrase. The Viper
// config key sseS3KEKPassphraseConfigKey ("s3.sse.kek.passphrase") is the
// preferred way to set it — same precedence as s3.sse.kek and s3.sse.key —
// but the env var is honoured as a fallback so deployments that wired only
// the env keep working.
const SSES3KEKPassphraseEnv = "WEED_S3_SSE_KEK_PASSPHRASE"

// Global SSE-S3 key manager instance
var globalSSES3KeyManager = NewSSES3KeyManager()

// SetKEKPassphrase configures the KEK-wrapping passphrase. Must be called
// before InitializeWithFiler — the load path reads the passphrase to decide
// whether to attempt unwrap or fall back to plaintext-hex parsing.
func (km *SSES3KeyManager) SetKEKPassphrase(passphrase string) {
	km.mu.Lock()
	defer km.mu.Unlock()
	km.kekPassphrase = passphrase
}

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

// InitializeGlobalSSES3KeyManager initializes the global key manager with
// filer access. The KEK-wrapping passphrase is sourced from the Viper
// config key s3.sse.kek.passphrase (matching the s3.sse.kek and
// s3.sse.key conventions, settable via security.toml or
// WEED_S3_SSE_KEK_PASSPHRASE env), with a fallback to the bare
// SSES3KEKPassphraseEnv lookup for deployments wired before the Viper key
// existed. If neither is set the KEK falls back to plaintext at-rest
// storage (with a startup warning).
func InitializeGlobalSSES3KeyManager(filerClient *wdclient.FilerClient, grpcDialOption grpc.DialOption) error {
	passphrase := util.GetViper().GetString(sseS3KEKPassphraseConfigKey)
	if passphrase == "" {
		passphrase = os.Getenv(SSES3KEKPassphraseEnv)
	}
	if passphrase != "" {
		globalSSES3KeyManager.SetKEKPassphrase(passphrase)
	} else {
		glog.Warningf("SSE-S3 KeyManager: neither %s nor %s is set; the KEK will be stored on the filer in plaintext. Set one to enable encrypted-at-rest KEK storage.", sseS3KEKPassphraseConfigKey, SSES3KEKPassphraseEnv)
	}

	wrapper := &KeyManagerFilerClient{
		FilerClient:    filerClient,
		grpcDialOption: grpcDialOption,
	}
	return globalSSES3KeyManager.InitializeWithFiler(wrapper)
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

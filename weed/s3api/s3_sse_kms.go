package s3api

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// Compiled regex patterns for KMS key validation
var (
	uuidRegex = regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)
	arnRegex  = regexp.MustCompile(`^arn:aws:kms:[a-z0-9-]+:\d{12}:(key|alias)/.+$`)
)

// SSEKMSKey contains the metadata for an SSE-KMS encrypted object
type SSEKMSKey struct {
	KeyID             string            // The KMS key ID used
	EncryptedDataKey  []byte            // The encrypted data encryption key
	EncryptionContext map[string]string // The encryption context used
	BucketKeyEnabled  bool              // Whether S3 Bucket Keys are enabled
	IV                []byte            // The initialization vector for encryption
}

// SSEKMSMetadata represents the metadata stored with SSE-KMS objects
type SSEKMSMetadata struct {
	Algorithm         string            `json:"algorithm"`         // "aws:kms"
	KeyID             string            `json:"keyId"`             // KMS key identifier
	EncryptedDataKey  string            `json:"encryptedDataKey"`  // Base64-encoded encrypted data key
	EncryptionContext map[string]string `json:"encryptionContext"` // Encryption context
	BucketKeyEnabled  bool              `json:"bucketKeyEnabled"`  // S3 Bucket Key optimization
}

const (
	// Default data key size (256 bits)
	DataKeySize = 32
	// Bucket key cache TTL (1 hour)
	BucketKeyCacheTTL = time.Hour
)

// BucketKeyCache represents a cached bucket-level data key
type BucketKeyCache struct {
	DataKey     *kms.GenerateDataKeyResponse
	ExpiresAt   time.Time
	KeyID       string
	ContextHash string // Hash of encryption context for cache key
}

// Global bucket key cache (in production, this should be distributed)
var bucketKeyCache = make(map[string]*BucketKeyCache)
var bucketKeyCacheMutex sync.RWMutex

// CreateSSEKMSEncryptedReader creates an encrypted reader using KMS envelope encryption
func CreateSSEKMSEncryptedReader(r io.Reader, keyID string, encryptionContext map[string]string) (io.Reader, *SSEKMSKey, error) {
	return CreateSSEKMSEncryptedReaderWithBucketKey(r, keyID, encryptionContext, false)
}

// CreateSSEKMSEncryptedReaderWithBucketKey creates an encrypted reader with optional S3 Bucket Keys optimization
func CreateSSEKMSEncryptedReaderWithBucketKey(r io.Reader, keyID string, encryptionContext map[string]string, bucketKeyEnabled bool) (io.Reader, *SSEKMSKey, error) {
	kmsProvider := kms.GetGlobalKMS()
	if kmsProvider == nil {
		return nil, nil, fmt.Errorf("KMS is not configured")
	}

	var dataKeyResp *kms.GenerateDataKeyResponse
	var err error

	if bucketKeyEnabled {
		// Use S3 Bucket Keys optimization - try to get or create a bucket-level data key
		dataKeyResp, err = getBucketDataKey(keyID, encryptionContext)
		if err != nil {
			// Fall back to per-object key generation if bucket key fails
			glog.V(2).Infof("Bucket key generation failed, falling back to per-object key: %v", err)
			bucketKeyEnabled = false
		}
	}

	if !bucketKeyEnabled {
		// Generate a per-object data encryption key using KMS
		dataKeyReq := &kms.GenerateDataKeyRequest{
			KeyID:             keyID,
			KeySpec:           kms.KeySpecAES256,
			EncryptionContext: encryptionContext,
		}

		ctx := context.Background()
		dataKeyResp, err = kmsProvider.GenerateDataKey(ctx, dataKeyReq)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to generate data key: %v", err)
		}
	}

	// Ensure we clear the plaintext data key from memory when done
	defer kms.ClearSensitiveData(dataKeyResp.Plaintext)

	// Create AES cipher with the data key
	block, err := aes.NewCipher(dataKeyResp.Plaintext)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}

	// Generate a random IV for CTR mode
	// Note: AES-CTR is used for object data encryption (not AES-GCM) because:
	// 1. CTR mode supports streaming encryption for large objects
	// 2. CTR mode supports range requests (seek to arbitrary positions)
	// 3. This matches AWS S3 and other S3-compatible implementations
	// The KMS data key encryption (separate layer) uses AES-GCM for authentication
	iv := make([]byte, 16) // AES block size
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, nil, fmt.Errorf("failed to generate IV: %v", err)
	}

	// Create CTR mode cipher stream
	stream := cipher.NewCTR(block, iv)

	// Create the SSE-KMS metadata
	sseKey := &SSEKMSKey{
		KeyID:             dataKeyResp.KeyID,
		EncryptedDataKey:  dataKeyResp.CiphertextBlob,
		EncryptionContext: encryptionContext,
		BucketKeyEnabled:  bucketKeyEnabled,
	}

	// Return encrypted reader and SSE key with IV for metadata storage
	encryptedReader := &cipher.StreamReader{S: stream, R: r}

	// Store IV in the SSE key for metadata storage
	sseKey.IV = iv

	return encryptedReader, sseKey, nil
}

// getBucketDataKey retrieves or generates a bucket-level data key for S3 Bucket Keys optimization
func getBucketDataKey(keyID string, encryptionContext map[string]string) (*kms.GenerateDataKeyResponse, error) {
	// Create cache key from keyID and encryption context
	cacheKey := createBucketKeyCacheKey(keyID, encryptionContext)

	bucketKeyCacheMutex.RLock()
	cached, exists := bucketKeyCache[cacheKey]
	bucketKeyCacheMutex.RUnlock()

	// Check if we have a valid cached key
	if exists && time.Now().Before(cached.ExpiresAt) {
		glog.V(3).Infof("Using cached bucket key for keyID: %s", keyID)
		return cached.DataKey, nil
	}

	// Generate a new bucket-level data key
	kmsProvider := kms.GetGlobalKMS()
	if kmsProvider == nil {
		return nil, fmt.Errorf("KMS is not configured")
	}

	dataKeyReq := &kms.GenerateDataKeyRequest{
		KeyID:             keyID,
		KeySpec:           kms.KeySpecAES256,
		EncryptionContext: encryptionContext,
	}

	ctx := context.Background()
	dataKeyResp, err := kmsProvider.GenerateDataKey(ctx, dataKeyReq)
	if err != nil {
		return nil, fmt.Errorf("failed to generate bucket data key: %v", err)
	}

	// Cache the new key
	bucketKeyCacheMutex.Lock()
	bucketKeyCache[cacheKey] = &BucketKeyCache{
		DataKey:     dataKeyResp,
		ExpiresAt:   time.Now().Add(BucketKeyCacheTTL),
		KeyID:       keyID,
		ContextHash: createEncryptionContextHash(encryptionContext),
	}
	bucketKeyCacheMutex.Unlock()

	glog.V(3).Infof("Generated new bucket key for keyID: %s", keyID)
	return dataKeyResp, nil
}

// createBucketKeyCacheKey creates a cache key from keyID and encryption context
func createBucketKeyCacheKey(keyID string, encryptionContext map[string]string) string {
	contextHash := createEncryptionContextHash(encryptionContext)
	return fmt.Sprintf("%s:%s", keyID, contextHash)
}

// createEncryptionContextHash creates a deterministic hash of the encryption context
func createEncryptionContextHash(encryptionContext map[string]string) string {
	if len(encryptionContext) == 0 {
		return "empty"
	}

	// Create a deterministic representation of the context
	hash := sha256.New()

	// Sort keys to ensure deterministic hash
	keys := make([]string, 0, len(encryptionContext))
	for k := range encryptionContext {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	// Hash the sorted key-value pairs
	for _, k := range keys {
		hash.Write([]byte(k))
		hash.Write([]byte("="))
		hash.Write([]byte(encryptionContext[k]))
		hash.Write([]byte(";"))
	}

	return hex.EncodeToString(hash.Sum(nil))[:16] // Use first 16 chars for brevity
}

// cleanupExpiredBucketKeys removes expired keys from the cache
func cleanupExpiredBucketKeys() {
	bucketKeyCacheMutex.Lock()
	defer bucketKeyCacheMutex.Unlock()

	now := time.Now()
	for key, cached := range bucketKeyCache {
		if now.After(cached.ExpiresAt) {
			// Clear sensitive data before removing
			if cached.DataKey != nil {
				kms.ClearSensitiveData(cached.DataKey.Plaintext)
			}
			delete(bucketKeyCache, key)
		}
	}
}

// CreateSSEKMSDecryptedReader creates a decrypted reader using KMS envelope encryption
func CreateSSEKMSDecryptedReader(r io.Reader, sseKey *SSEKMSKey) (io.Reader, error) {
	kmsProvider := kms.GetGlobalKMS()
	if kmsProvider == nil {
		return nil, fmt.Errorf("KMS is not configured")
	}

	// Decrypt the data encryption key using KMS
	decryptReq := &kms.DecryptRequest{
		CiphertextBlob:    sseKey.EncryptedDataKey,
		EncryptionContext: sseKey.EncryptionContext,
	}

	ctx := context.Background()
	decryptResp, err := kmsProvider.Decrypt(ctx, decryptReq)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt data key: %v", err)
	}

	// Ensure we clear the plaintext data key from memory when done
	defer kms.ClearSensitiveData(decryptResp.Plaintext)

	// Verify the key ID matches (security check)
	if decryptResp.KeyID != sseKey.KeyID {
		return nil, fmt.Errorf("KMS key ID mismatch: expected %s, got %s", sseKey.KeyID, decryptResp.KeyID)
	}

	// Use the IV from the SSE key metadata
	if len(sseKey.IV) != 16 {
		return nil, fmt.Errorf("invalid IV length: expected 16 bytes, got %d", len(sseKey.IV))
	}
	iv := sseKey.IV

	// Create AES cipher with the decrypted data key
	block, err := aes.NewCipher(decryptResp.Plaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}

	// Create CTR mode cipher stream for decryption
	// Note: AES-CTR is used for object data decryption to match the encryption mode
	stream := cipher.NewCTR(block, iv)

	// Return the decrypted reader
	return &cipher.StreamReader{S: stream, R: r}, nil
}

// ParseSSEKMSHeaders parses SSE-KMS headers from an HTTP request
func ParseSSEKMSHeaders(r *http.Request) (*SSEKMSKey, error) {
	sseAlgorithm := r.Header.Get(s3_constants.AmzServerSideEncryption)

	// Check if SSE-KMS is requested
	if sseAlgorithm == "" {
		return nil, nil // No SSE headers present
	}
	if sseAlgorithm != "aws:kms" {
		return nil, fmt.Errorf("invalid SSE algorithm: %s", sseAlgorithm)
	}

	keyID := r.Header.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId)
	encryptionContextHeader := r.Header.Get(s3_constants.AmzServerSideEncryptionContext)
	bucketKeyEnabledHeader := r.Header.Get(s3_constants.AmzServerSideEncryptionBucketKeyEnabled)

	// Parse encryption context if provided
	var encryptionContext map[string]string
	if encryptionContextHeader != "" {
		// Decode base64-encoded JSON encryption context
		contextBytes, err := base64.StdEncoding.DecodeString(encryptionContextHeader)
		if err != nil {
			return nil, fmt.Errorf("invalid encryption context format: %v", err)
		}

		if err := json.Unmarshal(contextBytes, &encryptionContext); err != nil {
			return nil, fmt.Errorf("invalid encryption context JSON: %v", err)
		}
	}

	// Parse bucket key enabled flag
	bucketKeyEnabled := strings.ToLower(bucketKeyEnabledHeader) == "true"

	sseKey := &SSEKMSKey{
		KeyID:             keyID,
		EncryptionContext: encryptionContext,
		BucketKeyEnabled:  bucketKeyEnabled,
	}

	// Validate the parsed key
	if err := ValidateSSEKMSKey(sseKey); err != nil {
		return nil, err
	}

	return sseKey, nil
}

// ValidateSSEKMSKey validates an SSE-KMS key configuration
func ValidateSSEKMSKey(sseKey *SSEKMSKey) error {
	if sseKey == nil {
		return fmt.Errorf("SSE-KMS key is required")
	}

	// An empty key ID is valid and means the default KMS key should be used.
	if sseKey.KeyID != "" && !isValidKMSKeyID(sseKey.KeyID) {
		return fmt.Errorf("invalid KMS key ID format: %s", sseKey.KeyID)
	}

	return nil
}

// isValidKMSKeyID performs basic validation of KMS key identifiers
func isValidKMSKeyID(keyID string) bool {
	if keyID == "" {
		return false
	}

	// Key IDs should not contain spaces or other invalid characters
	if strings.Contains(keyID, " ") || strings.Contains(keyID, "\t") || strings.Contains(keyID, "\n") {
		return false
	}

	// AWS KMS key ID formats:
	// 1. Key ID: 1234abcd-12ab-34cd-56ef-1234567890ab
	// 2. Key ARN: arn:aws:kms:region:account:key/1234abcd-12ab-34cd-56ef-1234567890ab
	// 3. Alias: alias/my-key
	// 4. Alias ARN: arn:aws:kms:region:account:alias/my-key

	if strings.HasPrefix(keyID, "arn:aws:kms:") {
		// ARN format validation using compiled regex
		return arnRegex.MatchString(keyID)
	}

	if strings.HasPrefix(keyID, "alias/") {
		// Alias format validation
		return len(keyID) > 6 // "alias/" + at least one character
	}

	// UUID format validation
	if uuidRegex.MatchString(keyID) {
		return true
	}

	return false
}

// BuildEncryptionContext creates the encryption context for S3 objects
func BuildEncryptionContext(bucketName, objectKey string, useBucketKey bool) map[string]string {
	return kms.BuildS3EncryptionContext(bucketName, objectKey, useBucketKey)
}

// SerializeSSEKMSMetadata serializes SSE-KMS metadata for storage
func SerializeSSEKMSMetadata(sseKey *SSEKMSKey) ([]byte, error) {
	metadata := &SSEKMSMetadata{
		Algorithm:         "aws:kms",
		KeyID:             sseKey.KeyID,
		EncryptedDataKey:  base64.StdEncoding.EncodeToString(sseKey.EncryptedDataKey),
		EncryptionContext: sseKey.EncryptionContext,
		BucketKeyEnabled:  sseKey.BucketKeyEnabled,
	}

	return json.Marshal(metadata)
}

// DeserializeSSEKMSMetadata deserializes SSE-KMS metadata from storage
func DeserializeSSEKMSMetadata(data []byte) (*SSEKMSKey, error) {
	var metadata SSEKMSMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal SSE-KMS metadata: %v", err)
	}

	// Decode the encrypted data key
	encryptedDataKey, err := base64.StdEncoding.DecodeString(metadata.EncryptedDataKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decode encrypted data key: %v", err)
	}

	return &SSEKMSKey{
		KeyID:             metadata.KeyID,
		EncryptedDataKey:  encryptedDataKey,
		EncryptionContext: metadata.EncryptionContext,
		BucketKeyEnabled:  metadata.BucketKeyEnabled,
	}, nil
}

// AddSSEKMSResponseHeaders adds SSE-KMS response headers to an HTTP response
func AddSSEKMSResponseHeaders(w http.ResponseWriter, sseKey *SSEKMSKey) {
	w.Header().Set(s3_constants.AmzServerSideEncryption, "aws:kms")
	w.Header().Set(s3_constants.AmzServerSideEncryptionAwsKmsKeyId, sseKey.KeyID)

	if len(sseKey.EncryptionContext) > 0 {
		// Encode encryption context as base64 JSON
		contextBytes, err := json.Marshal(sseKey.EncryptionContext)
		if err == nil {
			contextB64 := base64.StdEncoding.EncodeToString(contextBytes)
			w.Header().Set(s3_constants.AmzServerSideEncryptionContext, contextB64)
		} else {
			glog.Errorf("Failed to encode encryption context: %v", err)
		}
	}

	if sseKey.BucketKeyEnabled {
		w.Header().Set(s3_constants.AmzServerSideEncryptionBucketKeyEnabled, "true")
	}
}

// IsSSEKMSRequest checks if the request contains SSE-KMS headers
func IsSSEKMSRequest(r *http.Request) bool {
	// If SSE-C headers are present, this is not an SSE-KMS request (they are mutually exclusive)
	if r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm) != "" {
		return false
	}

	sseAlgorithm := r.Header.Get(s3_constants.AmzServerSideEncryption)
	return sseAlgorithm == "aws:kms" || r.Header.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId) != ""
}

// IsSSEKMSEncrypted checks if the metadata indicates SSE-KMS encryption
func IsSSEKMSEncrypted(metadata map[string][]byte) bool {
	if metadata == nil {
		return false
	}

	// The canonical way to identify an SSE-KMS encrypted object is by this header.
	if sseAlgorithm, exists := metadata[s3_constants.AmzServerSideEncryption]; exists {
		return string(sseAlgorithm) == "aws:kms"
	}

	return false
}

// IsAnySSEEncrypted checks if metadata indicates any type of SSE encryption
func IsAnySSEEncrypted(metadata map[string][]byte) bool {
	if metadata == nil {
		return false
	}

	// Check for any SSE type
	if IsSSECEncrypted(metadata) {
		return true
	}
	if IsSSEKMSEncrypted(metadata) {
		return true
	}

	// Check for SSE-S3
	if sseAlgorithm, exists := metadata[s3_constants.AmzServerSideEncryption]; exists {
		return string(sseAlgorithm) == "AES256"
	}

	return false
}

// MapKMSErrorToS3Error maps KMS errors to appropriate S3 error codes
func MapKMSErrorToS3Error(err error) s3err.ErrorCode {
	if err == nil {
		return s3err.ErrNone
	}

	// Check if it's a KMS error
	kmsErr, ok := err.(*kms.KMSError)
	if !ok {
		return s3err.ErrInternalError
	}

	switch kmsErr.Code {
	case kms.ErrCodeNotFoundException:
		return s3err.ErrKMSKeyNotFound
	case kms.ErrCodeAccessDenied:
		return s3err.ErrKMSAccessDenied
	case kms.ErrCodeKeyUnavailable:
		return s3err.ErrKMSDisabled
	case kms.ErrCodeInvalidKeyUsage:
		return s3err.ErrKMSAccessDenied
	case kms.ErrCodeInvalidCiphertext:
		return s3err.ErrKMSInvalidCiphertext
	default:
		glog.Errorf("Unmapped KMS error: %s - %s", kmsErr.Code, kmsErr.Message)
		return s3err.ErrInternalError
	}
}

// SSEKMSCopyStrategy represents different strategies for copying SSE-KMS encrypted objects
type SSEKMSCopyStrategy int

const (
	// SSEKMSCopyStrategyDirect - Direct chunk copy (same key, no re-encryption needed)
	SSEKMSCopyStrategyDirect SSEKMSCopyStrategy = iota
	// SSEKMSCopyStrategyDecryptEncrypt - Decrypt source and re-encrypt for destination
	SSEKMSCopyStrategyDecryptEncrypt
)

// String returns string representation of the strategy
func (s SSEKMSCopyStrategy) String() string {
	switch s {
	case SSEKMSCopyStrategyDirect:
		return "Direct"
	case SSEKMSCopyStrategyDecryptEncrypt:
		return "DecryptEncrypt"
	default:
		return "Unknown"
	}
}

// GetSourceSSEKMSInfo extracts SSE-KMS information from source object metadata
func GetSourceSSEKMSInfo(metadata map[string][]byte) (keyID string, isEncrypted bool) {
	if sseAlgorithm, exists := metadata[s3_constants.AmzServerSideEncryption]; exists && string(sseAlgorithm) == "aws:kms" {
		if kmsKeyID, exists := metadata[s3_constants.AmzServerSideEncryptionAwsKmsKeyId]; exists {
			return string(kmsKeyID), true
		}
		return "", true // SSE-KMS with default key
	}
	return "", false
}

// CanDirectCopySSEKMS determines if we can directly copy chunks without decrypt/re-encrypt
func CanDirectCopySSEKMS(srcMetadata map[string][]byte, destKeyID string) bool {
	srcKeyID, srcEncrypted := GetSourceSSEKMSInfo(srcMetadata)

	// Case 1: Source unencrypted, destination unencrypted -> Direct copy
	if !srcEncrypted && destKeyID == "" {
		return true
	}

	// Case 2: Source encrypted with same KMS key as destination -> Direct copy
	if srcEncrypted && destKeyID != "" {
		// Same key if key IDs match (empty means default key)
		return srcKeyID == destKeyID
	}

	// All other cases require decrypt/re-encrypt
	return false
}

// DetermineSSEKMSCopyStrategy determines the optimal copy strategy for SSE-KMS
func DetermineSSEKMSCopyStrategy(srcMetadata map[string][]byte, destKeyID string) (SSEKMSCopyStrategy, error) {
	if CanDirectCopySSEKMS(srcMetadata, destKeyID) {
		return SSEKMSCopyStrategyDirect, nil
	}
	return SSEKMSCopyStrategyDecryptEncrypt, nil
}

// ParseSSEKMSCopyHeaders parses SSE-KMS headers from copy request
func ParseSSEKMSCopyHeaders(r *http.Request) (destKeyID string, encryptionContext map[string]string, bucketKeyEnabled bool, err error) {
	// Check if this is an SSE-KMS request
	if !IsSSEKMSRequest(r) {
		return "", nil, false, nil
	}

	// Get destination KMS key ID
	destKeyID = r.Header.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId)

	// Validate key ID if provided
	if destKeyID != "" && !isValidKMSKeyID(destKeyID) {
		return "", nil, false, fmt.Errorf("invalid KMS key ID: %s", destKeyID)
	}

	// Parse encryption context if provided
	if contextHeader := r.Header.Get(s3_constants.AmzServerSideEncryptionContext); contextHeader != "" {
		contextBytes, decodeErr := base64.StdEncoding.DecodeString(contextHeader)
		if decodeErr != nil {
			return "", nil, false, fmt.Errorf("invalid encryption context encoding: %v", decodeErr)
		}

		if unmarshalErr := json.Unmarshal(contextBytes, &encryptionContext); unmarshalErr != nil {
			return "", nil, false, fmt.Errorf("invalid encryption context JSON: %v", unmarshalErr)
		}
	}

	// Parse bucket key enabled flag
	if bucketKeyHeader := r.Header.Get(s3_constants.AmzServerSideEncryptionBucketKeyEnabled); bucketKeyHeader != "" {
		bucketKeyEnabled = strings.ToLower(bucketKeyHeader) == "true"
	}

	return destKeyID, encryptionContext, bucketKeyEnabled, nil
}

// UnifiedCopyStrategy represents all possible copy strategies across encryption types
type UnifiedCopyStrategy int

const (
	// CopyStrategyDirect - Direct chunk copy (no encryption changes)
	CopyStrategyDirect UnifiedCopyStrategy = iota
	// CopyStrategyEncrypt - Encrypt during copy (plain → encrypted)
	CopyStrategyEncrypt
	// CopyStrategyDecrypt - Decrypt during copy (encrypted → plain)
	CopyStrategyDecrypt
	// CopyStrategyReencrypt - Decrypt and re-encrypt (different keys/methods)
	CopyStrategyReencrypt
	// CopyStrategyKeyRotation - Same object, different key (metadata-only update)
	CopyStrategyKeyRotation
)

// String returns string representation of the unified strategy
func (s UnifiedCopyStrategy) String() string {
	switch s {
	case CopyStrategyDirect:
		return "Direct"
	case CopyStrategyEncrypt:
		return "Encrypt"
	case CopyStrategyDecrypt:
		return "Decrypt"
	case CopyStrategyReencrypt:
		return "Reencrypt"
	case CopyStrategyKeyRotation:
		return "KeyRotation"
	default:
		return "Unknown"
	}
}

// EncryptionState represents the encryption state of source and destination
type EncryptionState struct {
	SrcSSEC    bool
	SrcSSEKMS  bool
	SrcSSES3   bool
	DstSSEC    bool
	DstSSEKMS  bool
	DstSSES3   bool
	SameObject bool
}

// IsSourceEncrypted returns true if source has any encryption
func (e *EncryptionState) IsSourceEncrypted() bool {
	return e.SrcSSEC || e.SrcSSEKMS || e.SrcSSES3
}

// IsTargetEncrypted returns true if target should be encrypted
func (e *EncryptionState) IsTargetEncrypted() bool {
	return e.DstSSEC || e.DstSSEKMS || e.DstSSES3
}

// DetermineUnifiedCopyStrategy determines the optimal copy strategy for all encryption types
func DetermineUnifiedCopyStrategy(state *EncryptionState, srcMetadata map[string][]byte, r *http.Request) (UnifiedCopyStrategy, error) {
	// Key rotation: same object with different encryption
	if state.SameObject && state.IsSourceEncrypted() && state.IsTargetEncrypted() {
		// Check if it's actually a key change
		if state.SrcSSEC && state.DstSSEC {
			// SSE-C key rotation - need to compare keys
			return CopyStrategyKeyRotation, nil
		}
		if state.SrcSSEKMS && state.DstSSEKMS {
			// SSE-KMS key rotation - need to compare key IDs
			srcKeyID, _ := GetSourceSSEKMSInfo(srcMetadata)
			dstKeyID := r.Header.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId)
			if srcKeyID != dstKeyID {
				return CopyStrategyKeyRotation, nil
			}
		}
	}

	// Direct copy: no encryption changes
	if !state.IsSourceEncrypted() && !state.IsTargetEncrypted() {
		return CopyStrategyDirect, nil
	}

	// Same encryption type and key
	if state.SrcSSEKMS && state.DstSSEKMS {
		srcKeyID, _ := GetSourceSSEKMSInfo(srcMetadata)
		dstKeyID := r.Header.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId)
		if srcKeyID == dstKeyID {
			return CopyStrategyDirect, nil
		}
	}

	if state.SrcSSEC && state.DstSSEC {
		// For SSE-C, we'd need to compare the actual keys, but we can't do that securely
		// So we assume different keys and use reencrypt strategy
		return CopyStrategyReencrypt, nil
	}

	// Encrypt: plain → encrypted
	if !state.IsSourceEncrypted() && state.IsTargetEncrypted() {
		return CopyStrategyEncrypt, nil
	}

	// Decrypt: encrypted → plain
	if state.IsSourceEncrypted() && !state.IsTargetEncrypted() {
		return CopyStrategyDecrypt, nil
	}

	// Reencrypt: different encryption types or keys
	if state.IsSourceEncrypted() && state.IsTargetEncrypted() {
		return CopyStrategyReencrypt, nil
	}

	return CopyStrategyDirect, nil
}

// DetectEncryptionState analyzes the source metadata and request headers to determine encryption state
func DetectEncryptionState(srcMetadata map[string][]byte, r *http.Request, srcPath, dstPath string) *EncryptionState {
	state := &EncryptionState{
		SrcSSEC:    IsSSECEncrypted(srcMetadata),
		SrcSSEKMS:  IsSSEKMSEncrypted(srcMetadata),
		SrcSSES3:   IsSSES3EncryptedInternal(srcMetadata),
		DstSSEC:    IsSSECRequest(r),
		DstSSEKMS:  IsSSEKMSRequest(r),
		DstSSES3:   IsSSES3RequestInternal(r),
		SameObject: srcPath == dstPath,
	}

	return state
}

// Helper functions for SSE-C detection are in s3_sse_c.go

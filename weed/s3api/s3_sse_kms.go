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
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
	ChunkOffset       int64             // Offset of this chunk within the original part (for IV calculation)
}

// SSEKMSMetadata represents the metadata stored with SSE-KMS objects
type SSEKMSMetadata struct {
	Algorithm         string            `json:"algorithm"`         // "aws:kms"
	KeyID             string            `json:"keyId"`             // KMS key identifier
	EncryptedDataKey  string            `json:"encryptedDataKey"`  // Base64-encoded encrypted data key
	EncryptionContext map[string]string `json:"encryptionContext"` // Encryption context
	BucketKeyEnabled  bool              `json:"bucketKeyEnabled"`  // S3 Bucket Key optimization
	IV                string            `json:"iv"`                // Base64-encoded initialization vector
	PartOffset        int64             `json:"partOffset"`        // Offset within original multipart part (for IV calculation)
}

const (
	// Default data key size (256 bits)
	DataKeySize = 32
)

// Bucket key cache TTL (moved to be used with per-bucket cache)
const BucketKeyCacheTTL = time.Hour

// CreateSSEKMSEncryptedReader creates an encrypted reader using KMS envelope encryption
func CreateSSEKMSEncryptedReader(r io.Reader, keyID string, encryptionContext map[string]string) (io.Reader, *SSEKMSKey, error) {
	return CreateSSEKMSEncryptedReaderWithBucketKey(r, keyID, encryptionContext, false)
}

// CreateSSEKMSEncryptedReaderWithBucketKey creates an encrypted reader with optional S3 Bucket Keys optimization
func CreateSSEKMSEncryptedReaderWithBucketKey(r io.Reader, keyID string, encryptionContext map[string]string, bucketKeyEnabled bool) (io.Reader, *SSEKMSKey, error) {
	if bucketKeyEnabled {
		// Use S3 Bucket Keys optimization - try to get or create a bucket-level data key
		// Note: This is a simplified implementation. In practice, this would need
		// access to the bucket name and S3ApiServer instance for proper per-bucket caching.
		// For now, generate per-object keys (bucket key optimization disabled)
		glog.V(2).Infof("Bucket key optimization requested but not fully implemented yet - using per-object keys")
		bucketKeyEnabled = false
	}

	// Generate data key using common utility
	dataKeyResult, err := generateKMSDataKey(keyID, encryptionContext)
	if err != nil {
		return nil, nil, err
	}

	// Ensure we clear the plaintext data key from memory when done
	defer clearKMSDataKey(dataKeyResult)

	// Generate a random IV for CTR mode
	// Note: AES-CTR is used for object data encryption (not AES-GCM) because:
	// 1. CTR mode supports streaming encryption for large objects
	// 2. CTR mode supports range requests (seek to arbitrary positions)
	// 3. This matches AWS S3 and other S3-compatible implementations
	// The KMS data key encryption (separate layer) uses AES-GCM for authentication
	iv := make([]byte, s3_constants.AESBlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, nil, fmt.Errorf("failed to generate IV: %v", err)
	}

	// Create CTR mode cipher stream
	stream := cipher.NewCTR(dataKeyResult.Block, iv)

	// Create the SSE-KMS metadata using utility function
	sseKey := createSSEKMSKey(dataKeyResult, encryptionContext, bucketKeyEnabled, iv, 0)

	// The IV is stored in SSE key metadata, so the encrypted stream does not need to prepend the IV
	// This ensures correct Content-Length for clients
	encryptedReader := &cipher.StreamReader{S: stream, R: r}

	// Store IV in the SSE key for metadata storage
	sseKey.IV = iv

	return encryptedReader, sseKey, nil
}

// CreateSSEKMSEncryptedReaderWithBaseIV creates an SSE-KMS encrypted reader using a provided base IV
// This is used for multipart uploads where all chunks need to use the same base IV
func CreateSSEKMSEncryptedReaderWithBaseIV(r io.Reader, keyID string, encryptionContext map[string]string, bucketKeyEnabled bool, baseIV []byte) (io.Reader, *SSEKMSKey, error) {
	if err := ValidateIV(baseIV, "base IV"); err != nil {
		return nil, nil, err
	}

	// Generate data key using common utility
	dataKeyResult, err := generateKMSDataKey(keyID, encryptionContext)
	if err != nil {
		return nil, nil, err
	}

	// Ensure we clear the plaintext data key from memory when done
	defer clearKMSDataKey(dataKeyResult)

	// Use the provided base IV instead of generating a new one
	iv := make([]byte, s3_constants.AESBlockSize)
	copy(iv, baseIV)

	// Create CTR mode cipher stream
	stream := cipher.NewCTR(dataKeyResult.Block, iv)

	// Create the SSE-KMS metadata using utility function
	sseKey := createSSEKMSKey(dataKeyResult, encryptionContext, bucketKeyEnabled, iv, 0)

	// The IV is stored in SSE key metadata, so the encrypted stream does not need to prepend the IV
	// This ensures correct Content-Length for clients
	encryptedReader := &cipher.StreamReader{S: stream, R: r}

	// Store the base IV in the SSE key for metadata storage
	sseKey.IV = iv

	return encryptedReader, sseKey, nil
}

// CreateSSEKMSEncryptedReaderWithBaseIVAndOffset creates an SSE-KMS encrypted reader using a provided base IV and offset
// This is used for multipart uploads where all chunks need unique IVs to prevent IV reuse vulnerabilities
func CreateSSEKMSEncryptedReaderWithBaseIVAndOffset(r io.Reader, keyID string, encryptionContext map[string]string, bucketKeyEnabled bool, baseIV []byte, offset int64) (io.Reader, *SSEKMSKey, error) {
	if err := ValidateIV(baseIV, "base IV"); err != nil {
		return nil, nil, err
	}

	// Generate data key using common utility
	dataKeyResult, err := generateKMSDataKey(keyID, encryptionContext)
	if err != nil {
		return nil, nil, err
	}

	// Ensure we clear the plaintext data key from memory when done
	defer clearKMSDataKey(dataKeyResult)

	// Calculate unique IV using base IV and offset to prevent IV reuse in multipart uploads
	iv := calculateIVWithOffset(baseIV, offset)

	// Create CTR mode cipher stream
	stream := cipher.NewCTR(dataKeyResult.Block, iv)

	// Create the SSE-KMS metadata using utility function
	sseKey := createSSEKMSKey(dataKeyResult, encryptionContext, bucketKeyEnabled, iv, offset)

	// The IV is stored in SSE key metadata, so the encrypted stream does not need to prepend the IV
	// This ensures correct Content-Length for clients
	encryptedReader := &cipher.StreamReader{S: stream, R: r}

	return encryptedReader, sseKey, nil
}

// hashEncryptionContext creates a deterministic hash of the encryption context
func hashEncryptionContext(encryptionContext map[string]string) string {
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

// getBucketDataKey retrieves or creates a cached bucket-level data key for SSE-KMS
// This is a simplified implementation that demonstrates the per-bucket caching concept
// In a full implementation, this would integrate with the actual bucket configuration system
func getBucketDataKey(bucketName, keyID string, encryptionContext map[string]string, bucketCache *BucketKMSCache) (*kms.GenerateDataKeyResponse, error) {
	// Create context hash for cache key
	contextHash := hashEncryptionContext(encryptionContext)
	cacheKey := fmt.Sprintf("%s:%s", keyID, contextHash)

	// Try to get from cache first if cache is available
	if bucketCache != nil {
		if cacheEntry, found := bucketCache.Get(cacheKey); found {
			if dataKey, ok := cacheEntry.DataKey.(*kms.GenerateDataKeyResponse); ok {
				glog.V(3).Infof("Using cached bucket key for bucket %s, keyID %s", bucketName, keyID)
				return dataKey, nil
			}
		}
	}

	// Cache miss - generate new data key
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

	// Cache the data key for future use if cache is available
	if bucketCache != nil {
		bucketCache.Set(cacheKey, keyID, dataKeyResp, BucketKeyCacheTTL)
		glog.V(2).Infof("Generated and cached new bucket key for bucket %s, keyID %s", bucketName, keyID)
	} else {
		glog.V(2).Infof("Generated new bucket key for bucket %s, keyID %s (caching disabled)", bucketName, keyID)
	}

	return dataKeyResp, nil
}

// CreateSSEKMSEncryptedReaderForBucket creates an encrypted reader with bucket-specific caching
// This method is part of S3ApiServer to access bucket configuration and caching
func (s3a *S3ApiServer) CreateSSEKMSEncryptedReaderForBucket(r io.Reader, bucketName, keyID string, encryptionContext map[string]string, bucketKeyEnabled bool) (io.Reader, *SSEKMSKey, error) {
	var dataKeyResp *kms.GenerateDataKeyResponse
	var err error

	if bucketKeyEnabled {
		// Use S3 Bucket Keys optimization with persistent per-bucket caching
		bucketCache, err := s3a.getBucketKMSCache(bucketName)
		if err != nil {
			glog.V(2).Infof("Failed to get bucket KMS cache for %s, falling back to per-object key: %v", bucketName, err)
			bucketKeyEnabled = false
		} else {
			dataKeyResp, err = getBucketDataKey(bucketName, keyID, encryptionContext, bucketCache)
			if err != nil {
				// Fall back to per-object key generation if bucket key fails
				glog.V(2).Infof("Bucket key generation failed for bucket %s, falling back to per-object key: %v", bucketName, err)
				bucketKeyEnabled = false
			}
		}
	}

	if !bucketKeyEnabled {
		// Generate a per-object data encryption key using KMS
		kmsProvider := kms.GetGlobalKMS()
		if kmsProvider == nil {
			return nil, nil, fmt.Errorf("KMS is not configured")
		}

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
	iv := make([]byte, 16) // AES block size
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, nil, fmt.Errorf("failed to generate IV: %v", err)
	}

	// Create CTR mode cipher stream
	stream := cipher.NewCTR(block, iv)

	// Create the encrypting reader
	sseKey := &SSEKMSKey{
		KeyID:             keyID,
		EncryptedDataKey:  dataKeyResp.CiphertextBlob,
		EncryptionContext: encryptionContext,
		BucketKeyEnabled:  bucketKeyEnabled,
		IV:                iv,
	}

	return &cipher.StreamReader{S: stream, R: r}, sseKey, nil
}

// getBucketKMSCache gets or creates the persistent KMS cache for a bucket
func (s3a *S3ApiServer) getBucketKMSCache(bucketName string) (*BucketKMSCache, error) {
	// Get bucket configuration
	bucketConfig, errCode := s3a.getBucketConfig(bucketName)
	if errCode != s3err.ErrNone {
		if errCode == s3err.ErrNoSuchBucket {
			return nil, fmt.Errorf("bucket %s does not exist", bucketName)
		}
		return nil, fmt.Errorf("failed to get bucket config: %v", errCode)
	}

	// Initialize KMS cache if it doesn't exist
	if bucketConfig.KMSKeyCache == nil {
		bucketConfig.KMSKeyCache = NewBucketKMSCache(bucketName, BucketKeyCacheTTL)
		glog.V(3).Infof("Initialized new KMS cache for bucket %s", bucketName)
	}

	return bucketConfig.KMSKeyCache, nil
}

// CleanupBucketKMSCache performs cleanup of expired KMS keys for a specific bucket
func (s3a *S3ApiServer) CleanupBucketKMSCache(bucketName string) int {
	bucketCache, err := s3a.getBucketKMSCache(bucketName)
	if err != nil {
		glog.V(3).Infof("Could not get KMS cache for bucket %s: %v", bucketName, err)
		return 0
	}

	cleaned := bucketCache.CleanupExpired()
	if cleaned > 0 {
		glog.V(2).Infof("Cleaned up %d expired KMS keys for bucket %s", cleaned, bucketName)
	}
	return cleaned
}

// CleanupAllBucketKMSCaches performs cleanup of expired KMS keys for all buckets
func (s3a *S3ApiServer) CleanupAllBucketKMSCaches() int {
	totalCleaned := 0

	// Access the bucket config cache safely
	if s3a.bucketConfigCache != nil {
		s3a.bucketConfigCache.mutex.RLock()
		bucketNames := make([]string, 0, len(s3a.bucketConfigCache.cache))
		for bucketName := range s3a.bucketConfigCache.cache {
			bucketNames = append(bucketNames, bucketName)
		}
		s3a.bucketConfigCache.mutex.RUnlock()

		// Clean up each bucket's KMS cache
		for _, bucketName := range bucketNames {
			cleaned := s3a.CleanupBucketKMSCache(bucketName)
			totalCleaned += cleaned
		}
	}

	if totalCleaned > 0 {
		glog.V(2).Infof("Cleaned up %d expired KMS keys across %d bucket caches", totalCleaned, len(s3a.bucketConfigCache.cache))
	}
	return totalCleaned
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

	// Use the IV from the SSE key metadata, calculating offset if this is a chunked part
	if err := ValidateIV(sseKey.IV, "SSE key IV"); err != nil {
		return nil, fmt.Errorf("invalid IV in SSE key: %w", err)
	}

	// Calculate the correct IV for this chunk's offset within the original part
	var iv []byte
	if sseKey.ChunkOffset > 0 {
		iv = calculateIVWithOffset(sseKey.IV, sseKey.ChunkOffset)
	} else {
		iv = sseKey.IV
	}

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
	if sseAlgorithm != s3_constants.SSEAlgorithmKMS {
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

	// Validate the parsed key including key ID format
	if err := ValidateSSEKMSKeyInternal(sseKey); err != nil {
		return nil, err
	}

	return sseKey, nil
}

// ValidateSSEKMSKey validates an SSE-KMS key configuration
func ValidateSSEKMSKeyInternal(sseKey *SSEKMSKey) error {
	if err := ValidateSSEKMSKey(sseKey); err != nil {
		return err
	}

	// An empty key ID is valid and means the default KMS key should be used.
	if sseKey.KeyID != "" && !isValidKMSKeyID(sseKey.KeyID) {
		return fmt.Errorf("invalid KMS key ID format: %s", sseKey.KeyID)
	}

	return nil
}

// BuildEncryptionContext creates the encryption context for S3 objects
func BuildEncryptionContext(bucketName, objectKey string, useBucketKey bool) map[string]string {
	return kms.BuildS3EncryptionContext(bucketName, objectKey, useBucketKey)
}

// parseEncryptionContext parses the user-provided encryption context from base64 JSON
func parseEncryptionContext(contextHeader string) (map[string]string, error) {
	if contextHeader == "" {
		return nil, nil
	}

	// Decode base64
	contextBytes, err := base64.StdEncoding.DecodeString(contextHeader)
	if err != nil {
		return nil, fmt.Errorf("invalid base64 encoding in encryption context: %w", err)
	}

	// Parse JSON
	var context map[string]string
	if err := json.Unmarshal(contextBytes, &context); err != nil {
		return nil, fmt.Errorf("invalid JSON in encryption context: %w", err)
	}

	// Validate context keys and values
	for k, v := range context {
		if k == "" || v == "" {
			return nil, fmt.Errorf("encryption context keys and values cannot be empty")
		}
		// AWS KMS has limits on context key/value length (256 chars each)
		if len(k) > 256 || len(v) > 256 {
			return nil, fmt.Errorf("encryption context key or value too long (max 256 characters)")
		}
	}

	return context, nil
}

// SerializeSSEKMSMetadata serializes SSE-KMS metadata for storage in object metadata
func SerializeSSEKMSMetadata(sseKey *SSEKMSKey) ([]byte, error) {
	if err := ValidateSSEKMSKey(sseKey); err != nil {
		return nil, err
	}

	metadata := &SSEKMSMetadata{
		Algorithm:         s3_constants.SSEAlgorithmKMS,
		KeyID:             sseKey.KeyID,
		EncryptedDataKey:  base64.StdEncoding.EncodeToString(sseKey.EncryptedDataKey),
		EncryptionContext: sseKey.EncryptionContext,
		BucketKeyEnabled:  sseKey.BucketKeyEnabled,
		IV:                base64.StdEncoding.EncodeToString(sseKey.IV), // Store IV for decryption
		PartOffset:        sseKey.ChunkOffset,                           // Store within-part offset
	}

	data, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal SSE-KMS metadata: %w", err)
	}

	glog.V(4).Infof("Serialized SSE-KMS metadata: keyID=%s, bucketKey=%t", sseKey.KeyID, sseKey.BucketKeyEnabled)
	return data, nil
}

// DeserializeSSEKMSMetadata deserializes SSE-KMS metadata from storage and reconstructs the SSE-KMS key
func DeserializeSSEKMSMetadata(data []byte) (*SSEKMSKey, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty SSE-KMS metadata")
	}

	var metadata SSEKMSMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal SSE-KMS metadata: %w", err)
	}

	// Validate algorithm - be lenient with missing/empty algorithm for backward compatibility
	if metadata.Algorithm != "" && metadata.Algorithm != s3_constants.SSEAlgorithmKMS {
		return nil, fmt.Errorf("invalid SSE-KMS algorithm: %s", metadata.Algorithm)
	}

	// Set default algorithm if empty
	if metadata.Algorithm == "" {
		metadata.Algorithm = s3_constants.SSEAlgorithmKMS
	}

	// Decode the encrypted data key
	encryptedDataKey, err := base64.StdEncoding.DecodeString(metadata.EncryptedDataKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decode encrypted data key: %w", err)
	}

	// Decode the IV
	var iv []byte
	if metadata.IV != "" {
		iv, err = base64.StdEncoding.DecodeString(metadata.IV)
		if err != nil {
			return nil, fmt.Errorf("failed to decode IV: %w", err)
		}
	}

	sseKey := &SSEKMSKey{
		KeyID:             metadata.KeyID,
		EncryptedDataKey:  encryptedDataKey,
		EncryptionContext: metadata.EncryptionContext,
		BucketKeyEnabled:  metadata.BucketKeyEnabled,
		IV:                iv,                  // Restore IV for decryption
		ChunkOffset:       metadata.PartOffset, // Use stored within-part offset
	}

	glog.V(4).Infof("Deserialized SSE-KMS metadata: keyID=%s, bucketKey=%t", sseKey.KeyID, sseKey.BucketKeyEnabled)
	return sseKey, nil
}

// SSECMetadata represents SSE-C metadata for per-chunk storage (unified with SSE-KMS approach)
type SSECMetadata struct {
	Algorithm  string `json:"algorithm"`  // SSE-C algorithm (always "AES256")
	IV         string `json:"iv"`         // Base64-encoded initialization vector for this chunk
	KeyMD5     string `json:"keyMD5"`     // MD5 of the customer-provided key
	PartOffset int64  `json:"partOffset"` // Offset within original multipart part (for IV calculation)
}

// SerializeSSECMetadata serializes SSE-C metadata for storage in chunk metadata
func SerializeSSECMetadata(iv []byte, keyMD5 string, partOffset int64) ([]byte, error) {
	if err := ValidateIV(iv, "IV"); err != nil {
		return nil, err
	}

	metadata := &SSECMetadata{
		Algorithm:  s3_constants.SSEAlgorithmAES256,
		IV:         base64.StdEncoding.EncodeToString(iv),
		KeyMD5:     keyMD5,
		PartOffset: partOffset,
	}

	data, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal SSE-C metadata: %w", err)
	}

	glog.V(4).Infof("Serialized SSE-C metadata: keyMD5=%s, partOffset=%d", keyMD5, partOffset)
	return data, nil
}

// DeserializeSSECMetadata deserializes SSE-C metadata from chunk storage
func DeserializeSSECMetadata(data []byte) (*SSECMetadata, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty SSE-C metadata")
	}

	var metadata SSECMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal SSE-C metadata: %w", err)
	}

	// Validate algorithm
	if metadata.Algorithm != s3_constants.SSEAlgorithmAES256 {
		return nil, fmt.Errorf("invalid SSE-C algorithm: %s", metadata.Algorithm)
	}

	// Validate IV
	if metadata.IV == "" {
		return nil, fmt.Errorf("missing IV in SSE-C metadata")
	}

	if _, err := base64.StdEncoding.DecodeString(metadata.IV); err != nil {
		return nil, fmt.Errorf("invalid base64 IV in SSE-C metadata: %w", err)
	}

	glog.V(4).Infof("Deserialized SSE-C metadata: keyMD5=%s, partOffset=%d", metadata.KeyMD5, metadata.PartOffset)
	return &metadata, nil
}

// AddSSEKMSResponseHeaders adds SSE-KMS response headers to an HTTP response
func AddSSEKMSResponseHeaders(w http.ResponseWriter, sseKey *SSEKMSKey) {
	w.Header().Set(s3_constants.AmzServerSideEncryption, s3_constants.SSEAlgorithmKMS)
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

	// According to AWS S3 specification, SSE-KMS is only valid when the encryption header
	// is explicitly set to "aws:kms". The KMS key ID header alone is not sufficient.
	sseAlgorithm := r.Header.Get(s3_constants.AmzServerSideEncryption)
	return sseAlgorithm == s3_constants.SSEAlgorithmKMS
}

// IsSSEKMSEncrypted checks if the metadata indicates SSE-KMS encryption
func IsSSEKMSEncrypted(metadata map[string][]byte) bool {
	if metadata == nil {
		return false
	}

	// The canonical way to identify an SSE-KMS encrypted object is by this header.
	if sseAlgorithm, exists := metadata[s3_constants.AmzServerSideEncryption]; exists {
		return string(sseAlgorithm) == s3_constants.SSEAlgorithmKMS
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
		return string(sseAlgorithm) == s3_constants.SSEAlgorithmAES256
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
	if sseAlgorithm, exists := metadata[s3_constants.AmzServerSideEncryption]; exists && string(sseAlgorithm) == s3_constants.SSEAlgorithmKMS {
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

// DetectEncryptionStateWithEntry analyzes the source entry and request headers to determine encryption state
// This version can detect multipart encrypted objects by examining chunks
func DetectEncryptionStateWithEntry(entry *filer_pb.Entry, r *http.Request, srcPath, dstPath string) *EncryptionState {
	state := &EncryptionState{
		SrcSSEC:    IsSSECEncryptedWithEntry(entry),
		SrcSSEKMS:  IsSSEKMSEncryptedWithEntry(entry),
		SrcSSES3:   IsSSES3EncryptedInternal(entry.Extended),
		DstSSEC:    IsSSECRequest(r),
		DstSSEKMS:  IsSSEKMSRequest(r),
		DstSSES3:   IsSSES3RequestInternal(r),
		SameObject: srcPath == dstPath,
	}

	return state
}

// IsSSEKMSEncryptedWithEntry detects SSE-KMS encryption from entry (including multipart objects)
func IsSSEKMSEncryptedWithEntry(entry *filer_pb.Entry) bool {
	if entry == nil {
		return false
	}

	// Check object-level metadata first
	if IsSSEKMSEncrypted(entry.Extended) {
		return true
	}

	// Check for multipart SSE-KMS by examining chunks
	if len(entry.GetChunks()) > 0 {
		for _, chunk := range entry.GetChunks() {
			if chunk.GetSseType() == filer_pb.SSEType_SSE_KMS {
				return true
			}
		}
	}

	return false
}

// IsSSECEncryptedWithEntry detects SSE-C encryption from entry (including multipart objects)
func IsSSECEncryptedWithEntry(entry *filer_pb.Entry) bool {
	if entry == nil {
		return false
	}

	// Check object-level metadata first
	if IsSSECEncrypted(entry.Extended) {
		return true
	}

	// Check for multipart SSE-C by examining chunks
	if len(entry.GetChunks()) > 0 {
		for _, chunk := range entry.GetChunks() {
			if chunk.GetSseType() == filer_pb.SSEType_SSE_C {
				return true
			}
		}
	}

	return false
}

// Helper functions for SSE-C detection are in s3_sse_c.go

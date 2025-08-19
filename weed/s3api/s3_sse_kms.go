package s3api

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// SSEKMSKey contains the metadata for an SSE-KMS encrypted object
type SSEKMSKey struct {
	KeyID             string            // The KMS key ID used
	EncryptedDataKey  []byte            // The encrypted data encryption key
	EncryptionContext map[string]string // The encryption context used
	BucketKeyEnabled  bool              // Whether S3 Bucket Keys are enabled
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
)

// CreateSSEKMSEncryptedReader creates an encrypted reader using KMS envelope encryption
func CreateSSEKMSEncryptedReader(r io.Reader, keyID string, encryptionContext map[string]string) (io.Reader, *SSEKMSKey, error) {
	kmsProvider := kms.GetGlobalKMS()
	if kmsProvider == nil {
		return nil, nil, fmt.Errorf("KMS is not configured")
	}

	// Generate a data encryption key using KMS
	dataKeyReq := &kms.GenerateDataKeyRequest{
		KeyID:             keyID,
		KeySpec:           kms.KeySpecAES256,
		EncryptionContext: encryptionContext,
	}

	ctx := context.Background()
	dataKeyResp, err := kmsProvider.GenerateDataKey(ctx, dataKeyReq)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate data key: %v", err)
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

	// Create the SSE-KMS metadata
	sseKey := &SSEKMSKey{
		KeyID:             dataKeyResp.KeyID,
		EncryptedDataKey:  dataKeyResp.CiphertextBlob,
		EncryptionContext: encryptionContext,
		BucketKeyEnabled:  false, // TODO: Implement S3 Bucket Keys optimization
	}

	// The encrypted stream format: [IV][EncryptedData]
	// Similar to SSE-C, we prepend the IV to the encrypted data stream
	encryptedReader := io.MultiReader(
		bytes.NewReader(iv),
		&cipher.StreamReader{S: stream, R: r},
	)

	return encryptedReader, sseKey, nil
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

	// Read the IV from the beginning of the stream
	iv := make([]byte, 16) // AES block size
	if _, err := io.ReadFull(r, iv); err != nil {
		return nil, fmt.Errorf("failed to read IV: %v", err)
	}

	// Create AES cipher with the decrypted data key
	block, err := aes.NewCipher(decryptResp.Plaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}

	// Create CTR mode cipher stream for decryption
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
		// ARN format validation - must have exactly 6 parts
		parts := strings.Split(keyID, ":")
		if len(parts) != 6 {
			return false
		}
		resource := parts[5]
		return (strings.HasPrefix(resource, "key/") && len(resource) > 4) || (strings.HasPrefix(resource, "alias/") && len(resource) > 6)
	}

	if strings.HasPrefix(keyID, "alias/") {
		// Alias format validation
		return len(keyID) > 6 // "alias/" + at least one character
	}

	// UUID format validation
	uuidRegex := `^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`
	if match, _ := regexp.MatchString(uuidRegex, keyID); match {
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

// GetSSEKMSFromMetadata extracts SSE-KMS information from object metadata
func GetSSEKMSFromMetadata(metadata map[string][]byte) (*SSEKMSKey, error) {
	// Check if this is an SSE-KMS encrypted object
	sseAlgorithm, exists := metadata[s3_constants.AmzServerSideEncryption]
	if !exists || string(sseAlgorithm) != "aws:kms" {
		return nil, nil // Not an SSE-KMS object
	}

	// Extract SSE-KMS metadata
	sseMetadataBytes, exists := metadata[s3_constants.AmzEncryptedDataKey]
	if !exists {
		return nil, fmt.Errorf("SSE-KMS object missing encrypted data key metadata")
	}

	return DeserializeSSEKMSMetadata(sseMetadataBytes)
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
		return s3err.ErrKMSKeyNotFound
	case kms.ErrCodeInvalidCiphertext:
		return s3err.ErrKMSInvalidCiphertext
	default:
		glog.Errorf("Unmapped KMS error: %s - %s", kmsErr.Code, kmsErr.Message)
		return s3err.ErrInternalError
	}
}

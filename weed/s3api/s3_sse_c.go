package s3api

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// decryptReaderCloser wraps a cipher.StreamReader with proper Close() support
// This ensures the underlying io.ReadCloser (like http.Response.Body) is properly closed
type decryptReaderCloser struct {
	io.Reader
	underlyingCloser io.Closer
}

func (d *decryptReaderCloser) Close() error {
	if d.underlyingCloser != nil {
		return d.underlyingCloser.Close()
	}
	return nil
}

// SSECCopyStrategy represents different strategies for copying SSE-C objects
type SSECCopyStrategy int

const (
	// SSECCopyStrategyDirect indicates the object can be copied directly without decryption
	SSECCopyStrategyDirect SSECCopyStrategy = iota
	// SSECCopyStrategyDecryptEncrypt indicates the object must be decrypted then re-encrypted
	SSECCopyStrategyDecryptEncrypt
)

const (
	// SSE-C constants
	SSECustomerAlgorithmAES256 = s3_constants.SSEAlgorithmAES256
	SSECustomerKeySize         = 32 // 256 bits
)

// SSE-C related errors
var (
	ErrInvalidRequest             = errors.New("invalid request")
	ErrInvalidEncryptionAlgorithm = errors.New("invalid encryption algorithm")
	ErrInvalidEncryptionKey       = errors.New("invalid encryption key")
	ErrSSECustomerKeyMD5Mismatch  = errors.New("customer key MD5 mismatch")
	ErrSSECustomerKeyMissing      = errors.New("customer key missing")
	ErrSSECustomerKeyNotNeeded    = errors.New("customer key not needed")
)

// SSECustomerKey represents a customer-provided encryption key for SSE-C
type SSECustomerKey struct {
	Algorithm string
	Key       []byte
	KeyMD5    string
}

// IsSSECRequest checks if the request contains SSE-C headers
func IsSSECRequest(r *http.Request) bool {
	// If SSE-KMS headers are present, this is not an SSE-C request (they are mutually exclusive)
	sseAlgorithm := r.Header.Get(s3_constants.AmzServerSideEncryption)
	if sseAlgorithm == "aws:kms" || r.Header.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId) != "" {
		return false
	}

	return r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm) != ""
}

// IsSSECEncrypted checks if the metadata indicates SSE-C encryption
func IsSSECEncrypted(metadata map[string][]byte) bool {
	if metadata == nil {
		return false
	}

	// Check for SSE-C specific metadata keys
	if _, exists := metadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm]; exists {
		return true
	}
	if _, exists := metadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5]; exists {
		return true
	}

	return false
}

// validateAndParseSSECHeaders does the core validation and parsing logic
func validateAndParseSSECHeaders(algorithm, key, keyMD5 string) (*SSECustomerKey, error) {
	if algorithm == "" && key == "" && keyMD5 == "" {
		return nil, nil // No SSE-C headers
	}

	if algorithm == "" || key == "" || keyMD5 == "" {
		return nil, ErrInvalidRequest
	}

	if algorithm != SSECustomerAlgorithmAES256 {
		return nil, ErrInvalidEncryptionAlgorithm
	}

	// Decode and validate key
	keyBytes, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		return nil, ErrInvalidEncryptionKey
	}

	if len(keyBytes) != SSECustomerKeySize {
		return nil, ErrInvalidEncryptionKey
	}

	// Validate key MD5 (base64-encoded MD5 of the raw key bytes; case-sensitive)
	sum := md5.Sum(keyBytes)
	expectedMD5 := base64.StdEncoding.EncodeToString(sum[:])

	// Debug logging for MD5 validation
	glog.V(4).Infof("SSE-C MD5 validation: provided='%s', expected='%s', keyBytes=%x", keyMD5, expectedMD5, keyBytes)

	if keyMD5 != expectedMD5 {
		glog.Errorf("SSE-C MD5 mismatch: provided='%s', expected='%s'", keyMD5, expectedMD5)
		return nil, ErrSSECustomerKeyMD5Mismatch
	}

	return &SSECustomerKey{
		Algorithm: algorithm,
		Key:       keyBytes,
		KeyMD5:    keyMD5,
	}, nil
}

// ValidateSSECHeaders validates SSE-C headers in the request
func ValidateSSECHeaders(r *http.Request) error {
	algorithm := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm)
	key := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerKey)
	keyMD5 := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5)

	_, err := validateAndParseSSECHeaders(algorithm, key, keyMD5)
	return err
}

// ParseSSECHeaders parses and validates SSE-C headers from the request
func ParseSSECHeaders(r *http.Request) (*SSECustomerKey, error) {
	algorithm := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm)
	key := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerKey)
	keyMD5 := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5)

	return validateAndParseSSECHeaders(algorithm, key, keyMD5)
}

// ParseSSECCopySourceHeaders parses and validates SSE-C copy source headers from the request
func ParseSSECCopySourceHeaders(r *http.Request) (*SSECustomerKey, error) {
	algorithm := r.Header.Get(s3_constants.AmzCopySourceServerSideEncryptionCustomerAlgorithm)
	key := r.Header.Get(s3_constants.AmzCopySourceServerSideEncryptionCustomerKey)
	keyMD5 := r.Header.Get(s3_constants.AmzCopySourceServerSideEncryptionCustomerKeyMD5)

	return validateAndParseSSECHeaders(algorithm, key, keyMD5)
}

// CreateSSECEncryptedReader creates a new encrypted reader for SSE-C
// Returns the encrypted reader and the IV for metadata storage
func CreateSSECEncryptedReader(r io.Reader, customerKey *SSECustomerKey) (io.Reader, []byte, error) {
	if customerKey == nil {
		return r, nil, nil
	}

	// Create AES cipher
	block, err := aes.NewCipher(customerKey.Key)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}

	// Generate random IV
	iv := make([]byte, s3_constants.AESBlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, nil, fmt.Errorf("failed to generate IV: %v", err)
	}

	// Create CTR mode cipher
	stream := cipher.NewCTR(block, iv)

	// The IV is stored in metadata, so the encrypted stream does not need to prepend the IV
	// This ensures correct Content-Length for clients
	encryptedReader := &cipher.StreamReader{S: stream, R: r}

	return encryptedReader, iv, nil
}

// CreateSSECDecryptedReader creates a new decrypted reader for SSE-C
// The IV comes from metadata, not from the encrypted data stream
func CreateSSECDecryptedReader(r io.Reader, customerKey *SSECustomerKey, iv []byte) (io.Reader, error) {
	if customerKey == nil {
		return r, nil
	}

	// IV must be provided from metadata
	if err := ValidateIV(iv, "IV"); err != nil {
		return nil, fmt.Errorf("invalid IV from metadata: %w", err)
	}

	// Create AES cipher
	block, err := aes.NewCipher(customerKey.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}

	// Create CTR mode cipher using the IV from metadata
	stream := cipher.NewCTR(block, iv)
	decryptReader := &cipher.StreamReader{S: stream, R: r}

	// Wrap with closer if the underlying reader implements io.Closer
	if closer, ok := r.(io.Closer); ok {
		return &decryptReaderCloser{
			Reader:           decryptReader,
			underlyingCloser: closer,
		}, nil
	}

	return decryptReader, nil
}

// CreateSSECEncryptedReaderWithOffset creates an encrypted reader with a specific counter offset
// This is used for chunk-level encryption where each chunk needs a different counter position
func CreateSSECEncryptedReaderWithOffset(r io.Reader, customerKey *SSECustomerKey, iv []byte, counterOffset uint64) (io.Reader, error) {
	if customerKey == nil {
		return r, nil
	}

	// Create AES cipher
	block, err := aes.NewCipher(customerKey.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}

	// Create CTR mode cipher with offset
	stream := createCTRStreamWithOffset(block, iv, counterOffset)

	return &cipher.StreamReader{S: stream, R: r}, nil
}

// CreateSSECDecryptedReaderWithOffset creates a decrypted reader with a specific counter offset
func CreateSSECDecryptedReaderWithOffset(r io.Reader, customerKey *SSECustomerKey, iv []byte, counterOffset uint64) (io.Reader, error) {
	if customerKey == nil {
		return r, nil
	}

	// Create AES cipher
	block, err := aes.NewCipher(customerKey.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}

	// Create CTR mode cipher with offset
	stream := createCTRStreamWithOffset(block, iv, counterOffset)

	return &cipher.StreamReader{S: stream, R: r}, nil
}

// createCTRStreamWithOffset creates a CTR stream positioned at a specific counter offset
func createCTRStreamWithOffset(block cipher.Block, iv []byte, counterOffset uint64) cipher.Stream {
	adjustedIV, skip := calculateIVWithOffset(iv, int64(counterOffset))
	stream := cipher.NewCTR(block, adjustedIV)
	if skip > 0 {
		dummy := make([]byte, skip)
		stream.XORKeyStream(dummy, dummy)
	}
	return stream
}

// addCounterToIV adds a counter value to the IV (treating last 8 bytes as big-endian counter)
func addCounterToIV(iv []byte, counter uint64) {
	// Use the last 8 bytes as a big-endian counter
	for i := 7; i >= 0; i-- {
		carry := counter & 0xff
		iv[len(iv)-8+i] += byte(carry)
		if iv[len(iv)-8+i] >= byte(carry) {
			break // No overflow
		}
		counter >>= 8
	}
}

// GetSourceSSECInfo extracts SSE-C information from source object metadata
func GetSourceSSECInfo(metadata map[string][]byte) (algorithm string, keyMD5 string, isEncrypted bool) {
	if alg, exists := metadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm]; exists {
		algorithm = string(alg)
	}
	if md5, exists := metadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5]; exists {
		keyMD5 = string(md5)
	}
	isEncrypted = algorithm != "" && keyMD5 != ""
	return
}

// CanDirectCopySSEC determines if we can directly copy chunks without decrypt/re-encrypt
func CanDirectCopySSEC(srcMetadata map[string][]byte, copySourceKey *SSECustomerKey, destKey *SSECustomerKey) bool {
	_, srcKeyMD5, srcEncrypted := GetSourceSSECInfo(srcMetadata)

	// Case 1: Source unencrypted, destination unencrypted -> Direct copy
	if !srcEncrypted && destKey == nil {
		return true
	}

	// Case 2: Source encrypted, same key for decryption and destination -> Direct copy
	if srcEncrypted && copySourceKey != nil && destKey != nil {
		// Same key if MD5 matches exactly (base64 encoding is case-sensitive)
		return copySourceKey.KeyMD5 == srcKeyMD5 &&
			destKey.KeyMD5 == srcKeyMD5
	}

	// All other cases require decrypt/re-encrypt
	return false
}

// Note: SSECCopyStrategy is defined above

// DetermineSSECCopyStrategy determines the optimal copy strategy
func DetermineSSECCopyStrategy(srcMetadata map[string][]byte, copySourceKey *SSECustomerKey, destKey *SSECustomerKey) (SSECCopyStrategy, error) {
	_, srcKeyMD5, srcEncrypted := GetSourceSSECInfo(srcMetadata)

	// Validate source key if source is encrypted
	if srcEncrypted {
		if copySourceKey == nil {
			return SSECCopyStrategyDecryptEncrypt, ErrSSECustomerKeyMissing
		}
		if copySourceKey.KeyMD5 != srcKeyMD5 {
			return SSECCopyStrategyDecryptEncrypt, ErrSSECustomerKeyMD5Mismatch
		}
	} else if copySourceKey != nil {
		// Source not encrypted but copy source key provided
		return SSECCopyStrategyDecryptEncrypt, ErrSSECustomerKeyNotNeeded
	}

	if CanDirectCopySSEC(srcMetadata, copySourceKey, destKey) {
		return SSECCopyStrategyDirect, nil
	}

	return SSECCopyStrategyDecryptEncrypt, nil
}

// MapSSECErrorToS3Error maps SSE-C custom errors to S3 API error codes
func MapSSECErrorToS3Error(err error) s3err.ErrorCode {
	switch err {
	case ErrInvalidEncryptionAlgorithm:
		return s3err.ErrInvalidEncryptionAlgorithm
	case ErrInvalidEncryptionKey:
		return s3err.ErrInvalidEncryptionKey
	case ErrSSECustomerKeyMD5Mismatch:
		return s3err.ErrSSECustomerKeyMD5Mismatch
	case ErrSSECustomerKeyMissing:
		return s3err.ErrSSECustomerKeyMissing
	case ErrSSECustomerKeyNotNeeded:
		return s3err.ErrSSECustomerKeyNotNeeded
	default:
		return s3err.ErrInvalidRequest
	}
}

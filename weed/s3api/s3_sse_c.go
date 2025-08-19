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
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

const (
	// SSE-C constants
	SSECustomerAlgorithmAES256 = "AES256"
	SSECustomerKeySize         = 32 // 256 bits
	AESBlockSize               = 16 // AES block size in bytes
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

// SSECEncryptedReader wraps an io.Reader to provide SSE-C encryption
type SSECEncryptedReader struct {
	reader io.Reader
	cipher cipher.Stream
	iv     []byte
	first  bool
}

// SSECDecryptedReader wraps an io.Reader to provide SSE-C decryption
type SSECDecryptedReader struct {
	reader      io.Reader
	cipher      cipher.Stream
	customerKey *SSECustomerKey
	first       bool
}

// IsSSECRequest checks if the request contains SSE-C headers
func IsSSECRequest(r *http.Request) bool {
	return r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm) != ""
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

	// Validate key MD5
	expectedMD5 := fmt.Sprintf("%x", md5.Sum(keyBytes))
	if strings.ToLower(keyMD5) != expectedMD5 {
		return nil, ErrSSECustomerKeyMD5Mismatch
	}

	return &SSECustomerKey{
		Algorithm: algorithm,
		Key:       keyBytes,
		KeyMD5:    strings.ToLower(keyMD5),
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

// ValidateSSECKeyForObject validates that the provided SSE-C key matches the object's stored key MD5
func ValidateSSECKeyForObject(customerKey *SSECustomerKey, storedKeyMD5 string) error {
	if customerKey == nil {
		if storedKeyMD5 != "" {
			return ErrSSECustomerKeyMissing
		}
		return nil
	}

	if storedKeyMD5 == "" {
		return ErrSSECustomerKeyNotNeeded
	}

	if customerKey.KeyMD5 != strings.ToLower(storedKeyMD5) {
		return ErrSSECustomerKeyMD5Mismatch
	}

	return nil
}

// CreateSSECEncryptedReader creates a new encrypted reader for SSE-C
func CreateSSECEncryptedReader(r io.Reader, customerKey *SSECustomerKey) (io.Reader, error) {
	if customerKey == nil {
		return r, nil
	}

	// Create AES cipher
	block, err := aes.NewCipher(customerKey.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}

	// Generate random IV
	iv := make([]byte, AESBlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, fmt.Errorf("failed to generate IV: %v", err)
	}

	// Create CTR mode cipher
	stream := cipher.NewCTR(block, iv)

	return &SSECEncryptedReader{
		reader: r,
		cipher: stream,
		iv:     iv,
		first:  true,
	}, nil
}

// CreateSSECDecryptedReader creates a new decrypted reader for SSE-C
func CreateSSECDecryptedReader(r io.Reader, customerKey *SSECustomerKey) (io.Reader, error) {
	if customerKey == nil {
		return r, nil
	}

	return &SSECDecryptedReader{
		reader:      r,
		customerKey: customerKey,
		cipher:      nil, // Will be initialized when we read the IV
		first:       true,
	}, nil
}

// Read implements io.Reader for SSECEncryptedReader
func (r *SSECEncryptedReader) Read(p []byte) (n int, err error) {
	if r.first {
		// Prepend IV to the encrypted data
		r.first = false
		if len(p) < len(r.iv) {
			copy(p, r.iv[:len(p)])
			return len(p), nil
		}
		copy(p, r.iv)

		// Read and encrypt the rest
		remaining := p[len(r.iv):]
		n, err = r.reader.Read(remaining)
		if n > 0 {
			r.cipher.XORKeyStream(remaining[:n], remaining[:n])
		}
		return n + len(r.iv), err
	}

	// Encrypt data
	n, err = r.reader.Read(p)
	if n > 0 {
		r.cipher.XORKeyStream(p[:n], p[:n])
	}
	return n, err
}

// Read implements io.Reader for SSECDecryptedReader
func (r *SSECDecryptedReader) Read(p []byte) (n int, err error) {
	if r.first {
		// First read: extract IV and initialize cipher
		r.first = false
		iv := make([]byte, AESBlockSize)

		// Read IV from the beginning of the data
		_, err = io.ReadFull(r.reader, iv)
		if err != nil {
			return 0, fmt.Errorf("failed to read IV: %v", err)
		}

		// Create cipher with the extracted IV
		block, err := aes.NewCipher(r.customerKey.Key)
		if err != nil {
			return 0, fmt.Errorf("failed to create AES cipher: %v", err)
		}
		r.cipher = cipher.NewCTR(block, iv)
	}

	// Decrypt data
	n, err = r.reader.Read(p)
	if n > 0 {
		r.cipher.XORKeyStream(p[:n], p[:n])
	}
	return n, err
}

// GetSSECMetadataFromHeaders extracts SSE-C metadata for storage
func GetSSECMetadataFromHeaders(r *http.Request) map[string][]byte {
	metadata := make(map[string][]byte)

	if algorithm := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm); algorithm != "" {
		metadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] = []byte(algorithm)
	}

	if keyMD5 := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5); keyMD5 != "" {
		metadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5] = []byte(strings.ToLower(keyMD5))
	}

	return metadata
}

// AddSSECResponseHeaders adds SSE-C headers to the response
func AddSSECResponseHeaders(w http.ResponseWriter, metadata map[string][]byte) {
	if algorithm, exists := metadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm]; exists {
		w.Header().Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, string(algorithm))
	}

	if keyMD5, exists := metadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5]; exists {
		w.Header().Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, string(keyMD5))
	}
}

// IsSSECEncrypted checks if the object metadata indicates SSE-C encryption
func IsSSECEncrypted(metadata map[string][]byte) bool {
	_, hasAlgorithm := metadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm]
	_, hasKeyMD5 := metadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5]
	return hasAlgorithm && hasKeyMD5
}

// GetStoredKeyMD5 extracts the stored key MD5 from object metadata
func GetStoredKeyMD5(metadata map[string][]byte) string {
	if keyMD5Bytes, exists := metadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5]; exists {
		return string(keyMD5Bytes)
	}
	return ""
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
		// Same key if MD5 matches
		return strings.ToLower(copySourceKey.KeyMD5) == strings.ToLower(srcKeyMD5) &&
			strings.ToLower(destKey.KeyMD5) == strings.ToLower(srcKeyMD5)
	}

	// All other cases require decrypt/re-encrypt
	return false
}

// SSECCopyStrategy represents the strategy for copying SSE-C objects
type SSECCopyStrategy int

const (
	SSECCopyDirect    SSECCopyStrategy = iota // Direct chunk copy (fast)
	SSECCopyReencrypt                         // Decrypt and re-encrypt (slow)
)

// DetermineSSECCopyStrategy determines the optimal copy strategy
func DetermineSSECCopyStrategy(srcMetadata map[string][]byte, copySourceKey *SSECustomerKey, destKey *SSECustomerKey) (SSECCopyStrategy, error) {
	_, srcKeyMD5, srcEncrypted := GetSourceSSECInfo(srcMetadata)

	// Validate source key if source is encrypted
	if srcEncrypted {
		if copySourceKey == nil {
			return SSECCopyReencrypt, ErrSSECustomerKeyMissing
		}
		if !strings.EqualFold(copySourceKey.KeyMD5, srcKeyMD5) {
			return SSECCopyReencrypt, ErrSSECustomerKeyMD5Mismatch
		}
	} else if copySourceKey != nil {
		// Source not encrypted but copy source key provided
		return SSECCopyReencrypt, ErrSSECustomerKeyNotNeeded
	}

	if CanDirectCopySSEC(srcMetadata, copySourceKey, destKey) {
		return SSECCopyDirect, nil
	}

	return SSECCopyReencrypt, nil
}

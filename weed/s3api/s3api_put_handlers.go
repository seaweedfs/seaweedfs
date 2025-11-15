package s3api

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// PutToFilerEncryptionResult holds the result of encryption processing
type PutToFilerEncryptionResult struct {
	DataReader     io.Reader
	SSEType        string
	CustomerKey    *SSECustomerKey
	SSEIV          []byte
	SSEKMSKey      *SSEKMSKey
	SSES3Key       *SSES3Key
	SSEKMSMetadata []byte
	SSES3Metadata  []byte
}

// calculatePartOffset calculates unique offset for each part to prevent IV reuse in multipart uploads
// AWS S3 part numbers must start from 1, never 0 or negative
func calculatePartOffset(partNumber int) int64 {
	// AWS S3 part numbers must start from 1, never 0 or negative
	if partNumber < 1 {
		glog.Errorf("Invalid partNumber: %d. Must be >= 1.", partNumber)
		return 0
	}
	// Using a large multiplier to ensure block offsets for different parts do not overlap.
	// S3 part size limit is 5GB, so this provides a large safety margin.
	partOffset := int64(partNumber-1) * s3_constants.PartOffsetMultiplier
	return partOffset
}

// handleSSECEncryption processes SSE-C encryption for the data reader
func (s3a *S3ApiServer) handleSSECEncryption(r *http.Request, dataReader io.Reader) (io.Reader, *SSECustomerKey, []byte, s3err.ErrorCode) {
	// Handle SSE-C encryption if requested
	customerKey, err := ParseSSECHeaders(r)
	if err != nil {
		glog.Errorf("SSE-C header validation failed: %v", err)
		// Use shared error mapping helper
		errCode := MapSSECErrorToS3Error(err)
		return nil, nil, nil, errCode
	}

	// Apply SSE-C encryption if customer key is provided
	var sseIV []byte
	if customerKey != nil {
		encryptedReader, iv, encErr := CreateSSECEncryptedReader(dataReader, customerKey)
		if encErr != nil {
			return nil, nil, nil, s3err.ErrInternalError
		}
		dataReader = encryptedReader
		sseIV = iv
	}

	return dataReader, customerKey, sseIV, s3err.ErrNone
}

// handleSSEKMSEncryption processes SSE-KMS encryption for the data reader
func (s3a *S3ApiServer) handleSSEKMSEncryption(r *http.Request, dataReader io.Reader, partOffset int64) (io.Reader, *SSEKMSKey, []byte, s3err.ErrorCode) {
	// Handle SSE-KMS encryption if requested
	if !IsSSEKMSRequest(r) {
		return dataReader, nil, nil, s3err.ErrNone
	}

	glog.V(3).Infof("handleSSEKMSEncryption: SSE-KMS request detected, processing encryption")

	// Parse SSE-KMS headers
	keyID := r.Header.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId)
	bucketKeyEnabled := strings.ToLower(r.Header.Get(s3_constants.AmzServerSideEncryptionBucketKeyEnabled)) == "true"

	// Build encryption context
	bucket, object := s3_constants.GetBucketAndObject(r)
	encryptionContext := BuildEncryptionContext(bucket, object, bucketKeyEnabled)

	// Add any user-provided encryption context
	if contextHeader := r.Header.Get(s3_constants.AmzServerSideEncryptionContext); contextHeader != "" {
		userContext, err := parseEncryptionContext(contextHeader)
		if err != nil {
			return nil, nil, nil, s3err.ErrInvalidRequest
		}
		// Merge user context with default context
		for k, v := range userContext {
			encryptionContext[k] = v
		}
	}

	// Check if a base IV is provided (for multipart uploads)
	var encryptedReader io.Reader
	var sseKey *SSEKMSKey
	var encErr error

	baseIVHeader := r.Header.Get(s3_constants.SeaweedFSSSEKMSBaseIVHeader)
	if baseIVHeader != "" {
		// Decode the base IV from the header
		baseIV, decodeErr := base64.StdEncoding.DecodeString(baseIVHeader)
		if decodeErr != nil {
			glog.Errorf("handleSSEKMSEncryption: Failed to decode base IV: %v", decodeErr)
			fmt.Printf("[SSE-KMS DEBUG] Failed to decode base IV: %v\n", decodeErr)
			return nil, nil, nil, s3err.ErrInternalError
		}
		if len(baseIV) != 16 {
			glog.Errorf("handleSSEKMSEncryption: Invalid base IV length: %d (expected 16)", len(baseIV))
			fmt.Printf("[SSE-KMS DEBUG] Invalid base IV length: %d\n", len(baseIV))
			return nil, nil, nil, s3err.ErrInternalError
		}
		// Use the provided base IV with unique part offset for multipart upload consistency
		fmt.Printf("[SSE-KMS DEBUG] Creating encrypted reader with baseIV=%x, partOffset=%d\n", baseIV[:8], partOffset)
		encryptedReader, sseKey, encErr = CreateSSEKMSEncryptedReaderWithBaseIVAndOffset(dataReader, keyID, encryptionContext, bucketKeyEnabled, baseIV, partOffset)
		glog.V(4).Infof("Using provided base IV %x for SSE-KMS encryption", baseIV[:8])
	} else {
		// Generate a new IV for single-part uploads
		fmt.Printf("[SSE-KMS DEBUG] Creating encrypted reader for single-part (no base IV)\n")
		encryptedReader, sseKey, encErr = CreateSSEKMSEncryptedReaderWithBucketKey(dataReader, keyID, encryptionContext, bucketKeyEnabled)
	}

	if encErr != nil {
		glog.Errorf("handleSSEKMSEncryption: Encryption failed: %v", encErr)
		fmt.Printf("[SSE-KMS DEBUG] Encryption failed: %v\n", encErr)
		return nil, nil, nil, s3err.ErrInternalError
	}
	fmt.Printf("[SSE-KMS DEBUG] Encryption successful, keyID=%s\n", keyID)

	// Prepare SSE-KMS metadata for later header setting
	sseKMSMetadata, metaErr := SerializeSSEKMSMetadata(sseKey)
	if metaErr != nil {
		return nil, nil, nil, s3err.ErrInternalError
	}

	return encryptedReader, sseKey, sseKMSMetadata, s3err.ErrNone
}

// handleSSES3MultipartEncryption handles multipart upload logic for SSE-S3 encryption
func (s3a *S3ApiServer) handleSSES3MultipartEncryption(r *http.Request, dataReader io.Reader, partOffset int64) (io.Reader, *SSES3Key, s3err.ErrorCode) {
	keyDataHeader := r.Header.Get(s3_constants.SeaweedFSSSES3KeyDataHeader)
	baseIVHeader := r.Header.Get(s3_constants.SeaweedFSSSES3BaseIVHeader)

	glog.V(4).Infof("handleSSES3MultipartEncryption: using provided key and base IV for multipart part")

	// Decode the key data
	keyData, decodeErr := base64.StdEncoding.DecodeString(keyDataHeader)
	if decodeErr != nil {
		return nil, nil, s3err.ErrInternalError
	}

	// Deserialize the SSE-S3 key
	keyManager := GetSSES3KeyManager()
	key, deserializeErr := DeserializeSSES3Metadata(keyData, keyManager)
	if deserializeErr != nil {
		return nil, nil, s3err.ErrInternalError
	}

	// Decode the base IV
	baseIV, decodeErr := base64.StdEncoding.DecodeString(baseIVHeader)
	if decodeErr != nil || len(baseIV) != s3_constants.AESBlockSize {
		return nil, nil, s3err.ErrInternalError
	}

	// Use the provided base IV with unique part offset for multipart upload consistency
	encryptedReader, _, encErr := CreateSSES3EncryptedReaderWithBaseIV(dataReader, key, baseIV, partOffset)
	if encErr != nil {
		return nil, nil, s3err.ErrInternalError
	}

	glog.V(4).Infof("handleSSES3MultipartEncryption: using provided base IV %x", baseIV[:8])
	return encryptedReader, key, s3err.ErrNone
}

// handleSSES3SinglePartEncryption handles single-part upload logic for SSE-S3 encryption
func (s3a *S3ApiServer) handleSSES3SinglePartEncryption(dataReader io.Reader) (io.Reader, *SSES3Key, s3err.ErrorCode) {
	glog.V(4).Infof("handleSSES3SinglePartEncryption: generating new key for single-part upload")

	keyManager := GetSSES3KeyManager()
	key, err := keyManager.GetOrCreateKey("")
	if err != nil {
		return nil, nil, s3err.ErrInternalError
	}

	// Create encrypted reader
	encryptedReader, iv, encErr := CreateSSES3EncryptedReader(dataReader, key)
	if encErr != nil {
		return nil, nil, s3err.ErrInternalError
	}

	// Store IV on the key object for later decryption
	key.IV = iv

	// Store the key for later use
	keyManager.StoreKey(key)

	return encryptedReader, key, s3err.ErrNone
}

// handleSSES3Encryption processes SSE-S3 encryption for the data reader
func (s3a *S3ApiServer) handleSSES3Encryption(r *http.Request, dataReader io.Reader, partOffset int64) (io.Reader, *SSES3Key, []byte, s3err.ErrorCode) {
	if !IsSSES3RequestInternal(r) {
		return dataReader, nil, nil, s3err.ErrNone
	}

	glog.V(3).Infof("handleSSES3Encryption: SSE-S3 request detected, processing encryption")

	var encryptedReader io.Reader
	var sseS3Key *SSES3Key
	var errCode s3err.ErrorCode

	// Check if this is multipart upload (key data and base IV provided)
	keyDataHeader := r.Header.Get(s3_constants.SeaweedFSSSES3KeyDataHeader)
	baseIVHeader := r.Header.Get(s3_constants.SeaweedFSSSES3BaseIVHeader)

	if keyDataHeader != "" && baseIVHeader != "" {
		// Multipart upload: use provided key and base IV
		encryptedReader, sseS3Key, errCode = s3a.handleSSES3MultipartEncryption(r, dataReader, partOffset)
	} else {
		// Single-part upload: generate new key and IV
		encryptedReader, sseS3Key, errCode = s3a.handleSSES3SinglePartEncryption(dataReader)
	}

	if errCode != s3err.ErrNone {
		return nil, nil, nil, errCode
	}

	// Prepare SSE-S3 metadata for later header setting
	sseS3Metadata, metaErr := SerializeSSES3Metadata(sseS3Key)
	if metaErr != nil {
		return nil, nil, nil, s3err.ErrInternalError
	}

	glog.V(3).Infof("handleSSES3Encryption: prepared SSE-S3 metadata for object")
	return encryptedReader, sseS3Key, sseS3Metadata, s3err.ErrNone
}

// handleAllSSEEncryption processes all SSE types in sequence and returns the final encrypted reader
// This eliminates repetitive dataReader assignments and centralizes SSE processing
func (s3a *S3ApiServer) handleAllSSEEncryption(r *http.Request, dataReader io.Reader, partOffset int64) (*PutToFilerEncryptionResult, s3err.ErrorCode) {
	result := &PutToFilerEncryptionResult{
		DataReader: dataReader,
	}

	// Handle SSE-C encryption first
	encryptedReader, customerKey, sseIV, errCode := s3a.handleSSECEncryption(r, result.DataReader)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}
	result.DataReader = encryptedReader
	result.CustomerKey = customerKey
	result.SSEIV = sseIV

	// Handle SSE-KMS encryption
	encryptedReader, sseKMSKey, sseKMSMetadata, errCode := s3a.handleSSEKMSEncryption(r, result.DataReader, partOffset)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}
	result.DataReader = encryptedReader
	result.SSEKMSKey = sseKMSKey
	result.SSEKMSMetadata = sseKMSMetadata

	// Handle SSE-S3 encryption
	encryptedReader, sseS3Key, sseS3Metadata, errCode := s3a.handleSSES3Encryption(r, result.DataReader, partOffset)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}
	result.DataReader = encryptedReader
	result.SSES3Key = sseS3Key
	result.SSES3Metadata = sseS3Metadata

	// Set SSE type for response headers
	if customerKey != nil {
		result.SSEType = s3_constants.SSETypeC
	} else if sseKMSKey != nil {
		result.SSEType = s3_constants.SSETypeKMS
	} else if sseS3Key != nil {
		result.SSEType = s3_constants.SSETypeS3
	}

	return result, s3err.ErrNone
}

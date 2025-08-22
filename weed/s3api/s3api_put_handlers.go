package s3api

import (
	"encoding/base64"
	"io"
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// PutToFilerEncryptionResult holds the result of encryption processing
type PutToFilerEncryptionResult struct {
	DataReader        io.Reader
	SSEType          string
	CustomerKey      *SSECustomerKey
	SSEIV            []byte
	SSEKMSKey        *SSEKMSKey
	SSES3Key         *SSES3Key
	SSEKMSMetadata   []byte
	SSES3Metadata    []byte
}

// calculatePartOffset calculates unique offset for each part to prevent IV reuse in multipart uploads
func calculatePartOffset(partNumber int) int64 {
	if partNumber <= 0 {
		return 0
	}
	// Using a large multiplier to ensure block offsets for different parts do not overlap.
	// S3 part size limit is 5GB, so this provides a large safety margin.
	partOffset := int64(partNumber-1) * s3_constants.PartOffsetMultiplier
	glog.V(4).Infof("calculatePartOffset: calculated partOffset=%d for partNumber=%d", partOffset, partNumber)
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
		if decodeErr != nil || len(baseIV) != 16 {
			return nil, nil, nil, s3err.ErrInternalError
		}
		// Use the provided base IV with unique part offset for multipart upload consistency
		encryptedReader, sseKey, encErr = CreateSSEKMSEncryptedReaderWithBaseIVAndOffset(dataReader, keyID, encryptionContext, bucketKeyEnabled, baseIV, partOffset)
		glog.V(4).Infof("Using provided base IV %x for SSE-KMS encryption", baseIV[:8])
	} else {
		// Generate a new IV for single-part uploads
		encryptedReader, sseKey, encErr = CreateSSEKMSEncryptedReaderWithBucketKey(dataReader, keyID, encryptionContext, bucketKeyEnabled)
	}

	if encErr != nil {
		return nil, nil, nil, s3err.ErrInternalError
	}

	// Prepare SSE-KMS metadata for later header setting
	sseKMSMetadata, metaErr := SerializeSSEKMSMetadata(sseKey)
	if metaErr != nil {
		return nil, nil, nil, s3err.ErrInternalError
	}

	return encryptedReader, sseKey, sseKMSMetadata, s3err.ErrNone
}

// handleSSES3Encryption processes SSE-S3 encryption for the data reader
func (s3a *S3ApiServer) handleSSES3Encryption(r *http.Request, dataReader io.Reader, partOffset int64) (io.Reader, *SSES3Key, []byte, s3err.ErrorCode) {
	// Handle SSE-S3 encryption if requested
	if !IsSSES3RequestInternal(r) {
		return dataReader, nil, nil, s3err.ErrNone
	}

	glog.V(3).Infof("handleSSES3Encryption: SSE-S3 request detected, processing encryption")
	
	var sseS3Key *SSES3Key
	
	// Check if key data and base IV are provided (for multipart uploads)
	keyDataHeader := r.Header.Get(s3_constants.SeaweedFSSSES3KeyDataHeader)
	baseIVHeader := r.Header.Get(s3_constants.SeaweedFSSSES3BaseIVHeader)
	
	if keyDataHeader != "" && baseIVHeader != "" {
		// Multipart upload: use provided key data and base IV with offset calculation
		glog.V(4).Infof("handleSSES3Encryption: SSE-S3 multipart part detected, using provided key and base IV")
		
		// Decode the key data
		keyData, decodeErr := base64.StdEncoding.DecodeString(keyDataHeader)
		if decodeErr != nil {
			return nil, nil, nil, s3err.ErrInternalError
		}
		
		// Deserialize the SSE-S3 key
		keyManager := GetSSES3KeyManager()
		key, deserializeErr := DeserializeSSES3Metadata(keyData, keyManager)
		if deserializeErr != nil {
			return nil, nil, nil, s3err.ErrInternalError
		}
		sseS3Key = key
		
		// Decode the base IV
		baseIV, decodeErr := base64.StdEncoding.DecodeString(baseIVHeader)
		if decodeErr != nil || len(baseIV) != 16 {
			return nil, nil, nil, s3err.ErrInternalError
		}
		
		// Use the provided base IV with unique part offset for multipart upload consistency
		encryptedReader, _, encErr := CreateSSES3EncryptedReaderWithBaseIV(dataReader, sseS3Key, baseIV, partOffset)
		if encErr != nil {
			return nil, nil, nil, s3err.ErrInternalError
		}
		
		dataReader = encryptedReader
		glog.V(4).Infof("Using provided base IV %x for SSE-S3 multipart encryption", baseIV[:8])
	} else {
		// Single-part upload: generate new key and IV
		glog.V(4).Infof("handleSSES3Encryption: SSE-S3 single-part upload, generating new key")
		
		keyManager := GetSSES3KeyManager()
		key, err := keyManager.GetOrCreateKey("")
		if err != nil {
			return nil, nil, nil, s3err.ErrInternalError
		}
		sseS3Key = key
		
		// Create encrypted reader
		encryptedReader, iv, encErr := CreateSSES3EncryptedReader(dataReader, sseS3Key)
		if encErr != nil {
			return nil, nil, nil, s3err.ErrInternalError
		}
		// Store IV on the key object for later decryption
		sseS3Key.IV = iv
		
		dataReader = encryptedReader
		
		// Store the key for later use
		keyManager.StoreKey(sseS3Key)
	}
	
	// Prepare SSE-S3 metadata for later header setting
	sseS3Metadata, metaErr := SerializeSSES3Metadata(sseS3Key)
	if metaErr != nil {
		return nil, nil, nil, s3err.ErrInternalError
	}
	
	glog.V(3).Infof("handleSSES3Encryption: prepared SSE-S3 metadata for object")
	return dataReader, sseS3Key, sseS3Metadata, s3err.ErrNone
}

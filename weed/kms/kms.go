package kms

import (
	"context"
	"fmt"
)

// KMSProvider defines the interface for Key Management Service implementations
type KMSProvider interface {
	// GenerateDataKey creates a new data encryption key encrypted under the specified KMS key
	GenerateDataKey(ctx context.Context, req *GenerateDataKeyRequest) (*GenerateDataKeyResponse, error)

	// Decrypt decrypts an encrypted data key using the KMS
	Decrypt(ctx context.Context, req *DecryptRequest) (*DecryptResponse, error)

	// DescribeKey validates that a key exists and returns its metadata
	DescribeKey(ctx context.Context, req *DescribeKeyRequest) (*DescribeKeyResponse, error)

	// GetKeyID resolves a key alias or ARN to the actual key ID
	GetKeyID(ctx context.Context, keyIdentifier string) (string, error)

	// Close cleans up any resources used by the provider
	Close() error
}

// GenerateDataKeyRequest contains parameters for generating a data key
type GenerateDataKeyRequest struct {
	KeyID             string            // KMS key identifier (ID, ARN, or alias)
	KeySpec           KeySpec           // Specification for the data key
	EncryptionContext map[string]string // Additional authenticated data
}

// GenerateDataKeyResponse contains the generated data key
type GenerateDataKeyResponse struct {
	KeyID          string // The actual KMS key ID used
	Plaintext      []byte // The plaintext data key (sensitive - clear from memory ASAP)
	CiphertextBlob []byte // The encrypted data key for storage
}

// DecryptRequest contains parameters for decrypting a data key
type DecryptRequest struct {
	CiphertextBlob    []byte            // The encrypted data key
	EncryptionContext map[string]string // Must match the context used during encryption
}

// DecryptResponse contains the decrypted data key
type DecryptResponse struct {
	KeyID     string // The KMS key ID that was used for encryption
	Plaintext []byte // The decrypted data key (sensitive - clear from memory ASAP)
}

// DescribeKeyRequest contains parameters for describing a key
type DescribeKeyRequest struct {
	KeyID string // KMS key identifier (ID, ARN, or alias)
}

// DescribeKeyResponse contains key metadata
type DescribeKeyResponse struct {
	KeyID       string    // The actual key ID
	ARN         string    // The key ARN
	Description string    // Key description
	KeyUsage    KeyUsage  // How the key can be used
	KeyState    KeyState  // Current state of the key
	Origin      KeyOrigin // Where the key material originated
}

// KeySpec specifies the type of data key to generate
type KeySpec string

const (
	KeySpecAES256 KeySpec = "AES_256" // 256-bit AES key
)

// KeyUsage specifies how a key can be used
type KeyUsage string

const (
	KeyUsageEncryptDecrypt  KeyUsage = "ENCRYPT_DECRYPT"
	KeyUsageGenerateDataKey KeyUsage = "GENERATE_DATA_KEY"
)

// KeyState represents the current state of a KMS key
type KeyState string

const (
	KeyStateEnabled         KeyState = "Enabled"
	KeyStateDisabled        KeyState = "Disabled"
	KeyStatePendingDeletion KeyState = "PendingDeletion"
	KeyStateUnavailable     KeyState = "Unavailable"
)

// KeyOrigin indicates where the key material came from
type KeyOrigin string

const (
	KeyOriginAWS      KeyOrigin = "AWS_KMS"
	KeyOriginExternal KeyOrigin = "EXTERNAL"
	KeyOriginCloudHSM KeyOrigin = "AWS_CLOUDHSM"
	KeyOriginAzure    KeyOrigin = "AZURE_KEY_VAULT"
	KeyOriginGCP      KeyOrigin = "GCP_KMS"
	KeyOriginOpenBao  KeyOrigin = "OPENBAO"
	KeyOriginLocal    KeyOrigin = "LOCAL"
)

// KMSError represents an error from the KMS service
type KMSError struct {
	Code    string // Error code (e.g., "KeyUnavailableException")
	Message string // Human-readable error message
	KeyID   string // Key ID that caused the error (if applicable)
}

func (e *KMSError) Error() string {
	if e.KeyID != "" {
		return fmt.Sprintf("KMS error %s for key %s: %s", e.Code, e.KeyID, e.Message)
	}
	return fmt.Sprintf("KMS error %s: %s", e.Code, e.Message)
}

// Common KMS error codes
const (
	ErrCodeKeyUnavailable     = "KeyUnavailableException"
	ErrCodeAccessDenied       = "AccessDeniedException"
	ErrCodeNotFoundException  = "NotFoundException"
	ErrCodeInvalidKeyUsage    = "InvalidKeyUsageException"
	ErrCodeKMSInternalFailure = "KMSInternalException"
	ErrCodeInvalidCiphertext  = "InvalidCiphertextException"
)

// EncryptionContextKey constants for building encryption context
const (
	EncryptionContextS3ARN    = "aws:s3:arn"
	EncryptionContextS3Bucket = "aws:s3:bucket"
	EncryptionContextS3Object = "aws:s3:object"
)

// BuildS3EncryptionContext creates the standard encryption context for S3 objects
// Following AWS S3 conventions from the documentation
func BuildS3EncryptionContext(bucketName, objectKey string, useBucketKey bool) map[string]string {
	context := make(map[string]string)

	if useBucketKey {
		// When using S3 Bucket Keys, use bucket ARN as encryption context
		context[EncryptionContextS3ARN] = fmt.Sprintf("arn:aws:s3:::%s", bucketName)
	} else {
		// For individual object encryption, use object ARN as encryption context
		context[EncryptionContextS3ARN] = fmt.Sprintf("arn:aws:s3:::%s/%s", bucketName, objectKey)
	}

	return context
}

// ClearSensitiveData securely clears sensitive byte slices
func ClearSensitiveData(data []byte) {
	if data != nil {
		for i := range data {
			data[i] = 0
		}
	}
}

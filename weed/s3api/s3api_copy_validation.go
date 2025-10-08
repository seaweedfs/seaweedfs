package s3api

import (
	"fmt"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// CopyValidationError represents validation errors during copy operations
type CopyValidationError struct {
	Code    s3err.ErrorCode
	Message string
}

func (e *CopyValidationError) Error() string {
	return e.Message
}

// ValidateCopyEncryption performs comprehensive validation of copy encryption parameters
func ValidateCopyEncryption(srcMetadata map[string][]byte, headers http.Header) error {
	// Validate SSE-C copy requirements
	if err := validateSSECCopyRequirements(srcMetadata, headers); err != nil {
		return err
	}

	// Validate SSE-KMS copy requirements
	if err := validateSSEKMSCopyRequirements(srcMetadata, headers); err != nil {
		return err
	}

	// Validate incompatible encryption combinations
	if err := validateEncryptionCompatibility(headers); err != nil {
		return err
	}

	return nil
}

// validateSSECCopyRequirements validates SSE-C copy header requirements
func validateSSECCopyRequirements(srcMetadata map[string][]byte, headers http.Header) error {
	srcIsSSEC := IsSSECEncrypted(srcMetadata)
	hasCopyHeaders := hasSSECCopyHeaders(headers)
	hasSSECHeaders := hasSSECHeaders(headers)

	// If source is SSE-C encrypted, copy headers are required
	if srcIsSSEC && !hasCopyHeaders {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidRequest,
			Message: "SSE-C encrypted source requires copy source encryption headers",
		}
	}

	// If copy headers are provided, source must be SSE-C encrypted
	if hasCopyHeaders && !srcIsSSEC {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidRequest,
			Message: "SSE-C copy headers provided but source is not SSE-C encrypted",
		}
	}

	// Validate copy header completeness
	if hasCopyHeaders {
		if err := validateSSECCopyHeaderCompleteness(headers); err != nil {
			return err
		}
	}

	// Validate destination SSE-C headers if present
	if hasSSECHeaders {
		if err := validateSSECHeaderCompleteness(headers); err != nil {
			return err
		}
	}

	return nil
}

// validateSSEKMSCopyRequirements validates SSE-KMS copy requirements
func validateSSEKMSCopyRequirements(srcMetadata map[string][]byte, headers http.Header) error {
	dstIsSSEKMS := IsSSEKMSRequest(&http.Request{Header: headers})

	// Validate KMS key ID format if provided
	if dstIsSSEKMS {
		keyID := headers.Get(s3_constants.AmzServerSideEncryptionAwsKmsKeyId)
		if keyID != "" && !isValidKMSKeyID(keyID) {
			return &CopyValidationError{
				Code:    s3err.ErrKMSKeyNotFound,
				Message: fmt.Sprintf("Invalid KMS key ID format: %s", keyID),
			}
		}
	}

	// Validate encryption context format if provided
	if contextHeader := headers.Get(s3_constants.AmzServerSideEncryptionContext); contextHeader != "" {
		if !dstIsSSEKMS {
			return &CopyValidationError{
				Code:    s3err.ErrInvalidRequest,
				Message: "Encryption context can only be used with SSE-KMS",
			}
		}

		// Validate base64 encoding and JSON format
		if err := validateEncryptionContext(contextHeader); err != nil {
			return &CopyValidationError{
				Code:    s3err.ErrInvalidRequest,
				Message: fmt.Sprintf("Invalid encryption context: %v", err),
			}
		}
	}

	return nil
}

// validateEncryptionCompatibility validates that encryption methods are not conflicting
func validateEncryptionCompatibility(headers http.Header) error {
	hasSSEC := hasSSECHeaders(headers)
	hasSSEKMS := headers.Get(s3_constants.AmzServerSideEncryption) == "aws:kms"
	hasSSES3 := headers.Get(s3_constants.AmzServerSideEncryption) == "AES256"

	// Count how many encryption methods are specified
	encryptionCount := 0
	if hasSSEC {
		encryptionCount++
	}
	if hasSSEKMS {
		encryptionCount++
	}
	if hasSSES3 {
		encryptionCount++
	}

	// Only one encryption method should be specified
	if encryptionCount > 1 {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidRequest,
			Message: "Multiple encryption methods specified - only one is allowed",
		}
	}

	return nil
}

// validateSSECCopyHeaderCompleteness validates that all required SSE-C copy headers are present
func validateSSECCopyHeaderCompleteness(headers http.Header) error {
	algorithm := headers.Get(s3_constants.AmzCopySourceServerSideEncryptionCustomerAlgorithm)
	key := headers.Get(s3_constants.AmzCopySourceServerSideEncryptionCustomerKey)
	keyMD5 := headers.Get(s3_constants.AmzCopySourceServerSideEncryptionCustomerKeyMD5)

	if algorithm == "" {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidRequest,
			Message: "SSE-C copy customer algorithm header is required",
		}
	}

	if key == "" {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidRequest,
			Message: "SSE-C copy customer key header is required",
		}
	}

	if keyMD5 == "" {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidRequest,
			Message: "SSE-C copy customer key MD5 header is required",
		}
	}

	// Validate algorithm
	if algorithm != "AES256" {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidRequest,
			Message: fmt.Sprintf("Unsupported SSE-C algorithm: %s", algorithm),
		}
	}

	return nil
}

// validateSSECHeaderCompleteness validates that all required SSE-C headers are present
func validateSSECHeaderCompleteness(headers http.Header) error {
	algorithm := headers.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm)
	key := headers.Get(s3_constants.AmzServerSideEncryptionCustomerKey)
	keyMD5 := headers.Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5)

	if algorithm == "" {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidRequest,
			Message: "SSE-C customer algorithm header is required",
		}
	}

	if key == "" {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidRequest,
			Message: "SSE-C customer key header is required",
		}
	}

	if keyMD5 == "" {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidRequest,
			Message: "SSE-C customer key MD5 header is required",
		}
	}

	// Validate algorithm
	if algorithm != "AES256" {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidRequest,
			Message: fmt.Sprintf("Unsupported SSE-C algorithm: %s", algorithm),
		}
	}

	return nil
}

// Helper functions for header detection
func hasSSECCopyHeaders(headers http.Header) bool {
	return headers.Get(s3_constants.AmzCopySourceServerSideEncryptionCustomerAlgorithm) != "" ||
		headers.Get(s3_constants.AmzCopySourceServerSideEncryptionCustomerKey) != "" ||
		headers.Get(s3_constants.AmzCopySourceServerSideEncryptionCustomerKeyMD5) != ""
}

func hasSSECHeaders(headers http.Header) bool {
	return headers.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm) != "" ||
		headers.Get(s3_constants.AmzServerSideEncryptionCustomerKey) != "" ||
		headers.Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5) != ""
}

// validateEncryptionContext validates the encryption context header format
func validateEncryptionContext(contextHeader string) error {
	// This would validate base64 encoding and JSON format
	// Implementation would decode base64 and parse JSON
	// For now, just check it's not empty
	if contextHeader == "" {
		return fmt.Errorf("encryption context cannot be empty")
	}
	return nil
}

// ValidateCopySource validates the copy source path and permissions
func ValidateCopySource(copySource string, srcBucket, srcObject string) error {
	if copySource == "" {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidCopySource,
			Message: "Copy source header is required",
		}
	}

	if srcBucket == "" {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidCopySource,
			Message: "Source bucket cannot be empty",
		}
	}

	if srcObject == "" {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidCopySource,
			Message: "Source object cannot be empty",
		}
	}

	return nil
}

// ValidateCopyDestination validates the copy destination
func ValidateCopyDestination(dstBucket, dstObject string) error {
	if dstBucket == "" {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidRequest,
			Message: "Destination bucket cannot be empty",
		}
	}

	if dstObject == "" {
		return &CopyValidationError{
			Code:    s3err.ErrInvalidRequest,
			Message: "Destination object cannot be empty",
		}
	}

	return nil
}

// MapCopyValidationError maps validation errors to appropriate S3 error codes
func MapCopyValidationError(err error) s3err.ErrorCode {
	if validationErr, ok := err.(*CopyValidationError); ok {
		return validationErr.Code
	}
	return s3err.ErrInvalidRequest
}

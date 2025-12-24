package iam

import (
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"math/big"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// Hash computes a SHA1 hash of the input string.
func Hash(s *string) string {
	h := sha1.New()
	h.Write([]byte(*s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// GenerateRandomString generates a cryptographically secure random string.
// Uses crypto/rand for security-sensitive credential generation.
func GenerateRandomString(length int, charset string) (string, error) {
	if length <= 0 {
		return "", fmt.Errorf("length must be positive, got %d", length)
	}
	if charset == "" {
		return "", fmt.Errorf("charset must not be empty")
	}
	b := make([]byte, length)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			return "", fmt.Errorf("failed to generate random index: %w", err)
		}
		b[i] = charset[n.Int64()]
	}
	return string(b), nil
}

// GenerateAccessKeyId generates a new access key ID.
func GenerateAccessKeyId() (string, error) {
	return GenerateRandomString(AccessKeyIdLength, CharsetUpper)
}

// GenerateSecretAccessKey generates a new secret access key.
func GenerateSecretAccessKey() (string, error) {
	return GenerateRandomString(SecretAccessKeyLength, Charset)
}

// StringSlicesEqual compares two string slices for equality, ignoring order.
// This is used instead of reflect.DeepEqual to avoid order-dependent comparisons.
func StringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	// Make copies to avoid modifying the originals
	aCopy := make([]string, len(a))
	bCopy := make([]string, len(b))
	copy(aCopy, a)
	copy(bCopy, b)
	sort.Strings(aCopy)
	sort.Strings(bCopy)
	for i := range aCopy {
		if aCopy[i] != bCopy[i] {
			return false
		}
	}
	return true
}

// MapToStatementAction converts a policy statement action to an S3 action constant.
// It handles both coarse-grained action patterns (e.g., "Put*", "Get*") and
// fine-grained S3 actions (e.g., "s3:DeleteObject", "s3:PutObject").
func MapToStatementAction(action string) string {
	// Handle coarse-grained action patterns
	switch action {
	case StatementActionAdmin:
		return s3_constants.ACTION_ADMIN
	case StatementActionWrite:
		return s3_constants.ACTION_WRITE
	case StatementActionWriteAcp:
		return s3_constants.ACTION_WRITE_ACP
	case StatementActionRead:
		return s3_constants.ACTION_READ
	case StatementActionReadAcp:
		return s3_constants.ACTION_READ_ACP
	case StatementActionList:
		return s3_constants.ACTION_LIST
	case StatementActionTagging:
		return s3_constants.ACTION_TAGGING
	case StatementActionDelete:
		return s3_constants.ACTION_DELETE_BUCKET
	}

	// Handle fine-grained S3 actions from IAM policies
	// Map S3-specific actions to internal action constants
	// These are used when policies specify actions like "s3:DeleteObject"
	switch action {
	case "DeleteObject", "s3:DeleteObject":
		// DeleteObject is a write operation
		return s3_constants.ACTION_WRITE
	case "PutObject", "s3:PutObject":
		return s3_constants.ACTION_WRITE
	case "GetObject", "s3:GetObject":
		return s3_constants.ACTION_READ
	case "ListBucket", "s3:ListBucket":
		return s3_constants.ACTION_LIST
	case "PutObjectAcl", "s3:PutObjectAcl":
		return s3_constants.ACTION_WRITE_ACP
	case "GetObjectAcl", "s3:GetObjectAcl":
		return s3_constants.ACTION_READ_ACP
	case "GetObjectTagging", "s3:GetObjectTagging":
		return s3_constants.ACTION_READ
	case "PutObjectTagging", "s3:PutObjectTagging":
		return s3_constants.ACTION_TAGGING
	case "DeleteObjectTagging", "s3:DeleteObjectTagging":
		return s3_constants.ACTION_TAGGING
	case "DeleteBucket", "s3:DeleteBucket":
		return s3_constants.ACTION_DELETE_BUCKET
	case "DeleteBucketPolicy", "s3:DeleteBucketPolicy":
		return s3_constants.ACTION_ADMIN
	case "DeleteObjectVersion", "s3:DeleteObjectVersion":
		return s3_constants.ACTION_WRITE
	case "GetObjectVersion", "s3:GetObjectVersion":
		return s3_constants.ACTION_READ
	case "CreateMultipartUpload", "s3:CreateMultipartUpload":
		return s3_constants.ACTION_WRITE
	case "UploadPart", "s3:UploadPart":
		return s3_constants.ACTION_WRITE
	case "CompleteMultipartUpload", "s3:CompleteMultipartUpload":
		return s3_constants.ACTION_WRITE
	case "AbortMultipartUpload", "s3:AbortMultipartUpload":
		return s3_constants.ACTION_WRITE
	case "ListMultipartUploads", "s3:ListMultipartUploads":
		return s3_constants.ACTION_LIST
	case "ListParts", "s3:ListParts":
		return s3_constants.ACTION_LIST
	case "GetBucketLocation", "s3:GetBucketLocation":
		return s3_constants.ACTION_READ
	case "GetBucketVersioning", "s3:GetBucketVersioning":
		return s3_constants.ACTION_READ
	case "PutBucketVersioning", "s3:PutBucketVersioning":
		return s3_constants.ACTION_WRITE
	case "GetBucketAcl", "s3:GetBucketAcl":
		return s3_constants.ACTION_READ_ACP
	case "GetObjectRetention", "s3:GetObjectRetention":
		return s3_constants.ACTION_READ
	case "PutObjectRetention", "s3:PutObjectRetention":
		return s3_constants.ACTION_WRITE
	case "GetObjectLegalHold", "s3:GetObjectLegalHold":
		return s3_constants.ACTION_READ
	case "PutObjectLegalHold", "s3:PutObjectLegalHold":
		return s3_constants.ACTION_WRITE
	case "BypassGovernanceRetention", "s3:BypassGovernanceRetention":
		return s3_constants.ACTION_WRITE
	default:
		return ""
	}
}

// MapToIdentitiesAction converts an S3 action constant to a policy statement action.
func MapToIdentitiesAction(action string) string {
	switch action {
	case s3_constants.ACTION_ADMIN:
		return StatementActionAdmin
	case s3_constants.ACTION_WRITE:
		return StatementActionWrite
	case s3_constants.ACTION_WRITE_ACP:
		return StatementActionWriteAcp
	case s3_constants.ACTION_READ:
		return StatementActionRead
	case s3_constants.ACTION_READ_ACP:
		return StatementActionReadAcp
	case s3_constants.ACTION_LIST:
		return StatementActionList
	case s3_constants.ACTION_TAGGING:
		return StatementActionTagging
	case s3_constants.ACTION_DELETE_BUCKET:
		return StatementActionDelete
	default:
		return ""
	}
}

// MaskAccessKey masks an access key for logging, showing only the first 4 characters.
func MaskAccessKey(accessKeyId string) string {
	if len(accessKeyId) > 4 {
		return accessKeyId[:4] + "***"
	}
	return accessKeyId
}

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

// fineGrainedActionMap maps S3 IAM action names to internal S3 action constants.
// Supports both prefixed (e.g., "s3:DeleteObject") and unprefixed (e.g., "DeleteObject") formats.
var fineGrainedActionMap = map[string]string{
	// Coarse-grained actions
	StatementActionAdmin:     s3_constants.ACTION_ADMIN,
	StatementActionWrite:     s3_constants.ACTION_WRITE,
	StatementActionWriteAcp:  s3_constants.ACTION_WRITE_ACP,
	StatementActionRead:      s3_constants.ACTION_READ,
	StatementActionReadAcp:   s3_constants.ACTION_READ_ACP,
	StatementActionList:      s3_constants.ACTION_LIST,
	StatementActionTagging:   s3_constants.ACTION_TAGGING,
	StatementActionDelete:    s3_constants.ACTION_DELETE_BUCKET,
	// Object operations
	"DeleteObject":                s3_constants.ACTION_WRITE,
	"s3:DeleteObject":             s3_constants.ACTION_WRITE,
	"PutObject":                   s3_constants.ACTION_WRITE,
	"s3:PutObject":                s3_constants.ACTION_WRITE,
	"GetObject":                   s3_constants.ACTION_READ,
	"s3:GetObject":                s3_constants.ACTION_READ,
	"DeleteObjectVersion":         s3_constants.ACTION_WRITE,
	"s3:DeleteObjectVersion":      s3_constants.ACTION_WRITE,
	"GetObjectVersion":            s3_constants.ACTION_READ,
	"s3:GetObjectVersion":         s3_constants.ACTION_READ,
	// Tagging operations
	"GetObjectTagging":            s3_constants.ACTION_READ,
	"s3:GetObjectTagging":         s3_constants.ACTION_READ,
	"PutObjectTagging":            s3_constants.ACTION_TAGGING,
	"s3:PutObjectTagging":         s3_constants.ACTION_TAGGING,
	"DeleteObjectTagging":         s3_constants.ACTION_TAGGING,
	"s3:DeleteObjectTagging":      s3_constants.ACTION_TAGGING,
	// ACL operations
	"PutObjectAcl":                s3_constants.ACTION_WRITE_ACP,
	"s3:PutObjectAcl":             s3_constants.ACTION_WRITE_ACP,
	"GetObjectAcl":                s3_constants.ACTION_READ_ACP,
	"s3:GetObjectAcl":             s3_constants.ACTION_READ_ACP,
	"s3:GetBucketAcl":             s3_constants.ACTION_READ_ACP,
	// Bucket operations
	"DeleteBucket":                s3_constants.ACTION_DELETE_BUCKET,
	"s3:DeleteBucket":             s3_constants.ACTION_DELETE_BUCKET,
	"DeleteBucketPolicy":          s3_constants.ACTION_ADMIN,
	"s3:DeleteBucketPolicy":       s3_constants.ACTION_ADMIN,
	"ListBucket":                  s3_constants.ACTION_LIST,
	"s3:ListBucket":               s3_constants.ACTION_LIST,
	"GetBucketLocation":           s3_constants.ACTION_READ,
	"s3:GetBucketLocation":        s3_constants.ACTION_READ,
	"GetBucketVersioning":         s3_constants.ACTION_READ,
	"s3:GetBucketVersioning":      s3_constants.ACTION_READ,
	"PutBucketVersioning":         s3_constants.ACTION_WRITE,
	"s3:PutBucketVersioning":      s3_constants.ACTION_WRITE,
	// Multipart upload operations
	"CreateMultipartUpload":       s3_constants.ACTION_WRITE,
	"s3:CreateMultipartUpload":    s3_constants.ACTION_WRITE,
	"UploadPart":                  s3_constants.ACTION_WRITE,
	"s3:UploadPart":               s3_constants.ACTION_WRITE,
	"CompleteMultipartUpload":     s3_constants.ACTION_WRITE,
	"s3:CompleteMultipartUpload":  s3_constants.ACTION_WRITE,
	"AbortMultipartUpload":        s3_constants.ACTION_WRITE,
	"s3:AbortMultipartUpload":     s3_constants.ACTION_WRITE,
	"ListMultipartUploads":        s3_constants.ACTION_LIST,
	"s3:ListMultipartUploads":     s3_constants.ACTION_LIST,
	"ListParts":                   s3_constants.ACTION_LIST,
	"s3:ListParts":                s3_constants.ACTION_LIST,
	// Retention and legal hold operations
	"GetObjectRetention":          s3_constants.ACTION_READ,
	"s3:GetObjectRetention":       s3_constants.ACTION_READ,
	"PutObjectRetention":          s3_constants.ACTION_WRITE,
	"s3:PutObjectRetention":       s3_constants.ACTION_WRITE,
	"GetObjectLegalHold":          s3_constants.ACTION_READ,
	"s3:GetObjectLegalHold":       s3_constants.ACTION_READ,
	"PutObjectLegalHold":          s3_constants.ACTION_WRITE,
	"s3:PutObjectLegalHold":       s3_constants.ACTION_WRITE,
	"BypassGovernanceRetention":   s3_constants.ACTION_WRITE,
	"s3:BypassGovernanceRetention": s3_constants.ACTION_WRITE,
}

// MapToStatementAction converts a policy statement action to an S3 action constant.
// It handles both coarse-grained action patterns (e.g., "Put*", "Get*") and
// fine-grained S3 actions (e.g., "s3:DeleteObject", "s3:PutObject").
func MapToStatementAction(action string) string {
	if val, ok := fineGrainedActionMap[action]; ok {
		return val
	}
	return ""
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

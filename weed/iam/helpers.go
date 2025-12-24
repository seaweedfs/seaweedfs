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
// Populated in init() to avoid duplication of prefixed/unprefixed variants.
var fineGrainedActionMap = map[string]string{
	// Coarse-grained actions (populated statically)
	StatementActionAdmin:    s3_constants.ACTION_ADMIN,
	StatementActionWrite:    s3_constants.ACTION_WRITE,
	StatementActionWriteAcp: s3_constants.ACTION_WRITE_ACP,
	StatementActionRead:     s3_constants.ACTION_READ,
	StatementActionReadAcp:  s3_constants.ACTION_READ_ACP,
	StatementActionList:     s3_constants.ACTION_LIST,
	StatementActionTagging:  s3_constants.ACTION_TAGGING,
	StatementActionDelete:   s3_constants.ACTION_DELETE_BUCKET,
}

// baseS3ActionMap defines the base S3 actions that will be populated with both
// prefixed (s3:Action) and unprefixed (Action) variants in init().
var baseS3ActionMap = map[string]string{
	// Object operations
	"DeleteObject":        s3_constants.ACTION_WRITE,
	"PutObject":           s3_constants.ACTION_WRITE,
	"GetObject":           s3_constants.ACTION_READ,
	"DeleteObjectVersion": s3_constants.ACTION_WRITE,
	"GetObjectVersion":    s3_constants.ACTION_READ,
	// Tagging operations
	"GetObjectTagging":      s3_constants.ACTION_TAGGING,
	"GetObjectVersionTagging": s3_constants.ACTION_TAGGING,
	"PutObjectTagging":      s3_constants.ACTION_TAGGING,
	"DeleteObjectTagging":   s3_constants.ACTION_TAGGING,
	"GetBucketTagging":      s3_constants.ACTION_TAGGING,
	"PutBucketTagging":      s3_constants.ACTION_TAGGING,
	"DeleteBucketTagging":   s3_constants.ACTION_TAGGING,
	// ACL operations
	"PutObjectAcl":        s3_constants.ACTION_WRITE_ACP,
	"GetObjectAcl":        s3_constants.ACTION_READ_ACP,
	"GetObjectVersionAcl": s3_constants.ACTION_READ_ACP,
	"PutBucketAcl":        s3_constants.ACTION_WRITE_ACP,
	"GetBucketAcl":        s3_constants.ACTION_READ_ACP,
	// Bucket operations
	"DeleteBucket":        s3_constants.ACTION_DELETE_BUCKET,
	"DeleteBucketPolicy":  s3_constants.ACTION_ADMIN,
	"ListBucket":          s3_constants.ACTION_LIST,
	"ListBucketVersions":  s3_constants.ACTION_LIST,
	"ListAllMyBuckets":    s3_constants.ACTION_LIST,
	"GetBucketLocation":   s3_constants.ACTION_READ,
	"GetBucketVersioning": s3_constants.ACTION_READ,
	"PutBucketVersioning": s3_constants.ACTION_WRITE,
	"GetBucketCors":       s3_constants.ACTION_READ,
	"PutBucketCors":       s3_constants.ACTION_WRITE,
	"DeleteBucketCors":    s3_constants.ACTION_WRITE,
	"GetBucketNotification": s3_constants.ACTION_READ,
	"PutBucketNotification": s3_constants.ACTION_WRITE,
	"GetBucketObjectLockConfiguration": s3_constants.ACTION_READ,
	"PutBucketObjectLockConfiguration": s3_constants.ACTION_WRITE,
	// Multipart upload operations
	"CreateMultipartUpload":   s3_constants.ACTION_WRITE,
	"UploadPart":              s3_constants.ACTION_WRITE,
	"CompleteMultipartUpload": s3_constants.ACTION_WRITE,
	"AbortMultipartUpload":    s3_constants.ACTION_WRITE,
	"ListMultipartUploads":    s3_constants.ACTION_LIST,
	"ListParts":               s3_constants.ACTION_LIST,
	// Retention and legal hold operations
	"GetObjectRetention":        s3_constants.ACTION_READ,
	"PutObjectRetention":        s3_constants.ACTION_WRITE,
	"GetObjectLegalHold":        s3_constants.ACTION_READ,
	"PutObjectLegalHold":        s3_constants.ACTION_WRITE,
	"BypassGovernanceRetention": s3_constants.ACTION_WRITE,
}

func init() {
	// Populate both prefixed and unprefixed variants for all base S3 actions.
	// This avoids duplication and makes it easy to add new actions in one place.
	for action, constant := range baseS3ActionMap {
		fineGrainedActionMap[action] = constant       // unprefixed: "DeleteObject"
		fineGrainedActionMap["s3:"+action] = constant // prefixed: "s3:DeleteObject"
	}
}

// MapToStatementAction converts a policy statement action to an S3 action constant.
// It handles both coarse-grained statement actions (e.g., "Put*", "Get*") and
// fine-grained S3 actions (e.g., "s3:DeleteObject", "s3:PutObject") via exact lookup.
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

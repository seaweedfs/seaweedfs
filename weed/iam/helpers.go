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
func MapToStatementAction(action string) string {
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


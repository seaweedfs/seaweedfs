package iam

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

// ValidateCallerSuppliedAccessKeyId checks that a caller-supplied AccessKeyId
// is 4 to 128 ASCII alphanumeric characters. Returns nil if valid.
//
// The alphanumeric restriction avoids characters that would break SigV4
// canonicalization (e.g. '/' and '=' appear as delimiters in Credential
// headers), so this is a stricter superset of the rule AWS enforces.
func ValidateCallerSuppliedAccessKeyId(accessKeyId string) error {
	if len(accessKeyId) < 4 || len(accessKeyId) > 128 {
		return fmt.Errorf("AccessKeyId must be 4 to 128 alphanumeric characters")
	}
	for _, r := range accessKeyId {
		if !((r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return fmt.Errorf("AccessKeyId must be 4 to 128 alphanumeric characters")
		}
	}
	return nil
}

// ValidateCallerSuppliedSecretAccessKey checks that a caller-supplied
// SecretAccessKey is 8 to 128 characters. Returns nil if valid.
func ValidateCallerSuppliedSecretAccessKey(secretAccessKey string) error {
	if len(secretAccessKey) < 8 || len(secretAccessKey) > 128 {
		return fmt.Errorf("SecretAccessKey must be between 8 and 128 characters")
	}
	return nil
}

// AccessKeyOwner identifies which entity in an S3ApiConfiguration already owns
// a given AccessKeyId. Returned by FindAccessKeyOwner for collision checks on
// caller-supplied credentials.
type AccessKeyOwner struct {
	// Type is "user" or "service account".
	Type string
	// Name is the identity's Name (for users) or the service account's Id.
	Name string
}

// FindAccessKeyOwner scans s3cfg for an identity or service account whose
// credentials already contain accessKeyId. Returns nil if the key is free.
//
// Callers should log Name only at debug level — error responses returned to
// the caller should not include owner identity to avoid information leaks.
func FindAccessKeyOwner(s3cfg *iam_pb.S3ApiConfiguration, accessKeyId string) *AccessKeyOwner {
	if s3cfg == nil || accessKeyId == "" {
		return nil
	}
	for _, ident := range s3cfg.Identities {
		for _, cred := range ident.Credentials {
			if cred.AccessKey == accessKeyId {
				return &AccessKeyOwner{Type: "user", Name: ident.Name}
			}
		}
	}
	for _, sa := range s3cfg.ServiceAccounts {
		if sa.Credential != nil && sa.Credential.AccessKey == accessKeyId {
			return &AccessKeyOwner{Type: "service account", Name: sa.Id}
		}
	}
	return nil
}

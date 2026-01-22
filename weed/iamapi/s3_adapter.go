package iamapi

import (
)

// S3IdentityManager defines the minimal interface iamapi needs from S3
// In PR2, this is implemented by a stub
// In PR4 (S3 Integration), this is implemented by s3api.IdentityAccessManagement
type S3IdentityManager interface {
	LoadS3ApiConfigurationFromCredentialManager() error
}

// Stub implementation for PR2
type StubS3IdentityManager struct{}

func (s *StubS3IdentityManager) LoadS3ApiConfigurationFromCredentialManager() error {
	return nil
}

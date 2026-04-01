package s3api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadIAMManagerWithNoConfig(t *testing.T) {
	// Verify that IAM can be initialized without any config
	option := &S3ApiServerOption{
		Config: "",
	}
	iamManager := NewIdentityAccessManagementWithStore(option, nil, "memory")
	assert.NotNil(t, iamManager)
	// Internal state might be hard to access directly, but successful init implies defaults worked.
}

func TestLoadIAMManagerFromConfig_EmptyConfigWithFallbackKey(t *testing.T) {
	// Initialize IAM with empty config — no anonymous identity is configured,
	// so LookupAnonymous should return not-found.
	option := &S3ApiServerOption{
		Config:    "",
		IamConfig: "",
		EnableIam: true,
	}
	iamManager := NewIdentityAccessManagementWithStore(option, nil, "memory")

	_, found := iamManager.LookupAnonymous()
	assert.False(t, found, "Anonymous identity should not be found when not explicitly configured")
}

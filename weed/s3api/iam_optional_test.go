package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/stretchr/testify/assert"
)

// resetMemoryStore resets the shared in-memory credential store so that tests
// that rely on an empty store are not polluted by earlier tests.
func resetMemoryStore() {
	for _, store := range credential.Stores {
		if store.GetName() == credential.StoreTypeMemory {
			if resettable, ok := store.(interface{ Reset() }); ok {
				resettable.Reset()
			}
		}
	}
}

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
	// Reset the shared memory store to avoid state leaking from other tests.
	resetMemoryStore()

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

// TestSetIAMIntegrationKeepsAuthDisabledWithoutConfig is a regression test for
// issue #9557. The `weed mini` defaults (and the bare `docker run seaweedfs`
// image) start with EnableIam=true but no IAM config file and no identities.
// The advanced-IAM init path used to also flip isAuthEnabled to true via
// SetIAMIntegration, which then rejected every anonymous request as
// AccessDenied — breaking out-of-the-box S3 access. Setting an integration
// must not, on its own, enable auth enforcement; explicit configs use
// EnableAuthEnforcement to opt in.
func TestSetIAMIntegrationKeepsAuthDisabledWithoutConfig(t *testing.T) {
	resetMemoryStore()

	option := &S3ApiServerOption{
		EnableIam: true,
	}
	iam := NewIdentityAccessManagementWithStore(option, nil, "memory")

	// Simulate an integration object being plugged in (the constructor in
	// s3api_server.go does this when EnableIam=true, even with no config file).
	// We only care that auth stays off — the integration value itself is opaque.
	iam.SetIAMIntegration(&S3IAMIntegration{})

	assert.False(t, iam.isEnabled(), "Auth must stay disabled when no identities and no IamConfig are configured")

	// And EnableAuthEnforcement does flip it on — this is what the startup
	// path runs when the operator explicitly passes -s3.iam.config.
	iam.EnableAuthEnforcement()
	assert.True(t, iam.isEnabled(), "EnableAuthEnforcement must turn auth on")
}

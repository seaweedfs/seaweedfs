package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

// A config file the proto parser reads as empty - a singular "identity", an
// unpopulated secret mount - used to leave the gateway serving every
// anonymous request, including ListBuckets and bucket creation.
func TestConfigWithoutIdentitiesDeniesAnonymous(t *testing.T) {
	resetMemoryStore()
	clearEnvironmentVariableCredentials(t)

	path := writeTempIamConfig(t, `{"identity":[{"name":"admin","credentials":[{"accessKey":"adminkey","secretKey":"adminsecret"}],"actions":["Admin"]}]}`)
	iam := NewIdentityAccessManagementWithStore(&S3ApiServerOption{Config: path}, nil, "memory")

	assert.True(t, iam.isEnabled(), "naming a config file asks for authentication, even if it yields no identity")

	handlerCalled := false
	handler := iam.Auth(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
	}, s3_constants.ACTION_LIST)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	assert.False(t, handlerCalled, "ListBuckets must not run for an anonymous caller")
	assert.Equal(t, http.StatusForbidden, recorder.Code)
}

// `weed mini` and `docker run seaweedfs` name no config file and stay open.
func TestNoConfigKeepsAnonymousAllowed(t *testing.T) {
	resetMemoryStore()
	clearEnvironmentVariableCredentials(t)

	iam := NewIdentityAccessManagementWithStore(&S3ApiServerOption{}, nil, "memory")

	assert.False(t, iam.isEnabled(), "auth must stay off when no config file and no identities are configured")
}

// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY register an admin identity of
// their own, which would decide isAuthEnabled before the config file does.
func clearEnvironmentVariableCredentials(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
}

// The proto parser drops what it does not recognise, so a typo has to be named
// at startup or the resulting lockout has no visible cause.
func TestUnknownS3ConfigKeys(t *testing.T) {
	assert.Equal(t, []string{"identity"}, unknownS3ConfigKeys([]byte(`{"identity":[],"accounts":[]}`)))
	assert.Empty(t, unknownS3ConfigKeys([]byte(`{"identities":[],"service_accounts":[],"serviceAccounts":[],"policies":[],"groups":[]}`)))
	assert.Empty(t, unknownS3ConfigKeys([]byte(`{"kms":{},"sts":{},"policy":{},"providers":[],"roles":[]}`)), "sections owned by other subsystems are not typos")
	assert.Empty(t, unknownS3ConfigKeys([]byte(`not json`)))
}

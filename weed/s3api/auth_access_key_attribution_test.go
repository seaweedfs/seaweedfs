package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

const (
	sharedKeyA    = "AKIASHAREDKEYAAAAAAA"
	sharedSecretA = "secret-a-wJalrXUtnFEMI/K7MDENG/bPxRfiCY"
	sharedKeyB    = "AKIASHAREDKEYBBBBBBB"
	sharedSecretB = "secret-b-wJalrXUtnFEMI/K7MDENG/bPxRfiCY"
)

// newSharedIdentityIAM returns one identity holding two key pairs, as when
// several people or services are given their own key on one identity.
func newSharedIdentityIAM() *IdentityAccessManagement {
	iam := &IdentityAccessManagement{}
	shared := &Identity{
		Name: "shared-user",
		Credentials: []*Credential{
			{AccessKey: sharedKeyA, SecretKey: sharedSecretA},
			{AccessKey: sharedKeyB, SecretKey: sharedSecretB},
		},
		Actions: []Action{s3_constants.ACTION_READ},
	}
	iam.identities = []*Identity{shared}
	iam.accessKeyIdent = map[string]*Identity{sharedKeyA: shared, sharedKeyB: shared}
	iam.nameToIdentity = map[string]*Identity{shared.Name: shared}
	return iam
}

// Two keys of one identity: the audit entry names the same requester for both
// and tells them apart by the key each request was signed with.
func TestAuditLogCarriesTheSigningAccessKey(t *testing.T) {
	iam := newSharedIdentityIAM()
	for _, key := range []struct{ id, secret string }{{sharedKeyA, sharedSecretA}, {sharedKeyB, sharedSecretB}} {
		req, err := newTestRequest(http.MethodGet, "https://example.com/bucket/ns/t/data/f.parquet", 0, nil)
		require.NoError(t, err)
		require.NoError(t, signRequestV4(req, key.id, key.secret))
		// track() installs the holder before authentication runs.
		outer := s3_constants.EnsureIdentityHolder(req)

		identity, errCode := iam.reqSignatureV4Verify(outer)
		require.Equal(t, s3err.ErrNone, errCode)
		iam.handleAuthResult(httptest.NewRecorder(), outer, identity, s3err.ErrNone, func(http.ResponseWriter, *http.Request) {})

		log := s3err.GetAccessLog(outer, http.StatusOK, s3err.ErrNone)
		assert.Equal(t, "shared-user", log.Requester)
		assert.Equal(t, key.id, log.AccessKeyId, "audit entry must name the key the request was signed with")
	}
}

// A request refused after authentication still names its key, like its requester.
func TestAuditLogCarriesTheAccessKeyOnTheDenyPath(t *testing.T) {
	iam := newSharedIdentityIAM()
	req, err := newTestRequest(http.MethodGet, "https://example.com/bucket/f.parquet", 0, nil)
	require.NoError(t, err)
	require.NoError(t, signRequestV4(req, sharedKeyB, sharedSecretB))
	outer := s3_constants.EnsureIdentityHolder(req)
	identity, errCode := iam.reqSignatureV4Verify(outer)
	require.Equal(t, s3err.ErrNone, errCode)

	iam.handleAuthResult(httptest.NewRecorder(), outer, identity, s3err.ErrAccessDenied, func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not run on the deny path")
	})
	assert.Equal(t, sharedKeyB, s3err.GetAccessLog(outer, http.StatusForbidden, s3err.ErrAccessDenied).AccessKeyId)
}

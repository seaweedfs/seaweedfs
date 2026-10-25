package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestIcebergSessionNameWithoutKeyIsUnchanged(t *testing.T) {
	assert.Equal(t, "iceberg-analytics-ns-t", icebergSessionName("analytics", "b", "ns/t", ""))
	assert.Equal(t, "iceberg-anonymous-ns-t", icebergSessionName("anonymous", "b", "ns/t", ""))
}

func TestIcebergSessionNameEndsWithTheKeyTag(t *testing.T) {
	a := icebergSessionName("analytics", "b", "ns/t", sharedKeyA)
	b := icebergSessionName("analytics", "b", "ns/t", sharedKeyB)
	assert.NotEqual(t, a, b, "two keys of one identity must give distinguishable session names")
	for _, name := range []string{a, b} {
		assert.True(t, strings.HasPrefix(name, "iceberg-analytics-ns-t-k"), name)
		assert.Len(t, name, len("iceberg-analytics-ns-t-k")+icebergSessionKeyTagLength)
	}
	assert.Equal(t, a, icebergSessionName("analytics", "b", "ns/t", sharedKeyA), "the tag is stable")
	assert.NotContains(t, a, sharedKeyA, "the name carries a hash, not the key id")
}

// A long identity and table give way to the tag; the tag is never cut.
func TestIcebergSessionNameKeepsTheWholeTagWithinTheLimit(t *testing.T) {
	principal := "svc-0b6f6d1c-6f1e-4c1a-9f57-3a8f4c0f1d2e"
	name := icebergSessionName(principal, "b", "a_long_namespace/a_long_table_name", sharedKeyA)
	tag := icebergSessionKeyTag(sharedKeyA)
	assert.LessOrEqual(t, len(name), 64)
	assert.True(t, strings.HasSuffix(name, tag), name)
	assert.True(t, strings.HasPrefix(name, "iceberg-"+principal), name)
	for _, r := range name {
		ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_'
		assert.True(t, ok, "session name %q contains %q", name, r)
	}
	long := icebergSessionName(strings.Repeat("principal", 10), "b", "ns/t", sharedKeyB)
	assert.Len(t, long, 64)
	assert.True(t, strings.HasSuffix(long, icebergSessionKeyTag(sharedKeyB)))
}

// The vendor reads the caller's key from the request context.
func TestVendTableCredentialsReadsTheKeyFromTheContext(t *testing.T) {
	ctx := s3_constants.SetAccessKeyIdInContext(context.Background(), sharedKeyA)
	assert.Equal(t, sharedKeyA, s3_constants.AccessKeyIdFromContext(ctx))
	assert.Equal(t, "", s3_constants.AccessKeyIdFromContext(context.Background()))
}

// VendTableCredentials mints a real session from the caller's key: the session
// name ends with the key's tag and the token carries the key id.
func TestVendTableCredentialsRecordsTheCallersKey(t *testing.T) {
	mgr := newTestIAMManager(t)
	require.NoError(t, mgr.CreateRole(context.Background(), "", "IcebergTableAccess", &integration.RoleDefinition{
		RoleName: "IcebergTableAccess",
		TrustPolicy: &policy.PolicyDocument{
			Version: "2012-10-17",
			Statement: []policy.Statement{{
				Effect:    "Allow",
				Principal: map[string]interface{}{"Federated": icebergVendedCredentialsProvider},
				Action:    []string{"sts:AssumeRoleWithCredentials"},
			}},
		},
	}))
	iam := newSharedIdentityIAM()
	iam.SetIAMIntegration(NewS3IAMIntegration(mgr, ""))
	s3a := &S3ApiServer{iam: iam}
	s3a.SetIcebergCredentialRole("arn:aws:iam::role/IcebergTableAccess", 3600)

	for _, k := range []struct{ id, secret string }{{sharedKeyA, sharedSecretA}, {sharedKeyB, sharedSecretB}, {"", ""}} {
		key := k.id
		ctx := context.Background()
		if key != "" {
			// As the catalog's auth does: the holder is installed, the verifier records into it.
			r := s3_constants.EnsureIdentityHolder(httptest.NewRequest(http.MethodGet, "/", nil))
			s3_constants.RecordAccessKeyId(r.Context(), key)
			s3_constants.RecordStoredAccessKeyId(r.Context(), k.id, k.secret)
			ctx = r.Context()
		}
		creds, err := s3a.VendTableCredentials(ctx, "shared-user", "warehouse", "ns/t", false)
		require.NoError(t, err)
		require.NotNil(t, creds)
		info, err := s3a.stsService().ValidateSessionToken(context.Background(), creds.SessionToken)
		require.NoError(t, err)
		assert.Equal(t, key, info.SourceAccessKeyId, "key %q", key)
		assert.Equal(t, icebergSessionName("shared-user", "warehouse", "ns/t", key), info.SessionName, "key %q", key)
		if key != "" {
			assert.Contains(t, info.SessionName, icebergSessionKeyTag(key))
		}
	}
}

package s3api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// vendingServer is an S3ApiServer whose catalog role trusts the vendor, with
// the identity of newSharedIdentityIAM (one identity, two keys).
func vendingServer(t *testing.T) *S3ApiServer {
	t.Helper()
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
	return s3a
}

// vend mints table credentials for principal as if its request had been
// signed with accessKey ("" for a request that recorded none); a non-empty
// secret marks the credential as a stored identity's, as a verifier would.
func vend(t *testing.T, s3a *S3ApiServer, principal, accessKey, secret string) *VendedCredentials {
	t.Helper()
	ctx := context.Background()
	if accessKey != "" {
		ctx = s3_constants.SetAccessKeyIdInContext(ctx, accessKey)
	}
	if secret != "" {
		ctx = s3_constants.SetStoredAccessKeyIdInContext(ctx, accessKey, secret)
	}
	creds, err := s3a.VendTableCredentials(ctx, principal, "warehouse", "ns/t", false)
	require.NoError(t, err)
	require.NotNil(t, creds)
	return creds
}

// use validates a vended session the way a data-path request carrying it is
// validated.
func use(t *testing.T, s3a *S3ApiServer, creds *VendedCredentials) s3err.ErrorCode {
	t.Helper()
	r, err := newTestRequest("GET", "https://example.com/warehouse/ns/t/data/f.parquet", 0, nil)
	require.NoError(t, err)
	_, _, errCode := s3a.iam.validateSTSSessionToken(r, creds.SessionToken, creds.AccessKeyID)
	return errCode
}

func sessionInfo(t *testing.T, s3a *S3ApiServer, creds *VendedCredentials) *sts.SessionInfo {
	t.Helper()
	info, err := s3a.stsService().ValidateSessionToken(context.Background(), creds.SessionToken)
	require.NoError(t, err)
	return info
}

func TestVendedSessionEndsWithItsAccessKey(t *testing.T) {
	s3a := vendingServer(t)
	sessionA := vend(t, s3a, "shared-user", sharedKeyA, sharedSecretA)
	sessionB := vend(t, s3a, "shared-user", sharedKeyB, sharedSecretB)
	require.Equal(t, s3err.ErrNone, use(t, s3a, sessionA), "control: the session works while its key does")
	require.Equal(t, s3err.ErrNone, use(t, s3a, sessionB))

	identity := s3a.iam.nameToIdentity["shared-user"]
	identity.Credentials[0].Status = iamAccessKeyStatusInactive
	assert.Equal(t, s3err.ErrInvalidAccessKeyID, use(t, s3a, sessionA), "key A deactivated")
	assert.Equal(t, s3err.ErrNone, use(t, s3a, sessionB), "the session vended through key B keeps working")

	identity.Credentials[0].Status = iamAccessKeyStatusActive
	assert.Equal(t, s3err.ErrNone, use(t, s3a, sessionA), "key A reactivated")

	identity.Credentials[0].Expiration = time.Now().Add(-time.Minute).Unix()
	assert.Equal(t, s3err.ErrInvalidAccessKeyID, use(t, s3a, sessionA), "key A expired")
	identity.Credentials[0].Expiration = 0

	identity.Credentials = identity.Credentials[1:]
	assert.Equal(t, s3err.ErrInvalidAccessKeyID, use(t, s3a, sessionA), "key A deleted")
	assert.Equal(t, s3err.ErrNone, use(t, s3a, sessionB), "the session vended through key B keeps working")
}

func TestVendedSessionEndsWithItsIdentity(t *testing.T) {
	s3a := vendingServer(t)
	session := vend(t, s3a, "shared-user", sharedKeyA, sharedSecretA)
	require.Equal(t, s3err.ErrNone, use(t, s3a, session))

	s3a.iam.nameToIdentity["shared-user"].Disabled = true
	assert.Equal(t, s3err.ErrInvalidAccessKeyID, use(t, s3a, session), "identity disabled")
	s3a.iam.nameToIdentity["shared-user"].Disabled = false
	assert.Equal(t, s3err.ErrNone, use(t, s3a, session), "identity enabled again")

	delete(s3a.iam.nameToIdentity, "shared-user")
	assert.Equal(t, s3err.ErrInvalidAccessKeyID, use(t, s3a, session), "identity deleted")
}

// A caller that is not a stored identity (a caller signing with another STS
// session's key, an OIDC user, a request that recorded no key) still gets
// credentials, unbound as before: binding them would refuse every request
// made with them.
func TestVendedSessionIsBoundOnlyForAStoredIdentitysKey(t *testing.T) {
	s3a := vendingServer(t)
	control := sessionInfo(t, s3a, vend(t, s3a, "shared-user", sharedKeyA, sharedSecretA))
	assert.Equal(t, sharedKeyA, control.SourceAccessKeyId, "control")
	assert.Equal(t, sharedKeyA, control.BoundAccessKeyId, "control")

	for _, tc := range []struct{ principal, key, why string }{
		{"47ad4828c45b3f337bc3146081ba8f0f", "ASIATEMPORARYKEY0001", "an STS session's subject and temporary key"},
		{"shared-user", "ASIATEMPORARYKEY0001", "an STS session whose subject names a stored identity"},
		{"alice@example.com", "", "an OIDC user"},
		{"shared-user", "", "no recorded key"},
	} {
		session := vend(t, s3a, tc.principal, tc.key, "")
		info := sessionInfo(t, s3a, session)
		assert.Equal(t, tc.key, info.SourceAccessKeyId, tc.why)
		assert.Empty(t, info.BoundAccessKeyId, tc.why)
		assert.Equal(t, s3err.ErrNone, use(t, s3a, session), tc.why)
	}
}

// Tokens minted before the binding existed record the caller's key under the
// audit claim only. Validating them as bound would end live table credentials
// at upgrade — a chained STS caller's key can never resolve to a stored
// credential. They stay valid until expiry, as they always did.
func TestLegacyVendedSessionStaysUnbound(t *testing.T) {
	iam := newSharedIdentityIAM()
	iam.iamIntegration = &MockIAMIntegration{
		validateSessionFunc: func(ctx context.Context, token string) (*sts.SessionInfo, error) {
			return &sts.SessionInfo{
				SessionId:         "sid",
				AssumedRoleUser:   "arn:aws:sts::assumed-role/Reader/session",
				Principal:         "arn:aws:sts::assumed-role/Reader/session",
				IdentityProvider:  icebergVendedCredentialsProvider,
				ExternalUserId:    "someone-not-in-the-identity-table",
				SourceAccessKeyId: "ASIATEMPORARYKEY0001",
				Credentials:       &sts.Credentials{AccessKeyId: "ASIALEGACY", SecretAccessKey: "secret"},
				ExpiresAt:         time.Now().Add(time.Hour),
			}, nil
		},
	}
	r, err := newTestRequest("GET", "https://example.com/bucket/f.parquet", 0, nil)
	require.NoError(t, err)
	_, _, errCode := iam.validateSTSSessionToken(r, "token", "ASIALEGACY")
	assert.Equal(t, s3err.ErrNone, errCode)
}

// A key re-created under the same id with a different secret does not revive
// sessions bound to the deleted original: the fingerprint recorded at mint
// time no longer matches.
func TestRecreatedKeyDoesNotReviveVendedSession(t *testing.T) {
	s3a := vendingServer(t)
	session := vend(t, s3a, "shared-user", sharedKeyA, sharedSecretA)
	require.Equal(t, s3err.ErrNone, use(t, s3a, session))

	identity := s3a.iam.nameToIdentity["shared-user"]
	identity.Credentials[0] = &Credential{AccessKey: sharedKeyA, SecretKey: "recreated-secret"}
	assert.Equal(t, s3err.ErrInvalidAccessKeyID, use(t, s3a, session), "same id, different secret")

	identity.Credentials[0] = &Credential{AccessKey: sharedKeyA, SecretKey: sharedSecretA}
	assert.Equal(t, s3err.ErrNone, use(t, s3a, session), "the identical credential is indistinguishable")
}

// A session for a stored identity stays bound even when the key no longer
// resolves at vend time — revoked between the request's authentication and
// the vend, or held by a different identity now. The session dies on its
// first request instead of living unbound until it expires.
func TestVendedSessionStaysBoundWhenTheKeyIsGoneAtVend(t *testing.T) {
	s3a := vendingServer(t)

	identity := s3a.iam.nameToIdentity["shared-user"]
	identity.Credentials = identity.Credentials[1:] // sharedKeyA revoked mid-request
	session := vend(t, s3a, "shared-user", sharedKeyA, sharedSecretA)
	assert.Equal(t, sharedKeyA, sessionInfo(t, s3a, session).SourceAccessKeyId, "still bound")
	assert.Equal(t, s3err.ErrInvalidAccessKeyID, use(t, s3a, session), "the dead key's session is refused")
}

// Only sessions the catalog vended are bound; any other STS session (web
// identity, AssumeRole) is validated as before.
func TestOtherSessionsAreNotBoundToAKey(t *testing.T) {
	iam := newSharedIdentityIAM()
	iam.iamIntegration = &MockIAMIntegration{
		validateSessionFunc: func(ctx context.Context, token string) (*sts.SessionInfo, error) {
			return &sts.SessionInfo{
				SessionId:         "sid",
				AssumedRoleUser:   "arn:aws:sts::assumed-role/Reader/session",
				Principal:         "arn:aws:sts::assumed-role/Reader/session",
				IdentityProvider:  "oidc-provider",
				ExternalUserId:    "someone-not-in-the-identity-table",
				SourceAccessKeyId: "AKIAGONE",
				Credentials:       &sts.Credentials{AccessKeyId: "ASIAWEBID", SecretAccessKey: "secret"},
				ExpiresAt:         time.Now().Add(time.Hour),
			}, nil
		},
	}
	r, err := newTestRequest("GET", "https://example.com/bucket/f.parquet", 0, nil)
	require.NoError(t, err)
	_, _, errCode := iam.validateSTSSessionToken(r, "token", "ASIAWEBID")
	assert.Equal(t, s3err.ErrNone, errCode)
}

// The check on the request path is one read-locked map lookup and a scan of
// one identity's keys.
func BenchmarkVendedSessionKeyLive(b *testing.B) {
	iam := newSharedIdentityIAM()
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("user-%04d", i)
		iam.nameToIdentity[name] = &Identity{Name: name, Credentials: []*Credential{{AccessKey: name}}}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if iam.vendedSessionKeyLive("shared-user", sharedKeyB, "") != s3err.ErrNone {
			b.Fatal("refused")
		}
	}
}

package s3tables

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A table bucket a web-identity role session created before owners were
// issuer-scoped is owned by the bare OIDC subject. The same subject's later
// sessions, signed with SigV4 session credentials, carry the scoped aws:userid
// and no asserted issuers, and must still own the bucket.
func TestSTSSessionKeepsLegacySubjectOwnedBucket(t *testing.T) {
	const (
		bucket = "lake-norm"
		sub    = "project_path:dev/infra/datalake/normalisation:ref_type:branch:ref:main"
		issuer = "https://gitlab.example.com"
	)
	arn, err := BuildBucketARN(DefaultRegion, DefaultAccountID, bucket)
	require.NoError(t, err)
	body, err := json.Marshal(&CreateNamespaceRequest{TableBucketARN: arn, Namespace: []string{"some_ns"}})
	require.NoError(t, err)

	createNamespace := func(t *testing.T, sessionIssuer string, configured ...string) int {
		fs := s3tablestest.Start(t)
		metadata, err := json.Marshal(&tableBucketMetadata{Name: bucket, CreatedAt: time.Now(), OwnerAccountID: sub})
		require.NoError(t, err)
		fs.Put(TablesPath, bucket, map[string][]byte{ExtendedKeyTableBucket: []byte("true"), ExtendedKeyMetadata: metadata})
		m := NewManager()
		m.SetDefaultAllow(false)
		if configured != nil {
			m.SetTrustedIssuerSource(func(context.Context) []string { return configured })
		}
		identity := &testIdentity{
			Name:    sub,
			Account: &testIdentityAccount{Id: sub},
			Claims: map[string]interface{}{
				"sub":        sub,
				"aws:userid": sts.ComputeParentUser(sub, sessionIssuer),
			},
		}
		req, err := newManagerRequest(s3_constants.SetIdentityInContext(context.Background(), identity), "CreateNamespace", body, sub)
		require.NoError(t, err)
		recorder := httptest.NewRecorder()
		m.handler.HandleRequest(recorder, req, NewManagerClient(fs.Client))
		return recorder.Code
	}

	assert.Equal(t, http.StatusOK, createNamespace(t, issuer, issuer), "the subject's own session lost its bucket")
	assert.Equal(t, http.StatusForbidden, createNamespace(t, issuer), "no configured issuer set must not re-anchor")
	assert.Equal(t, http.StatusForbidden, createNamespace(t, issuer, issuer, "https://other.example.com"),
		"a legacy subject is ambiguous once two issuers are configured")
	assert.Equal(t, http.StatusForbidden, createNamespace(t, "https://other.example.com", issuer),
		"another issuer's user with the same subject must not take the bucket")
}

// Issuers asserted by the bearer path win, even when empty; an identity that
// is not a federated session never reads the configured set.
func TestTrustedIssuersSourceOnlyForFederatedSessions(t *testing.T) {
	h := NewS3TablesHandler()
	h.SetTrustedIssuerSource(func(context.Context) []string { return []string{"configured"} })
	issuersFor := func(claims map[string]interface{}) []string {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req = req.WithContext(s3_constants.SetIdentityInContext(req.Context(), &testIdentity{Claims: claims}))
		return h.trustedIssuers(req)
	}
	scoped := sts.ComputeParentUser("alice", "configured")
	assert.Equal(t, []string{"configured"}, issuersFor(map[string]interface{}{"aws:userid": scoped}))
	assert.Equal(t, []string{"asserted"}, issuersFor(map[string]interface{}{"aws:userid": scoped, "seaweed:oidc-issuers": "asserted"}))
	assert.Empty(t, issuersFor(map[string]interface{}{"aws:userid": scoped, "seaweed:oidc-issuers": ""}))
	assert.Empty(t, issuersFor(map[string]interface{}{"aws:userid": "alice"}))
	assert.Empty(t, issuersFor(map[string]interface{}{"sub": "alice"}))
}

// A table bucket policy written before issuer scoping names role sessions by
// a pattern over their bare subjects. A session whose subject matches it keeps
// the grant; a subject that does not hash to the session's aws:userid does not.
func TestSTSSessionMatchesLegacySubjectPatternPrincipal(t *testing.T) {
	const (
		bucket = "lake-norm"
		sub    = "project_path:dev/infra/datalake/normalisation:ref_type:branch:ref:main"
		issuer = "https://gitlab.example.com"
	)
	arn, err := BuildBucketARN(DefaultRegion, DefaultAccountID, bucket)
	require.NoError(t, err)
	body, err := json.Marshal(&CreateNamespaceRequest{TableBucketARN: arn, Namespace: []string{"some_ns"}})
	require.NoError(t, err)
	policy := func(principal string) []byte {
		return []byte(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":["` + principal +
			`"],"Action":["s3tables:CreateNamespace"],"Resource":"*"}]}`)
	}

	createNamespace := func(t *testing.T, principalSpec, claimedSub, scopedSub, sessionIssuer string, configured ...string) int {
		fs := s3tablestest.Start(t)
		metadata, err := json.Marshal(&tableBucketMetadata{Name: bucket, CreatedAt: time.Now(), OwnerAccountID: s3_constants.AccountAdminId})
		require.NoError(t, err)
		fs.Put(TablesPath, bucket, map[string][]byte{
			ExtendedKeyTableBucket: []byte("true"),
			ExtendedKeyMetadata:    metadata,
			ExtendedKeyPolicy:      policy(principalSpec),
		})
		m := NewManager()
		m.SetDefaultAllow(false)
		if configured != nil {
			m.SetTrustedIssuerSource(func(context.Context) []string { return configured })
		}
		scoped := sts.ComputeParentUser(scopedSub, sessionIssuer)
		identity := &testIdentity{
			Name:    scoped,
			Account: &testIdentityAccount{Id: scoped},
			Claims:  map[string]interface{}{"sub": claimedSub, "aws:userid": scoped},
		}
		req, err := newManagerRequest(s3_constants.SetIdentityInContext(context.Background(), identity), "CreateNamespace", body, scoped)
		require.NoError(t, err)
		recorder := httptest.NewRecorder()
		m.handler.HandleRequest(recorder, req, NewManagerClient(fs.Client))
		return recorder.Code
	}

	const pattern = "project_path:dev/infra/datalake/normalisation:*"
	assert.Equal(t, http.StatusOK, createNamespace(t, pattern, sub, sub, issuer, issuer), "the subject pattern no longer grants")
	assert.Equal(t, http.StatusOK, createNamespace(t, sub, sub, sub, issuer, issuer), "the exact subject no longer grants")
	assert.Equal(t, http.StatusForbidden, createNamespace(t, pattern, sub, sub, issuer), "no configured issuer set must not re-anchor")
	assert.Equal(t, http.StatusForbidden, createNamespace(t, pattern, sub, sub, issuer, issuer, "https://other.example.com"),
		"a subject pattern is ambiguous once two issuers are configured")
	assert.Equal(t, http.StatusForbidden, createNamespace(t, pattern, sub, sub, "https://other.example.com", issuer),
		"another issuer's user with the same subject must not match")
	assert.Equal(t, http.StatusForbidden, createNamespace(t, pattern, sub, "project_path:other/repo:ref_type:branch:ref:main", issuer, issuer),
		"a subject claim the session's aws:userid was not derived from must not match")
	assert.Equal(t, http.StatusForbidden, createNamespace(t, "arn:aws:iam::*", sub, sub, issuer, issuer),
		"an ARN pattern is not a subject pattern")
}

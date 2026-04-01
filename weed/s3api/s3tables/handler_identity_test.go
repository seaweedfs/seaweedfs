package s3tables

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

// testIdentity/testIdentityAccount mirror the production identity shape used via reflection.
// Keep these field names in sync with getAccountID to avoid silent breaks.
type testIdentityAccount struct {
	Id string
}

type testIdentity struct {
	Account *testIdentityAccount
	Claims  map[string]interface{}
}

func TestGetAccountIDPrefersClaimsOverAccountID(t *testing.T) {
	h := NewS3TablesHandler()
	id := &testIdentity{
		Account: &testIdentityAccount{Id: s3_constants.AccountAdminId},
		Claims: map[string]interface{}{
			"preferred_username": "alice",
			"sub":                "alice-sub",
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(s3_constants.SetIdentityInContext(req.Context(), id))

	got := h.getAccountID(req)
	assert.Equal(t, "alice-sub", got, "expected sub claim to be used before preferred_username")
	assert.NotEqual(t, DefaultAccountID, got, "claims should override default handler account")
}

func TestGetAccountIDUsesSubWhenPreferredUsernameMissing(t *testing.T) {
	h := NewS3TablesHandler()
	id := &testIdentity{
		Account: &testIdentityAccount{Id: s3_constants.AccountAdminId},
		Claims: map[string]interface{}{
			"sub": "user-123",
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(s3_constants.SetIdentityInContext(req.Context(), id))

	got := h.getAccountID(req)
	assert.Equal(t, "user-123", got, "expected sub claim to be used when preferred_username missing")
	assert.NotEqual(t, DefaultAccountID, got, "claims should override default handler account")
}

func TestGetAccountIDFallsBackToHandlerDefaultAccount(t *testing.T) {
	h := NewS3TablesHandler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	assert.Equal(t, DefaultAccountID, h.getAccountID(req), "expected handler default account to be returned when no identity is set")
}

func TestGetAccountIDIgnoresEmptyClaimValues(t *testing.T) {
	h := NewS3TablesHandler()
	id := &testIdentity{
		Account: &testIdentityAccount{Id: s3_constants.AccountAdminId},
		Claims: map[string]interface{}{
			"preferred_username": " ",
			"sub":                "user-123",
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(s3_constants.SetIdentityInContext(req.Context(), id))

	assert.Equal(t, "user-123", h.getAccountID(req), "expected whitespace preferred_username to be ignored")
}

func TestGetAccountIDFallsBackToIdentityName(t *testing.T) {
	h := NewS3TablesHandler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(s3_constants.SetIdentityNameInContext(req.Context(), "arn:aws:sts::123456789012:assumed-role/S3UserRole/alice-session"))

	assert.Equal(t, "alice-session", h.getAccountID(req), "expected ARN session suffix to be extracted")
}

func TestGetAccountIDFallsBackToARNColonSegment(t *testing.T) {
	h := NewS3TablesHandler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(s3_constants.SetIdentityNameInContext(req.Context(), "arn:aws:iam::123456789012:root"))

	assert.Equal(t, "root", h.getAccountID(req), "expected ARN colon segment to be returned as principal")
}

func TestGetAccountIDFallsBackToAmzAccountIdHeader(t *testing.T) {
	h := NewS3TablesHandler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(s3_constants.AmzAccountId, "header-account")

	assert.Equal(t, "header-account", h.getAccountID(req), "expected header value to be used when no identity is present")
}

func TestGetAccountIDFallsBackToAccountID(t *testing.T) {
	h := NewS3TablesHandler()
	id := &testIdentity{
		Account: &testIdentityAccount{Id: "my-account-id"},
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(s3_constants.SetIdentityInContext(req.Context(), id))

	assert.Equal(t, "my-account-id", h.getAccountID(req), "expected Account.Id to be returned when claims are missing")
}

func TestGetAccountIDNormalizesAccountIDARN(t *testing.T) {
	h := NewS3TablesHandler()
	id := &testIdentity{
		Account: &testIdentityAccount{Id: "arn:aws:iam::123456789012:user/bob"},
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(s3_constants.SetIdentityInContext(req.Context(), id))

	assert.Equal(t, "bob", h.getAccountID(req), "expected ARN account ID to be normalized to the suffix")
}

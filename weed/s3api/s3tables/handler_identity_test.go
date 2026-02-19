package s3tables

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
)

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

	assert.Equal(t, "alice", h.getAccountID(req), "expected preferred_username claim to be used")
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

	assert.Equal(t, "user-123", h.getAccountID(req), "expected sub claim to be used")
}

func TestGetAccountIDPrefersEmailWhenOtherClaimsMissing(t *testing.T) {
	h := NewS3TablesHandler()
	id := &testIdentity{
		Account: &testIdentityAccount{Id: s3_constants.AccountAdminId},
		Claims: map[string]interface{}{
			"email": "alice@example.com",
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(s3_constants.SetIdentityInContext(req.Context(), id))

	assert.Equal(t, "alice@example.com", h.getAccountID(req), "expected email claim to be used when preferred_username/sub missing")
}

func TestGetAccountIDFallsBackToHandlerDefaultAccount(t *testing.T) {
	h := NewS3TablesHandler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	assert.Equal(t, h.accountID, h.getAccountID(req), "expected handler default account to be returned when no identity is set")
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

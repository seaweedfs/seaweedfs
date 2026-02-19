package s3tables

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
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

	got := h.getAccountID(req)
	if got != "alice" {
		t.Fatalf("expected preferred_username claim to be used, got %q", got)
	}
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
	if got != "user-123" {
		t.Fatalf("expected sub claim to be used, got %q", got)
	}
}

func TestGetAccountIDFallsBackToAccountID(t *testing.T) {
	h := NewS3TablesHandler()
	id := &testIdentity{
		Account: &testIdentityAccount{Id: s3_constants.AccountAdminId},
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(s3_constants.SetIdentityInContext(req.Context(), id))

	got := h.getAccountID(req)
	if got != s3_constants.AccountAdminId {
		t.Fatalf("expected fallback account id %q, got %q", s3_constants.AccountAdminId, got)
	}
}

func TestGetAccountIDNormalizesArnIdentityName(t *testing.T) {
	h := NewS3TablesHandler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req = req.WithContext(s3_constants.SetIdentityNameInContext(req.Context(), "arn:aws:sts::123456789012:assumed-role/S3UserRole/alice-session"))

	got := h.getAccountID(req)
	if got != "alice-session" {
		t.Fatalf("expected ARN session suffix, got %q", got)
	}
}

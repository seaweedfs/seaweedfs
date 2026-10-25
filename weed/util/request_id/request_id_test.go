package request_id

import (
	"net/http/httptest"
	"regexp"
	"testing"
)

var requestIDPattern = regexp.MustCompile(`^[0-9A-F]+$`)

func TestNewUsesUppercaseHexFormat(t *testing.T) {
	id := New()
	if !requestIDPattern.MatchString(id) {
		t.Fatalf("expected uppercase hex request id, got %q", id)
	}
	if len(id) < 24 {
		t.Fatalf("expected request id to be at least 24 characters, got %q (len=%d)", id, len(id))
	}
}

func TestNewIsUnique(t *testing.T) {
	a := New()
	b := New()
	if a == b {
		t.Fatalf("expected unique request ids, got %q twice", a)
	}
}

func TestEnsureIgnoresClientHeader(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set(AmzRequestIDHeader, "spoofed-id")

	req, id := Ensure(req)
	if id == "spoofed-id" {
		t.Fatal("Ensure should not trust client-sent x-amz-request-id header")
	}
	if !requestIDPattern.MatchString(id) {
		t.Fatalf("expected server-generated hex id, got %q", id)
	}
}

func TestEnsureReusesContextID(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	req = req.WithContext(Set(req.Context(), "ctx-id-123"))

	req, id := Ensure(req)
	if id != "ctx-id-123" {
		t.Fatalf("expected context id ctx-id-123, got %q", id)
	}
}

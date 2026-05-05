package s3api

import (
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// TestAssumeRoleWithWebIdentity_AllowsEmptyRoleArn confirms the HTTP handler
// no longer rejects empty RoleArn before STS sees the request. Phase 3b's
// claim-based mode advertises that callers MAY omit RoleArn so the policy
// claim derives the assumed-role ARN; the handler must let that flow
// through. STS-layer failures (invalid token, claim-mode not configured) are
// surfaced separately and don't read "RoleArn is required".
func TestAssumeRoleWithWebIdentity_AllowsEmptyRoleArn(t *testing.T) {
	stsService, _ := setupTestSTSService(t)
	h := &STSHandlers{
		stsService: stsService,
		iam:        &IdentityAccessManagement{},
	}

	form := url.Values{}
	form.Set("Action", "AssumeRoleWithWebIdentity")
	form.Set("WebIdentityToken", "not-a-real-jwt")
	form.Set("RoleSessionName", "session-1")
	// RoleArn intentionally omitted.

	req := httptest.NewRequest("POST", "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := req.ParseForm(); err != nil {
		t.Fatalf("ParseForm: %v", err)
	}

	rr := httptest.NewRecorder()
	h.handleAssumeRoleWithWebIdentity(rr, req)

	body := rr.Body.String()
	if strings.Contains(body, "RoleArn is required") {
		t.Fatalf("HTTP handler rejected empty RoleArn pre-STS; body=%q", body)
	}
}

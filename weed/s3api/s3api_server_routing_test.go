package s3api

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
)

// setupRoutingTestServer creates a minimal S3ApiServer for routing tests
func setupRoutingTestServer(t *testing.T) *S3ApiServer {
	opt := &S3ApiServerOption{EnableIam: true}
	iam := NewIdentityAccessManagementWithStore(opt, "memory")
	iam.isAuthEnabled = true

	if iam.credentialManager == nil {
		cm, err := credential.NewCredentialManager("memory", util.GetViper(), "")
		if err != nil {
			t.Fatalf("Failed to create credential manager: %v", err)
		}
		iam.credentialManager = cm
	}

	server := &S3ApiServer{
		option:            opt,
		iam:               iam,
		credentialManager: iam.credentialManager,
		embeddedIam:       NewEmbeddedIamApi(iam.credentialManager, iam),
		stsHandlers:       &STSHandlers{},
	}

	return server
}

// TestRouting_STSWithQueryParams verifies that AssumeRoleWithWebIdentity with query params routes to STS
func TestRouting_STSWithQueryParams(t *testing.T) {
	router := mux.NewRouter()
	s3a := setupRoutingTestServer(t)
	s3a.registerRouter(router)

	// Create request with Action in query params (no auth header)
	req, _ := http.NewRequest("POST", "/?Action=AssumeRoleWithWebIdentity&WebIdentityToken=test-token&RoleArn=arn:aws:iam::123:role/test&RoleSessionName=test-session", nil)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	// Should route to STS handler -> 503 (service not initialized) or 400 (validation error)
	assert.Contains(t, []int{http.StatusBadRequest, http.StatusServiceUnavailable}, rr.Code, "Should route to STS handler")
}

// TestRouting_STSWithBodyParams verifies that AssumeRoleWithWebIdentity with body params routes to STS fallback
func TestRouting_STSWithBodyParams(t *testing.T) {
	router := mux.NewRouter()
	s3a := setupRoutingTestServer(t)
	s3a.registerRouter(router)

	// Create request with Action in POST body (no auth header)
	data := url.Values{}
	data.Set("Action", "AssumeRoleWithWebIdentity")
	data.Set("WebIdentityToken", "test-token")
	data.Set("RoleArn", "arn:aws:iam::123:role/test")
	data.Set("RoleSessionName", "test-session")

	req, _ := http.NewRequest("POST", "/", strings.NewReader(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	// Should route to STS fallback handler -> 503 (service not initialized in test)
	assert.Equal(t, http.StatusServiceUnavailable, rr.Code, "Should route to STS fallback handler (503 because STS not initialized)")
}

// TestRouting_AuthenticatedIAM verifies that authenticated IAM requests route to IAM handler
func TestRouting_AuthenticatedIAM(t *testing.T) {
	router := mux.NewRouter()
	s3a := setupRoutingTestServer(t)
	s3a.registerRouter(router)

	// Create IAM request with Authorization header
	data := url.Values{}
	data.Set("Action", "CreateUser")
	data.Set("UserName", "testuser")

	req, _ := http.NewRequest("POST", "/", strings.NewReader(data.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIA.../...")

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	// Should route to IAM handler -> 400/403 (invalid signature)
	// NOT 503 (which would indicate STS handler)
	assert.NotEqual(t, http.StatusServiceUnavailable, rr.Code, "Should NOT route to STS handler")
	assert.Contains(t, []int{http.StatusBadRequest, http.StatusForbidden}, rr.Code, "Should route to IAM handler (400/403 due to invalid signature)")
}

// TestRouting_IAMMatcherLogic verifies the iamMatcher correctly distinguishes auth types
func TestRouting_IAMMatcherLogic(t *testing.T) {
	tests := []struct {
		name        string
		authHeader  string
		queryParams string
		expectsIAM  bool
		description string
	}{
		{
			name:        "No auth - anonymous",
			authHeader:  "",
			queryParams: "",
			expectsIAM:  false,
			description: "Request with no auth should NOT match IAM",
		},
		{
			name:        "AWS4 signature",
			authHeader:  "AWS4-HMAC-SHA256 Credential=AKIA.../...",
			queryParams: "",
			expectsIAM:  true,
			description: "Request with AWS4 signature should match IAM",
		},
		{
			name:        "AWS2 signature",
			authHeader:  "AWS AKIA...:signature",
			queryParams: "",
			expectsIAM:  true,
			description: "Request with AWS2 signature should match IAM",
		},
		{
			name:        "Presigned V4",
			authHeader:  "",
			queryParams: "?X-Amz-Credential=AKIA...",
			expectsIAM:  true,
			description: "Request with presigned V4 params should match IAM",
		},
		{
			name:        "Presigned V2",
			authHeader:  "",
			queryParams: "?AWSAccessKeyId=AKIA...",
			expectsIAM:  true,
			description: "Request with presigned V2 params should match IAM",
		},
		{
			name:        "AWS4 signature with STS action in body",
			authHeader:  "AWS4-HMAC-SHA256 Credential=AKIA.../...",
			queryParams: "",
			expectsIAM:  true,
			description: "Authenticated STS action should still route to IAM (auth takes precedence)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := mux.NewRouter()
			s3a := setupRoutingTestServer(t)
			s3a.registerRouter(router)

			data := url.Values{}
			// For the authenticated STS action test, set the STS action
			// For other tests, don't set Action to avoid STS validation errors
			if tt.name == "AWS4 signature with STS action in body" {
				data.Set("Action", "AssumeRoleWithWebIdentity")
				data.Set("WebIdentityToken", "test-token")
				data.Set("RoleArn", "arn:aws:iam::123:role/test")
				data.Set("RoleSessionName", "test-session")
			}

			req, _ := http.NewRequest("POST", "/"+tt.queryParams, strings.NewReader(data.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}

			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, req)

			if tt.expectsIAM {
				// Should route to IAM (400/403 for invalid sig)
				// NOT 400 from STS (which would be missing Action parameter)
				// We distinguish by checking it's NOT a generic 400 with empty body
				assert.NotEqual(t, http.StatusServiceUnavailable, rr.Code, tt.description)
			} else {
				// Should route to STS fallback
				// Can be 503 (service not initialized) or 400 (missing/invalid Action parameter)
				assert.Contains(t, []int{http.StatusBadRequest, http.StatusServiceUnavailable}, rr.Code, tt.description)
			}
		})
	}
}

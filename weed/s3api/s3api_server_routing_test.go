package s3api

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
)

// routingTestAccessKey/routingTestSecretKey are the credentials seeded into
// the IAM for tests that need to exercise code paths behind SigV4
// verification (e.g., UnifiedPostHandler's STS dispatch).
const (
	routingTestAccessKey = "routing-test-ak"
	routingTestSecretKey = "routing-test-sk"
	routingTestUser      = "routing-test-user"
)

// setupRoutingTestServer creates a minimal S3ApiServer for routing tests
func setupRoutingTestServer(t *testing.T) *S3ApiServer {
	opt := &S3ApiServerOption{EnableIam: true}
	iam := NewIdentityAccessManagementWithStore(opt, nil, "memory")
	iam.isAuthEnabled = true

	if iam.credentialManager == nil {
		cm, err := credential.NewCredentialManager("memory", util.GetViper(), "")
		if err != nil {
			t.Fatalf("Failed to create credential manager: %v", err)
		}
		iam.credentialManager = cm
	}

	// Seed a test identity with known credentials so SigV4-signed requests
	// can pass AuthSignatureOnly and reach downstream handlers.
	testIdent := &Identity{
		Name:     routingTestUser,
		Actions:  []Action{s3_constants.ACTION_ADMIN},
		IsStatic: true,
		Credentials: []*Credential{{
			AccessKey: routingTestAccessKey,
			SecretKey: routingTestSecretKey,
		}},
	}
	iam.m.Lock()
	if iam.accessKeyIdent == nil {
		iam.accessKeyIdent = make(map[string]*Identity)
	}
	if iam.nameToIdentity == nil {
		iam.nameToIdentity = make(map[string]*Identity)
	}
	iam.identities = append(iam.identities, testIdent)
	iam.accessKeyIdent[routingTestAccessKey] = testIdent
	iam.nameToIdentity[routingTestUser] = testIdent
	iam.m.Unlock()

	server := &S3ApiServer{
		option:            opt,
		iam:               iam,
		credentialManager: iam.credentialManager,
		embeddedIam:       NewEmbeddedIamApi(iam.credentialManager, iam, false),
		stsHandlers:       &STSHandlers{},
	}

	return server
}

// signRoutingTestRequest signs req with the seeded routing-test credentials
// for the given AWS service. Fails the test on signing errors.
func signRoutingTestRequest(t *testing.T, req *http.Request, body, service string) {
	t.Helper()
	creds := credentials.NewStaticCredentials(routingTestAccessKey, routingTestSecretKey, "")
	signer := v4.NewSigner(creds)
	if _, err := signer.Sign(req, strings.NewReader(body), service, "us-east-1", time.Now()); err != nil {
		t.Fatalf("sign request: %v", err)
	}
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

// TestRouting_GetFederationTokenWithQueryParams verifies that GetFederationToken with
// Action in the query string routes to the STS handler (not IAM / not S3).
// Regression test for https://github.com/seaweedfs/seaweedfs/issues/9157
func TestRouting_GetFederationTokenWithQueryParams(t *testing.T) {
	router := mux.NewRouter()
	s3a := setupRoutingTestServer(t)
	s3a.registerRouter(router)

	req, _ := http.NewRequest("POST", "/?Action=GetFederationToken&Name=admin", nil)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	// Must not be 501 NotImplemented (previous buggy behavior).
	// Expected: routes to STS -> 503 (service not initialized in test) or 400 (validation).
	assert.NotEqual(t, http.StatusNotImplemented, rr.Code, "Should route to STS, not fall through to S3 NotImplemented")
	assert.Contains(t, []int{http.StatusBadRequest, http.StatusServiceUnavailable, http.StatusForbidden}, rr.Code, "Should route to STS handler")
}

// TestRouting_GetFederationTokenAuthenticatedBody verifies that an authenticated
// POST with Action=GetFederationToken in the form body is dispatched by
// UnifiedPostHandler to the STS handler instead of being treated as an IAM action.
// Regression test for https://github.com/seaweedfs/seaweedfs/issues/9157
//
// The request is signed with seeded test credentials so it passes
// AuthSignatureOnly in UnifiedPostHandler and actually reaches STSHandlers.
// STSHandlers is a zero value in the test server (no stsService set), so a
// correctly routed request must return 503 ServiceUnavailable from
// writeSTSErrorResponse(STSErrSTSNotReady). Any other status means we didn't
// reach STSHandlers.HandleSTSRequest.
func TestRouting_GetFederationTokenAuthenticatedBody(t *testing.T) {
	router := mux.NewRouter()
	s3a := setupRoutingTestServer(t)
	s3a.registerRouter(router)

	data := url.Values{}
	data.Set("Action", "GetFederationToken")
	data.Set("Name", "admin")
	data.Set("Version", "2011-06-15")
	body := data.Encode()

	req, _ := http.NewRequest("POST", "http://localhost/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	signRoutingTestRequest(t, req, body, "sts")

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	// Reaching STSHandlers with an uninitialized stsService yields 503.
	// 501 would mean we fell through to the S3 NotImplemented handler.
	// 403 would mean AuthSignatureOnly rejected us (test seed broken).
	assert.Equal(t, http.StatusServiceUnavailable, rr.Code,
		"should reach STS handler; got %d body=%s", rr.Code, rr.Body.String())
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
			expectsIAM:  false,
			description: "Authenticated STS action should route to STS handler (STS handlers handle their own auth)",
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

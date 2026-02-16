package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
)

// Minimal mock implementation of AuthenticateJWT needed for testing
type mockIAMIntegration struct{}

func (m *mockIAMIntegration) AuthenticateJWT(ctx context.Context, r *http.Request) (*IAMIdentity, s3err.ErrorCode) {
	return &IAMIdentity{
		Name: "test-user",
		Account: &Account{
			Id:           "test-account",
			DisplayName:  "test-account",
			EmailAddress: "test@example.com",
		},
		Principal:    "arn:aws:iam::test-account:user/test-user",
		SessionToken: "mock-session-token",
	}, s3err.ErrNone
}
func (m *mockIAMIntegration) AuthorizeAction(ctx context.Context, identity *IAMIdentity, action Action, bucket, object string, r *http.Request) s3err.ErrorCode {
	return s3err.ErrNone
}
func (m *mockIAMIntegration) ValidateTrustPolicyForPrincipal(ctx context.Context, roleArn, principalArn string) error {
	return nil
}
func (m *mockIAMIntegration) ValidateSessionToken(ctx context.Context, token string) (*sts.SessionInfo, error) {
	return nil, nil
}
func (m *mockIAMIntegration) DefaultAllow() bool {
	return true
}

func TestSTSAssumeRolePostBody(t *testing.T) {
	// Setup S3ApiServer with IAM enabled
	option := &S3ApiServerOption{
		DomainName: "localhost",
		EnableIam:  true,
		Filers:     []pb.ServerAddress{"localhost:8888"},
	}

	// Create IAM instance that we can control
	// We need to bypass the file/store loading logic in NewIdentityAccessManagement
	// So we construct it manually similarly to how it's done for tests
	iam := &IdentityAccessManagement{
		identities:     []*Identity{{Name: "test-user"}},
		isAuthEnabled:  true,
		accessKeyIdent: make(map[string]*Identity),
		nameToIdentity: make(map[string]*Identity),
		iamIntegration: &mockIAMIntegration{},
	}

	// Pre-populate an identity for testing
	ident := &Identity{
		Name: "test-user",
		Credentials: []*Credential{
			{AccessKey: "test", SecretKey: "test", Status: "Active"},
		},
		Actions:  nil, // Admin
		IsStatic: true,
	}
	iam.identities[0] = ident
	iam.accessKeyIdent["test"] = ident
	iam.nameToIdentity["test-user"] = ident

	s3a := &S3ApiServer{
		option:            option,
		iam:               iam,
		embeddedIam:       &EmbeddedIamApi{iam: iam, getS3ApiConfigurationFunc: func(cfg *iam_pb.S3ApiConfiguration) error { return nil }},
		stsHandlers:       NewSTSHandlers(nil, iam), // STS service nil -> will return STSErrSTSNotReady (503)
		credentialManager: nil,                      // Not needed for this test as we pre-populated IAM
		cb: &CircuitBreaker{
			counters:    make(map[string]*int64),
			limitations: make(map[string]int64),
		},
	}
	s3a.cb.s3a = s3a
	s3a.inFlightDataLimitCond = sync.NewCond(&sync.Mutex{})

	// Create router and register routes
	router := mux.NewRouter()
	s3a.registerRouter(router)

	// Test Case 1: STS Action in Query String (Should work - routed to STS)
	t.Run("ActionInQuery", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/?Action=AssumeRole", nil)
		// We aren't signing requests, so we expect STSErrAccessDenied (403) from STS handler
		// due to invalid signature, OR STSErrSTSNotReady (503) if it gets past auth.
		// The key is it should NOT be 501 Not Implemented (which comes from IAM handler)

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		// If routed to STS, we expect 400 (Bad Request) - MissingParameter
		// because we didn't provide RoleArn/RoleSessionName etc.
		// Or 503 if it checks STS service readiness first.

		// Let's see what we get. The STS handler checks parameters first.
		// "RoleArn is required" -> 400 Bad Request

		assert.NotEqual(t, http.StatusNotImplemented, rr.Code, "Should not return 501 (IAM handler)")
		assert.Equal(t, http.StatusBadRequest, rr.Code, "Should return 400 (STS handler) for missing params")
	})

	// Test Case 2: STS Action in Body (Should FAIL current implementation - routed to IAM)
	t.Run("ActionInBody", func(t *testing.T) {
		form := url.Values{}
		form.Add("Action", "AssumeRole")
		form.Add("RoleArn", "arn:aws:iam::123:role/test")
		form.Add("RoleSessionName", "session")

		req := httptest.NewRequest("POST", "/", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		// We need an Authorization header to trigger the IAM matcher
		// The matcher checks: getRequestAuthType(r) != authTypeAnonymous
		// So we provide a dummy auth header

		req.Header.Set("Authorization", "Bearer test-token")

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		// CURRENT BEHAVIOR:
		// The Router does not match "/" for STS because Action is not in query.
		// The Router matches "/" for IAM because it has Authorization header.
		// IAM handler (AuthIam) calls DoActions.
		// DoActions switches on "AssumeRole" -> default -> Not Implemented (501).

		// DESIRED BEHAVIOR (after fix):
		// Should be routed to UnifiedPostHandler (or similar), detected as STS action,
		// and routed to STS handler.
		// STS handler should return 403 Forbidden (Access Denied) or 400 Bad Request
		// because of signature mismatch (since we provided dummy auth).
		// It should NOT be 501.

		// For verification of fix, we assert it IS 503 (STS Service Not Initialized).
		// This confirms it was routed to STS handler.
		if rr.Code != http.StatusServiceUnavailable {
			t.Logf("Unexpected status code: %d", rr.Code)
			t.Logf("Response body: %s", rr.Body.String())
		}
		// Confirm it routed to STS
		assert.Equal(t, http.StatusServiceUnavailable, rr.Code, "Fixed behavior: Should return 503 from STS handler (service not ready)")
	})

	// Test Case 3: STS Action in Body with SigV4-style Authorization (Real-world scenario)
	// This test validates that requests with AWS SigV4 Authorization headers and POST body
	// parameters are correctly routed to the STS handler.
	t.Run("ActionInBodyWithSigV4Style", func(t *testing.T) {
		form := url.Values{}
		form.Add("Action", "AssumeRole")
		form.Add("RoleArn", "arn:aws:iam::123:role/test")
		form.Add("RoleSessionName", "session")

		bodyContent := form.Encode()
		req := httptest.NewRequest("POST", "/", strings.NewReader(bodyContent))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		// Set AWS SigV4-style Authorization header
		// This simulates a real SigV4-signed request without needing perfect signature
		// The key is to validate that UnifiedPostHandler correctly routes based on Action
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260212/us-east-1/sts/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=dummy")
		req.Header.Set("x-amz-date", "20260212T000000Z")

		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)

		// With SigV4-style Authorization header, the request should:
		// 1. Be recognized as authenticated (not anonymous)
		// 2. Be routed to UnifiedPostHandler
		// 3. UnifiedPostHandler should parse Action=AssumeRole from body
		// 4. Route to STS handler (which returns 503 because stsService is nil)
		//    OR return 403 if signature validation fails (which is acceptable)

		// The key validation is that it should NOT return 501 (IAM handler's "Not Implemented")
		// This confirms the routing fix works for SigV4-signed requests with POST body params

		if rr.Code != http.StatusServiceUnavailable && rr.Code != http.StatusForbidden {
			t.Logf("Unexpected status code: %d", rr.Code)
			t.Logf("Response body: %s", rr.Body.String())
		}

		// Accept either 503 (routed to STS, service unavailable) or 403 (signature failed)
		// Both indicate correct routing to STS handler, not IAM handler
		assert.NotEqual(t, http.StatusNotImplemented, rr.Code, "Should not return 501 (IAM handler)")
		assert.Contains(t, []int{http.StatusServiceUnavailable, http.StatusForbidden}, rr.Code,
			"Should return 503 (STS unavailable) or 403 (auth failed), confirming STS routing")
	})
}

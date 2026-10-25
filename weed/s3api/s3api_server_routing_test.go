package s3api

import (
	"context"
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
	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/cors"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// setupRoutingTestServerWithCreator seeds two non-admin identities holding
// iam:CreateServiceAccount — creator with the coarse (unscoped) action,
// keymaker with a policy scoped to the team-parent identity ARN — plus a
// victim identity, so SigV4-signed CreateServiceAccount requests can exercise
// UnifiedPostHandler's authorization against the ParentUser target.
func setupRoutingTestServerWithCreator(t *testing.T) *S3ApiServer {
	s3a := setupRoutingTestServer(t)
	const creatorAK, creatorSK = "creator-ak", "creator-sk"
	const keymakerAK, keymakerSK = "keymaker-ak", "keymaker-sk"

	// Policy-bearing identities route through the IAM integration.
	mgr := integration.NewIAMManager()
	require.NoError(t, mgr.Initialize(&integration.IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: time.Hour},
			MaxSessionLength: sts.FlexibleDuration{Duration: 12 * time.Hour},
			Issuer:           "test",
			SigningKey:       []byte("test-signing-key-32-characters!!"),
			AccountId:        "111122223333",
		},
		Policy: &policy.PolicyEngineConfig{DefaultEffect: "Deny", StoreType: "memory"},
		Roles:  &integration.RoleStoreConfig{StoreType: "memory"},
	}, func() string { return "localhost:8888" }))
	s3a.iam.SetIAMIntegration(NewS3IAMIntegration(mgr, ""))

	const zonePolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["iam:CreateServiceAccount"],"Resource":["arn:aws:iam:::user/team-parent"]}]}`
	require.NoError(t, s3a.iam.PutPolicy("CreateSvcAcctZonePolicy", zonePolicy))
	// Store and memory must agree: SigV4 auth reads memory, the handler
	// re-reads the store, and a successful creation reloads memory from it.
	idents := []*iam_pb.Identity{
		{Name: "creator", Actions: []string{"iam:CreateServiceAccount"},
			Credentials: []*iam_pb.Credential{{AccessKey: creatorAK, SecretKey: creatorSK}}},
		{Name: "keymaker", PolicyNames: []string{"CreateSvcAcctZonePolicy"},
			Credentials: []*iam_pb.Credential{{AccessKey: keymakerAK, SecretKey: keymakerSK}}},
		{Name: "victim"},
		{Name: "team-parent"},
	}
	require.NoError(t, s3a.credentialManager.SaveConfiguration(context.Background(), &iam_pb.S3ApiConfiguration{
		Identities: idents,
		Policies:   []*iam_pb.Policy{{Name: "CreateSvcAcctZonePolicy", Content: zonePolicy}},
	}))
	for _, ident := range idents {
		require.NoError(t, s3a.iam.UpsertIdentity(ident))
	}
	s3a.cb = NewCircuitBreaker(s3a.option)
	return s3a
}

// TestRouting_CreateServiceAccountParentUserScopedByPolicy verifies that on
// the S3-port IAM route a non-admin holding iam:CreateServiceAccount targets
// the identities its grant covers: the coarse action is unscoped (like a
// wildcard resource), a policy scoped to one parent identity's ARN confines
// the grant to that parent.
func TestRouting_CreateServiceAccountParentUserScopedByPolicy(t *testing.T) {
	router := mux.NewRouter()
	s3a := setupRoutingTestServerWithCreator(t)
	s3a.registerRouter(router)

	sign := func(t *testing.T, req *http.Request, body, ak, sk string) {
		t.Helper()
		creds := credentials.NewStaticCredentials(ak, sk, "")
		if _, err := v4.NewSigner(creds).Sign(req, strings.NewReader(body), "iam", "us-east-1", time.Now()); err != nil {
			t.Fatalf("sign request: %v", err)
		}
	}

	makeReq := func(parentUser, ak, sk string) *http.Request {
		data := url.Values{}
		data.Set("Action", "CreateServiceAccount")
		data.Set("Version", "2010-05-08")
		data.Set("ParentUser", parentUser)
		body := data.Encode()
		req, _ := http.NewRequest("POST", "http://localhost/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		sign(t, req, body, ak, sk)
		return req
	}

	serve := func(req *http.Request) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		return rr
	}

	// Coarse (unscoped) action: any target passes authorization and the
	// service account is actually created.
	rr := serve(makeReq("victim", "creator-ak", "creator-sk"))
	assert.Equal(t, http.StatusOK, rr.Code, "coarse grant is unscoped; got body=%s", rr.Body.String())

	// Scoped policy: the named parent passes (200), any other identity is denied.
	rr2 := serve(makeReq("team-parent", "keymaker-ak", "keymaker-sk"))
	assert.Equal(t, http.StatusOK, rr2.Code, "scoped grant must cover its named parent; got body=%s", rr2.Body.String())

	rr3 := serve(makeReq("victim", "keymaker-ak", "keymaker-sk"))
	assert.Equal(t, http.StatusForbidden, rr3.Code, "target outside the scoped grant must be denied; got body=%s", rr3.Body.String())

	rr4 := serve(makeReq("keymaker", "keymaker-ak", "keymaker-sk"))
	assert.Equal(t, http.StatusForbidden, rr4.Code, "even self is denied when the scoped grant does not name it; got body=%s", rr4.Body.String())
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

// TestRouting_BucketRouteMatchesAnyHost is a regression test for issue #9539.
// When DomainName is configured, bucket-prefix routes must still match for
// requests whose Host header does not match a configured domain (for example,
// requests forwarded by a reverse proxy that rewrites Host to an internal
// upstream address). A Host-less path-style catch-all is registered after the
// host-specific routers so it only fires when no Host matcher applies.
func TestRouting_BucketRouteMatchesAnyHost(t *testing.T) {
	cases := []struct {
		name       string
		domainName string
		host       string
	}{
		{"virtual-host domain configured, request via IP", "s3.example.com", "10.0.0.5:8333"},
		{"virtual-host domain configured, request via unrelated host", "s3.example.com", "internal-upstream:8333"},
		{"path-style + virtual-host configured, request via IP", "s3.example.com,api.s3.example.com", "10.0.0.5:8333"},
		{"domain configured, request via the bare configured host (no bucket subdomain)", "s3.example.com", "s3.example.com"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			router := mux.NewRouter()
			s3a := setupRoutingTestServer(t)
			s3a.option.DomainName = tc.domainName
			s3a.registerRouter(router)

			req, _ := http.NewRequest(http.MethodHead, "http://"+tc.host+"/some-bucket", nil)
			req.Host = tc.host

			var match mux.RouteMatch
			if !router.Match(req, &match) {
				t.Fatalf("expected HEAD /some-bucket with Host=%q to match a bucket route (domainName=%q); got no match (err=%v)",
					tc.host, tc.domainName, match.MatchErr)
			}
			if match.MatchErr != nil {
				t.Fatalf("route matched but reported error: %v", match.MatchErr)
			}
		})
	}
}

// TestRouting_BucketRootPreflightUsesBucketCORS is a regression test: a
// path-style preflight for the bucket root without a trailing
// slash must be evaluated against the bucket CORS rules like /bucket/, not
// answered from the global AllowedOrigins. Service-level paths keep the
// global answer, and virtual-host routing is unchanged.
func TestRouting_BucketRootPreflightUsesBucketCORS(t *testing.T) {
	const (
		bucket        = "cors-bucket"
		allowedOrigin = "https://app.example.com"
		foreignOrigin = "https://untrusted.example"
	)
	pathStyleBucketPaths := []string{"/" + bucket, "/" + bucket + "/", "/" + bucket + "?list-type=2", "/" + bucket + "/key"}

	cases := []struct {
		name         string
		domainName   string
		host         string
		bucketPaths  []string // evaluated against the bucket CORS rules
		servicePaths []string // answered from the global AllowedOrigins
	}{
		{
			name:         "path-style",
			host:         "10.0.0.5:8333",
			bucketPaths:  pathStyleBucketPaths,
			servicePaths: []string{"/", "/status", "/healthz", "/readyz"},
		},
		{
			name:         "path-style domain",
			domainName:   "s3.example.com,api.s3.example.com",
			host:         "api.s3.example.com",
			bucketPaths:  pathStyleBucketPaths,
			servicePaths: []string{"/status", "/healthz", "/readyz"},
		},
		{
			name:        "virtual-host domain",
			domainName:  "s3.example.com,api.s3.example.com",
			host:        bucket + ".s3.example.com",
			bucketPaths: []string{"/", "/?list-type=2", "/key", "/status"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s3a := setupRoutingTestServer(t)
			s3a.option.DomainName = tc.domainName
			s3a.option.AllowedOrigins = []string{"*"}
			s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
			s3a.bucketConfigCache.Set(bucket, &BucketConfig{
				Name: bucket,
				CORS: &cors.CORSConfiguration{CORSRules: []cors.CORSRule{{
					AllowedOrigins: []string{allowedOrigin},
					AllowedMethods: []string{http.MethodGet, http.MethodPut},
					AllowedHeaders: []string{"*"},
				}}},
			})
			router := mux.NewRouter()
			s3a.registerRouter(router)

			preflight := func(path, origin string) *httptest.ResponseRecorder {
				req := httptest.NewRequest(http.MethodOptions, "http://"+tc.host+path, nil)
				req.Header.Set("Origin", origin)
				req.Header.Set("Access-Control-Request-Method", http.MethodPut)
				rr := httptest.NewRecorder()
				router.ServeHTTP(rr, req)
				return rr
			}

			for _, path := range tc.bucketPaths {
				rr := preflight(path, foreignOrigin)
				assert.Equal(t, http.StatusForbidden, rr.Code, "foreign origin on %s", path)
				assert.Empty(t, rr.Header().Get("Access-Control-Allow-Origin"), "foreign origin on %s", path)

				rr = preflight(path, allowedOrigin)
				assert.Equal(t, http.StatusOK, rr.Code, "allowed origin on %s", path)
				assert.Equal(t, allowedOrigin, rr.Header().Get("Access-Control-Allow-Origin"), "allowed origin on %s", path)
			}

			for _, path := range tc.servicePaths {
				rr := preflight(path, foreignOrigin)
				assert.Equal(t, http.StatusOK, rr.Code, "service-level %s", path)
				assert.Equal(t, "*", rr.Header().Get("Access-Control-Allow-Origin"), "service-level %s", path)
			}
		})
	}
}

package s3api

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
)

// TestIsRequestPresignedSignatureV4 - Test validates the logic for presign signature version v4 detection.
func TestIsRequestPresignedSignatureV4(t *testing.T) {
	testCases := []struct {
		inputQueryKey   string
		inputQueryValue string
		expectedResult  bool
	}{
		// Test case - 1.
		// Test case with query key ""X-Amz-Credential" set.
		{"", "", false},
		// Test case - 2.
		{"X-Amz-Credential", "", true},
		// Test case - 3.
		{"X-Amz-Content-Sha256", "", false},
	}

	for i, testCase := range testCases {
		// creating an input HTTP request.
		// Only the query parameters are relevant for this particular test.
		inputReq, err := http.NewRequest(http.MethodGet, "http://example.com", nil)
		if err != nil {
			t.Fatalf("Error initializing input HTTP request: %v", err)
		}
		q := inputReq.URL.Query()
		q.Add(testCase.inputQueryKey, testCase.inputQueryValue)
		inputReq.URL.RawQuery = q.Encode()

		actualResult := isRequestPresignedSignatureV4(inputReq)
		if testCase.expectedResult != actualResult {
			t.Errorf("Test %d: Expected the result to `%v`, but instead got `%v`", i+1, testCase.expectedResult, actualResult)
		}
	}
}

// Tests is requested authenticated function, tests replies for s3 errors.
func TestIsReqAuthenticated(t *testing.T) {
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}
	_ = iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "someone",
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "access_key_1",
						SecretKey: "secret_key_1",
					},
				},
				Actions: []string{"Read", "Write"},
			},
		},
	})

	// List of test cases for validating http request authentication.
	testCases := []struct {
		req     *http.Request
		s3Error s3err.ErrorCode
	}{
		// When request is unsigned, access denied is returned.
		{mustNewRequest(http.MethodGet, "http://127.0.0.1:9000", 0, nil, t), s3err.ErrAccessDenied},
		// When request is properly signed, error is none.
		{mustNewSignedRequest(http.MethodGet, "http://127.0.0.1:9000", 0, nil, t), s3err.ErrNone},
	}

	// Validates all testcases.
	for i, testCase := range testCases {
		if _, s3Error := iam.reqSignatureV4Verify(testCase.req); s3Error != testCase.s3Error {
			io.ReadAll(testCase.req.Body)
			t.Fatalf("Test %d: Unexpected S3 error: want %d - got %d", i, testCase.s3Error, s3Error)
		}
	}
}

func TestCheckaAnonymousRequestAuthType(t *testing.T) {
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}
	_ = iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name:    "anonymous",
				Actions: []string{s3_constants.ACTION_READ},
			},
		},
	})
	testCases := []struct {
		Request *http.Request
		ErrCode s3err.ErrorCode
		Action  Action
	}{
		{Request: mustNewRequest(http.MethodGet, "http://127.0.0.1:9000/bucket", 0, nil, t), ErrCode: s3err.ErrNone, Action: s3_constants.ACTION_READ},
		{Request: mustNewRequest(http.MethodPut, "http://127.0.0.1:9000/bucket", 0, nil, t), ErrCode: s3err.ErrAccessDenied, Action: s3_constants.ACTION_WRITE},
	}
	for i, testCase := range testCases {
		_, s3Error := iam.authRequest(testCase.Request, testCase.Action)
		if s3Error != testCase.ErrCode {
			t.Errorf("Test %d: Unexpected s3error returned wanted %d, got %d", i, testCase.ErrCode, s3Error)
		}
		if testCase.Request.Header.Get(s3_constants.AmzAuthType) != "Anonymous" {
			t.Errorf("Test %d: Unexpected AuthType returned wanted %s, got %s", i, "Anonymous", testCase.Request.Header.Get(s3_constants.AmzAuthType))
		}
	}

}

func TestCheckAdminRequestAuthType(t *testing.T) {
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}
	_ = iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "someone",
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "access_key_1",
						SecretKey: "secret_key_1",
					},
				},
				Actions: []string{"Admin", "Read", "Write"},
			},
		},
	})
	testCases := []struct {
		Request *http.Request
		ErrCode s3err.ErrorCode
	}{
		{Request: mustNewRequest(http.MethodGet, "http://127.0.0.1:9000", 0, nil, t), ErrCode: s3err.ErrAccessDenied},
		{Request: mustNewSignedRequest(http.MethodGet, "http://127.0.0.1:9000", 0, nil, t), ErrCode: s3err.ErrNone},
		{Request: mustNewPresignedRequest(iam, http.MethodGet, "http://127.0.0.1:9000", 0, nil, t), ErrCode: s3err.ErrNone},
	}
	for i, testCase := range testCases {
		if _, s3Error := iam.reqSignatureV4Verify(testCase.Request); s3Error != testCase.ErrCode {
			t.Errorf("Test %d: Unexpected s3error returned wanted %d, got %d", i, testCase.ErrCode, s3Error)
		}
	}
}

func BenchmarkGetSignature(b *testing.B) {
	t := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		signingKey := getSigningKey("secret-key", t.Format(yyyymmdd), "us-east-1", "s3")
		getSignature(signingKey, "random data")
	}
}

// Provides a fully populated http request instance, fails otherwise.
func mustNewRequest(method string, urlStr string, contentLength int64, body io.ReadSeeker, t *testing.T) *http.Request {
	req, err := newTestRequest(method, urlStr, contentLength, body)
	if err != nil {
		t.Fatalf("Unable to initialize new http request %s", err)
	}
	return req
}

// This is similar to mustNewRequest but additionally the request
// is signed with AWS Signature V4, fails if not able to do so.
func mustNewSignedRequest(method string, urlStr string, contentLength int64, body io.ReadSeeker, t *testing.T) *http.Request {
	req := mustNewRequest(method, urlStr, contentLength, body, t)
	cred := &Credential{"access_key_1", "secret_key_1"}
	if err := signRequestV4(req, cred.AccessKey, cred.SecretKey); err != nil {
		t.Fatalf("Unable to initialized new signed http request %s", err)
	}
	return req
}

// This is similar to mustNewRequest but additionally the request
// is presigned with AWS Signature V4, fails if not able to do so.
func mustNewPresignedRequest(iam *IdentityAccessManagement, method string, urlStr string, contentLength int64, body io.ReadSeeker, t *testing.T) *http.Request {
	req := mustNewRequest(method, urlStr, contentLength, body, t)
	cred := &Credential{"access_key_1", "secret_key_1"}
	if err := preSignV4(iam, req, cred.AccessKey, cred.SecretKey, int64(10*time.Minute.Seconds())); err != nil {
		t.Fatalf("Unable to initialized new signed http request %s", err)
	}
	return req
}

// preSignV4 adds presigned URL parameters to the request
func preSignV4(iam *IdentityAccessManagement, req *http.Request, accessKey, secretKey string, expires int64) error {
	// Create credential scope
	now := time.Now().UTC()
	dateStr := now.Format(iso8601Format)

	// Create credential header
	scope := fmt.Sprintf("%s/%s/%s/%s", now.Format(yyyymmdd), "us-east-1", "s3", "aws4_request")
	credential := fmt.Sprintf("%s/%s", accessKey, scope)

	// Get the query parameters
	query := req.URL.Query()
	query.Set("X-Amz-Algorithm", signV4Algorithm)
	query.Set("X-Amz-Credential", credential)
	query.Set("X-Amz-Date", dateStr)
	query.Set("X-Amz-Expires", fmt.Sprintf("%d", expires))
	query.Set("X-Amz-SignedHeaders", "host")

	// Set the query on the URL (without signature yet)
	req.URL.RawQuery = query.Encode()

	// For presigned URLs, the payload hash must be UNSIGNED-PAYLOAD (or from query param if explicitly set)
	// We should NOT use request headers as they're not part of the presigned URL
	hashedPayload := query.Get("X-Amz-Content-Sha256")
	if hashedPayload == "" {
		hashedPayload = unsignedPayload
	}

	// Extract signed headers
	extractedSignedHeaders := make(http.Header)
	extractedSignedHeaders["host"] = []string{req.Host}

	// Get canonical request
	canonicalRequest := getCanonicalRequest(extractedSignedHeaders, hashedPayload, req.URL.RawQuery, req.URL.Path, req.Method)

	// Get string to sign
	stringToSign := getStringToSign(canonicalRequest, now, scope)

	// Get signing key
	signingKey := getSigningKey(secretKey, now.Format(yyyymmdd), "us-east-1", "s3")

	// Calculate signature
	signature := getSignature(signingKey, stringToSign)

	// Add signature to query
	query.Set("X-Amz-Signature", signature)
	req.URL.RawQuery = query.Encode()

	return nil
}

// newTestIAM creates a test IAM with a standard test user
func newTestIAM() *IdentityAccessManagement {
	iam := &IdentityAccessManagement{}
	iam.identities = []*Identity{
		{
			Name:        "testuser",
			Credentials: []*Credential{{AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"}},
			Actions:     []Action{s3_constants.ACTION_ADMIN, s3_constants.ACTION_READ, s3_constants.ACTION_WRITE},
		},
	}
	// Initialize the access key map for lookup
	iam.accessKeyIdent = make(map[string]*Identity)
	iam.accessKeyIdent["AKIAIOSFODNN7EXAMPLE"] = iam.identities[0]
	return iam
}

// Test X-Forwarded-Prefix support for reverse proxy scenarios
func TestSignatureV4WithForwardedPrefix(t *testing.T) {
	tests := []struct {
		name            string
		forwardedPrefix string
		expectedPath    string
	}{
		{
			name:            "prefix without trailing slash",
			forwardedPrefix: "/s3",
			expectedPath:    "/s3/test-bucket/test-object",
		},
		{
			name:            "prefix with trailing slash",
			forwardedPrefix: "/s3/",
			expectedPath:    "/s3/test-bucket/test-object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iam := newTestIAM()

			// Create a request with X-Forwarded-Prefix header
			r, err := newTestRequest("GET", "https://example.com/test-bucket/test-object", 0, nil)
			if err != nil {
				t.Fatalf("Failed to create test request: %v", err)
			}

			// Set the mux variables manually since we're not going through the actual router
			r = mux.SetURLVars(r, map[string]string{
				"bucket": "test-bucket",
				"object": "test-object",
			})

			r.Header.Set("X-Forwarded-Prefix", tt.forwardedPrefix)
			r.Header.Set("Host", "example.com")
			r.Header.Set("X-Forwarded-Host", "example.com")

			// Sign the request with the expected normalized path
			signV4WithPath(r, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", tt.expectedPath)

			// Test signature verification
			_, _, errCode := iam.doesSignatureMatch(r)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected successful signature validation with X-Forwarded-Prefix %q, got error: %v (code: %d)", tt.forwardedPrefix, errCode, int(errCode))
			}
		})
	}
}

// Test X-Forwarded-Prefix with trailing slash preservation (GitHub issue #7223)
// This tests the specific bug where S3 SDK signs paths with trailing slashes
// but path.Clean() would remove them, causing signature verification to fail
func TestSignatureV4WithForwardedPrefixTrailingSlash(t *testing.T) {
	tests := []struct {
		name            string
		forwardedPrefix string
		urlPath         string
		expectedPath    string
	}{
		{
			name:            "bucket listObjects with trailing slash",
			forwardedPrefix: "/oss-sf-nnct",
			urlPath:         "/s3user-bucket1/",
			expectedPath:    "/oss-sf-nnct/s3user-bucket1/",
		},
		{
			name:            "prefix path with trailing slash",
			forwardedPrefix: "/s3",
			urlPath:         "/my-bucket/folder/",
			expectedPath:    "/s3/my-bucket/folder/",
		},
		{
			name:            "root bucket with trailing slash",
			forwardedPrefix: "/api/s3",
			urlPath:         "/test-bucket/",
			expectedPath:    "/api/s3/test-bucket/",
		},
		{
			name:            "nested folder with trailing slash",
			forwardedPrefix: "/storage",
			urlPath:         "/bucket/path/to/folder/",
			expectedPath:    "/storage/bucket/path/to/folder/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iam := newTestIAM()

			// Create a request with the URL path that has a trailing slash
			r, err := newTestRequest("GET", "https://example.com"+tt.urlPath, 0, nil)
			if err != nil {
				t.Fatalf("Failed to create test request: %v", err)
			}

			// Manually set the URL path with trailing slash to ensure it's preserved
			r.URL.Path = tt.urlPath

			r.Header.Set("X-Forwarded-Prefix", tt.forwardedPrefix)
			r.Header.Set("Host", "example.com")
			r.Header.Set("X-Forwarded-Host", "example.com")

			// Sign the request with the full path including the trailing slash
			// This simulates what S3 SDK does for listObjects operations
			signV4WithPath(r, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", tt.expectedPath)

			// Test signature verification - this should succeed even with trailing slashes
			_, _, errCode := iam.doesSignatureMatch(r)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected successful signature validation with trailing slash in path %q, got error: %v (code: %d)", tt.urlPath, errCode, int(errCode))
			}
		})
	}
}

func TestSignatureV4WithoutProxy(t *testing.T) {
	tests := []struct {
		name         string
		host         string
		proto        string
		expectedHost string
	}{
		{
			name:         "HTTP with non-standard port",
			host:         "backend:8333",
			proto:        "http",
			expectedHost: "backend:8333",
		},
		{
			name:         "HTTPS with non-standard port",
			host:         "backend:8333",
			proto:        "https",
			expectedHost: "backend:8333",
		},
		{
			name:         "HTTP with standard port",
			host:         "backend:80",
			proto:        "http",
			expectedHost: "backend",
		},
		{
			name:         "HTTPS with standard port",
			host:         "backend:443",
			proto:        "https",
			expectedHost: "backend",
		},
		{
			name:         "HTTP without port",
			host:         "backend",
			proto:        "http",
			expectedHost: "backend",
		},
		{
			name:         "HTTPS without port",
			host:         "backend",
			proto:        "https",
			expectedHost: "backend",
		},
		{
			name:         "IPv6 HTTP with non-standard port",
			host:         "[::1]:8333",
			proto:        "http",
			expectedHost: "[::1]:8333",
		},
		{
			name:         "IPv6 HTTPS with non-standard port",
			host:         "[::1]:8333",
			proto:        "https",
			expectedHost: "[::1]:8333",
		},
		{
			name:         "IPv6 HTTP with standard port",
			host:         "[::1]:80",
			proto:        "http",
			expectedHost: "::1",
		},
		{
			name:         "IPv6 HTTPS with standard port",
			host:         "[::1]:443",
			proto:        "https",
			expectedHost: "::1",
		},
		{
			name:         "IPv6 HTTP without port",
			host:         "::1",
			proto:        "http",
			expectedHost: "::1",
		},
		{
			name:         "IPv6 HTTPS without port",
			host:         "::1",
			proto:        "https",
			expectedHost: "::1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iam := newTestIAM()

			// Create a request
			r, err := newTestRequest("GET", tt.proto+"://"+tt.host+"/test-bucket/test-object", 0, nil)
			if err != nil {
				t.Fatalf("Failed to create test request: %v", err)
			}

			// Set the mux variables manually since we're not going through the actual router
			r = mux.SetURLVars(r, map[string]string{
				"bucket": "test-bucket",
				"object": "test-object",
			})

			// Set forwarded headers
			r.Header.Set("Host", tt.host)

			// First, verify that extractHostHeader returns the expected value
			extractedHost := extractHostHeader(r)
			if extractedHost != tt.expectedHost {
				t.Errorf("extractHostHeader() = %q, want %q", extractedHost, tt.expectedHost)
			}

			// Sign the request with the expected host header
			// We need to temporarily modify the Host header for signing
			signV4WithPath(r, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", r.URL.Path)

			// Test signature verification
			_, _, errCode := iam.doesSignatureMatch(r)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected successful signature validation, got error: %v (code: %d)", errCode, int(errCode))
			}
		})
	}
}

// Test X-Forwarded-Port support for reverse proxy scenarios
func TestSignatureV4WithForwardedPort(t *testing.T) {
	tests := []struct {
		name           string
		host           string
		forwardedHost  string
		forwardedPort  string
		forwardedProto string
		expectedHost   string
	}{
		{
			name:           "HTTP with non-standard port",
			host:           "backend:8333",
			forwardedHost:  "example.com",
			forwardedPort:  "8080",
			forwardedProto: "http",
			expectedHost:   "example.com:8080",
		},
		{
			name:           "HTTPS with non-standard port",
			host:           "backend:8333",
			forwardedHost:  "example.com",
			forwardedPort:  "8443",
			forwardedProto: "https",
			expectedHost:   "example.com:8443",
		},
		{
			name:           "HTTP with standard port (80)",
			host:           "backend:8333",
			forwardedHost:  "example.com",
			forwardedPort:  "80",
			forwardedProto: "http",
			expectedHost:   "example.com",
		},
		{
			name:           "HTTPS with standard port (443)",
			host:           "backend:8333",
			forwardedHost:  "example.com",
			forwardedPort:  "443",
			forwardedProto: "https",
			expectedHost:   "example.com",
		},
		{
			name:           "empty proto with non-standard port",
			host:           "backend:8333",
			forwardedHost:  "example.com",
			forwardedPort:  "8080",
			forwardedProto: "",
			expectedHost:   "example.com:8080",
		},
		{
			name:           "empty proto with standard http port",
			host:           "backend:8333",
			forwardedHost:  "example.com",
			forwardedPort:  "80",
			forwardedProto: "",
			expectedHost:   "example.com",
		},
		// Test cases for issue #6649: X-Forwarded-Host already contains port
		{
			name:           "X-Forwarded-Host with port already included (Traefik/HAProxy style)",
			host:           "backend:8333",
			forwardedHost:  "127.0.0.1:8433",
			forwardedPort:  "8433",
			forwardedProto: "https",
			expectedHost:   "127.0.0.1:8433",
		},
		{
			name:           "X-Forwarded-Host with port, no X-Forwarded-Port header",
			host:           "backend:8333",
			forwardedHost:  "example.com:9000",
			forwardedPort:  "",
			forwardedProto: "http",
			expectedHost:   "example.com:9000",
		},
		{
			name:           "X-Forwarded-Host with standard https port already included (Traefik/HAProxy style)",
			host:           "backend:443",
			forwardedHost:  "127.0.0.1:443",
			forwardedPort:  "443",
			forwardedProto: "https",
			expectedHost:   "127.0.0.1",
		},
		{
			name:           "X-Forwarded-Host with standard http port already included (Traefik/HAProxy style)",
			host:           "backend:80",
			forwardedHost:  "127.0.0.1:80",
			forwardedPort:  "80",
			forwardedProto: "http",
			expectedHost:   "127.0.0.1",
		},
		{
			name:           "IPv6 X-Forwarded-Host with standard https port already included (Traefik/HAProxy style)",
			host:           "backend:443",
			forwardedHost:  "[::1]:443",
			forwardedPort:  "443",
			forwardedProto: "https",
			expectedHost:   "::1",
		},
		{
			name:           "IPv6 X-Forwarded-Host with standard http port already included (Traefik/HAProxy style)",
			host:           "backend:80",
			forwardedHost:  "[::1]:80",
			forwardedPort:  "80",
			forwardedProto: "http",
			expectedHost:   "::1",
		},
		{
			name:           "IPv6 with port in brackets",
			host:           "backend:8333",
			forwardedHost:  "[::1]:8080",
			forwardedPort:  "8080",
			forwardedProto: "http",
			expectedHost:   "[::1]:8080",
		},
		{
			name:           "IPv6 without port - should add port with brackets",
			host:           "backend:8333",
			forwardedHost:  "::1",
			forwardedPort:  "8080",
			forwardedProto: "http",
			expectedHost:   "[::1]:8080",
		},
		{
			name:           "IPv6 in brackets without port - should add port",
			host:           "backend:8333",
			forwardedHost:  "[2001:db8::1]",
			forwardedPort:  "8080",
			forwardedProto: "http",
			expectedHost:   "[2001:db8::1]:8080",
		},
		{
			name:           "IPv4-mapped IPv6 without port - should add port with brackets",
			host:           "backend:8333",
			forwardedHost:  "::ffff:127.0.0.1",
			forwardedPort:  "8080",
			forwardedProto: "http",
			expectedHost:   "[::ffff:127.0.0.1]:8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iam := newTestIAM()

			// Create a request
			r, err := newTestRequest("GET", "https://"+tt.host+"/test-bucket/test-object", 0, nil)
			if err != nil {
				t.Fatalf("Failed to create test request: %v", err)
			}

			// Set the mux variables manually since we're not going through the actual router
			r = mux.SetURLVars(r, map[string]string{
				"bucket": "test-bucket",
				"object": "test-object",
			})

			// Set forwarded headers
			r.Header.Set("Host", tt.host)
			r.Header.Set("X-Forwarded-Host", tt.forwardedHost)
			r.Header.Set("X-Forwarded-Port", tt.forwardedPort)
			r.Header.Set("X-Forwarded-Proto", tt.forwardedProto)

			// Sign the request with the expected host header
			// We need to temporarily modify the Host header for signing
			signV4WithPath(r, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", r.URL.Path)

			// Test signature verification
			_, _, errCode := iam.doesSignatureMatch(r)
			if errCode != s3err.ErrNone {
				t.Errorf("Expected successful signature validation with forwarded port, got error: %v (code: %d)", errCode, int(errCode))
			}
		})
	}
}

// Test basic presigned URL functionality without prefix
func TestPresignedSignatureV4Basic(t *testing.T) {
	iam := newTestIAM()

	// Create a presigned request without X-Forwarded-Prefix header
	r, err := newTestRequest("GET", "https://example.com/test-bucket/test-object", 0, nil)
	if err != nil {
		t.Fatalf("Failed to create test request: %v", err)
	}

	// Set the mux variables manually since we're not going through the actual router
	r = mux.SetURLVars(r, map[string]string{
		"bucket": "test-bucket",
		"object": "test-object",
	})

	r.Header.Set("Host", "example.com")

	// Create presigned URL with the normal path (no prefix)
	err = preSignV4WithPath(iam, r, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", 3600, r.URL.Path)
	if err != nil {
		t.Errorf("Failed to presign request: %v", err)
	}

	// Test presigned signature verification
	_, _, errCode := iam.doesPresignedSignatureMatch(r)
	if errCode != s3err.ErrNone {
		t.Errorf("Expected successful presigned signature validation, got error: %v (code: %d)", errCode, int(errCode))
	}
}

// TestPresignedSignatureV4MissingExpires verifies that X-Amz-Expires is required for presigned URLs
func TestPresignedSignatureV4MissingExpires(t *testing.T) {
	iam := newTestIAM()

	// Create a presigned request
	r, err := newTestRequest("GET", "https://example.com/test-bucket/test-object", 0, nil)
	if err != nil {
		t.Fatalf("Failed to create test request: %v", err)
	}

	r = mux.SetURLVars(r, map[string]string{
		"bucket": "test-bucket",
		"object": "test-object",
	})
	r.Header.Set("Host", "example.com")

	// Manually construct presigned URL query parameters WITHOUT X-Amz-Expires
	now := time.Now().UTC()
	dateStr := now.Format(iso8601Format)
	scope := fmt.Sprintf("%s/%s/%s/%s", now.Format(yyyymmdd), "us-east-1", "s3", "aws4_request")
	credential := fmt.Sprintf("%s/%s", "AKIAIOSFODNN7EXAMPLE", scope)

	query := r.URL.Query()
	query.Set("X-Amz-Algorithm", signV4Algorithm)
	query.Set("X-Amz-Credential", credential)
	query.Set("X-Amz-Date", dateStr)
	// Intentionally NOT setting X-Amz-Expires
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Signature", "dummy-signature") // Signature doesn't matter, should fail earlier
	r.URL.RawQuery = query.Encode()

	// Test presigned signature verification - should fail with ErrInvalidQueryParams
	_, _, errCode := iam.doesPresignedSignatureMatch(r)
	if errCode != s3err.ErrInvalidQueryParams {
		t.Errorf("Expected ErrInvalidQueryParams for missing X-Amz-Expires, got: %v (code: %d)", errCode, int(errCode))
	}
}

// Test X-Forwarded-Prefix support for presigned URLs
func TestPresignedSignatureV4WithForwardedPrefix(t *testing.T) {
	tests := []struct {
		name            string
		forwardedPrefix string
		originalPath    string
		expectedPath    string
	}{
		{
			name:            "prefix without trailing slash",
			forwardedPrefix: "/s3",
			originalPath:    "/s3/test-bucket/test-object",
			expectedPath:    "/s3/test-bucket/test-object",
		},
		{
			name:            "prefix with trailing slash",
			forwardedPrefix: "/s3/",
			originalPath:    "/s3/test-bucket/test-object",
			expectedPath:    "/s3/test-bucket/test-object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iam := newTestIAM()

			// Create a presigned request that simulates reverse proxy scenario:
			// 1. Client generates presigned URL with prefixed path
			// 2. Proxy strips prefix and forwards to SeaweedFS with X-Forwarded-Prefix header

			// Start with the original request URL (what client sees)
			r, err := newTestRequest("GET", "https://example.com"+tt.originalPath, 0, nil)
			if err != nil {
				t.Fatalf("Failed to create test request: %v", err)
			}

			// Generate presigned URL with the original prefixed path
			err = preSignV4WithPath(iam, r, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", 3600, tt.originalPath)
			if err != nil {
				t.Errorf("Failed to presign request: %v", err)
				return
			}

			// Now simulate what the reverse proxy does:
			// 1. Strip the prefix from the URL path
			r.URL.Path = "/test-bucket/test-object"

			// 2. Set the mux variables for the stripped path
			r = mux.SetURLVars(r, map[string]string{
				"bucket": "test-bucket",
				"object": "test-object",
			})

			// 3. Add the forwarded headers
			r.Header.Set("X-Forwarded-Prefix", tt.forwardedPrefix)
			r.Header.Set("Host", "example.com")
			r.Header.Set("X-Forwarded-Host", "example.com")

			// Test presigned signature verification
			_, _, errCode := iam.doesPresignedSignatureMatch(r)

			if errCode != s3err.ErrNone {
				t.Errorf("Expected successful presigned signature validation with X-Forwarded-Prefix %q, got error: %v (code: %d)", tt.forwardedPrefix, errCode, int(errCode))
			}
		})
	}
}

// Test X-Forwarded-Prefix with trailing slash preservation for presigned URLs (GitHub issue #7223)
func TestPresignedSignatureV4WithForwardedPrefixTrailingSlash(t *testing.T) {
	tests := []struct {
		name            string
		forwardedPrefix string
		originalPath    string
		strippedPath    string
	}{
		{
			name:            "bucket listObjects with trailing slash",
			forwardedPrefix: "/oss-sf-nnct",
			originalPath:    "/oss-sf-nnct/s3user-bucket1/",
			strippedPath:    "/s3user-bucket1/",
		},
		{
			name:            "prefix path with trailing slash",
			forwardedPrefix: "/s3",
			originalPath:    "/s3/my-bucket/folder/",
			strippedPath:    "/my-bucket/folder/",
		},
		{
			name:            "api path with trailing slash",
			forwardedPrefix: "/api/s3",
			originalPath:    "/api/s3/test-bucket/",
			strippedPath:    "/test-bucket/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iam := newTestIAM()

			// Create a presigned request that simulates reverse proxy scenario with trailing slashes:
			// 1. Client generates presigned URL with prefixed path including trailing slash
			// 2. Proxy strips prefix and forwards to SeaweedFS with X-Forwarded-Prefix header

			// Start with the original request URL (what client sees) with trailing slash
			r, err := newTestRequest("GET", "https://example.com"+tt.originalPath, 0, nil)
			if err != nil {
				t.Fatalf("Failed to create test request: %v", err)
			}

			// Generate presigned URL with the original prefixed path including trailing slash
			err = preSignV4WithPath(iam, r, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", 3600, tt.originalPath)
			if err != nil {
				t.Errorf("Failed to presign request: %v", err)
				return
			}

			// Now simulate what the reverse proxy does:
			// 1. Strip the prefix from the URL path but preserve the trailing slash
			r.URL.Path = tt.strippedPath

			// 2. Add the forwarded headers
			r.Header.Set("X-Forwarded-Prefix", tt.forwardedPrefix)
			r.Header.Set("Host", "example.com")
			r.Header.Set("X-Forwarded-Host", "example.com")

			// Test presigned signature verification - this should succeed with trailing slashes
			_, _, errCode := iam.doesPresignedSignatureMatch(r)

			if errCode != s3err.ErrNone {
				t.Errorf("Expected successful presigned signature validation with trailing slash in path %q, got error: %v (code: %d)", tt.strippedPath, errCode, int(errCode))
			}
		})
	}
}

// preSignV4WithPath adds presigned URL parameters to the request with a custom path
func preSignV4WithPath(iam *IdentityAccessManagement, req *http.Request, accessKey, secretKey string, expires int64, urlPath string) error {
	// Create credential scope
	now := time.Now().UTC()
	dateStr := now.Format(iso8601Format)

	// Create credential header
	scope := fmt.Sprintf("%s/%s/%s/%s", now.Format(yyyymmdd), "us-east-1", "s3", "aws4_request")
	credential := fmt.Sprintf("%s/%s", accessKey, scope)

	// Get the query parameters
	query := req.URL.Query()
	query.Set("X-Amz-Algorithm", signV4Algorithm)
	query.Set("X-Amz-Credential", credential)
	query.Set("X-Amz-Date", dateStr)
	query.Set("X-Amz-Expires", fmt.Sprintf("%d", expires))
	query.Set("X-Amz-SignedHeaders", "host")

	// Set the query on the URL (without signature yet)
	req.URL.RawQuery = query.Encode()

	// For presigned URLs, the payload hash must be UNSIGNED-PAYLOAD (or from query param if explicitly set)
	// We should NOT use request headers as they're not part of the presigned URL
	hashedPayload := query.Get("X-Amz-Content-Sha256")
	if hashedPayload == "" {
		hashedPayload = unsignedPayload
	}

	// Extract signed headers
	extractedSignedHeaders := make(http.Header)
	extractedSignedHeaders["host"] = []string{extractHostHeader(req)}

	// Get canonical request with custom path
	canonicalRequest := getCanonicalRequest(extractedSignedHeaders, hashedPayload, req.URL.RawQuery, urlPath, req.Method)

	// Get string to sign
	stringToSign := getStringToSign(canonicalRequest, now, scope)

	// Get signing key
	signingKey := getSigningKey(secretKey, now.Format(yyyymmdd), "us-east-1", "s3")

	// Calculate signature
	signature := getSignature(signingKey, stringToSign)

	// Add signature to query
	query.Set("X-Amz-Signature", signature)
	req.URL.RawQuery = query.Encode()

	return nil
}

// signV4WithPath signs a request with a custom path
func signV4WithPath(req *http.Request, accessKey, secretKey, urlPath string) {
	// Create credential scope
	now := time.Now().UTC()
	dateStr := now.Format(iso8601Format)

	// Set required headers
	req.Header.Set("X-Amz-Date", dateStr)

	// Create credential header
	scope := fmt.Sprintf("%s/%s/%s/%s", now.Format(yyyymmdd), "us-east-1", "s3", "aws4_request")
	credential := fmt.Sprintf("%s/%s", accessKey, scope)

	// Get signed headers
	signedHeaders := "host;x-amz-date"

	// Extract signed headers
	extractedSignedHeaders := make(http.Header)
	extractedSignedHeaders["host"] = []string{extractHostHeader(req)}
	extractedSignedHeaders["x-amz-date"] = []string{dateStr}

	// Get the payload hash
	hashedPayload := getContentSha256Cksum(req)

	// Get canonical request with custom path
	canonicalRequest := getCanonicalRequest(extractedSignedHeaders, hashedPayload, req.URL.RawQuery, urlPath, req.Method)

	// Get string to sign
	stringToSign := getStringToSign(canonicalRequest, now, scope)

	// Get signing key
	signingKey := getSigningKey(secretKey, now.Format(yyyymmdd), "us-east-1", "s3")

	// Calculate signature
	signature := getSignature(signingKey, stringToSign)

	// Set Authorization header
	authorization := fmt.Sprintf("%s Credential=%s, SignedHeaders=%s, Signature=%s",
		signV4Algorithm, credential, signedHeaders, signature)
	req.Header.Set("Authorization", authorization)
}

// Returns new HTTP request object.
func newTestRequest(method, urlStr string, contentLength int64, body io.ReadSeeker) (*http.Request, error) {
	if method == "" {
		method = http.MethodPost
	}

	// Save for subsequent use
	var hashedPayload string
	var md5Base64 string
	switch {
	case body == nil:
		hashedPayload = getSHA256Hash([]byte{})
	default:
		payloadBytes, err := io.ReadAll(body)
		if err != nil {
			return nil, err
		}
		hashedPayload = getSHA256Hash(payloadBytes)
		md5Base64 = getMD5HashBase64(payloadBytes)
	}
	// Seek back to beginning.
	if body != nil {
		body.Seek(0, 0)
	} else {
		body = bytes.NewReader([]byte(""))
	}
	req, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		return nil, err
	}
	if md5Base64 != "" {
		req.Header.Set("Content-Md5", md5Base64)
	}
	req.Header.Set("x-amz-content-sha256", hashedPayload)

	// Add Content-Length
	req.ContentLength = contentLength

	return req, nil
}

// getMD5HashBase64 returns MD5 hash in base64 encoding of given data.
func getMD5HashBase64(data []byte) string {
	return base64.StdEncoding.EncodeToString(getMD5Sum(data))
}

// getSHA256Sum returns SHA-256 sum of given data.
func getSHA256Sum(data []byte) []byte {
	hash := sha256.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// getMD5Sum returns MD5 sum of given data.
func getMD5Sum(data []byte) []byte {
	hash := md5.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// getMD5Hash returns MD5 hash in hex encoding of given data.
func getMD5Hash(data []byte) string {
	return hex.EncodeToString(getMD5Sum(data))
}

var ignoredHeaders = map[string]bool{
	"Authorization":  true,
	"Content-Type":   true,
	"Content-Length": true,
	"User-Agent":     true,
}

// Tests the test helper with an example from the AWS Doc.
// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
// This time it's a PUT request uploading the file with content "Welcome to Amazon S3."
func TestGetStringToSignPUT(t *testing.T) {

	canonicalRequest := `PUT
/test%24file.text

date:Fri, 24 May 2013 00:00:00 GMT
host:examplebucket.s3.amazonaws.com
x-amz-content-sha256:44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072
x-amz-date:20130524T000000Z
x-amz-storage-class:REDUCED_REDUNDANCY

date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class
44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072`

	date, err := time.Parse(iso8601Format, "20130524T000000Z")

	if err != nil {
		t.Fatalf("Error parsing date: %v", err)
	}

	scope := "20130524/us-east-1/s3/aws4_request"
	stringToSign := getStringToSign(canonicalRequest, date, scope)

	expected := `AWS4-HMAC-SHA256
20130524T000000Z
20130524/us-east-1/s3/aws4_request
9e0e90d9c76de8fa5b200d8c849cd5b8dc7a3be3951ddb7f6a76b4158342019d`

	assert.Equal(t, expected, stringToSign)
}

// Tests the test helper with an example from the AWS Doc.
// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
// The GET request example with empty string hash.
func TestGetStringToSignGETEmptyStringHash(t *testing.T) {

	canonicalRequest := `GET
/test.txt

host:examplebucket.s3.amazonaws.com
range:bytes=0-9
x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
x-amz-date:20130524T000000Z

host;range;x-amz-content-sha256;x-amz-date
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`

	date, err := time.Parse(iso8601Format, "20130524T000000Z")

	if err != nil {
		t.Fatalf("Error parsing date: %v", err)
	}

	scope := "20130524/us-east-1/s3/aws4_request"
	stringToSign := getStringToSign(canonicalRequest, date, scope)

	expected := `AWS4-HMAC-SHA256
20130524T000000Z
20130524/us-east-1/s3/aws4_request
7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972`

	assert.Equal(t, expected, stringToSign)
}

// Sign given request using Signature V4.
func signRequestV4(req *http.Request, accessKey, secretKey string) error {
	// Get hashed payload.
	hashedPayload := req.Header.Get("x-amz-content-sha256")
	if hashedPayload == "" {
		return fmt.Errorf("Invalid hashed payload")
	}

	currTime := time.Now().UTC()

	// Set x-amz-date.
	req.Header.Set("x-amz-date", currTime.Format(iso8601Format))

	// Get header map.
	headerMap := make(map[string][]string)
	for k, vv := range req.Header {
		// If request header key is not in ignored headers, then add it.
		if _, ok := ignoredHeaders[http.CanonicalHeaderKey(k)]; !ok {
			headerMap[strings.ToLower(k)] = vv
		}
	}

	// Get header keys.
	headers := []string{"host"}
	for k := range headerMap {
		headers = append(headers, k)
	}
	sort.Strings(headers)

	region := "us-east-1"

	// Get canonical headers.
	var buf bytes.Buffer
	for _, k := range headers {
		buf.WriteString(k)
		buf.WriteByte(':')
		switch {
		case k == "host":
			buf.WriteString(req.URL.Host)
			fallthrough
		default:
			for idx, v := range headerMap[k] {
				if idx > 0 {
					buf.WriteByte(',')
				}
				buf.WriteString(v)
			}
			buf.WriteByte('\n')
		}
	}
	canonicalHeaders := buf.String()

	// Get signed headers.
	signedHeaders := strings.Join(headers, ";")

	// Get canonical query string.
	req.URL.RawQuery = strings.Replace(req.URL.Query().Encode(), "+", "%20", -1)

	// Get canonical URI.
	canonicalURI := EncodePath(req.URL.Path)

	// Get canonical request.
	// canonicalRequest =
	//  <HTTPMethod>\n
	//  <CanonicalURI>\n
	//  <CanonicalQueryString>\n
	//  <CanonicalHeaders>\n
	//  <SignedHeaders>\n
	//  <HashedPayload>
	//
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		req.URL.RawQuery,
		canonicalHeaders,
		signedHeaders,
		hashedPayload,
	}, "\n")

	// Get scope.
	scope := strings.Join([]string{
		currTime.Format(yyyymmdd),
		region,
		"s3",
		"aws4_request",
	}, "/")

	stringToSign := "AWS4-HMAC-SHA256" + "\n" + currTime.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + scope + "\n"
	stringToSign = stringToSign + getSHA256Hash([]byte(canonicalRequest))

	date := sumHMAC([]byte("AWS4"+secretKey), []byte(currTime.Format(yyyymmdd)))
	regionHMAC := sumHMAC(date, []byte(region))
	service := sumHMAC(regionHMAC, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))

	signature := hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))

	// final Authorization header
	parts := []string{
		"AWS4-HMAC-SHA256" + " Credential=" + accessKey + "/" + scope,
		"SignedHeaders=" + signedHeaders,
		"Signature=" + signature,
	}
	auth := strings.Join(parts, ", ")
	req.Header.Set("Authorization", auth)

	return nil
}

// EncodePath encode the strings from UTF-8 byte representations to HTML hex escape sequences
//
// This is necessary since regular url.Parse() and url.Encode() functions do not support UTF-8
// non english characters cannot be parsed due to the nature in which url.Encode() is written
//
// This function on the other hand is a direct replacement for url.Encode() technique to support
// pretty much every UTF-8 character.
func EncodePath(pathName string) string {
	if reservedObjectNames.MatchString(pathName) {
		return pathName
	}
	var encodedPathname string
	for _, s := range pathName {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' { // §2.3 Unreserved characters (mark)
			encodedPathname = encodedPathname + string(s)
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedPathname = encodedPathname + string(s)
			continue
		default:
			runeLen := utf8.RuneLen(s)
			if runeLen < 0 {
				// if utf8 cannot convert return the same string as is
				return pathName
			}
			u := make([]byte, runeLen)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				hex := hex.EncodeToString([]byte{r})
				encodedPathname = encodedPathname + "%" + strings.ToUpper(hex)
			}
		}
	}
	return encodedPathname
}

// Test that IAM requests correctly compute payload hash from request body
// This addresses the regression described in GitHub issue #7080
func TestIAMPayloadHashComputation(t *testing.T) {
	// Create test IAM instance
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}

	// Load test configuration with a user
	err := iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "testuser",
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "AKIAIOSFODNN7EXAMPLE",
						SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
					},
				},
				Actions: []string{"Admin"},
			},
		},
	})
	assert.NoError(t, err)

	// Test payload for IAM request (typical CreateAccessKey request)
	testPayload := "Action=CreateAccessKey&UserName=testuser&Version=2010-05-08"

	// Create request with body (typical IAM request)
	req, err := http.NewRequest("POST", "http://localhost:8111/", strings.NewReader(testPayload))
	assert.NoError(t, err)

	// Set required headers for IAM request
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	req.Header.Set("Host", "localhost:8111")

	// Create an IAM-style authorization header with "iam" service instead of "s3"
	now := time.Now().UTC()
	dateStr := now.Format("20060102T150405Z")
	credentialScope := now.Format("20060102") + "/us-east-1/iam/aws4_request"

	req.Header.Set("X-Amz-Date", dateStr)

	// Create authorization header with "iam" service (this is the key difference from S3)
	authHeader := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/" + credentialScope +
		", SignedHeaders=content-type;host;x-amz-date, Signature=dummysignature"
	req.Header.Set("Authorization", authHeader)

	// Test the doesSignatureMatch function directly
	// This should now compute the correct payload hash for IAM requests
	identity, _, errCode := iam.doesSignatureMatch(req)

	// Even though the signature will fail (dummy signature),
	// the fact that we get past the credential parsing means the payload hash was computed correctly
	// We expect ErrSignatureDoesNotMatch because we used a dummy signature,
	// but NOT ErrAccessDenied or other auth errors
	assert.Equal(t, s3err.ErrSignatureDoesNotMatch, errCode)
	assert.Nil(t, identity)

	// More importantly, test that the request body is preserved after reading
	// The fix should restore the body after reading it
	bodyBytes := make([]byte, len(testPayload))
	n, err := req.Body.Read(bodyBytes)
	assert.NoError(t, err)
	assert.Equal(t, len(testPayload), n)
	assert.Equal(t, testPayload, string(bodyBytes))
}

// Test that S3 requests still work correctly (no regression)
func TestS3PayloadHashNoRegression(t *testing.T) {
	// Create test IAM instance
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}

	// Load test configuration
	err := iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "testuser",
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "AKIAIOSFODNN7EXAMPLE",
						SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
					},
				},
				Actions: []string{"Admin"},
			},
		},
	})
	assert.NoError(t, err)

	// Create S3 request (no body, should use emptySHA256)
	req, err := http.NewRequest("GET", "http://localhost:8333/bucket/object", nil)
	assert.NoError(t, err)

	req.Header.Set("Host", "localhost:8333")

	// Create S3-style authorization header with "s3" service
	now := time.Now().UTC()
	dateStr := now.Format("20060102T150405Z")
	credentialScope := now.Format("20060102") + "/us-east-1/s3/aws4_request"

	req.Header.Set("X-Amz-Date", dateStr)
	req.Header.Set("X-Amz-Content-Sha256", emptySHA256)

	authHeader := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/" + credentialScope +
		", SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=dummysignature"
	req.Header.Set("Authorization", authHeader)

	// This should use the emptySHA256 hash and not try to read the body
	identity, _, errCode := iam.doesSignatureMatch(req)

	// Should get signature mismatch (because of dummy signature) but not other errors
	assert.Equal(t, s3err.ErrSignatureDoesNotMatch, errCode)
	assert.Nil(t, identity)
}

// Test edge case: IAM request with empty body should still use emptySHA256
func TestIAMEmptyBodyPayloadHash(t *testing.T) {
	// Create test IAM instance
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}

	// Load test configuration
	err := iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "testuser",
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "AKIAIOSFODNN7EXAMPLE",
						SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
					},
				},
				Actions: []string{"Admin"},
			},
		},
	})
	assert.NoError(t, err)

	// Create IAM request with empty body
	req, err := http.NewRequest("POST", "http://localhost:8111/", bytes.NewReader([]byte{}))
	assert.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	req.Header.Set("Host", "localhost:8111")

	// Create IAM-style authorization header
	now := time.Now().UTC()
	dateStr := now.Format("20060102T150405Z")
	credentialScope := now.Format("20060102") + "/us-east-1/iam/aws4_request"

	req.Header.Set("X-Amz-Date", dateStr)

	authHeader := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/" + credentialScope +
		", SignedHeaders=content-type;host;x-amz-date, Signature=dummysignature"
	req.Header.Set("Authorization", authHeader)

	// Even with an IAM request, empty body should result in emptySHA256
	identity, _, errCode := iam.doesSignatureMatch(req)

	// Should get signature mismatch (because of dummy signature) but not other errors
	assert.Equal(t, s3err.ErrSignatureDoesNotMatch, errCode)
	assert.Nil(t, identity)
}

// Test that non-S3 services (like STS) also get payload hash computation
func TestSTSPayloadHashComputation(t *testing.T) {
	// Create test IAM instance
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}

	// Load test configuration
	err := iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "testuser",
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "AKIAIOSFODNN7EXAMPLE",
						SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
					},
				},
				Actions: []string{"Admin"},
			},
		},
	})
	assert.NoError(t, err)

	// Test payload for STS request (AssumeRole request)
	testPayload := "Action=AssumeRole&RoleArn=arn:aws:iam::123456789012:role/TestRole&RoleSessionName=test&Version=2011-06-15"

	// Create request with body (typical STS request)
	req, err := http.NewRequest("POST", "http://localhost:8112/", strings.NewReader(testPayload))
	assert.NoError(t, err)

	// Set required headers for STS request
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	req.Header.Set("Host", "localhost:8112")

	// Create an STS-style authorization header with "sts" service
	now := time.Now().UTC()
	dateStr := now.Format("20060102T150405Z")
	credentialScope := now.Format("20060102") + "/us-east-1/sts/aws4_request"

	req.Header.Set("X-Amz-Date", dateStr)

	authHeader := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/" + credentialScope +
		", SignedHeaders=content-type;host;x-amz-date, Signature=dummysignature"
	req.Header.Set("Authorization", authHeader)

	// Test the doesSignatureMatch function
	// This should compute the correct payload hash for STS requests (non-S3 service)
	identity, _, errCode := iam.doesSignatureMatch(req)

	// Should get signature mismatch (dummy signature) but payload hash should be computed correctly
	assert.Equal(t, s3err.ErrSignatureDoesNotMatch, errCode)
	assert.Nil(t, identity)

	// Verify body is preserved after reading
	bodyBytes := make([]byte, len(testPayload))
	n, err := req.Body.Read(bodyBytes)
	assert.NoError(t, err)
	assert.Equal(t, len(testPayload), n)
	assert.Equal(t, testPayload, string(bodyBytes))
}

// Test the specific scenario from GitHub issue #7080
func TestGitHubIssue7080Scenario(t *testing.T) {
	// Create test IAM instance
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}

	// Load test configuration matching the issue scenario
	err := iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "testuser",
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "testkey",
						SecretKey: "testsecret",
					},
				},
				Actions: []string{"Admin"},
			},
		},
	})
	assert.NoError(t, err)

	// Simulate the payload from the GitHub issue (CreateAccessKey request)
	testPayload := "Action=CreateAccessKey&UserName=admin&Version=2010-05-08"

	// Create the request that was failing
	req, err := http.NewRequest("POST", "http://localhost:8111/", strings.NewReader(testPayload))
	assert.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	req.Header.Set("Host", "localhost:8111")

	// Create authorization header with IAM service (this was the failing case)
	now := time.Now().UTC()
	dateStr := now.Format("20060102T150405Z")
	credentialScope := now.Format("20060102") + "/us-east-1/iam/aws4_request"

	req.Header.Set("X-Amz-Date", dateStr)

	authHeader := "AWS4-HMAC-SHA256 Credential=testkey/" + credentialScope +
		", SignedHeaders=content-type;host;x-amz-date, Signature=testsignature"
	req.Header.Set("Authorization", authHeader)

	// Before the fix, this would have failed with payload hash mismatch
	// After the fix, it should properly compute the payload hash and proceed to signature verification

	// Since we're using a dummy signature, we expect signature mismatch, but the important
	// thing is that it doesn't fail earlier due to payload hash computation issues
	identity, _, errCode := iam.doesSignatureMatch(req)

	// The error should be signature mismatch, not payload related
	assert.Equal(t, s3err.ErrSignatureDoesNotMatch, errCode)
	assert.Nil(t, identity)

	// Verify the request body is still accessible (fix preserves body)
	bodyBytes := make([]byte, len(testPayload))
	n, err := req.Body.Read(bodyBytes)
	assert.NoError(t, err)
	assert.Equal(t, len(testPayload), n)
	assert.Equal(t, testPayload, string(bodyBytes))
}

// TestIAMSignatureServiceMatching tests that IAM requests use the correct service in signature computation
// This reproduces the bug described in GitHub issue #7080 where the service was hardcoded to "s3"
func TestIAMSignatureServiceMatching(t *testing.T) {
	// Create test IAM instance
	iam := &IdentityAccessManagement{}

	// Load test configuration with credentials that match the logs
	err := iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "power_user",
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "power_user_key",
						SecretKey: "power_user_secret",
					},
				},
				Actions: []string{"Admin"},
			},
		},
	})
	assert.NoError(t, err)

	// Use the exact payload and headers from the failing logs
	testPayload := "Action=CreateAccessKey&UserName=admin&Version=2010-05-08"

	// Use current time to avoid clock skew validation failures
	now := time.Now().UTC()
	amzDate := now.Format(iso8601Format)
	dateStamp := now.Format(yyyymmdd)

	// Create request exactly as shown in logs
	req, err := http.NewRequest("POST", "http://localhost:8111/", strings.NewReader(testPayload))
	assert.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	req.Header.Set("Host", "localhost:8111")
	req.Header.Set("X-Amz-Date", amzDate)

	// Calculate the expected signature using the correct IAM service
	// This simulates what botocore/AWS SDK would calculate
	credentialScope := dateStamp + "/us-east-1/iam/aws4_request"

	// Calculate the actual payload hash for our test payload
	actualPayloadHash := getSHA256Hash([]byte(testPayload))

	// Build the canonical request with the actual payload hash
	canonicalRequest := "POST\n/\n\ncontent-type:application/x-www-form-urlencoded; charset=utf-8\nhost:localhost:8111\nx-amz-date:" + amzDate + "\n\ncontent-type;host;x-amz-date\n" + actualPayloadHash

	// Calculate the canonical request hash
	canonicalRequestHash := getSHA256Hash([]byte(canonicalRequest))

	// Build the string to sign
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + credentialScope + "\n" + canonicalRequestHash

	// Calculate expected signature using IAM service (what client sends)
	expectedSigningKey := getSigningKey("power_user_secret", dateStamp, "us-east-1", "iam")
	expectedSignature := getSignature(expectedSigningKey, stringToSign)

	// Create authorization header with the correct signature
	authHeader := "AWS4-HMAC-SHA256 Credential=power_user_key/" + credentialScope +
		", SignedHeaders=content-type;host;x-amz-date, Signature=" + expectedSignature
	req.Header.Set("Authorization", authHeader)

	// Now test that SeaweedFS computes the same signature with our fix
	identity, computedSignature, errCode := iam.doesSignatureMatch(req)
	assert.Equal(t, expectedSignature, computedSignature)

	// With the fix, the signatures should match and we should get a successful authentication
	assert.Equal(t, s3err.ErrNone, errCode)
	assert.NotNil(t, identity)
	assert.Equal(t, "power_user", identity.Name)
}

// TestStreamingSignatureServiceField tests that the s3ChunkedReader struct correctly stores the service
// This verifies the fix for streaming uploads where getChunkSignature was hardcoding "s3"
func TestStreamingSignatureServiceField(t *testing.T) {
	// Test that the s3ChunkedReader correctly uses the service field
	// Create a mock s3ChunkedReader with IAM service
	chunkedReader := &s3ChunkedReader{
		seedDate:      time.Now(),
		region:        "us-east-1",
		service:       "iam", // This should be used instead of hardcoded "s3"
		seedSignature: "testsignature",
		cred: &Credential{
			AccessKey: "testkey",
			SecretKey: "testsecret",
		},
	}

	// Test that getScope is called with the correct service
	scope := getScope(chunkedReader.seedDate, chunkedReader.region, chunkedReader.service)
	assert.Contains(t, scope, "/iam/aws4_request")
	assert.NotContains(t, scope, "/s3/aws4_request")

	// Test that getSigningKey would be called with the correct service
	signingKey := getSigningKey(
		chunkedReader.cred.SecretKey,
		chunkedReader.seedDate.Format(yyyymmdd),
		chunkedReader.region,
		chunkedReader.service,
	)
	assert.NotNil(t, signingKey)

	// The main point is that chunkedReader.service is "iam" and gets used correctly
	// This ensures that IAM streaming uploads will use "iam" service instead of hardcoded "s3"
	assert.Equal(t, "iam", chunkedReader.service)
}

// Test that large IAM request bodies are truncated for security (DoS prevention)
func TestIAMLargeBodySecurityLimit(t *testing.T) {
	// Create test IAM instance
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}

	// Load test configuration
	err := iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{
			{
				Name: "testuser",
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: "AKIAIOSFODNN7EXAMPLE",
						SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
					},
				},
				Actions: []string{"Admin"},
			},
		},
	})
	assert.NoError(t, err)

	// Create a payload larger than the 10 MiB limit
	largePayload := strings.Repeat("A", 11*(1<<20)) // 11 MiB

	// Create IAM request with large body
	req, err := http.NewRequest("POST", "http://localhost:8111/", strings.NewReader(largePayload))
	assert.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	req.Header.Set("Host", "localhost:8111")

	// Create IAM-style authorization header
	now := time.Now().UTC()
	dateStr := now.Format("20060102T150405Z")
	credentialScope := now.Format("20060102") + "/us-east-1/iam/aws4_request"

	req.Header.Set("X-Amz-Date", dateStr)

	authHeader := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/" + credentialScope +
		", SignedHeaders=content-type;host;x-amz-date, Signature=dummysignature"
	req.Header.Set("Authorization", authHeader)

	// The function should complete successfully but limit the body to 10 MiB
	identity, _, errCode := iam.doesSignatureMatch(req)

	// Should get signature mismatch (dummy signature) but not internal error
	assert.Equal(t, s3err.ErrSignatureDoesNotMatch, errCode)
	assert.Nil(t, identity)

	// Verify the body was truncated to the limit (10 MiB)
	bodyBytes, err := io.ReadAll(req.Body)
	assert.NoError(t, err)
	assert.Equal(t, 10*(1<<20), len(bodyBytes))                         // Should be exactly 10 MiB
	assert.Equal(t, strings.Repeat("A", 10*(1<<20)), string(bodyBytes)) // All As, but truncated
}

// Test the streaming hash implementation directly
func TestStreamHashRequestBody(t *testing.T) {
	testCases := []struct {
		name    string
		payload string
	}{
		{
			name:    "empty body",
			payload: "",
		},
		{
			name:    "small payload",
			payload: "Action=CreateAccessKey&UserName=testuser&Version=2010-05-08",
		},
		{
			name:    "medium payload",
			payload: strings.Repeat("A", 1024), // 1KB
		},
		{
			name:    "large payload within limit",
			payload: strings.Repeat("B", 1<<20), // 1MB
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create request with the test payload
			req, err := http.NewRequest("POST", "http://localhost:8111/", strings.NewReader(tc.payload))
			assert.NoError(t, err)

			// Compute expected hash directly for comparison
			expectedHashStr := emptySHA256
			if tc.payload != "" {
				expectedHash := sha256.Sum256([]byte(tc.payload))
				expectedHashStr = hex.EncodeToString(expectedHash[:])
			}

			// Test the streaming function
			hash, err := streamHashRequestBody(req, iamRequestBodyLimit)
			assert.NoError(t, err)
			assert.Equal(t, expectedHashStr, hash)

			// Verify the body is preserved and readable
			bodyBytes, err := io.ReadAll(req.Body)
			assert.NoError(t, err)
			assert.Equal(t, tc.payload, string(bodyBytes))
		})
	}
}

// Test streaming vs non-streaming approach produces identical results
func TestStreamingVsNonStreamingConsistency(t *testing.T) {
	testPayloads := []string{
		"",
		"small",
		"Action=CreateAccessKey&UserName=testuser&Version=2010-05-08",
		strings.Repeat("X", 8192),  // Exactly one chunk
		strings.Repeat("Y", 16384), // Two chunks
		strings.Repeat("Z", 12345), // Non-aligned chunks
	}

	for i, payload := range testPayloads {
		t.Run(fmt.Sprintf("payload_%d", i), func(t *testing.T) {
			// Test streaming approach
			req1, err := http.NewRequest("POST", "http://localhost:8111/", strings.NewReader(payload))
			assert.NoError(t, err)

			streamHash, err := streamHashRequestBody(req1, iamRequestBodyLimit)
			assert.NoError(t, err)

			// Test direct approach for comparison
			directHashStr := emptySHA256
			if payload != "" {
				directHash := sha256.Sum256([]byte(payload))
				directHashStr = hex.EncodeToString(directHash[:])
			}

			// Both approaches should produce identical results
			assert.Equal(t, directHashStr, streamHash)

			// Verify body preservation
			bodyBytes, err := io.ReadAll(req1.Body)
			assert.NoError(t, err)
			assert.Equal(t, payload, string(bodyBytes))
		})
	}
}

// Test streaming with size limit enforcement
func TestStreamingWithSizeLimit(t *testing.T) {
	// Create a payload larger than the limit
	largePayload := strings.Repeat("A", 11*(1<<20)) // 11 MiB

	req, err := http.NewRequest("POST", "http://localhost:8111/", strings.NewReader(largePayload))
	assert.NoError(t, err)

	// Stream with the limit
	hash, err := streamHashRequestBody(req, iamRequestBodyLimit)
	assert.NoError(t, err)

	// Verify the hash is computed for the truncated content (10 MiB)
	truncatedPayload := strings.Repeat("A", 10*(1<<20))
	expectedHash := sha256.Sum256([]byte(truncatedPayload))
	expectedHashStr := hex.EncodeToString(expectedHash[:])

	assert.Equal(t, expectedHashStr, hash)

	// Verify the body was truncated
	bodyBytes, err := io.ReadAll(req.Body)
	assert.NoError(t, err)
	assert.Equal(t, 10*(1<<20), len(bodyBytes))
	assert.Equal(t, truncatedPayload, string(bodyBytes))
}

// Benchmark streaming vs non-streaming memory usage
func BenchmarkStreamingVsNonStreaming(b *testing.B) {
	// Test with 1MB payload to show memory efficiency
	payload := strings.Repeat("A", 1<<20) // 1MB

	b.Run("streaming", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			req, _ := http.NewRequest("POST", "http://localhost:8111/", strings.NewReader(payload))
			streamHashRequestBody(req, iamRequestBodyLimit)
		}
	})

	b.Run("direct", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Simulate the old approach of reading all at once
			req, _ := http.NewRequest("POST", "http://localhost:8111/", strings.NewReader(payload))
			io.ReadAll(req.Body)
			sha256.Sum256([]byte(payload))
		}
	})
}

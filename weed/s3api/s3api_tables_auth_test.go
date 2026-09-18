package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/seaweedfs/seaweedfs/weed/s3api/iceberg"
	"github.com/seaweedfs/seaweedfs/weed/s3api/lance"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupDefaultAllowAuthServer builds the wiring NewS3ApiServer produces when
// S3 credentials are configured but no -iam.config file was given: auth is
// enforced while the IAM policy engine still defaults to allow.
func setupDefaultAllowAuthServer(t *testing.T) *S3ApiServer {
	t.Helper()
	s3a := setupRoutingTestServer(t)
	manager, err := loadIAMManagerFromConfig("",
		func() string { return "localhost:8888" },
		func() string { return "test-signing-key" })
	require.NoError(t, err)
	require.True(t, manager.DefaultAllow())
	s3a.iam.iamIntegration = NewS3IAMIntegration(manager, "")
	return s3a
}

func forgedS3TablesRequest(t *testing.T, method, target string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, "http://localhost"+target, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AAAA/20260917/us-east-1/s3tables/aws4_request, SignedHeaders=host, Signature=00")
	return req
}

func TestS3TablesAuthRejectsFailedSignature(t *testing.T) {
	s3a := setupDefaultAllowAuthServer(t)

	req := forgedS3TablesRequest(t, http.MethodGet, "/buckets")
	req.Header.Set(s3_constants.AmzAccountId, "admin")

	reached := false
	rr := httptest.NewRecorder()
	s3a.authenticateS3Tables(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})(rr, req)

	assert.False(t, reached, "failed authentication must not reach the S3 Tables handler")
	assert.GreaterOrEqual(t, rr.Code, http.StatusBadRequest)
	assert.Empty(t, req.Header.Get(s3_constants.AmzAccountId), "client-supplied account header must not survive authentication")
}

func TestS3TablesAuthControlDataPlane(t *testing.T) {
	s3a := setupDefaultAllowAuthServer(t)

	req := forgedS3TablesRequest(t, http.MethodGet, "/buckets")
	reached := false
	rr := httptest.NewRecorder()
	s3a.iam.Auth(func(w http.ResponseWriter, r *http.Request) {
		reached = true
	}, s3_constants.ACTION_READ)(rr, req)

	assert.False(t, reached, "control: data plane must reject the same forged request")
	assert.GreaterOrEqual(t, rr.Code, http.StatusBadRequest)
}

func TestS3TablesAuthOpenWhenAuthDisabled(t *testing.T) {
	s3a := setupDefaultAllowAuthServer(t)
	s3a.iam.isAuthEnabled = false

	req := forgedS3TablesRequest(t, http.MethodGet, "/buckets")
	reached := false
	rr := httptest.NewRecorder()
	s3a.authenticateS3Tables(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})(rr, req)

	assert.True(t, reached, "zero-config gateway keeps serving unauthenticated requests")
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestS3TablesAuthSignedRequestPassess(t *testing.T) {
	s3a := setupDefaultAllowAuthServer(t)

	req, err := http.NewRequest(http.MethodGet, "http://localhost/buckets", nil)
	require.NoError(t, err)
	creds := credentials.NewStaticCredentials(routingTestAccessKey, routingTestSecretKey, "")
	_, err = v4.NewSigner(creds).Sign(req, strings.NewReader(""), "s3tables", "us-east-1", time.Now())
	require.NoError(t, err)

	reached := false
	rr := httptest.NewRecorder()
	s3a.authenticateS3Tables(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})(rr, req)

	assert.True(t, reached, "properly signed request must still pass; got %d %s", rr.Code, rr.Body.String())
}

func TestSignedAccountHeaderDoesNotReachHandler(t *testing.T) {
	s3a := setupDefaultAllowAuthServer(t)

	req, err := http.NewRequest(http.MethodGet, "http://localhost/buckets", nil)
	require.NoError(t, err)
	req.Header.Set(s3_constants.AmzAccountId, "admin")
	signRoutingTestRequest(t, req, "", "s3tables")
	require.Contains(t, req.Header.Get("Authorization"), "s3-account-id", "the header must be covered by the signature")

	reached := false
	rr := httptest.NewRecorder()
	s3a.authenticateS3Tables(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})(rr, req)

	assert.True(t, reached, "a signature covering s3-account-id must still verify; got %d %s", rr.Code, rr.Body.String())
	assert.Empty(t, req.Header.Get(s3_constants.AmzAccountId), "the signed-in header value must not survive authentication")
}

func TestIcebergAuthRejectsFailedSignature(t *testing.T) {
	s3a := setupDefaultAllowAuthServer(t)
	server := iceberg.NewServer(nil, s3a)

	req := forgedS3TablesRequest(t, http.MethodGet, "/v1/namespaces")
	reached := false
	rr := httptest.NewRecorder()
	server.Auth(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})(rr, req)

	assert.False(t, reached, "failed authentication must not reach the Iceberg handler")
	assert.NotEqual(t, http.StatusOK, rr.Code)
}

func TestLanceAuthRejectsFailedSignature(t *testing.T) {
	s3a := setupDefaultAllowAuthServer(t)
	server := lance.NewServer(nil, s3a)

	req := forgedS3TablesRequest(t, http.MethodGet, "/v1/namespace/list")
	reached := false
	rr := httptest.NewRecorder()
	server.Auth(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})(rr, req)

	assert.False(t, reached, "failed authentication must not reach the Lance handler")
	assert.NotEqual(t, http.StatusOK, rr.Code)
}

func TestAuthSignatureOnlyScrubsAccountHeader(t *testing.T) {
	s3a := setupDefaultAllowAuthServer(t)

	req := forgedS3TablesRequest(t, http.MethodGet, "/buckets")
	req.Header.Set(s3_constants.AmzAccountId, "admin")

	_, errCode := s3a.iam.AuthSignatureOnly(req)
	assert.NotEqual(t, s3err.ErrNone, errCode)
	assert.Empty(t, req.Header.Get(s3_constants.AmzAccountId))
}

package s3api

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// The key recorded for a request is the key of the credential that verified,
// whatever else the request carries. A request names its key in several
// places (SigV4 header, X-Amz-Credential, SigV2 header, AWSAccessKeyId);
// getRequestAuthType picks one and that verifier records it, so a second,
// unverified key in another place never reaches the audit log or a vended
// session.

func authAndRecordedKey(t *testing.T, iam *IdentityAccessManagement, r *http.Request) (s3err.ErrorCode, string) {
	t.Helper()
	r = s3_constants.EnsureIdentityHolder(r)
	_, errCode, _ := iam.authenticateRequestInternal(r)
	return errCode, s3_constants.GetAccessKeyIdFromContext(r)
}

func credOf(key, secret string) *Credential { return &Credential{AccessKey: key, SecretKey: secret} }

func TestRecordedKeyIsTheVerifiedKeyForSigV4Header(t *testing.T) {
	iam := newSharedIdentityIAM()
	r, err := newTestRequest(http.MethodGet, "https://example.com/bucket/f.parquet", 0, nil)
	require.NoError(t, err)
	require.NoError(t, signRequestV4(r, sharedKeyA, sharedSecretA))
	errCode, got := authAndRecordedKey(t, iam, r)
	require.Equal(t, s3err.ErrNone, errCode)
	assert.Equal(t, sharedKeyA, got)
}

// SigV2 header signed with A, plus X-Amz-Credential naming B or garbage: the
// request authenticates as A (SigV2 is checked first) and A is recorded.
func TestRecordedKeyIgnoresAJunkCredentialQueryOnASigV2Request(t *testing.T) {
	iam := newSharedIdentityIAM()
	for _, junk := range []string{sharedKeyB + "/20261016/us-east-1/s3/aws4_request", "GARBAGE"} {
		r, err := http.NewRequest(http.MethodGet, "http://example.com/bucket/f.parquet?X-Amz-Credential="+url.QueryEscape(junk), nil)
		require.NoError(t, err)
		r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
		r.Header.Set("Authorization", signatureV2(credOf(sharedKeyA, sharedSecretA), r.Method, r.URL.EscapedPath(), r.URL.Query().Encode(), r.Header))
		errCode, got := authAndRecordedKey(t, iam, r)
		require.Equal(t, s3err.ErrNone, errCode, "the SigV2 signature with A verifies (junk %q)", junk)
		assert.Equal(t, sharedKeyA, got, "the key that verified, not the junk %q", junk)
	}
}

// SigV2 presigned with A, plus a junk SigV4 Authorization naming B: the
// presigned A verifies and A is recorded.
func TestRecordedKeyIgnoresAJunkSigV4HeaderOnASigV2PresignedRequest(t *testing.T) {
	iam := newSharedIdentityIAM()
	expires := strconv.FormatInt(time.Now().Add(10*time.Minute).Unix(), 10)
	q := url.Values{"AWSAccessKeyId": {sharedKeyA}, "Expires": {expires}}
	r, err := http.NewRequest(http.MethodGet, "http://example.com/bucket/f.parquet?"+q.Encode(), nil)
	require.NoError(t, err)
	sig := preSignatureV2(credOf(sharedKeyA, sharedSecretA), r.Method, r.URL.EscapedPath(), r.URL.Query().Encode(), r.Header, expires)
	q.Set("Signature", sig)
	r.URL.RawQuery = q.Encode()
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+sharedKeyB+"/20261016/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00")

	errCode, got := authAndRecordedKey(t, iam, r)
	require.Equal(t, s3err.ErrNone, errCode, "the SigV2 presigned A verifies (SigV2 is dispatched first)")
	assert.Equal(t, sharedKeyA, got, "the presigned key A is recorded, never the unverified B")
}

// A refused signature records nothing, even when the key exists.
func TestRecordedKeyIsEmptyWhenTheSignatureFails(t *testing.T) {
	iam := newSharedIdentityIAM()
	r, err := newTestRequest(http.MethodGet, "https://example.com/bucket/f.parquet", 0, nil)
	require.NoError(t, err)
	require.NoError(t, signRequestV4(r, sharedKeyA, "not-the-secret"))
	errCode, got := authAndRecordedKey(t, iam, r)
	require.NotEqual(t, s3err.ErrNone, errCode)
	assert.Equal(t, "", got)

	r2 := httptest.NewRequest(http.MethodGet, "http://example.com/bucket/f.parquet", nil)
	r2.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	r2.Header.Set("Authorization", "AWS "+sharedKeyB+":bm90LWEtc2lnbmF0dXJl")
	errCode, got = authAndRecordedKey(t, iam, r2)
	require.NotEqual(t, s3err.ErrNone, errCode)
	assert.Equal(t, "", got)
}

// A streaming (aws-chunked) upload records the key of its seed signature.
func TestRecordedKeyForAStreamingUpload(t *testing.T) {
	iam := newSharedIdentityIAM()
	now := time.Now().UTC()
	amzDate, dateStamp := now.Format(iso8601Format), now.Format(yyyymmdd)
	scope := dateStamp + "/us-east-1/s3/aws4_request"
	hashedPayload := "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	canonicalHeaders := "content-encoding:aws-chunked\n" + "host:s3.amazonaws.com\n" +
		"x-amz-content-sha256:" + hashedPayload + "\n" + "x-amz-date:" + amzDate + "\n" + "x-amz-decoded-content-length:4\n"
	signedHeaders := "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length"
	canonicalRequest := "PUT\n/bucket/ns/t/data/f.parquet\n\n" + canonicalHeaders + "\n" + signedHeaders + "\n" + hashedPayload
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + getSHA256Hash([]byte(canonicalRequest))
	seed := getSignature(getSigningKey(sharedSecretB, dateStamp, "us-east-1", "s3"), stringToSign)

	r, err := http.NewRequest(http.MethodPut, "http://s3.amazonaws.com/bucket/ns/t/data/f.parquet", bytes.NewReader([]byte("ignored")))
	require.NoError(t, err)
	r.Header.Set("Host", "s3.amazonaws.com")
	r.Header.Set("x-amz-date", amzDate)
	r.Header.Set("x-amz-content-sha256", hashedPayload)
	r.Header.Set("Content-Encoding", "aws-chunked")
	r.Header.Set("x-amz-decoded-content-length", "4")
	r.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", sharedKeyB, scope, signedHeaders, seed))

	errCode, got := authAndRecordedKey(t, iam, r)
	require.Equal(t, s3err.ErrNone, errCode)
	assert.Equal(t, sharedKeyB, got)
}

// An STS session's request records the session's temporary key.
func TestRecordedKeyForAnSTSSession(t *testing.T) {
	stsService, config := setupTestSTSService(t)
	iam := NewIdentityAccessManagementWithStore(&S3ApiServerOption{}, nil, "memory")
	iam.SetIAMIntegration(&S3IAMIntegration{stsService: stsService, enabled: true})
	sessionInfo, sessionToken := issueTestSession(t, stsService, config)

	r, err := newTestRequest(http.MethodGet, "https://example.com/reports/data.csv", 0, nil)
	require.NoError(t, err)
	r.Header.Set("X-Amz-Security-Token", sessionToken)
	require.NoError(t, signRequestV4(r, sessionInfo.Credentials.AccessKeyId, sessionInfo.Credentials.SecretAccessKey))
	outer := s3_constants.EnsureIdentityHolder(r)
	identity, errCode := iam.reqSignatureV4Verify(outer)
	require.Equal(t, s3err.ErrNone, errCode)
	iam.handleAuthResult(httptest.NewRecorder(), outer, identity, s3err.ErrNone, func(http.ResponseWriter, *http.Request) {})
	assert.Equal(t, sessionInfo.Credentials.AccessKeyId, s3err.GetAccessLog(outer, http.StatusOK, s3err.ErrNone).AccessKeyId)
}

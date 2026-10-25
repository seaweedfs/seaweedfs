package s3api

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// signedPostFormV2 builds a multipart POST Object request whose policy is
// signed with SigV2 under postAuthzAccessKey and secret.
func signedPostFormV2(t *testing.T, key, secret string) *http.Request {
	t.Helper()
	policyJSON := fmt.Sprintf(`{"expiration":"%s","conditions":[["eq","$bucket","%s"],["eq","$key","%s"]]}`,
		time.Now().UTC().Add(time.Hour).Format("2006-01-02T15:04:05.000Z"), postAuthzBucket, key)
	encodedPolicy := base64.StdEncoding.EncodeToString([]byte(policyJSON))

	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	for _, field := range [][2]string{{"bucket", postAuthzBucket}, {"key", key}, {"AWSAccessKeyId", postAuthzAccessKey},
		{"policy", encodedPolicy}, {"Signature", calculateSignatureV2(encodedPolicy, secret)}} {
		require.NoError(t, writer.WriteField(field[0], field[1]))
	}
	filePart, err := writer.CreateFormFile("file", "payload.txt")
	require.NoError(t, err)
	_, err = filePart.Write([]byte("payload"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	req := httptest.NewRequest(http.MethodPost, "/"+postAuthzBucket, &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	return mux.SetURLVars(req, map[string]string{"bucket": postAuthzBucket})
}

// A browser POST upload is verified in the handler from its form fields; the
// key of the verified form reaches the audit log like a signed PUT's. The
// bucket policy denies denied/ so the request ends before the filer.
func TestPostPolicyUploadRecordsTheSigningKey(t *testing.T) {
	s3a := newPostAuthzServer(t)
	for name, req := range map[string]*http.Request{
		"SigV4": newSignedPostRequest(t, "denied/secret.txt"),
		"SigV2": signedPostFormV2(t, "denied/secret.txt", postAuthzSecretKey),
	} {
		outer := s3_constants.EnsureIdentityHolder(req)
		rec := httptest.NewRecorder()
		s3a.PostPolicyBucketHandler(rec, outer)
		require.Equal(t, http.StatusForbidden, rec.Code, "%s: %s", name, rec.Body.String())
		assert.Equal(t, postAuthzAccessKey, s3err.GetAccessLog(outer, http.StatusForbidden, s3err.ErrAccessDenied).AccessKeyId, name)
	}

	// A form whose signature does not verify records nothing.
	outer := s3_constants.EnsureIdentityHolder(signedPostFormV2(t, "denied/secret.txt", "not-the-secret"))
	rec := httptest.NewRecorder()
	s3a.PostPolicyBucketHandler(rec, outer)
	require.Contains(t, rec.Body.String(), "SignatureDoesNotMatch")
	assert.Equal(t, "", s3_constants.GetAccessKeyIdFromContext(outer))
}

// A signature that verifies for one virtual-hosted bucket but is routed to
// another is refused after verification; the refusal still names the key.
func TestSignedVirtualHostRetargetRecordsTheSigningKey(t *testing.T) {
	iam := newVirtualHostTestIAM("s3.test")
	r, err := newTestRequest("GET", "http://alpha-bkt.s3.test/shared.txt", 0, nil)
	require.NoError(t, err)
	r.Header.Set("Host", "alpha-bkt.s3.test")
	signV4WithPath(r, "access_key_1", "secret_key_1", r.URL.Path)
	r.Host = "beta-bkt.s3.test"
	r.Header.Set("X-Forwarded-Host", "alpha-bkt.s3.test")
	r = mux.SetURLVars(r, map[string]string{"bucket": "beta-bkt", "object": "shared.txt"})
	r = s3_constants.EnsureIdentityHolder(r)

	_, _, errCode := iam.doesSignatureMatch(r)
	require.Equal(t, s3err.ErrAccessDenied, errCode)
	assert.Equal(t, "access_key_1", s3_constants.GetAccessKeyIdFromContext(r))
}

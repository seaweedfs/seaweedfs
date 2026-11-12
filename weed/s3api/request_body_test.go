package s3api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadRequestBodyLeavesOversizedBodyDrainable(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Body = io.NopCloser(strings.NewReader("abcdef"))

	body, err := readRequestBody(req, 3)
	remaining, readErr := io.ReadAll(req.Body)

	assert.ErrorIs(t, err, errRequestBodyTooLarge)
	assert.Nil(t, body)
	assert.NoError(t, readErr)
	assert.Equal(t, "ef", string(remaining))
}

func newSignedPayloadTestIAM(t *testing.T) *IdentityAccessManagement {
	iam := &IdentityAccessManagement{
		hashes:       make(map[string]*sync.Pool),
		hashCounters: make(map[string]*int32),
	}
	err := iam.loadS3ApiConfiguration(&iam_pb.S3ApiConfiguration{
		Identities: []*iam_pb.Identity{{
			Name:        "signer",
			Credentials: []*iam_pb.Credential{{AccessKey: "access_key_1", SecretKey: "secret_key_1"}},
			Actions:     []string{"Read", "Write"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return iam
}

func deleteObjectsBody(keys ...string) []byte {
	var b strings.Builder
	b.WriteString("<Delete>")
	for _, k := range keys {
		b.WriteString("<Object><Key>" + k + "</Key></Object>")
	}
	b.WriteString("</Delete>")
	return []byte(b.String())
}

func TestSignedRequestBodyIsBoundToPayloadHash(t *testing.T) {
	iam := newSignedPayloadTestIAM(t)
	approved := deleteObjectsBody("tables/a/own.txt")
	swapped := deleteObjectsBody("tables/b/victim.txt")

	newSwapped := func(body []byte) *http.Request {
		req := mustNewSignedRequest(http.MethodPost, "http://127.0.0.1:9000/warehouse?delete", int64(len(approved)), bytes.NewReader(approved), t)
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = int64(len(body))
		return req
	}

	t.Run("signed body is readable", func(t *testing.T) {
		req := mustNewSignedRequest(http.MethodPost, "http://127.0.0.1:9000/warehouse?delete", int64(len(approved)), bytes.NewReader(approved), t)
		_, errCode := iam.reqSignatureV4Verify(req)
		require.Equal(t, s3err.ErrNone, errCode)
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		assert.Equal(t, approved, body)
	})

	t.Run("replaced body is refused", func(t *testing.T) {
		req := newSwapped(swapped)
		_, errCode := iam.reqSignatureV4Verify(req)
		require.Equal(t, s3err.ErrNone, errCode)
		body, err := io.ReadAll(req.Body)
		assert.ErrorIs(t, err, errContentSha256Mismatch)
		assert.Empty(t, body)
	})

	t.Run("decoder that stops early sees no replaced bytes", func(t *testing.T) {
		padded := append(append([]byte{}, swapped...), bytes.Repeat([]byte(" "), 64<<10)...)
		req := newSwapped(padded)
		_, errCode := iam.reqSignatureV4Verify(req)
		require.Equal(t, s3err.ErrNone, errCode)
		var parsed DeleteObjectsRequest
		err := xml.NewDecoder(req.Body).Decode(&parsed)
		assert.ErrorIs(t, err, errContentSha256Mismatch)
		assert.Empty(t, parsed.Objects)
	})

	t.Run("emptied body is refused", func(t *testing.T) {
		req := newSwapped(nil)
		req.Body = http.NoBody
		_, errCode := iam.reqSignatureV4Verify(req)
		assert.Equal(t, s3err.ErrContentSHA256Mismatch, errCode)
	})

	t.Run("oversized body is refused before it is read", func(t *testing.T) {
		req := newSwapped(swapped)
		req.ContentLength = iamRequestBodyLimit + 1
		_, errCode := iam.reqSignatureV4Verify(req)
		require.Equal(t, s3err.ErrNone, errCode)
		_, err := io.ReadAll(req.Body)
		assert.ErrorIs(t, err, errRequestBodyTooLarge)
	})
}

func TestGetRequestDataReaderStreamsSignedUploads(t *testing.T) {
	iam := newSignedPayloadTestIAM(t)
	s3a := &S3ApiServer{iam: iam}
	signed := []byte("object data")
	req := mustNewSignedRequest(http.MethodPut, "http://127.0.0.1:9000/bucket/key", int64(len(signed)), bytes.NewReader(signed), t)
	raw := io.NopCloser(bytes.NewReader([]byte("other data!")))
	req.Body = raw

	_, errCode := iam.reqSignatureV4Verify(req)
	require.Equal(t, s3err.ErrNone, errCode)
	dataReader, errCode := getRequestDataReader(s3a, req)
	require.Equal(t, s3err.ErrNone, errCode)

	assert.Equal(t, raw, req.Body)
	_, err := io.ReadAll(dataReader)
	assert.ErrorContains(t, err, s3err.ErrMsgContentSha256Mismatch)
}

func TestDeleteObjectsRefusesReplacedBody(t *testing.T) {
	iam := newSignedPayloadTestIAM(t)
	s3a := &S3ApiServer{iam: iam}
	approved := deleteObjectsBody("tables/a/own.txt")
	swapped := deleteObjectsBody("tables/b/victim.txt")
	req := mustNewSignedRequest(http.MethodPost, "http://127.0.0.1:9000/warehouse?delete", int64(len(approved)), bytes.NewReader(approved), t)
	req.Body = io.NopCloser(bytes.NewReader(swapped))
	req.ContentLength = int64(len(swapped))
	_, errCode := iam.reqSignatureV4Verify(req)
	require.Equal(t, s3err.ErrNone, errCode)

	rec := httptest.NewRecorder()
	s3a.DeleteMultipleObjectsHandler(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "XAmzContentSHA256Mismatch")
}

func TestSignedRequestBodyIsBoundToContentMd5(t *testing.T) {
	approved := deleteObjectsBody("tables/a/own.txt")
	swapped := deleteObjectsBody("tables/b/victim.txt")
	const url = "http://127.0.0.1:9000/warehouse?delete"

	t.Run("v4 unsigned payload", func(t *testing.T) {
		iam := newSignedPayloadTestIAM(t)
		req := mustNewRequest(http.MethodPost, url, int64(len(approved)), bytes.NewReader(approved), t)
		req.Header.Set("X-Amz-Content-Sha256", unsignedPayload)
		require.NoError(t, signRequestV4(req, "access_key_1", "secret_key_1"))
		req.Body = io.NopCloser(bytes.NewReader(swapped))
		req.ContentLength = int64(len(swapped))

		_, errCode := iam.reqSignatureV4Verify(req)
		require.Equal(t, s3err.ErrNone, errCode)
		_, err := io.ReadAll(req.Body)
		assert.ErrorIs(t, err, errContentMd5Mismatch)
		assert.Equal(t, s3err.ErrBadDigest, requestBodyErrorCode(err, s3err.ErrInternalError))
	})

	t.Run("v2", func(t *testing.T) {
		iam := setupTestIAMForV2Auth()
		cred := iam.identities[0].Credentials[0]
		newV2Request := func(body []byte) *http.Request {
			req := httptest.NewRequest(http.MethodPost, url, bytes.NewReader(body))
			req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
			req.Header.Set("Content-Md5", getMD5HashBase64(approved))
			req.Header.Set("Authorization", signatureV2(cred, req.Method, req.URL.Path, req.URL.Query().Encode(), req.Header))
			return req
		}

		req := newV2Request(approved)
		_, errCode := iam.isReqAuthenticatedV2(req)
		require.Equal(t, s3err.ErrNone, errCode)
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		assert.Equal(t, approved, body)

		req = newV2Request(swapped)
		_, errCode = iam.isReqAuthenticatedV2(req)
		require.Equal(t, s3err.ErrNone, errCode)
		_, err = io.ReadAll(req.Body)
		assert.ErrorIs(t, err, errContentMd5Mismatch)
	})

	t.Run("malformed digest", func(t *testing.T) {
		iam := newSignedPayloadTestIAM(t)
		req := mustNewRequest(http.MethodPost, url, int64(len(approved)), bytes.NewReader(approved), t)
		req.Header.Set("Content-Md5", "bm90LWFuLW1kNQ==")
		require.NoError(t, signRequestV4(req, "access_key_1", "secret_key_1"))
		_, errCode := iam.reqSignatureV4Verify(req)
		assert.Equal(t, s3err.ErrInvalidDigest, errCode)
	})
}

func TestGetRequestDataReaderVerifiesPresignedPayloadHash(t *testing.T) {
	iam := newSignedPayloadTestIAM(t)
	s3a := &S3ApiServer{iam: iam}
	signed := []byte("object data")
	sum := sha256.Sum256(signed)
	req := mustNewRequest(http.MethodPut, "http://127.0.0.1:9000/bucket/key?X-Amz-Content-Sha256="+hex.EncodeToString(sum[:]), int64(len(signed)), bytes.NewReader(signed), t)
	req.Header.Del("X-Amz-Content-Sha256")
	req.Header.Del("Content-Md5")
	require.NoError(t, preSignV4(iam, req, "access_key_1", "secret_key_1", 600))
	req.Body = io.NopCloser(bytes.NewReader([]byte("other data!")))

	_, errCode := iam.reqSignatureV4Verify(req)
	require.Equal(t, s3err.ErrNone, errCode)
	dataReader, errCode := getRequestDataReader(s3a, req)
	require.Equal(t, s3err.ErrNone, errCode)
	_, err := io.ReadAll(dataReader)
	assert.ErrorContains(t, err, s3err.ErrMsgContentSha256Mismatch)
}

func TestPrebufferedSignedBodyChecksContentMd5(t *testing.T) {
	iam := newSignedPayloadTestIAM(t)
	newIAMRequest := func(contentMd5 string) *http.Request {
		body := []byte("Action=ListUsers&Version=2010-05-08")
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:9000/", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Content-Md5", contentMd5)
		now := time.Now().UTC()
		req.Header.Set("X-Amz-Date", now.Format(iso8601Format))
		scope := now.Format(yyyymmdd) + "/us-east-1/iam/aws4_request"
		signed := http.Header{
			"content-md5":  {contentMd5},
			"content-type": {req.Header.Get("Content-Type")},
			"host":         {req.Host},
			"x-amz-date":   {req.Header.Get("X-Amz-Date")},
		}
		sum := sha256.Sum256(body)
		canonical := getCanonicalRequest(signed, hex.EncodeToString(sum[:]), "", "/", http.MethodPost)
		signature := getSignature(getSigningKey("secret_key_1", now.Format(yyyymmdd), "us-east-1", "iam"), getStringToSign(canonical, now, scope))
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=access_key_1/"+scope+", SignedHeaders=content-md5;content-type;host;x-amz-date, Signature="+signature)
		return req
	}

	req := newIAMRequest(getMD5HashBase64([]byte("Action=ListUsers&Version=2010-05-08")))
	_, errCode := iam.reqSignatureV4Verify(req)
	require.Equal(t, s3err.ErrNone, errCode)
	_, err := io.ReadAll(req.Body)
	assert.NoError(t, err)

	req = newIAMRequest(getMD5HashBase64([]byte("other")))
	_, errCode = iam.reqSignatureV4Verify(req)
	require.Equal(t, s3err.ErrNone, errCode)
	_, err = io.ReadAll(req.Body)
	assert.ErrorIs(t, err, errContentMd5Mismatch)
}

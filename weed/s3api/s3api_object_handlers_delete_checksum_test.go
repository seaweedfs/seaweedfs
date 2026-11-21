package s3api

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/stretchr/testify/require"
)

type deleteChecksumTestEnv struct {
	filer   *ambiguousPutFiler
	s3a     *S3ApiServer
	handler http.HandlerFunc
}

func newDeleteChecksumTestEnv(t *testing.T, withIAM bool) *deleteChecksumTestEnv {
	t.Helper()
	util_http.InitGlobalHttpClient()

	volume := startFakeVolumeServer(t)
	filer := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	filerAddr := startFakeFiler(t, filer)

	s3a := newPutTestServer(t, filerAddr)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("warehouse", &BucketConfig{Name: "warehouse"})

	var handler http.HandlerFunc
	if withIAM {
		s3a.iam = newSignedPayloadTestIAM(t)
		handler = s3a.iam.Auth(s3a.DeleteMultipleObjectsHandler, s3_constants.ACTION_WRITE)
	} else {
		s3a.iam = &IdentityAccessManagement{}
		handler = s3a.DeleteMultipleObjectsHandler
	}

	return &deleteChecksumTestEnv{
		filer:   filer,
		s3a:     s3a,
		handler: handler,
	}
}

func (e *deleteChecksumTestEnv) seed(key string) {
	e.filer.mu.Lock()
	defer e.filer.mu.Unlock()
	e.filer.entries["/buckets/warehouse/"+key] = &filer_pb.Entry{
		Name:    key,
		Content: []byte("important-data"),
		Attributes: &filer_pb.FuseAttributes{
			FileSize: uint64(len("important-data")),
		},
	}
}

func (e *deleteChecksumTestEnv) exists(key string) bool {
	e.filer.mu.Lock()
	defer e.filer.mu.Unlock()
	_, ok := e.filer.entries["/buckets/warehouse/"+key]
	return ok
}

// newDeleteChecksumRequest builds a POST request without calling MD5 or setting
// an implicit Content-Md5 header, keeping flexible-checksum and FIPS tests isolated.
func newDeleteChecksumRequest(t *testing.T, urlStr string, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, urlStr, bytes.NewReader(body))
	require.NoError(t, err)
	req.ContentLength = int64(len(body))
	req.Header.Set("X-Amz-Content-Sha256", getSHA256Hash(body))
	return req
}

func computeChecksumBase64(alg ChecksumAlgorithm, body []byte) string {
	h := getCheckSumWriter(alg)
	h.Write(body)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func tamperedChecksumBase64(alg ChecksumAlgorithm, body []byte) string {
	h := getCheckSumWriter(alg)
	h.Write(body)
	sum := h.Sum(nil)
	sum[0] ^= 0xff
	return base64.StdEncoding.EncodeToString(sum)
}

func TestDeleteObjectsChecksumAllAlgorithmsAuth(t *testing.T) {
	env := newDeleteChecksumTestEnv(t, true)

	algs := []struct {
		name   string
		alg    ChecksumAlgorithm
		header string
	}{
		{"CRC32", ChecksumAlgorithmCRC32, s3_constants.AmzChecksumCRC32},
		{"CRC32C", ChecksumAlgorithmCRC32C, s3_constants.AmzChecksumCRC32C},
		{"CRC64NVME", ChecksumAlgorithmCRC64NVMe, s3_constants.AmzChecksumCRC64NVME},
		{"SHA1", ChecksumAlgorithmSHA1, s3_constants.AmzChecksumSHA1},
		{"SHA256", ChecksumAlgorithmSHA256, s3_constants.AmzChecksumSHA256},
	}

	for _, tc := range algs {
		t.Run(tc.name+"/valid", func(t *testing.T) {
			key := "valid-" + tc.name + ".txt"
			env.seed(key)
			body := deleteObjectsBody(key)
			req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
			req.Header.Set(s3_constants.AmzSdkChecksumAlgorithm, tc.name)
			req.Header.Set(tc.header, computeChecksumBase64(tc.alg, body))
			require.NoError(t, signRequestV4(req, "access_key_1", "secret_key_1"))
			req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
			rec := httptest.NewRecorder()
			env.handler(rec, req)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.False(t, env.exists(key), "object must be deleted on valid checksum")
		})

		t.Run(tc.name+"/tampered_with_algorithm_header", func(t *testing.T) {
			key := "tampered-alg-" + tc.name + ".txt"
			env.seed(key)
			body := deleteObjectsBody(key)
			req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
			req.Header.Set(s3_constants.AmzSdkChecksumAlgorithm, tc.name)
			req.Header.Set(tc.header, tamperedChecksumBase64(tc.alg, body))
			require.NoError(t, signRequestV4(req, "access_key_1", "secret_key_1"))
			req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
			rec := httptest.NewRecorder()
			env.handler(rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), "<Code>BadDigest</Code>")
			require.True(t, env.exists(key), "object must survive tampered checksum")
		})

		t.Run(tc.name+"/tampered_value_header_only", func(t *testing.T) {
			key := "tampered-val-" + tc.name + ".txt"
			env.seed(key)
			body := deleteObjectsBody(key)
			req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
			req.Header.Set(tc.header, tamperedChecksumBase64(tc.alg, body))
			require.NoError(t, signRequestV4(req, "access_key_1", "secret_key_1"))
			req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
			rec := httptest.NewRecorder()
			env.handler(rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), "<Code>BadDigest</Code>")
			require.True(t, env.exists(key), "object must survive tampered checksum")
		})

		t.Run(tc.name+"/tampered_unsigned_payload", func(t *testing.T) {
			key := "tampered-unsigned-" + tc.name + ".txt"
			env.seed(key)
			body := deleteObjectsBody(key)
			req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
			req.Header.Set("X-Amz-Content-Sha256", unsignedPayload)
			req.Header.Set(s3_constants.AmzSdkChecksumAlgorithm, tc.name)
			req.Header.Set(tc.header, tamperedChecksumBase64(tc.alg, body))
			require.NoError(t, signRequestV4(req, "access_key_1", "secret_key_1"))
			req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
			rec := httptest.NewRecorder()
			env.handler(rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), "<Code>BadDigest</Code>")
			require.True(t, env.exists(key), "object must survive tampered checksum with UNSIGNED-PAYLOAD")
		})
	}
}

func TestDeleteObjectsChecksumValidMD5WithTamperedCRC32(t *testing.T) {
	env := newDeleteChecksumTestEnv(t, true)
	key := "valid-md5-tampered-crc32.txt"
	env.seed(key)
	body := deleteObjectsBody(key)

	req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
	req.Header.Set("Content-Md5", getMD5HashBase64(body))
	req.Header.Set(s3_constants.AmzChecksumCRC32, tamperedChecksumBase64(ChecksumAlgorithmCRC32, body))
	require.NoError(t, signRequestV4(req, "access_key_1", "secret_key_1"))
	req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
	rec := httptest.NewRecorder()
	env.handler(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Code>BadDigest</Code>")
	require.True(t, env.exists(key), "object must survive when CRC32 fails despite valid Content-MD5")
}

func TestDeleteObjectsChecksumInvalidHeaders(t *testing.T) {
	env := newDeleteChecksumTestEnv(t, true)

	cases := []struct {
		name     string
		wantCode string
		prepare  func(req *http.Request, body []byte)
	}{
		{
			name:     "non_base64_crc32",
			wantCode: "InvalidRequest",
			prepare: func(req *http.Request, _ []byte) {
				req.Header.Set(s3_constants.AmzChecksumCRC32, "!!!not-base64!!!")
			},
		},
		{
			name:     "wrong_byte_length",
			wantCode: "InvalidRequest",
			prepare: func(req *http.Request, _ []byte) {
				req.Header.Set(s3_constants.AmzChecksumCRC32, "AAAA")
			},
		},
		{
			name:     "non_canonical_trailing_pad_bits",
			wantCode: "InvalidRequest",
			prepare: func(req *http.Request, _ []byte) {
				req.Header.Set(s3_constants.AmzChecksumCRC32, "AAAAAB==")
			},
		},
		{
			name:     "empty_crc32",
			wantCode: "InvalidRequest",
			prepare: func(req *http.Request, _ []byte) {
				req.Header.Set(s3_constants.AmzChecksumCRC32, "")
			},
		},
		{
			name:     "duplicate_crc32_header",
			wantCode: "InvalidRequest",
			prepare: func(req *http.Request, body []byte) {
				v := computeChecksumBase64(ChecksumAlgorithmCRC32, body)
				req.Header.Add(s3_constants.AmzChecksumCRC32, v)
				req.Header.Add(s3_constants.AmzChecksumCRC32, v)
			},
		},
		{
			name:     "multiple_checksum_value_headers",
			wantCode: "InvalidRequest",
			prepare: func(req *http.Request, body []byte) {
				req.Header.Set(s3_constants.AmzChecksumCRC32, computeChecksumBase64(ChecksumAlgorithmCRC32, body))
				req.Header.Set(s3_constants.AmzChecksumSHA256, computeChecksumBase64(ChecksumAlgorithmSHA256, body))
			},
		},
		{
			name:     "unsupported_algorithm",
			wantCode: "InvalidRequest",
			prepare: func(req *http.Request, _ []byte) {
				req.Header.Set(s3_constants.AmzSdkChecksumAlgorithm, "MD5")
			},
		},
		{
			name:     "mismatched_algorithm_and_value",
			wantCode: "BadDigest",
			prepare: func(req *http.Request, body []byte) {
				req.Header.Set(s3_constants.AmzSdkChecksumAlgorithm, "CRC32")
				req.Header.Set(s3_constants.AmzChecksumSHA256, computeChecksumBase64(ChecksumAlgorithmSHA256, body))
			},
		},
		{
			name:     "declared_algorithm_without_value",
			wantCode: "InvalidRequest",
			prepare: func(req *http.Request, _ []byte) {
				req.Header.Set(s3_constants.AmzSdkChecksumAlgorithm, "CRC32")
			},
		},
		{
			name:     "conflicting_declared_algorithms",
			wantCode: "InvalidRequest",
			prepare: func(req *http.Request, body []byte) {
				req.Header.Set(s3_constants.AmzSdkChecksumAlgorithm, "CRC32")
				req.Header.Set(s3_constants.AmzChecksumAlgorithm, "SHA256")
				req.Header.Set(s3_constants.AmzChecksumCRC32, computeChecksumBase64(ChecksumAlgorithmCRC32, body))
			},
		},
		{
			name:     "trailer_crc32",
			wantCode: "InvalidRequest",
			prepare: func(req *http.Request, _ []byte) {
				req.Header.Set(s3_constants.AmzTrailer, "x-amz-checksum-crc32")
			},
		},
		{
			name:     "streaming_aws4_hmac_sha256_payload",
			wantCode: "InvalidRequest",
			prepare: func(req *http.Request, _ []byte) {
				req.Header.Set("X-Amz-Content-Sha256", streamingContentSHA256)
			},
		},
		{
			name:     "streaming_unsigned_payload_trailer",
			wantCode: "InvalidRequest",
			prepare: func(req *http.Request, _ []byte) {
				req.Header.Set("X-Amz-Content-Sha256", streamingUnsignedPayload)
			},
		},
		{
			name:     "content_encoding_aws_chunked",
			wantCode: "InvalidRequest",
			prepare: func(req *http.Request, _ []byte) {
				req.Header.Set("Content-Encoding", "aws-chunked")
			},
		},
		{
			name:     "duplicate_content_md5_auth",
			wantCode: "InvalidDigest",
			prepare: func(req *http.Request, body []byte) {
				v := getMD5HashBase64(body)
				req.Header.Add("Content-Md5", v)
				req.Header.Add("Content-Md5", v)
			},
		},
		{
			name:     "non_canonical_md5_padding_auth",
			wantCode: "InvalidDigest",
			prepare: func(req *http.Request, _ []byte) {
				// Decodes to 16 bytes, but re-encoding yields a different
				// string, so the signed value is not canonical base64.
				req.Header.Set("Content-Md5", "AAAAAAAAAAAAAAAAAAAAAB==")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := "invalid-" + tc.name + ".txt"
			env.seed(key)
			body := deleteObjectsBody(key)
			req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
			tc.prepare(req, body)
			require.NoError(t, signRequestV4(req, "access_key_1", "secret_key_1"))
			req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
			rec := httptest.NewRecorder()
			env.handler(rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), "<Code>"+tc.wantCode+"</Code>")
			require.True(t, env.exists(key), "object must survive invalid checksum headers")
		})
	}
}

func TestDeleteObjectsChecksumFIPS140(t *testing.T) {
	setFIPS140Enforced(t, true)
	authEnv := newDeleteChecksumTestEnv(t, true)
	noAuthEnv := newDeleteChecksumTestEnv(t, false)

	// Hardcoded valid 16-byte MD5 base64 (MD5("")) so this test never calls
	// crypto/md5 when executed under real GODEBUG=fips140=only. It does not
	// match the delete body, so accepting it shows the header goes unchecked.
	const validMD5Base64 = "1B2M2Y8AsgTpgAmY7PhCfg=="

	rejected := []struct {
		name    string
		withIAM bool
		prepare func(req *http.Request)
	}{
		{
			name:    "sha1_value_header",
			withIAM: true,
			prepare: func(req *http.Request) {
				req.Header.Set(s3_constants.AmzChecksumSHA1, "AAAAAAAAAAAAAAAAAAAAAAAAAAA=")
			},
		},
		{
			name:    "sha1_algorithm_header",
			withIAM: true,
			prepare: func(req *http.Request) {
				req.Header.Set(s3_constants.AmzSdkChecksumAlgorithm, "SHA1")
			},
		},
		{
			name:    "sha1_trailer_header",
			withIAM: true,
			prepare: func(req *http.Request) {
				req.Header.Set(s3_constants.AmzTrailer, "x-amz-checksum-sha1")
			},
		},
		{
			name:    "sha1_value_header_iam_disabled",
			withIAM: false,
			prepare: func(req *http.Request) {
				req.Header.Set(s3_constants.AmzChecksumSHA1, "AAAAAAAAAAAAAAAAAAAAAAAAAAA=")
			},
		},
	}

	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			env := authEnv
			if !tc.withIAM {
				env = noAuthEnv
			}
			key := "fips-reject-" + tc.name + ".txt"
			env.seed(key)
			body := deleteObjectsBody(key)
			req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
			tc.prepare(req)
			if tc.withIAM {
				require.NoError(t, signRequestV4(req, "access_key_1", "secret_key_1"))
			}
			req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
			rec := httptest.NewRecorder()
			env.handler(rec, req)

			require.Equal(t, http.StatusNotImplemented, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), "<Code>NotImplemented</Code>")
			require.True(t, env.exists(key), "object must survive FIPS-rejected checksum")
		})
	}

	// Content-MD5 cannot be checked in this mode: valid syntax is accepted
	// unchecked, even when the value does not match the body.
	unchecked := []struct {
		name    string
		withIAM bool
	}{
		{"content_md5_auth", true},
		{"content_md5_iam_disabled", false},
	}
	for _, tc := range unchecked {
		t.Run(tc.name, func(t *testing.T) {
			env := authEnv
			if !tc.withIAM {
				env = noAuthEnv
			}
			key := "fips-unchecked-" + tc.name + ".txt"
			env.seed(key)
			body := deleteObjectsBody(key)
			req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
			req.Header.Set("Content-Md5", validMD5Base64)
			if tc.withIAM {
				require.NoError(t, signRequestV4(req, "access_key_1", "secret_key_1"))
			}
			req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
			rec := httptest.NewRecorder()
			env.handler(rec, req)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.False(t, env.exists(key), "object must be deleted when Content-MD5 is unchecked under FIPS")
		})
	}

	// Syntax is still enforced in this mode.
	invalidMD5 := []struct {
		name  string
		value string
	}{
		{"malformed", "not-valid-md5"},
		{"non_canonical_padding", "AAAAAAAAAAAAAAAAAAAAAB=="},
	}
	for _, tc := range invalidMD5 {
		t.Run("invalid_content_md5_"+tc.name, func(t *testing.T) {
			key := "fips-invalid-md5-" + tc.name + ".txt"
			authEnv.seed(key)
			body := deleteObjectsBody(key)
			req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
			req.Header.Set("Content-Md5", tc.value)
			require.NoError(t, signRequestV4(req, "access_key_1", "secret_key_1"))
			req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
			rec := httptest.NewRecorder()
			authEnv.handler(rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), "<Code>InvalidDigest</Code>")
			require.True(t, authEnv.exists(key), "object must survive an invalid Content-MD5 under FIPS")
		})
	}

	allowed := []struct {
		name   string
		alg    ChecksumAlgorithm
		header string
	}{
		{"CRC32", ChecksumAlgorithmCRC32, s3_constants.AmzChecksumCRC32},
		{"CRC32C", ChecksumAlgorithmCRC32C, s3_constants.AmzChecksumCRC32C},
		{"CRC64NVME", ChecksumAlgorithmCRC64NVMe, s3_constants.AmzChecksumCRC64NVME},
		{"SHA256", ChecksumAlgorithmSHA256, s3_constants.AmzChecksumSHA256},
	}
	for _, tc := range allowed {
		t.Run("allowed_"+tc.name, func(t *testing.T) {
			key := "fips-allow-" + tc.name + ".txt"
			authEnv.seed(key)
			body := deleteObjectsBody(key)
			req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
			req.Header.Set(s3_constants.AmzSdkChecksumAlgorithm, tc.name)
			req.Header.Set(tc.header, computeChecksumBase64(tc.alg, body))
			require.NoError(t, signRequestV4(req, "access_key_1", "secret_key_1"))
			req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
			rec := httptest.NewRecorder()
			authEnv.handler(rec, req)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.False(t, authEnv.exists(key), "object must be deleted under FIPS for %s", tc.name)
		})

		t.Run("tampered_"+tc.name, func(t *testing.T) {
			key := "fips-tampered-" + tc.name + ".txt"
			authEnv.seed(key)
			body := deleteObjectsBody(key)
			req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
			req.Header.Set(s3_constants.AmzSdkChecksumAlgorithm, tc.name)
			req.Header.Set(tc.header, tamperedChecksumBase64(tc.alg, body))
			require.NoError(t, signRequestV4(req, "access_key_1", "secret_key_1"))
			req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
			rec := httptest.NewRecorder()
			authEnv.handler(rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), "<Code>BadDigest</Code>")
			require.True(t, authEnv.exists(key), "object must survive a tampered %s checksum under FIPS", tc.name)
		})
	}
}

func TestDeleteObjectsChecksumIAMDisabled(t *testing.T) {
	env := newDeleteChecksumTestEnv(t, false)

	t.Run("tampered_crc32", func(t *testing.T) {
		key := "noiam-tampered-crc32.txt"
		env.seed(key)
		body := deleteObjectsBody(key)
		req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
		req.Header.Set(s3_constants.AmzChecksumCRC32, tamperedChecksumBase64(ChecksumAlgorithmCRC32, body))
		req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
		rec := httptest.NewRecorder()
		env.handler(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "<Code>BadDigest</Code>")
		require.True(t, env.exists(key))
	})

	t.Run("tampered_md5", func(t *testing.T) {
		key := "noiam-tampered-md5.txt"
		env.seed(key)
		body := deleteObjectsBody(key)
		req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
		req.Header.Set("Content-Md5", getMD5HashBase64([]byte("wrong-payload")))
		req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
		rec := httptest.NewRecorder()
		env.handler(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "<Code>BadDigest</Code>")
		require.True(t, env.exists(key))
	})

	t.Run("malformed_md5", func(t *testing.T) {
		key := "noiam-malformed-md5.txt"
		env.seed(key)
		body := deleteObjectsBody(key)
		req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
		req.Header.Set("Content-Md5", "not-valid-md5")
		req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
		rec := httptest.NewRecorder()
		env.handler(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "<Code>InvalidDigest</Code>")
		require.True(t, env.exists(key))
	})

	t.Run("duplicate_md5", func(t *testing.T) {
		key := "noiam-dup-md5.txt"
		env.seed(key)
		body := deleteObjectsBody(key)
		req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
		v := getMD5HashBase64(body)
		req.Header.Add("Content-Md5", v)
		req.Header.Add("Content-Md5", v)
		req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
		rec := httptest.NewRecorder()
		env.handler(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "<Code>InvalidDigest</Code>")
		require.True(t, env.exists(key))
	})

	t.Run("non_canonical_md5_padding", func(t *testing.T) {
		key := "noiam-noncanon-md5.txt"
		env.seed(key)
		body := deleteObjectsBody(key)
		req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
		req.Header.Set("Content-Md5", "AAAAAAAAAAAAAAAAAAAAAB==")
		req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
		rec := httptest.NewRecorder()
		env.handler(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "<Code>InvalidDigest</Code>")
		require.True(t, env.exists(key))
	})

	t.Run("valid_md5", func(t *testing.T) {
		key := "noiam-valid-md5.txt"
		env.seed(key)
		body := deleteObjectsBody(key)
		req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
		req.Header.Set("Content-Md5", getMD5HashBase64(body))
		req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
		rec := httptest.NewRecorder()
		env.handler(rec, req)

		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.False(t, env.exists(key))
	})

	t.Run("no_checksum", func(t *testing.T) {
		key := "noiam-none.txt"
		env.seed(key)
		body := deleteObjectsBody(key)
		req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
		req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
		rec := httptest.NewRecorder()
		env.handler(rec, req)

		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.False(t, env.exists(key))
	})

	t.Run("malformed_query_escape", func(t *testing.T) {
		key := "noiam-malformed-query.txt"
		env.seed(key)
		body := deleteObjectsBody(key)
		req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete&x-amz-checksum-crc32=%ZZ", body)
		req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
		rec := httptest.NewRecorder()
		env.handler(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "<Code>InvalidRequest</Code>")
		require.True(t, env.exists(key), "object must survive a malformed query escape")
	})

	t.Run("oversized_body", func(t *testing.T) {
		key := "noiam-oversized.txt"
		env.seed(key)
		body := deleteObjectsBody(key)
		req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?delete", body)
		req = req.WithContext(s3_constants.SetRequestBodyLimitInContext(req.Context(), int64(len(body)-1)))
		req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
		rec := httptest.NewRecorder()
		env.handler(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "<Code>EntityTooLarge</Code>")
		require.True(t, env.exists(key))
	})
}

func TestDeleteObjectsChecksumQueryParams(t *testing.T) {
	env := newDeleteChecksumTestEnv(t, false)

	t.Run("valid", func(t *testing.T) {
		key := "query-valid.txt"
		env.seed(key)
		body := deleteObjectsBody(key)
		q := url.Values{}
		q.Set("delete", "")
		q.Set(s3_constants.AmzSdkChecksumAlgorithm, "CRC32")
		q.Set(s3_constants.AmzChecksumCRC32, computeChecksumBase64(ChecksumAlgorithmCRC32, body))

		req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?"+q.Encode(), body)
		req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
		rec := httptest.NewRecorder()
		env.handler(rec, req)

		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.False(t, env.exists(key))
	})

	t.Run("tampered", func(t *testing.T) {
		key := "query-tampered.txt"
		env.seed(key)
		body := deleteObjectsBody(key)
		q := url.Values{}
		q.Set("delete", "")
		q.Set(s3_constants.AmzSdkChecksumAlgorithm, "CRC32")
		q.Set(s3_constants.AmzChecksumCRC32, tamperedChecksumBase64(ChecksumAlgorithmCRC32, body))

		req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?"+q.Encode(), body)
		req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
		rec := httptest.NewRecorder()
		env.handler(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "<Code>BadDigest</Code>")
		require.True(t, env.exists(key))
	})

	t.Run("header_over_query_smuggling", func(t *testing.T) {
		origKey := "query-orig.txt"
		victimKey := "query-victim.txt"
		env.seed(origKey)
		env.seed(victimKey)

		origBody := deleteObjectsBody(origKey)
		swappedBody := deleteObjectsBody(victimKey)

		q := url.Values{}
		q.Set("delete", "")
		q.Set(s3_constants.AmzChecksumCRC32, computeChecksumBase64(ChecksumAlgorithmCRC32, origBody))

		req := newDeleteChecksumRequest(t, "http://127.0.0.1:9000/warehouse?"+q.Encode(), swappedBody)
		req.Header.Set(s3_constants.AmzChecksumCRC32, computeChecksumBase64(ChecksumAlgorithmCRC32, swappedBody))
		req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
		rec := httptest.NewRecorder()
		env.handler(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "<Code>InvalidRequest</Code>")
		require.True(t, env.exists(victimKey), "victim object must survive header-over-query smuggling")
	})
}

// TestDeleteObjectsChecksumPresignedQuery covers DeleteObjects against a
// presigned URL, where AWS SDK presigners hoist x-amz-checksum-* into the
// query string that the signature covers.
func TestDeleteObjectsChecksumPresignedQuery(t *testing.T) {
	env := newDeleteChecksumTestEnv(t, true)

	presignedDeleteURL := func(checksum string) string {
		q := url.Values{}
		q.Set("delete", "")
		q.Set(s3_constants.AmzSdkChecksumAlgorithm, "CRC32")
		q.Set(s3_constants.AmzChecksumCRC32, checksum)
		return "http://127.0.0.1:9000/warehouse?" + q.Encode()
	}

	newPresigned := func(t *testing.T, urlStr string, body []byte) *http.Request {
		t.Helper()
		req := newDeleteChecksumRequest(t, urlStr, body)
		req.Header.Del("X-Amz-Content-Sha256")
		require.NoError(t, preSignV4(env.s3a.iam, req, "access_key_1", "secret_key_1", 600))
		return mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})
	}

	serve := func(req *http.Request) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		env.handler(rec, req)
		return rec
	}

	t.Run("valid_query_checksum", func(t *testing.T) {
		key := "presigned-valid.txt"
		env.seed(key)
		body := deleteObjectsBody(key)
		req := newPresigned(t, presignedDeleteURL(computeChecksumBase64(ChecksumAlgorithmCRC32, body)), body)

		rec := serve(req)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.False(t, env.exists(key), "object must be deleted on a valid presigned checksum")
	})

	t.Run("body_swapped_against_signed_query_checksum", func(t *testing.T) {
		origKey := "presigned-orig.txt"
		victimKey := "presigned-victim.txt"
		env.seed(origKey)
		env.seed(victimKey)
		origBody := deleteObjectsBody(origKey)
		swappedBody := deleteObjectsBody(victimKey)
		req := newPresigned(t, presignedDeleteURL(computeChecksumBase64(ChecksumAlgorithmCRC32, origBody)), swappedBody)

		rec := serve(req)
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "<Code>BadDigest</Code>")
		require.True(t, env.exists(victimKey), "victim object must survive a body swapped against the signed query checksum")
	})

	t.Run("query_checksum_edited_after_signing", func(t *testing.T) {
		key := "presigned-edited.txt"
		env.seed(key)
		body := deleteObjectsBody(key)
		req := newPresigned(t, presignedDeleteURL(computeChecksumBase64(ChecksumAlgorithmCRC32, body)), body)

		q := req.URL.Query()
		q.Set(s3_constants.AmzChecksumCRC32, computeChecksumBase64(ChecksumAlgorithmCRC32, deleteObjectsBody("presigned-victim.txt")))
		req.URL.RawQuery = q.Encode()

		rec := serve(req)
		require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "<Code>SignatureDoesNotMatch</Code>")
		require.True(t, env.exists(key), "object must survive a query checksum edited after signing")
	})

	t.Run("injected_checksum_header", func(t *testing.T) {
		key := "presigned-injected.txt"
		env.seed(key)
		body := deleteObjectsBody(key)
		swappedBody := deleteObjectsBody("presigned-swapped.txt")
		req := newPresigned(t, presignedDeleteURL(computeChecksumBase64(ChecksumAlgorithmCRC32, body)), swappedBody)
		req.Header.Set(s3_constants.AmzChecksumCRC32, computeChecksumBase64(ChecksumAlgorithmCRC32, swappedBody))

		rec := serve(req)
		require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "<Code>SignatureDoesNotMatch</Code>")
		require.True(t, env.exists(key), "object must survive an unsigned checksum header")
	})

	t.Run("injected_checksum_header_over_signed_query", func(t *testing.T) {
		origKey := "presigned-orig.txt"
		victimKey := "presigned-victim.txt"
		env.seed(origKey)
		env.seed(victimKey)

		origBody := deleteObjectsBody(origKey)
		swappedBody := deleteObjectsBody(victimKey)
		req := newDeleteChecksumRequest(t, presignedDeleteURL(computeChecksumBase64(ChecksumAlgorithmCRC32, origBody)), swappedBody)
		req.Header.Del("X-Amz-Content-Sha256")
		req.Header.Set(s3_constants.AmzChecksumCRC32, computeChecksumBase64(ChecksumAlgorithmCRC32, swappedBody))
		require.NoError(t, preSignV4WithHeaders(env.s3a.iam, req, "access_key_1", "secret_key_1", 600,
			[]string{"host", s3_constants.AmzChecksumCRC32}))
		req = mux.SetURLVars(req, map[string]string{"bucket": "warehouse"})

		rec := serve(req)
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Contains(t, rec.Body.String(), "<Code>InvalidRequest</Code>")
		require.True(t, env.exists(victimKey), "victim object must survive a checksum header duplicating the signed query")
	})
}

package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func TestDetectRequestedChecksumAlgorithm(t *testing.T) {
	cases := []struct {
		name       string
		setup      func(r *http.Request)
		wantAlg    ChecksumAlgorithm
		wantHeader string
		wantErr    s3err.ErrorCode
	}{
		{
			name: "sdk algorithm header",
			setup: func(r *http.Request) {
				r.Header.Set(s3_constants.AmzSdkChecksumAlgorithm, "SHA256")
			},
			wantAlg:    ChecksumAlgorithmSHA256,
			wantHeader: s3_constants.AmzChecksumSHA256,
		},
		{
			name: "algorithm header",
			setup: func(r *http.Request) {
				r.Header.Set(s3_constants.AmzChecksumAlgorithm, "CRC32")
			},
			wantAlg:    ChecksumAlgorithmCRC32,
			wantHeader: s3_constants.AmzChecksumCRC32,
		},
		{
			// Presigned URL: AWS SDK hoists the sdk-checksum-algorithm header into the query string.
			// Regression test for https://github.com/seaweedfs/seaweedfs/issues/9075
			name: "presigned url hoists sdk algorithm to query",
			setup: func(r *http.Request) {
				q := r.URL.Query()
				q.Set("X-Amz-Sdk-Checksum-Algorithm", "SHA256")
				r.URL.RawQuery = q.Encode()
			},
			wantAlg:    ChecksumAlgorithmSHA256,
			wantHeader: s3_constants.AmzChecksumSHA256,
		},
		{
			name: "presigned url hoists checksum-algorithm to query (lowercase)",
			setup: func(r *http.Request) {
				q := r.URL.Query()
				q.Set("x-amz-checksum-algorithm", "sha1")
				r.URL.RawQuery = q.Encode()
			},
			wantAlg:    ChecksumAlgorithmSHA1,
			wantHeader: s3_constants.AmzChecksumSHA1,
		},
		{
			name: "presigned url hoists individual checksum value",
			setup: func(r *http.Request) {
				q := r.URL.Query()
				q.Set("X-Amz-Checksum-Sha256", "abc")
				r.URL.RawQuery = q.Encode()
			},
			wantAlg:    ChecksumAlgorithmSHA256,
			wantHeader: s3_constants.AmzChecksumSHA256,
		},
		{
			name: "unsupported in query returns error",
			setup: func(r *http.Request) {
				q := r.URL.Query()
				q.Set("X-Amz-Sdk-Checksum-Algorithm", "MD5")
				r.URL.RawQuery = q.Encode()
			},
			wantAlg: ChecksumAlgorithmNone,
			wantErr: s3err.ErrInvalidRequest,
		},
		{
			name:    "no checksum",
			setup:   func(r *http.Request) {},
			wantAlg: ChecksumAlgorithmNone,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPut, "http://example.com/bucket/key", nil)
			tc.setup(r)
			alg, header, code := detectRequestedChecksumAlgorithm(r)
			if code != tc.wantErr {
				t.Fatalf("err code: got %v, want %v", code, tc.wantErr)
			}
			if alg != tc.wantAlg {
				t.Fatalf("algorithm: got %v, want %v", alg, tc.wantAlg)
			}
			if header != tc.wantHeader {
				t.Fatalf("header name: got %q, want %q", header, tc.wantHeader)
			}
		})
	}
}

func TestLookupHeaderOrQueryCaseInsensitive(t *testing.T) {
	r := httptest.NewRequest(http.MethodPut, "http://example.com/?x-AMZ-sdk-CHECKSUM-algorithm=SHA256", nil)
	q := parseRequestQuery(r)
	if got := lookupHeaderOrQuery(r, q, s3_constants.AmzSdkChecksumAlgorithm); got != "SHA256" {
		t.Fatalf("expected SHA256, got %q", got)
	}
}

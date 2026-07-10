package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// A presigned URL that signs content-type carries
// X-Amz-SignedHeaders=content-type%3Bhost. Some clients and proxies decode
// the %3B into a raw ';', which RFC 3986 permits in a query string and AWS
// accepts, but Go's url.ParseQuery drops the whole pair — verification then
// fails with MissingFields. The router middleware re-encodes the ';' so the
// parameter survives.
func TestPresignedPutSignedContentTypeWithRawSemicolon(t *testing.T) {
	iam := newTestIAM()

	req, err := newTestRequest(http.MethodPut, "http://127.0.0.1:9000/bucket/key.png", 0, nil)
	if err != nil {
		t.Fatalf("newTestRequest: %v", err)
	}
	req.Header.Set("Content-Type", "image/png")
	if err := preSignV4WithHeaders(iam, req, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", 600, []string{"host", "content-type"}); err != nil {
		t.Fatalf("preSignV4WithHeaders: %v", err)
	}
	req.URL.RawQuery = strings.ReplaceAll(req.URL.RawQuery, "%3B", ";")

	if _, errCode := iam.reqSignatureV4Verify(req); errCode == s3err.ErrNone {
		t.Fatal("raw-semicolon query unexpectedly verified without normalization")
	}

	errCode := s3err.ErrInternalError
	handler := escapeSemicolonsInQuery(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, errCode = iam.reqSignatureV4Verify(r)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if errCode != s3err.ErrNone {
		t.Fatalf("expected ErrNone through semicolon-normalizing middleware, got %v", errCode)
	}
}

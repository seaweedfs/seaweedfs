package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

// TestIsS3TablesSignedRequest covers the credential-scope parser used to
// disambiguate S3 Tables REST requests from regular S3 requests on paths
// the two APIs share (e.g. /buckets, /get-table).
func TestIsS3TablesSignedRequest(t *testing.T) {
	cases := []struct {
		name        string
		auth        string
		cred        string
		contentType string
		want        bool
	}{
		{
			name: "regular S3 auth header",
			auth: "AWS4-HMAC-SHA256 Credential=AKIA/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef",
			want: false,
		},
		{
			name: "S3 Tables auth header",
			auth: "AWS4-HMAC-SHA256 Credential=AKIA/20260101/us-east-1/s3tables/aws4_request, SignedHeaders=host;x-amz-date, Signature=deadbeef",
			want: true,
		},
		{
			name: "S3 Tables presigned query",
			cred: "AKIA/20260101/us-east-1/s3tables/aws4_request",
			want: true,
		},
		{
			name: "regular S3 presigned query",
			cred: "AKIA/20260101/us-east-1/s3/aws4_request",
			want: false,
		},
		{
			name: "unsigned request",
			want: false,
		},
		{
			name: "malformed credential",
			auth: "AWS4-HMAC-SHA256 Credential=AKIA, Signature=zzz",
			want: false,
		},
		{
			name: "service substring in access key must not match",
			auth: "AWS4-HMAC-SHA256 Credential=s3tablesAKIA/20260101/us-east-1/s3/aws4_request, Signature=zzz",
			want: false,
		},
		{
			name:        "unsigned with AWS JSON RPC content type",
			contentType: "application/x-amz-json-1.1",
			want:        true,
		},
		{
			name:        "unsigned with AWS JSON RPC content type and charset",
			contentType: "application/x-amz-json-1.1; charset=utf-8",
			want:        true,
		},
		{
			name:        "JSON content type must not override S3-signed scope",
			auth:        "AWS4-HMAC-SHA256 Credential=AKIA/20260101/us-east-1/s3/aws4_request, Signature=deadbeef",
			contentType: "application/x-amz-json-1.1",
			want:        false,
		},
		{
			name:        "regular JSON content type does not match",
			contentType: "application/json",
			want:        false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/buckets", nil)
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			if tc.cred != "" {
				q := req.URL.Query()
				q.Set("X-Amz-Credential", tc.cred)
				req.URL.RawQuery = q.Encode()
			}
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			if got := isS3TablesSignedRequest(req); got != tc.want {
				t.Fatalf("isS3TablesSignedRequest=%v, want %v", got, tc.want)
			}
		})
	}
}

// TestRouting_ListObjectsV2OnBucketNamedBuckets covers the route collision
// between regular S3 listObjectsV2 on a bucket named "buckets" and the
// S3 Tables ListTableBuckets endpoint, both of which use the path /buckets.
// The credential scope on a regular S3 request is /s3/aws4_request, so the
// S3 Tables route must reject it and let the bucket subrouter take it,
// otherwise AWS SDK V2 / s3a clients receive a JSON body in place of the
// expected XML and fail to parse the response.
func TestRouting_ListObjectsV2OnBucketNamedBuckets(t *testing.T) {
	router := mux.NewRouter()
	s3a := setupRoutingTestServer(t)
	s3a.registerRouter(router)

	req, _ := http.NewRequest(http.MethodGet, "/buckets?list-type=2&prefix=logs%2F", nil)
	signRoutingTestRequest(t, req, "", "s3")

	var match mux.RouteMatch
	if !router.Match(req, &match) {
		t.Fatalf("expected GET /buckets?list-type=2 (signed for s3) to match a route; got no match: %v", match.MatchErr)
	}
	if tmpl, _ := match.Route.GetPathTemplate(); tmpl == "/buckets" {
		t.Fatalf("GET /buckets?list-type=2 signed for service=s3 matched the S3 Tables ListTableBuckets route (Path=%q); it must fall through to the bucket subrouter", tmpl)
	}
}

// TestRouting_S3TablesListTableBucketsStillReachable verifies the matcher
// does not break legitimate S3 Tables traffic: a request signed for the
// s3tables service on GET /buckets must still reach ListTableBuckets.
func TestRouting_S3TablesListTableBucketsStillReachable(t *testing.T) {
	router := mux.NewRouter()
	s3a := setupRoutingTestServer(t)
	s3a.registerRouter(router)

	req, _ := http.NewRequest(http.MethodGet, "/buckets", nil)
	signRoutingTestRequest(t, req, "", "s3tables")

	var match mux.RouteMatch
	if !router.Match(req, &match) {
		t.Fatalf("expected GET /buckets (signed for s3tables) to match a route; got no match: %v", match.MatchErr)
	}
	tmpl, _ := match.Route.GetPathTemplate()
	if tmpl != "/buckets" {
		t.Fatalf("GET /buckets signed for service=s3tables matched %q; want the S3 Tables route /buckets", tmpl)
	}
}

// TestRouting_CreateBucketNamedBuckets covers the PUT collision: a PUT
// /buckets signed for s3 is CreateBucket on the regular S3 API, not
// CreateTableBucket.
func TestRouting_CreateBucketNamedBuckets(t *testing.T) {
	router := mux.NewRouter()
	s3a := setupRoutingTestServer(t)
	s3a.registerRouter(router)

	req, _ := http.NewRequest(http.MethodPut, "/buckets", nil)
	signRoutingTestRequest(t, req, "", "s3")

	var match mux.RouteMatch
	if !router.Match(req, &match) {
		t.Fatalf("expected PUT /buckets (signed for s3) to match a route; got no match: %v", match.MatchErr)
	}
	if tmpl, _ := match.Route.GetPathTemplate(); tmpl == "/buckets" {
		t.Fatalf("PUT /buckets signed for service=s3 matched the S3 Tables CreateTableBucket route (Path=%q); it must fall through to the bucket subrouter", tmpl)
	}
}

// TestRouting_UnsignedS3TablesCreateTableBucket mirrors the unsigned
// PUT /buckets traffic that the Iceberg catalog integration tests send
// (Content-Type: application/x-amz-json-1.1, no AWS V4 signature). The
// JSON-RPC content type must be treated as an S3 Tables signal so these
// requests reach CreateTableBucket instead of falling through to the
// regular S3 CreateBucket handler.
func TestRouting_UnsignedS3TablesCreateTableBucket(t *testing.T) {
	router := mux.NewRouter()
	s3a := setupRoutingTestServer(t)
	s3a.registerRouter(router)

	req, _ := http.NewRequest(http.MethodPut, "/buckets", strings.NewReader(`{"name":"warehouse-x"}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	var match mux.RouteMatch
	if !router.Match(req, &match) {
		t.Fatalf("expected unsigned PUT /buckets with JSON RPC content type to match a route; got no match: %v", match.MatchErr)
	}
	tmpl, _ := match.Route.GetPathTemplate()
	if tmpl != "/buckets" {
		t.Fatalf("unsigned PUT /buckets with JSON RPC content type matched %q; want the S3 Tables route /buckets", tmpl)
	}
}

// TestRouting_GetObjectOnBucketNamedGetTable covers the GET /get-table
// collision: a regular S3 request on a bucket named "get-table" must
// reach the bucket subrouter, not the S3 Tables GetTable handler.
func TestRouting_GetObjectOnBucketNamedGetTable(t *testing.T) {
	router := mux.NewRouter()
	s3a := setupRoutingTestServer(t)
	s3a.registerRouter(router)

	req, _ := http.NewRequest(http.MethodGet, "/get-table?list-type=2", nil)
	signRoutingTestRequest(t, req, "", "s3")

	var match mux.RouteMatch
	if !router.Match(req, &match) {
		t.Fatalf("expected GET /get-table?list-type=2 (signed for s3) to match a route; got no match: %v", match.MatchErr)
	}
	if tmpl, _ := match.Route.GetPathTemplate(); tmpl == "/get-table" {
		t.Fatalf("GET /get-table signed for service=s3 matched the S3 Tables GetTable route (Path=%q); it must fall through to the bucket subrouter", tmpl)
	}
}

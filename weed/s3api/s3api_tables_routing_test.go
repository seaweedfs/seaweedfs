package s3api

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestIsS3TablesSignedRequest covers the credential-scope parser used to
// disambiguate S3 Tables REST requests from regular S3 requests on paths
// the two APIs share (e.g. /buckets, /get-table).
func TestIsS3TablesSignedRequest(t *testing.T) {
	cases := []struct {
		name string
		auth string
		cred string
		want bool
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
			name: "region named s3tables on a regular S3 request",
			auth: "AWS4-HMAC-SHA256 Credential=AKIA/20260101/s3tables/s3/aws4_request, Signature=zzz",
			want: false,
		},
		{
			name: "region named s3tables on a presigned S3 request",
			cred: "AKIA/20260101/s3tables/s3/aws4_request",
			want: false,
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

// recordingFiler answers every lookup with not-found and records each call the
// gateway makes, with the path it names, so a test can tell whether a request
// reached a handler that talks to the filer at all.
type recordingFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu    sync.Mutex
	calls []string
}

func (f *recordingFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	return nil, status.Errorf(codes.NotFound, "%s: %s/%s", filer_pb.ErrNotFound.Error(), req.Directory, req.Name)
}

func (f *recordingFiler) record(method string, req interface{}) {
	call := method
	if d, ok := req.(interface{ GetDirectory() string }); ok {
		name := ""
		if n, ok := req.(interface{ GetName() string }); ok {
			name = n.GetName()
		} else if e, ok := req.(interface{ GetEntry() *filer_pb.Entry }); ok && e.GetEntry() != nil {
			name = e.GetEntry().Name
		}
		call += " " + path.Join(d.GetDirectory(), name)
	}
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
}

func (f *recordingFiler) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *recordingFiler) reset() {
	f.mu.Lock()
	f.calls = nil
	f.mu.Unlock()
}

type recordingServerStream struct {
	grpc.ServerStream
	f      *recordingFiler
	method string
}

func (s *recordingServerStream) RecvMsg(m interface{}) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil {
		s.f.record(s.method, m)
	}
	return err
}

// startRecordingFiler serves f on a random localhost port, recording every
// unary and streaming call before it is handled.
func startRecordingFiler(t *testing.T, f *recordingFiler) pb.ServerAddress {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			f.record(info.FullMethod, req)
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			return handler(srv, &recordingServerStream{ServerStream: ss, f: f, method: info.FullMethod})
		}),
	)
	filer_pb.RegisterSeaweedFilerServer(srv, f)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return fakeFilerAddress(lis)
}

// newS3TablesServeTestServer builds the production router over a gateway whose
// filer is a recordingFiler, wired far enough (identity account, circuit
// breaker, bucket config cache) that a request the S3 routes take runs its
// handler instead of panicking on a nil dependency. domainName is the
// -domainName option, empty for none.
func newS3TablesServeTestServer(t *testing.T, domainName string) (*mux.Router, *recordingFiler) {
	t.Helper()
	s3a := setupRoutingTestServer(t)
	s3a.iam.m.Lock()
	s3a.iam.accessKeyIdent[routingTestAccessKey].Account = &AccountAdmin
	s3a.iam.m.Unlock()
	// Built before the filer is wired so loading its config is not recorded.
	s3a.cb = NewCircuitBreaker(s3a.option)
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)

	f := &recordingFiler{}
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	s3a.option.Filers = []pb.ServerAddress{startRecordingFiler(t, f)}
	s3a.option.GrpcDialOption = dialOption
	s3a.option.BucketsPath = "/buckets"
	s3a.option.DomainName = domainName
	s3a.filerClient = wdclient.NewFilerClient(s3a.option.Filers, dialOption, "")

	router := mux.NewRouter().SkipClean(true)
	s3a.registerRouter(router)
	f.reset()
	return router, f
}

// routingTestTableBucketARN is a table bucket ARN %-encoded the way the AWS
// SDK puts it in a REST path.
const routingTestTableBucketARN = "arn%3Aaws%3As3tables%3Aus-east-1%3A123456789012%3Abucket%2Ftable-bucket"

// unroutedS3TablesOperations are S3 Tables REST shapes this gateway has no
// route for. Every one of them also parses as an object key in a bucket named
// after the first path segment.
var unroutedS3TablesOperations = []struct {
	name   string
	method string
	path   string
	body   string
}{
	{"UpdateTableMetadataLocation", http.MethodPut, "/tables/" + routingTestTableBucketARN + "/ns/t/metadata-location", `{"versionToken":"v","metadataLocation":"s3://b/m.json"}`},
	{"GetTableMetadataLocation", http.MethodGet, "/tables/" + routingTestTableBucketARN + "/ns/t/metadata-location", ""},
	{"PutTableBucketEncryption", http.MethodPut, "/buckets/" + routingTestTableBucketARN + "/encryption", `{"encryptionConfiguration":{"sseAlgorithm":"AES256"}}`},
	{"GetTableBucketEncryption", http.MethodGet, "/buckets/" + routingTestTableBucketARN + "/encryption", ""},
	{"DeleteTableBucketEncryption", http.MethodDelete, "/buckets/" + routingTestTableBucketARN + "/encryption", ""},
	{"GetTableEncryption", http.MethodGet, "/tables/" + routingTestTableBucketARN + "/ns/t/encryption", ""},
	// A served path under a method it does not serve: gorilla drops the
	// method mismatch once a later route matches, so these take the fallback
	// rather than answering 405.
	{"table policy under POST", http.MethodPost, "/tables/" + routingTestTableBucketARN + "/ns/t/policy", `{"resourcePolicy":"{}"}`},
	{"bucket policy under POST", http.MethodPost, "/buckets/" + routingTestTableBucketARN + "/policy", `{"resourcePolicy":"{}"}`},
	{"rename under GET", http.MethodGet, "/tables/" + routingTestTableBucketARN + "/ns/t/rename", ""},
}

func newRoutingTestRequest(t *testing.T, method, host, target, body, service string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, "http://"+host+target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	}
	if service != "" {
		signRoutingTestRequest(t, req, body, service)
	}
	return req
}

// isBucketOrObjectRoute reports whether a match landed on one of the S3
// bucket routers, which bind the bucket from the path or the host.
func isBucketOrObjectRoute(match *mux.RouteMatch) bool {
	if _, ok := match.Vars["bucket"]; ok {
		return true
	}
	_, ok := match.Vars["object"]
	return ok
}

// assertUnknownOperation checks rr is the fallback's answer.
func assertUnknownOperation(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body %s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/x-amz-json-1.1" {
		t.Fatalf("Content-Type = %q, want application/x-amz-json-1.1", ct)
	}
	var resp struct {
		Type string `json:"__type"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body %q: %v", rr.Body.String(), err)
	}
	if resp.Type != s3TablesErrUnknownOperation {
		t.Fatalf("__type = %q, want %q", resp.Type, s3TablesErrUnknownOperation)
	}
}

// TestRouting_UnroutedS3TablesOperationsNeverReachS3Handlers covers S3 Tables
// operations the gateway does not serve. Without a route of their own they
// fell through to the /{bucket} routers and ran as S3 requests — a PUT stored
// its body as an object in a bucket named "tables" and answered 2xx, which the
// SDK reports as success. They must be refused before any filer call, on the
// path-style, path-style-domain and virtual-host routers alike.
func TestRouting_UnroutedS3TablesOperationsNeverReachS3Handlers(t *testing.T) {
	routers := []struct {
		name       string
		domainName string
		host       string
	}{
		{"no domain", "", "localhost:8333"},
		{"path-style domain", "s3.example.com,example.com", "s3.example.com"},
		{"virtual-host domain", "s3.example.com,example.com", "tables.example.com"},
	}
	for _, rt := range routers {
		t.Run(rt.name, func(t *testing.T) {
			router, filer := newS3TablesServeTestServer(t, rt.domainName)
			for _, tc := range unroutedS3TablesOperations {
				t.Run(tc.name, func(t *testing.T) {
					filer.reset()
					req := newRoutingTestRequest(t, tc.method, rt.host, tc.path, tc.body, "s3tables")

					var match mux.RouteMatch
					if !router.Match(req, &match) {
						t.Fatalf("%s %s matched no route: %v", tc.method, tc.path, match.MatchErr)
					}
					tmpl, _ := match.Route.GetPathTemplate()
					if tmpl == "/{bucket}" || tmpl == "/{bucket}/{object:(?s).+}" || isBucketOrObjectRoute(&match) {
						t.Fatalf("%s %s signed for s3tables matched the S3 route %q (vars %v)", tc.method, tc.path, tmpl, match.Vars)
					}

					rr := httptest.NewRecorder()
					router.ServeHTTP(rr, newRoutingTestRequest(t, tc.method, rt.host, tc.path, tc.body, "s3tables"))
					assertUnknownOperation(t, rr)
					if calls := filer.Calls(); len(calls) != 0 {
						t.Fatalf("an unrouted S3 Tables request reached the filer: %v", calls)
					}
				})
			}
		})
	}
}

// TestRouting_S3SignedArnShapedKeysStayObjectRequests is the other side of the
// fallback: the same paths signed for s3 are object keys shaped like ARNs in a
// bucket named "tables" or "buckets", and must keep reaching the S3 routes.
func TestRouting_S3SignedArnShapedKeysStayObjectRequests(t *testing.T) {
	router, _ := newS3TablesServeTestServer(t, "")
	for _, tc := range unroutedS3TablesOperations {
		// A bare POST on an object path is no S3 operation, so the bucket
		// routers never took it; it is not what this test guards.
		if tc.method == http.MethodPost {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			req := newRoutingTestRequest(t, tc.method, "localhost:8333", tc.path, tc.body, "s3")
			var match mux.RouteMatch
			if !router.Match(req, &match) {
				t.Fatalf("%s %s matched no route: %v", tc.method, tc.path, match.MatchErr)
			}
			wantBucket := strings.SplitN(strings.TrimPrefix(tc.path, "/"), "/", 2)[0]
			if got := match.Vars["bucket"]; got != wantBucket {
				tmpl, _ := match.Route.GetPathTemplate()
				t.Fatalf("%s %s signed for s3 matched %q with bucket %q; want the S3 route for bucket %q", tc.method, tc.path, tmpl, got, wantBucket)
			}
		})
	}
}

// An S3 client configured with a region named s3tables still signs for
// service s3, so its requests must stay on the bucket routers.
func TestRouting_S3RegionNamedS3TablesStaysObjectRequest(t *testing.T) {
	router, _ := newS3TablesServeTestServer(t, "")
	const body = "data"
	req, err := http.NewRequest(http.MethodPut, "http://localhost:8333/tables/"+routingTestTableBucketARN+"/ns/t/metadata-location", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	signer := v4.NewSigner(credentials.NewStaticCredentials(routingTestAccessKey, routingTestSecretKey, ""))
	if _, err := signer.Sign(req, strings.NewReader(body), "s3", "s3tables", time.Now()); err != nil {
		t.Fatalf("sign request: %v", err)
	}
	var match mux.RouteMatch
	if !router.Match(req, &match) {
		t.Fatalf("PUT matched no route: %v", match.MatchErr)
	}
	if got := match.Vars["bucket"]; got != "tables" {
		tmpl, _ := match.Route.GetPathTemplate()
		t.Fatalf("S3 request in region s3tables matched %q with bucket %q; want the S3 route for bucket \"tables\"", tmpl, got)
	}
}

// TestRouting_RenameTableRESTReachesS3Tables covers the AWS RenameTable REST
// call. It used to fall through to the bucket router as a PutObject into a
// bucket named "tables"; it must reach the S3 Tables rename handler, answer in
// JSON, and never touch anything under that bucket.
func TestRouting_RenameTableRESTReachesS3Tables(t *testing.T) {
	router, filer := newS3TablesServeTestServer(t, "")
	const target = "/tables/" + routingTestTableBucketARN + "/ns/t/rename"
	const body = `{"newName":"t2"}`

	req := newRoutingTestRequest(t, http.MethodPut, "localhost:8333", target, body, "s3tables")
	var match mux.RouteMatch
	if !router.Match(req, &match) {
		t.Fatalf("RenameTable matched no route: %v", match.MatchErr)
	}
	tmpl, _ := match.Route.GetPathTemplate()
	if !strings.HasPrefix(tmpl, "/tables/{tableBucketARN:") || !strings.HasSuffix(tmpl, "}/{namespace}/{name}/rename") {
		t.Fatalf("RenameTable matched %q, want the rename route", tmpl)
	}

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, newRoutingTestRequest(t, http.MethodPut, "localhost:8333", target, body, "s3tables"))
	if ct := rr.Header().Get("Content-Type"); ct != "application/x-amz-json-1.1" {
		t.Fatalf("Content-Type = %q, want application/x-amz-json-1.1; body %s", ct, rr.Body.String())
	}
	var resp struct {
		Type string `json:"__type"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body %q: %v", rr.Body.String(), err)
	}
	// The recording filer holds no tables, so the handler reports the source
	// missing — an answer only the rename handler gives.
	if rr.Code != http.StatusNotFound || resp.Type != s3tables.ErrCodeNoSuchTable {
		t.Fatalf("status %d __type %q, want 404 %s from the rename handler; body %s", rr.Code, resp.Type, s3tables.ErrCodeNoSuchTable, rr.Body.String())
	}

	calls := filer.Calls()
	if len(calls) == 0 {
		t.Fatal("the rename handler never looked the source up")
	}
	for _, call := range calls {
		if fields := strings.Fields(call); len(fields) == 2 && (fields[1] == "/buckets/tables" || strings.HasPrefix(fields[1], "/buckets/tables/")) {
			t.Fatalf("RenameTable touched the S3 bucket \"tables\": %v", calls)
		}
	}
}

// TestRouting_S3TablesFallbackDoesNotShadowOtherRoutes checks that the
// fallback only takes requests no other route claims: unsigned CORS
// preflights, health probes and STS keep their routes, and a served S3 Tables
// operation keeps its own.
func TestRouting_S3TablesFallbackDoesNotShadowOtherRoutes(t *testing.T) {
	router, _ := newS3TablesServeTestServer(t, "")
	const host = "localhost:8333"

	t.Run("bucket CORS preflight", func(t *testing.T) {
		req := newRoutingTestRequest(t, http.MethodOptions, host, "/tables/"+routingTestTableBucketARN+"/ns/t", "", "")
		req.Header.Set("Origin", "https://example.com")
		req.Header.Set("Access-Control-Request-Method", http.MethodPut)
		var match mux.RouteMatch
		if !router.Match(req, &match) || match.Vars["bucket"] != "tables" {
			t.Fatalf("OPTIONS preflight matched vars %v, want the bucket OPTIONS route", match.Vars)
		}
	})

	t.Run("health probe preflight", func(t *testing.T) {
		req := newRoutingTestRequest(t, http.MethodOptions, host, "/healthz", "", "")
		req.Header.Set("Origin", "https://example.com")
		var match mux.RouteMatch
		if !router.Match(req, &match) {
			t.Fatalf("OPTIONS /healthz matched no route: %v", match.MatchErr)
		}
		if tmpl, _ := match.Route.GetPathTemplate(); tmpl != "/{probe:status|healthz|readyz}" {
			t.Fatalf("OPTIONS /healthz matched %q", tmpl)
		}
	})

	t.Run("health probe", func(t *testing.T) {
		req := newRoutingTestRequest(t, http.MethodGet, host, "/healthz", "", "")
		var match mux.RouteMatch
		if !router.Match(req, &match) {
			t.Fatalf("GET /healthz matched no route: %v", match.MatchErr)
		}
		if tmpl, _ := match.Route.GetPathTemplate(); tmpl != "/healthz" {
			t.Fatalf("GET /healthz matched %q", tmpl)
		}
	})

	t.Run("STS", func(t *testing.T) {
		req := newRoutingTestRequest(t, http.MethodPost, host, "/?Action=AssumeRoleWithWebIdentity&WebIdentityToken=test-token&RoleArn=arn:aws:iam::123:role/test&RoleSessionName=test-session", "", "")
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusBadRequest && rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("STS request answered %d (%s), want the STS handler's 400/503", rr.Code, rr.Body.String())
		}
		if ct := rr.Header().Get("Content-Type"); ct == "application/x-amz-json-1.1" {
			t.Fatalf("STS request answered with the S3 Tables fallback: %s", rr.Body.String())
		}
	})

	t.Run("served S3 Tables operation", func(t *testing.T) {
		req := newRoutingTestRequest(t, http.MethodDelete, host, "/tables/"+routingTestTableBucketARN+"/ns/t", "", "s3tables")
		var match mux.RouteMatch
		if !router.Match(req, &match) {
			t.Fatalf("DeleteTable matched no route: %v", match.MatchErr)
		}
		tmpl, _ := match.Route.GetPathTemplate()
		if !strings.HasPrefix(tmpl, "/tables/{tableBucketARN:") || !strings.HasSuffix(tmpl, "}/{namespace}/{name}") {
			t.Fatalf("DeleteTable matched %q, want its own route", tmpl)
		}
	})
}

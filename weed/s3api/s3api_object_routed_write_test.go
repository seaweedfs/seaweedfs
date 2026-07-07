package s3api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func reqWith(headers map[string]string) *http.Request {
	r, _ := http.NewRequest(http.MethodPut, "/b/o", nil)
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}

// oneClause returns the single clause of cond, failing if it does not hold
// exactly one.
func oneClause(t *testing.T, cond *filer_pb.WriteCondition) *filer_pb.WriteCondition_Clause {
	t.Helper()
	if cond == nil {
		t.Fatal("expected a condition, got nil")
	}
	if len(cond.Clauses) != 1 {
		t.Fatalf("expected 1 clause, got %d", len(cond.Clauses))
	}
	return cond.Clauses[0]
}

func TestBuildWriteCondition(t *testing.T) {
	t.Run("no headers is unconditional", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(nil))
		if !ok || cond != nil {
			t.Fatalf("want (nil, true), got (%v, %v)", cond, ok)
		}
	})
	t.Run("If-None-Match * to IF_NOT_EXISTS", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(map[string]string{s3_constants.IfNoneMatch: "*"}))
		if !ok {
			t.Fatal("want ok")
		}
		if c := oneClause(t, cond); c.Kind != filer_pb.WriteCondition_IF_NOT_EXISTS {
			t.Fatalf("kind = %v", c.Kind)
		}
	})
	t.Run("If-Match * to IF_EXISTS", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: "*"}))
		if !ok {
			t.Fatal("want ok")
		}
		if c := oneClause(t, cond); c.Kind != filer_pb.WriteCondition_IF_EXISTS {
			t.Fatalf("kind = %v", c.Kind)
		}
	})
	t.Run("If-Match strong etag to IF_ETAG_MATCH", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: `"abc123"`}))
		if !ok {
			t.Fatal("want ok")
		}
		c := oneClause(t, cond)
		if c.Kind != filer_pb.WriteCondition_IF_ETAG_MATCH || len(c.Etags) != 1 || c.Etags[0] != "abc123" {
			t.Fatalf("clause = %+v", c)
		}
	})
	t.Run("If-None-Match strong etag to IF_ETAG_NOT_MATCH", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(map[string]string{s3_constants.IfNoneMatch: `"abc123"`}))
		if !ok {
			t.Fatal("want ok")
		}
		c := oneClause(t, cond)
		if c.Kind != filer_pb.WriteCondition_IF_ETAG_NOT_MATCH || len(c.Etags) != 1 || c.Etags[0] != "abc123" {
			t.Fatalf("clause = %+v", c)
		}
	})
	t.Run("weak etag falls back", func(t *testing.T) {
		if _, ok := buildWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: `W/"abc"`})); ok {
			t.Fatal("weak etag must not take the fast path")
		}
	})
	t.Run("etag list falls back", func(t *testing.T) {
		if _, ok := buildWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: `"a","b"`})); ok {
			t.Fatal("etag list must not take the fast path")
		}
	})
	t.Run("both match and none-match falls back", func(t *testing.T) {
		if _, ok := buildWriteCondition(reqWith(map[string]string{
			s3_constants.IfMatch:     "*",
			s3_constants.IfNoneMatch: "*",
		})); ok {
			t.Fatal("ambiguous combination must not take the fast path")
		}
	})
	t.Run("time-based falls back", func(t *testing.T) {
		if _, ok := buildWriteCondition(reqWith(map[string]string{
			"If-Unmodified-Since": "Wed, 21 Oct 2015 07:28:00 GMT",
		})); ok {
			t.Fatal("time condition must not take the fast path")
		}
	})
}

func TestParseConditionalHeadersAcceptsHTTPDateFormats(t *testing.T) {
	testCases := []struct {
		name     string
		header   string
		value    string
		expected time.Time
	}{
		{
			name:     "If-Modified-Since RFC850",
			header:   s3_constants.IfModifiedSince,
			value:    "Sunday, 06-Nov-94 08:49:37 GMT",
			expected: time.Date(1994, time.November, 6, 8, 49, 37, 0, time.UTC),
		},
		{
			name:     "If-Unmodified-Since ANSIC",
			header:   s3_constants.IfUnmodifiedSince,
			value:    "Sun Nov  6 08:49:37 1994",
			expected: time.Date(1994, time.November, 6, 8, 49, 37, 0, time.UTC),
		},
		{
			// Go clients build this with t.UTC().Format(time.RFC1123); the "UTC"
			// zone is rejected by http.ParseTime but was accepted before, so the
			// RFC1123 fallback must keep it working.
			name:     "If-Modified-Since RFC1123 UTC zone",
			header:   s3_constants.IfModifiedSince,
			value:    "Wed, 21 Oct 2015 07:28:00 UTC",
			expected: time.Date(2015, time.October, 21, 7, 28, 0, 0, time.UTC),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			r := reqWith(map[string]string{testCase.header: testCase.value})

			headers, errCode := parseConditionalHeaders(r)
			if errCode != s3err.ErrNone {
				t.Fatalf("expected %s to be accepted, got %v", testCase.header, errCode)
			}
			if !headers.isSet {
				t.Fatal("expected conditional headers to be marked set")
			}
			parsed := headers.ifModifiedSince
			if testCase.header == s3_constants.IfUnmodifiedSince {
				parsed = headers.ifUnmodifiedSince
			}
			if !parsed.Equal(testCase.expected) {
				t.Fatalf("expected parsed time %v, got %v", testCase.expected, parsed)
			}
		})
	}
}

func TestValidateConditionalCopyHeadersAcceptsHTTPDateFormats(t *testing.T) {
	testCases := []struct {
		name   string
		header string
		value  string
		mtime  int64 // source mtime chosen so the condition passes
	}{
		{
			name:   "X-Amz-Copy-Source-If-Modified-Since RFC850",
			header: s3_constants.AmzCopySourceIfModifiedSince,
			value:  "Sunday, 06-Nov-94 08:49:37 GMT",
			mtime:  1577836800, // 2020-01-01, modified after the 1994 header
		},
		{
			name:   "X-Amz-Copy-Source-If-Unmodified-Since ANSIC",
			header: s3_constants.AmzCopySourceIfUnmodifiedSince,
			value:  "Sun Nov  6 08:49:37 1994",
			mtime:  631152000, // 1990-01-01, not modified after the 1994 header
		},
	}

	var s3a *S3ApiServer // method does not use the receiver
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			r := reqWith(map[string]string{testCase.header: testCase.value})
			entry := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: testCase.mtime}}

			if errCode := s3a.validateConditionalCopyHeaders(r, entry); errCode != s3err.ErrNone {
				t.Fatalf("expected %s to be accepted, got %v", testCase.header, errCode)
			}
		})
	}
}

func TestBuildDeleteCondition(t *testing.T) {
	t.Run("no If-Match is unconditional", func(t *testing.T) {
		cond, ok := buildDeleteCondition(reqWith(nil))
		if !ok || cond != nil {
			t.Fatalf("want (nil, true), got (%v, %v)", cond, ok)
		}
	})
	t.Run("If-Match * to IF_EXISTS", func(t *testing.T) {
		cond, ok := buildDeleteCondition(reqWith(map[string]string{s3_constants.IfMatch: "*"}))
		if !ok {
			t.Fatal("want ok")
		}
		if c := oneClause(t, cond); c.Kind != filer_pb.WriteCondition_IF_EXISTS {
			t.Fatalf("kind = %v", c.Kind)
		}
	})
	t.Run("If-Match etag to IF_ETAG_MATCH", func(t *testing.T) {
		cond, ok := buildDeleteCondition(reqWith(map[string]string{s3_constants.IfMatch: `"e"`}))
		if !ok {
			t.Fatal("want ok")
		}
		if c := oneClause(t, cond); c.Kind != filer_pb.WriteCondition_IF_ETAG_MATCH || c.Etags[0] != "e" {
			t.Fatalf("clause = %+v", c)
		}
	})
	t.Run("weak etag falls back", func(t *testing.T) {
		if _, ok := buildDeleteCondition(reqWith(map[string]string{s3_constants.IfMatch: `W/"e"`})); ok {
			t.Fatal("weak etag must not take the fast path")
		}
	})
}

func TestSingleStrongETag(t *testing.T) {
	cases := []struct {
		in     string
		want   string
		single bool
	}{
		{`"abc"`, "abc", true},
		{` "abc" `, "abc", true},
		{`abc`, "abc", true},
		{`W/"abc"`, "", false},
		{`w/"abc"`, "", false},
		{`"a","b"`, "", false},
	}
	for _, c := range cases {
		got, single := singleStrongETag(c.in)
		if single != c.single || (single && got != c.want) {
			t.Errorf("singleStrongETag(%q) = (%q, %v), want (%q, %v)", c.in, got, single, c.want, c.single)
		}
	}
}

// fakeTxnFiler is a minimal SeaweedFiler gRPC server that records ObjectTransaction
// calls, standing in for a live filer that a routed write fails over to.
type fakeTxnFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	calls int32
}

func (f *fakeTxnFiler) ObjectTransaction(ctx context.Context, req *filer_pb.ObjectTransactionRequest) (*filer_pb.ObjectTransactionResponse, error) {
	atomic.AddInt32(&f.calls, 1)
	return &filer_pb.ObjectTransactionResponse{}, nil
}

// startFakeTxnFiler serves impl on a random localhost port and returns the S3-style
// filer address whose ToGrpcAddress resolves back to that port.
func startFakeTxnFiler(t *testing.T, impl *fakeTxnFiler) pb.ServerAddress {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, impl)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	port := lis.Addr().(*net.TCPAddr).Port
	return pb.ServerAddress(fmt.Sprintf("127.0.0.1:1.%d", port))
}

// closedFilerAddress returns an address whose gRPC port has nothing listening,
// modeling a filer whose ring address is stale after a pod restart.
func closedFilerAddress(t *testing.T) pb.ServerAddress {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	lis.Close()
	return pb.ServerAddress(fmt.Sprintf("127.0.0.1:1.%d", port))
}

// A routed write whose owner address is dead fails over to a live filer (which
// forwards to the real owner by route_key) instead of hanging on the dead owner,
// so an object write survives a filer pod IP change without an S3 gateway restart.
func TestObjectTxnFailsOverStaleOwner(t *testing.T) {
	live := &fakeTxnFiler{}
	liveAddr := startFakeTxnFiler(t, live)
	deadOwner := closedFilerAddress(t)

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	s3a := &S3ApiServer{
		option:      &S3ApiServerOption{GrpcDialOption: dialOption},
		filerClient: wdclient.NewFilerClient([]pb.ServerAddress{liveAddr}, dialOption, ""),
	}
	req := &filer_pb.ObjectTransactionRequest{LockKey: "/buckets/b/o", RouteKey: "s3.object.write:/buckets/b/o"}

	// Owner not yet flagged: the first attempt dials it, fails fast, then fails
	// over to the live filer and flags the owner unreachable.
	resp, err := s3a.objectTxnOnFiler(deadOwner, req)
	if err != nil {
		t.Fatalf("expected failover success, got %v", err)
	}
	if resp == nil || resp.Error != "" {
		t.Fatalf("unexpected response %+v", resp)
	}
	if got := atomic.LoadInt32(&live.calls); got != 1 {
		t.Fatalf("live filer calls = %d, want 1", got)
	}
	if !s3a.ownerRecentlyUnreachable(deadOwner) {
		t.Fatal("dead owner should be flagged unreachable after the failed dial")
	}

	// Once flagged, the owner is skipped entirely and the write still lands.
	if _, err := s3a.objectTxnOnFiler(deadOwner, req); err != nil {
		t.Fatalf("expected success while owner flagged, got %v", err)
	}
	if got := atomic.LoadInt32(&live.calls); got != 2 {
		t.Fatalf("live filer calls = %d, want 2", got)
	}
}

func TestRouteWriteCondition(t *testing.T) {
	// Unconditional routes either way.
	if c, ok := routeWriteCondition(reqWith(nil), false); !ok || c != nil {
		t.Fatalf("overwrite unconditional: got (%v,%v)", c, ok)
	}
	if c, ok := routeWriteCondition(reqWith(nil), true); !ok || c != nil {
		t.Fatalf("unique unconditional: got (%v,%v)", c, ok)
	}
	// An overwrite carries a reducible condition.
	if c, ok := routeWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: `"e"`}), false); !ok || c == nil {
		t.Fatalf("overwrite conditional should route: got (%v,%v)", c, ok)
	}
	// A conditional unique (versioned) write bails to the lock path.
	if _, ok := routeWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: `"e"`}), true); ok {
		t.Fatal("conditional unique write must not route")
	}
	// A non-reducible condition bails regardless.
	if _, ok := routeWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: `W/"e"`}), false); ok {
		t.Fatal("weak etag must not route")
	}
}

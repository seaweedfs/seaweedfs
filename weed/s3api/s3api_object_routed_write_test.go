package s3api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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
		if c.Kind != filer_pb.WriteCondition_IF_ETAG_MATCH || len(c.Etags) != 1 || c.Etags[0] != `"abc123"` {
			t.Fatalf("clause = %+v", c)
		}
	})
	t.Run("If-None-Match strong etag to IF_ETAG_NOT_MATCH", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(map[string]string{s3_constants.IfNoneMatch: `"abc123"`}))
		if !ok {
			t.Fatal("want ok")
		}
		c := oneClause(t, cond)
		if c.Kind != filer_pb.WriteCondition_IF_ETAG_NOT_MATCH || len(c.Etags) != 1 || c.Etags[0] != `"abc123"` {
			t.Fatalf("clause = %+v", c)
		}
	})
	t.Run("weak etag reduces verbatim", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: `W/"abc"`}))
		if !ok {
			t.Fatal("want ok")
		}
		c := oneClause(t, cond)
		if c.Kind != filer_pb.WriteCondition_IF_ETAG_MATCH || len(c.Etags) != 1 || c.Etags[0] != `W/"abc"` || c.AllowWeak {
			t.Fatalf("clause = %+v", c)
		}
	})
	t.Run("etag list reduces", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: `"a", "b"`}))
		if !ok {
			t.Fatal("want ok")
		}
		c := oneClause(t, cond)
		if c.Kind != filer_pb.WriteCondition_IF_ETAG_MATCH || len(c.Etags) != 2 || c.Etags[0] != `"a"` || c.Etags[1] != `"b"` {
			t.Fatalf("clause = %+v", c)
		}
	})
	t.Run("both match and none-match reduce to clauses", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(map[string]string{
			s3_constants.IfMatch:     "*",
			s3_constants.IfNoneMatch: "*",
		}))
		if !ok {
			t.Fatal("want ok")
		}
		if len(cond.Clauses) != 2 || cond.Clauses[0].Kind != filer_pb.WriteCondition_IF_EXISTS || cond.Clauses[1].Kind != filer_pb.WriteCondition_IF_NOT_EXISTS {
			t.Fatalf("clauses = %+v", cond.Clauses)
		}
	})
	t.Run("time-based reduces to a unix bound", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(map[string]string{
			"If-Unmodified-Since": "Wed, 21 Oct 2015 07:28:00 GMT",
		}))
		if !ok {
			t.Fatal("want ok")
		}
		c := oneClause(t, cond)
		want := time.Date(2015, 10, 21, 7, 28, 0, 0, time.UTC).Unix()
		if c.Kind != filer_pb.WriteCondition_IF_UNMODIFIED_SINCE || c.UnixTime != want {
			t.Fatalf("clause = %+v, want unix %d", c, want)
		}
	})
	t.Run("If-Match hides If-Unmodified-Since", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(map[string]string{
			s3_constants.IfMatch:  `"abc"`,
			"If-Unmodified-Since": "Wed, 21 Oct 2015 07:28:00 GMT",
		}))
		if !ok {
			t.Fatal("want ok")
		}
		if c := oneClause(t, cond); c.Kind != filer_pb.WriteCondition_IF_ETAG_MATCH {
			t.Fatalf("clause = %+v", c)
		}
	})
	t.Run("If-None-Match hides If-Modified-Since", func(t *testing.T) {
		cond, ok := buildWriteCondition(reqWith(map[string]string{
			s3_constants.IfNoneMatch: `"abc"`,
			"If-Modified-Since":      "Wed, 21 Oct 2015 07:28:00 GMT",
		}))
		if !ok {
			t.Fatal("want ok")
		}
		if c := oneClause(t, cond); c.Kind != filer_pb.WriteCondition_IF_ETAG_NOT_MATCH {
			t.Fatalf("clause = %+v", c)
		}
	})
	t.Run("unparseable date falls back", func(t *testing.T) {
		if _, ok := buildWriteCondition(reqWith(map[string]string{
			"If-Modified-Since": "not a date",
		})); ok {
			t.Fatal("unparseable headers must not reduce")
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
		if cond := buildDeleteCondition(reqWith(nil)); cond != nil {
			t.Fatalf("want nil, got %v", cond)
		}
	})
	t.Run("If-Match * to IF_EXISTS", func(t *testing.T) {
		if c := oneClause(t, buildDeleteCondition(reqWith(map[string]string{s3_constants.IfMatch: "*"}))); c.Kind != filer_pb.WriteCondition_IF_EXISTS {
			t.Fatalf("kind = %v", c.Kind)
		}
	})
	t.Run("If-Match etag to IF_ETAG_MATCH", func(t *testing.T) {
		if c := oneClause(t, buildDeleteCondition(reqWith(map[string]string{s3_constants.IfMatch: `"e"`}))); c.Kind != filer_pb.WriteCondition_IF_ETAG_MATCH || len(c.Etags) != 1 || c.Etags[0] != `"e"` {
			t.Fatalf("clause = %+v", c)
		}
	})
	t.Run("an ETag list carries every candidate", func(t *testing.T) {
		c := oneClause(t, buildDeleteCondition(reqWith(map[string]string{s3_constants.IfMatch: `"a", W/"b"`})))
		if c.Kind != filer_pb.WriteCondition_IF_ETAG_MATCH || len(c.Etags) != 2 || c.Etags[0] != `"a"` || c.Etags[1] != `W/"b"` {
			t.Fatalf("clause = %+v", c)
		}
	})
}

// fakeTxnFiler is a minimal SeaweedFiler gRPC server that records ObjectTransaction
// calls, standing in for a live filer that a routed write fails over to.
type fakeTxnFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	calls   int32
	lastReq *filer_pb.ObjectTransactionRequest
	reqs    []*filer_pb.ObjectTransactionRequest
	resp    *filer_pb.ObjectTransactionResponse
	err     error
}

func (f *fakeTxnFiler) ObjectTransaction(ctx context.Context, req *filer_pb.ObjectTransactionRequest) (*filer_pb.ObjectTransactionResponse, error) {
	atomic.AddInt32(&f.calls, 1)
	f.lastReq = req
	f.reqs = append(f.reqs, req)
	if f.resp != nil || f.err != nil {
		return f.resp, f.err
	}
	return &filer_pb.ObjectTransactionResponse{}, nil
}

// startFakeFiler serves impl on a random localhost port and returns the S3-style
// filer address whose ToGrpcAddress resolves back to that port.
func startFakeFiler(t *testing.T, impl filer_pb.SeaweedFilerServer) pb.ServerAddress {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, impl)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return fakeFilerAddress(lis)
}

// fakeFilerAddress returns the S3-style filer address of lis. The gRPC
// connection cache is process-wide and keyed by address, so it drops whatever
// channel an earlier test left for this port: a failed one would fail the first
// RPC with a stale "connection refused", and one to a stopped fake filer can
// still look ready.
func fakeFilerAddress(lis net.Listener) pb.ServerAddress {
	address := pb.ServerAddress(fmt.Sprintf("127.0.0.1:1.%d", lis.Addr().(*net.TCPAddr).Port))
	pb.InvalidateGrpcConnection(address.ToGrpcAddress())
	return address
}

func TestRoutedDeleteSpecificVersionUsesWormCondition(t *testing.T) {
	versionId := "672a75d526ef29c79fe6b5680cad4a0d"
	versionFile := "v_" + versionId
	filer := &fakeTxnFiler{
		resp: &filer_pb.ObjectTransactionResponse{
			Error:     "precondition failed",
			ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED,
		},
	}
	owner := startFakeFiler(t, filer)
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath:    "/buckets",
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}

	if code := s3a.routedDeleteSpecificVersion(owner, "b", "obj", versionId, true, false, false, nil); code != s3err.ErrAccessDenied {
		t.Fatalf("locked version delete returned %v, want %v", code, s3err.ErrAccessDenied)
	}

	req := filer.lastReq
	if req == nil {
		t.Fatal("expected an ObjectTransaction request")
	}
	if req.LockKey != "/buckets/b/obj" {
		t.Fatalf("LockKey = %q", req.LockKey)
	}
	if req.ConditionKey != "/buckets/b/obj.versions/"+versionFile {
		t.Fatalf("ConditionKey = %q", req.ConditionKey)
	}
	if req.RouteKey != "s3.object.write:/buckets/b/obj" {
		t.Fatalf("RouteKey = %q", req.RouteKey)
	}

	if len(req.Condition.GetClauses()) != 2 {
		t.Fatalf("condition clauses = %d, want 2", len(req.Condition.GetClauses()))
	}
	hold := req.Condition.GetClauses()[0]
	if hold.Kind != filer_pb.WriteCondition_IF_EXTENDED_NOT_EQUAL ||
		hold.ExtKey != s3_constants.ExtLegalHoldKey ||
		hold.ExtValue != s3_constants.LegalHoldOn {
		t.Fatalf("legal hold clause = %+v", hold)
	}
	retain := req.Condition.GetClauses()[1]
	if retain.Kind != filer_pb.WriteCondition_IF_EXTENDED_TIME_ELAPSED ||
		retain.ExtKey != s3_constants.ExtRetentionUntilDateKey ||
		retain.GateKey != "" {
		t.Fatalf("retention clause = %+v", retain)
	}

	if len(req.Mutations) != 2 {
		t.Fatalf("mutations = %d, want 2", len(req.Mutations))
	}
	recompute := req.Mutations[0]
	if recompute.Type != filer_pb.ObjectMutation_RECOMPUTE_LATEST ||
		recompute.GetRecompute().GetExcludeName() != versionFile ||
		recompute.GetRecompute().GetOnlyIfCurrentName() != versionFile {
		t.Fatalf("recompute mutation = %+v", recompute)
	}
	deleteVersion := req.Mutations[1]
	if deleteVersion.Type != filer_pb.ObjectMutation_DELETE ||
		deleteVersion.Directory != "/buckets/b/obj.versions" ||
		deleteVersion.Name != versionFile ||
		!deleteVersion.IsDeleteData ||
		!deleteVersion.RemoveEmptyParent {
		t.Fatalf("delete mutation = %+v", deleteVersion)
	}
}

// TestRoutedDeleteSpecificVersionGuardsRepoint checks that the repoint rides in
// the version's own delete transaction, so the WORM guards gate both, and that
// the recompute carries an only-while-current guard — the filer applies it only
// while the pointer still names the deleted version, so a noncurrent delete
// leaves a cleared (suspended) or moved pointer alone.
func TestRoutedDeleteSpecificVersionGuardsRepoint(t *testing.T) {
	versionId := "672a75d526ef29c79fe6b5680cad4a0d"
	versionFile := "v_" + versionId
	newS3a := func() *S3ApiServer {
		return &S3ApiServer{option: &S3ApiServerOption{
			BucketsPath:    "/buckets",
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		}}
	}

	t.Run("one transaction behind the version's lock checks", func(t *testing.T) {
		filer := &fakeTxnFiler{}
		owner := startFakeFiler(t, filer)
		if code := newS3a().routedDeleteSpecificVersion(owner, "b", "obj", versionId, false, false, false, nil); code != s3err.ErrNone {
			t.Fatalf("delete returned %v, want %v", code, s3err.ErrNone)
		}
		if len(filer.reqs) != 1 {
			t.Fatalf("transactions = %d, want 1", len(filer.reqs))
		}
		req := filer.reqs[0]
		if req.ConditionKey != "/buckets/b/obj.versions/"+versionFile {
			t.Fatalf("ConditionKey = %q", req.ConditionKey)
		}
		if len(req.Mutations) != 2 ||
			req.Mutations[0].Type != filer_pb.ObjectMutation_RECOMPUTE_LATEST ||
			req.Mutations[0].GetRecompute().GetOnlyIfCurrentName() != versionFile ||
			req.Mutations[1].Type != filer_pb.ObjectMutation_DELETE {
			t.Fatalf("mutations = %+v, want guarded recompute + delete", req.Mutations)
		}
	})

	t.Run("metadata-only delete keeps the data", func(t *testing.T) {
		filer := &fakeTxnFiler{}
		owner := startFakeFiler(t, filer)
		if code := newS3a().routedDeleteSpecificVersion(owner, "b", "obj", versionId, false, false, true, nil); code != s3err.ErrNone {
			t.Fatalf("delete returned %v, want %v", code, s3err.ErrNone)
		}
		if filer.lastReq.Mutations[1].IsDeleteData {
			t.Fatal("metadata-only delete set IsDeleteData")
		}
	})
}

func TestWormDeleteConditionForGovernanceBypass(t *testing.T) {
	cond := wormDeleteCondition(true, true)
	if len(cond.GetClauses()) != 2 {
		t.Fatalf("condition clauses = %d, want 2", len(cond.GetClauses()))
	}
	retain := cond.GetClauses()[1]
	if retain.Kind != filer_pb.WriteCondition_IF_EXTENDED_TIME_ELAPSED ||
		retain.ExtKey != s3_constants.ExtRetentionUntilDateKey ||
		retain.GateKey != s3_constants.ExtObjectLockModeKey ||
		retain.GateValue != s3_constants.RetentionModeCompliance {
		t.Fatalf("retention clause = %+v", retain)
	}
}

// closedFilerAddress returns an address whose gRPC port has nothing listening,
// modeling a filer whose ring address is stale after a pod restart.
func closedFilerAddress(t *testing.T) pb.ServerAddress {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lis.Close()
	return fakeFilerAddress(lis)
}

// A fake filer that gets the port of a dead owner an earlier test dialed must
// not inherit that owner's failed channel.
func TestFakeFilerOnReusedPortIsReachable(t *testing.T) {
	live := &fakeTxnFiler{}
	liveAddr := startFakeFiler(t, live)
	deadOwner := closedFilerAddress(t)
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	s3a := &S3ApiServer{
		option:      &S3ApiServerOption{GrpcDialOption: dialOption},
		filerClient: wdclient.NewFilerClient([]pb.ServerAddress{liveAddr}, dialOption, ""),
	}
	if _, err := s3a.objectTxnOnFiler(deadOwner, &filer_pb.ObjectTransactionRequest{LockKey: "/buckets/b/o", RouteKey: "s3.object.write:/buckets/b/o"}); err != nil {
		t.Fatalf("expected failover success, got %v", err)
	}

	lis, err := net.Listen("tcp", deadOwner.ToGrpcAddress())
	if err != nil {
		t.Skipf("port of the dead owner was taken: %v", err)
	}
	reused := &fakeTxnFiler{}
	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, reused)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	direct := &S3ApiServer{option: &S3ApiServerOption{GrpcDialOption: dialOption}}
	if _, err := direct.objectTxnOnFiler(fakeFilerAddress(lis), &filer_pb.ObjectTransactionRequest{LockKey: "/buckets/b/o"}); err != nil {
		t.Fatalf("fake filer on a reused port: %v", err)
	}
	if got := atomic.LoadInt32(&reused.calls); got != 1 {
		t.Fatalf("reused-port filer calls = %d, want 1", got)
	}
}

// A routed write whose owner address is dead fails over to a live filer (which
// forwards to the real owner by route_key) instead of hanging on the dead owner,
// so an object write survives a filer pod IP change without an S3 gateway restart.
func TestObjectTxnFailsOverStaleOwner(t *testing.T) {
	live := &fakeTxnFiler{}
	liveAddr := startFakeFiler(t, live)
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

// Issue 11631: a transaction the owner may have applied must not be re-sent to
// another filer — a lost response is indistinguishable from a commit. Failover
// is only for channels that never came up, which provably sent nothing.
func TestObjectTxnNoReplayAfterSend(t *testing.T) {
	owner := &fakeTxnFiler{err: status.Error(codes.Unavailable, "response lost")}
	ownerAddr := startFakeFiler(t, owner)
	backup := &fakeTxnFiler{}
	backupAddr := startFakeFiler(t, backup)

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	s3a := &S3ApiServer{
		option:      &S3ApiServerOption{GrpcDialOption: dialOption},
		filerClient: wdclient.NewFilerClient([]pb.ServerAddress{ownerAddr, backupAddr}, dialOption, ""),
	}
	req := &filer_pb.ObjectTransactionRequest{LockKey: "/buckets/b/o", RouteKey: "s3.object.write:/buckets/b/o"}

	if _, err := s3a.objectTxnOnFiler(ownerAddr, req); err == nil {
		t.Fatal("expected the lost response to surface as an error")
	}
	if got := atomic.LoadInt32(&backup.calls); got != 0 {
		t.Fatalf("backup filer calls = %d — replayed a possibly-committed transaction", got)
	}
}

// A completed multipart upload commits in one transaction: the version file's
// PUT, the metadata-only removal of .uploads/<id> (its chunks are the object's
// chunks), and the latest-pointer recompute, in that order.
func TestRoutedMultipartFinalize(t *testing.T) {
	filer := &fakeTxnFiler{}
	owner := startFakeFiler(t, filer)
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath:    "/buckets",
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}

	chunks := []*filer_pb.FileChunk{{FileId: "1,01637037d6"}}
	code := s3a.routedMultipartFinalize(owner, "b", "obj", false, "/buckets/b/obj.versions", "v_1", chunks, func(entry *filer_pb.Entry) {
		entry.Extended = map[string][]byte{s3_constants.SeaweedFSUploadId: []byte("up1")}
	}, "up1", clause(filer_pb.WriteCondition_IF_NOT_EXISTS))
	if code != s3err.ErrNone {
		t.Fatalf("routedMultipartFinalize = %v", code)
	}

	req := filer.lastReq
	if req == nil {
		t.Fatal("expected an ObjectTransaction request")
	}
	if req.LockKey != "/buckets/b/obj" {
		t.Fatalf("LockKey = %q", req.LockKey)
	}
	if req.RouteKey != "s3.object.write:/buckets/b/obj" {
		t.Fatalf("RouteKey = %q", req.RouteKey)
	}
	// The request condition guards the object (at lock_key, falling back to
	// .versions); the upload directory rides the PUT as a mutation guard.
	if req.ConditionKey != "" {
		t.Fatalf("ConditionKey = %q", req.ConditionKey)
	}
	if req.ConditionAltKey != "/buckets/b/obj.versions" {
		t.Fatalf("ConditionAltKey = %q", req.ConditionAltKey)
	}
	if req.Condition == nil || len(req.Condition.Clauses) != 1 || req.Condition.Clauses[0].Kind != filer_pb.WriteCondition_IF_NOT_EXISTS {
		t.Fatalf("Condition = %+v", req.Condition)
	}
	if len(req.Mutations) != 3 {
		t.Fatalf("mutations = %d, want 3", len(req.Mutations))
	}

	put := req.Mutations[0]
	if put.Type != filer_pb.ObjectMutation_PUT ||
		put.Directory != "/buckets/b/obj.versions" ||
		put.Entry == nil ||
		put.Entry.Name != "v_1" ||
		len(put.Entry.Chunks) != 1 ||
		string(put.Entry.Extended[s3_constants.SeaweedFSUploadId]) != "up1" {
		t.Fatalf("put mutation = %+v", put)
	}
	if put.ConditionKey != "/buckets/b/.uploads/up1" ||
		put.Condition == nil || len(put.Condition.Clauses) != 1 || put.Condition.Clauses[0].Kind != filer_pb.WriteCondition_IF_EXISTS {
		t.Fatalf("put condition = %+v key %q", put.Condition, put.ConditionKey)
	}
	removeUpload := req.Mutations[1]
	if removeUpload.Type != filer_pb.ObjectMutation_DELETE ||
		removeUpload.Directory != "/buckets/b/.uploads" ||
		removeUpload.Name != "up1" ||
		!removeUpload.IsRecursive ||
		removeUpload.IsDeleteData {
		t.Fatalf("remove upload mutation = %+v", removeUpload)
	}
	if req.Mutations[2].Type != filer_pb.ObjectMutation_RECOMPUTE_LATEST {
		t.Fatalf("recompute mutation = %+v", req.Mutations[2])
	}
}

// A non-versioned multipart completion removes .uploads/<id> metadata-only in
// the same transaction as the object's PUT.
func TestWriteMultipartObjectRemovesUploadDir(t *testing.T) {
	filer := &fakeTxnFiler{}
	owner := startFakeFiler(t, filer)
	s3a := &S3ApiServer{
		option: &S3ApiServerOption{
			BucketsPath:    "/buckets",
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	}

	if removal, err := s3a.routedUploadRemoval(context.Background(), "", "/buckets/b/.uploads/up1", "b", "up1", &multipartCompletionState{}); removal != nil || err != nil {
		t.Fatalf("unrouted write should not carry the removal, got %+v, %v", removal, err)
	}
	removal, err := s3a.routedUploadRemoval(context.Background(), owner, "/buckets/b/.uploads/up1", "b", "up1", &multipartCompletionState{})
	if err != nil {
		t.Fatalf("routedUploadRemoval: %v", err)
	}
	if err := s3a.writeMultipartObject(owner, "s3.object.write:/buckets/b/o", "/buckets/b", "o", nil, nil, removal, nil, ""); err != nil {
		t.Fatalf("writeMultipartObject: %v", err)
	}

	req := filer.lastReq
	if req == nil {
		t.Fatal("expected an ObjectTransaction request")
	}
	if req.LockKey != "/buckets/b/o" {
		t.Fatalf("LockKey = %q", req.LockKey)
	}
	if len(req.Mutations) != 2 {
		t.Fatalf("mutations = %d, want 2", len(req.Mutations))
	}
	put := req.Mutations[0]
	if put.Type != filer_pb.ObjectMutation_PUT || put.Directory != "/buckets/b" || put.Entry == nil || put.Entry.Name != "o" {
		t.Fatalf("put mutation = %+v", put)
	}
	if put.ConditionKey != "/buckets/b/.uploads/up1" ||
		put.Condition == nil || len(put.Condition.Clauses) != 1 || put.Condition.Clauses[0].Kind != filer_pb.WriteCondition_IF_EXISTS {
		t.Fatalf("put condition = %+v key %q", put.Condition, put.ConditionKey)
	}
	removeUpload := req.Mutations[1]
	if removeUpload.Type != filer_pb.ObjectMutation_DELETE ||
		removeUpload.Directory != "/buckets/b/.uploads" ||
		removeUpload.Name != "up1" ||
		!removeUpload.IsRecursive ||
		removeUpload.IsDeleteData {
		t.Fatalf("remove upload mutation = %+v", removeUpload)
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
	// A weak ETag still routes — the clause carries it verbatim.
	if _, ok := routeWriteCondition(reqWith(map[string]string{s3_constants.IfMatch: `W/"e"`}), false); !ok {
		t.Fatal("weak etag should route")
	}
	// An unparseable condition bails regardless.
	if _, ok := routeWriteCondition(reqWith(map[string]string{"If-Modified-Since": "not a date"}), false); ok {
		t.Fatal("unparseable headers must not route")
	}
}

// A conditional version delete guards the version inside its own transaction,
// ahead of the repoint, and reports that guard failing as a failed
// precondition rather than an object-lock denial.
func TestRoutedVersionDeleteCarriesIfMatch(t *testing.T) {
	versionId := "672a75d526ef29c79fe6b5680cad4a0d"
	ifMatch := &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{Kind: filer_pb.WriteCondition_IF_ETAG_MATCH, Etags: []string{"abc"}}}}
	newS3a := func() *S3ApiServer {
		return &S3ApiServer{option: &S3ApiServerOption{
			BucketsPath:    "/buckets",
			GrpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		}}
	}
	guarded := func(t *testing.T, m *filer_pb.ObjectMutation, conditionKey string) {
		t.Helper()
		clauses := m.GetCondition().GetClauses()
		if len(clauses) != 1 || clauses[0].Kind != filer_pb.WriteCondition_IF_ETAG_MATCH || len(clauses[0].Etags) != 1 || clauses[0].Etags[0] != "abc" {
			t.Fatalf("mutation condition = %+v, want If-Match abc", m.GetCondition())
		}
		if m.ConditionKey != conditionKey {
			t.Fatalf("mutation condition key = %q, want %q", m.ConditionKey, conditionKey)
		}
	}
	firstMutation := int32(0)
	cases := []struct {
		name string
		resp *filer_pb.ObjectTransactionResponse
		want s3err.ErrorCode
	}{
		{"applied", nil, s3err.ErrNone},
		{"if-match failed", &filer_pb.ObjectTransactionResponse{ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED, FailedMutation: &firstMutation}, s3err.ErrPreconditionFailed},
		{"object lock denied", &filer_pb.ObjectTransactionResponse{ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, s3err.ErrAccessDenied},
	}
	for _, tc := range cases {
		t.Run("version/"+tc.name, func(t *testing.T) {
			filer := &fakeTxnFiler{resp: tc.resp}
			owner := startFakeFiler(t, filer)
			if code := newS3a().routedDeleteSpecificVersion(owner, "b", "obj", versionId, true, false, false, ifMatch); code != tc.want {
				t.Fatalf("delete returned %v, want %v", code, tc.want)
			}
			if filer.lastReq.Mutations[0].Type != filer_pb.ObjectMutation_RECOMPUTE_LATEST {
				t.Fatalf("first mutation = %v, want the repoint", filer.lastReq.Mutations[0].Type)
			}
			guarded(t, filer.lastReq.Mutations[0], "/buckets/b/obj.versions/v_"+versionId)
		})
		t.Run("null/"+tc.name, func(t *testing.T) {
			filer := &fakeTxnFiler{resp: tc.resp}
			owner := startFakeFiler(t, filer)
			code, settled := newS3a().routedDeleteNullVersion(owner, "b", "obj", true, false, false, ifMatch)
			if !settled || code != tc.want {
				t.Fatalf("delete returned (%v, %v), want (%v, true)", code, settled, tc.want)
			}
			guarded(t, filer.lastReq.Mutations[0], "")
		})
	}
	// The owner may have committed before the error, so only an unconditional
	// null delete may retry on the lock path.
	t.Run("null/transport error", func(t *testing.T) {
		filer := &fakeTxnFiler{err: status.Error(codes.Unavailable, "response lost")}
		owner := startFakeFiler(t, filer)
		if code, settled := newS3a().routedDeleteNullVersion(owner, "b", "obj", false, false, false, ifMatch); !settled || code != s3err.ErrInternalError {
			t.Fatalf("conditional delete returned (%v, %v), want (%v, true)", code, settled, s3err.ErrInternalError)
		}
		if code, settled := newS3a().routedDeleteNullVersion(owner, "b", "obj", false, false, false, nil); settled || code != s3err.ErrNone {
			t.Fatalf("unconditional delete returned (%v, %v), want a lock-path fallback", code, settled)
		}
	})
	// After an owner failure other than the nested-entry refusal, a
	// conditional null delete must not re-check on the lock path.
	t.Run("null/owner error", func(t *testing.T) {
		for _, msg := range []string{
			"mutation 0: delete file /buckets/b/obj: store unavailable",
			"mutation 0: delete file /buckets/b/" + filer.MsgFailDelNonEmptyFolder + ": store unavailable",
		} {
			fake := &fakeTxnFiler{resp: &filer_pb.ObjectTransactionResponse{Error: msg, FailedMutation: &firstMutation}}
			owner := startFakeFiler(t, fake)
			if code, settled := newS3a().routedDeleteNullVersion(owner, "b", "obj", false, false, false, ifMatch); !settled || code != s3err.ErrInternalError {
				t.Fatalf("%q: conditional delete returned (%v, %v), want (%v, true)", msg, code, settled, s3err.ErrInternalError)
			}
			if code, settled := newS3a().routedDeleteNullVersion(owner, "b", "obj", false, false, false, nil); settled || code != s3err.ErrNone {
				t.Fatalf("%q: unconditional delete returned (%v, %v), want a lock-path fallback", msg, code, settled)
			}
		}
	})
	// An entry other keys are nested under is stripped on the owner under the
	// same guard plus the entry it was read as, so a routed delete that runs
	// once the last nested key is gone cannot pass the check as well.
	t.Run("null/nested entry", func(t *testing.T) {
		object := &filer_pb.Entry{
			Name:        "obj",
			IsDirectory: true,
			Attributes:  &filer_pb.FuseAttributes{Mime: "text/plain", FileSize: 3, Md5: []byte{0xab}},
			Chunks:      []*filer_pb.FileChunk{{FileId: "1,ab", Size: 3}},
			Extended:    map[string][]byte{s3_constants.ExtETagKey: []byte(`"abc"`), "xattr-user.k": []byte("v")},
		}
		for _, tc := range []struct {
			name   string
			entry  *filer_pb.Entry
			demote *filer_pb.ObjectTransactionResponse
			want   s3err.ErrorCode
			txns   int
		}{
			{"demoted", object, nil, s3err.ErrNone, 2},
			{"guard failed", object, &filer_pb.ObjectTransactionResponse{ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED, FailedMutation: &firstMutation}, s3err.ErrPreconditionFailed, 2},
			{"object lock denied", object, &filer_pb.ObjectTransactionResponse{ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, s3err.ErrAccessDenied, 2},
			{"deleted by a routed delete", nil, nil, s3err.ErrPreconditionFailed, 1},
			{"already demoted", &filer_pb.Entry{Name: "obj", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}}, nil, s3err.ErrPreconditionFailed, 1},
		} {
			fake := &fakeNestedFiler{entry: tc.entry, demote: tc.demote}
			owner := startFakeFiler(t, fake)
			if code, settled := newS3a().routedDeleteNullVersion(owner, "b", "obj", true, false, false, ifMatch); !settled || code != tc.want {
				t.Fatalf("%s: delete returned (%v, %v), want (%v, true)", tc.name, code, settled, tc.want)
			}
			if len(fake.reqs) != tc.txns {
				t.Fatalf("%s: %d transactions, want %d", tc.name, len(fake.reqs), tc.txns)
			}
			if tc.txns < 2 {
				continue
			}
			req := fake.reqs[1]
			if len(req.GetCondition().GetClauses()) == 0 || req.LockKey != "/buckets/b/obj" {
				t.Fatalf("%s: demote request = %+v, want the object lock guards on its key", tc.name, req)
			}
			m := req.Mutations[0]
			if m.Type != filer_pb.ObjectMutation_PUT || m.Directory != "/buckets/b" || m.Entry.GetName() != "obj" || !m.Entry.IsDirectory {
				t.Fatalf("%s: demote mutation = %+v", tc.name, m)
			}
			if m.Entry.IsDirectoryKeyObject() || len(m.Entry.Attributes.Md5) > 0 || m.Entry.Extended[s3_constants.ExtETagKey] != nil || string(m.Entry.Extended["xattr-user.k"]) != "v" {
				t.Fatalf("%s: demoted entry %+v still holds the object or lost the directory xattr", tc.name, m.Entry)
			}
			clauses := m.GetCondition().GetClauses()
			if len(clauses) != 2 || clauses[0].Kind != filer_pb.WriteCondition_IF_ETAG_MATCH ||
				clauses[1].Kind != filer_pb.WriteCondition_IF_ENTRY_EQUAL || !proto.Equal(clauses[1].ExpectedEntry, tc.entry) {
				t.Fatalf("%s: demote condition = %+v, want If-Match and the entry it was read as", tc.name, m.GetCondition())
			}
		}
		fake := &fakeNestedFiler{entry: object}
		owner := startFakeFiler(t, fake)
		if code, settled := newS3a().routedDeleteNullVersion(owner, "b", "obj", false, false, false, nil); settled || code != s3err.ErrNone || len(fake.reqs) != 1 {
			t.Fatalf("unconditional delete returned (%v, %v) after %d transactions, want a lock-path fallback", code, settled, len(fake.reqs))
		}
	})
	// A stripped nested entry stays behind as a plain directory, which
	// IF_EXISTS still matches, so If-Match: * asks the owner for an object.
	t.Run("null/If-Match *", func(t *testing.T) {
		star := &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{Kind: filer_pb.WriteCondition_IF_EXISTS}}}
		objectKind := func(t *testing.T, m *filer_pb.ObjectMutation) {
			t.Helper()
			if c := m.GetCondition().GetClauses(); len(c) == 0 || c[0].Kind != filer_pb.WriteCondition_IF_OBJECT_EXISTS {
				t.Fatalf("mutation condition = %+v, want IF_OBJECT_EXISTS first", m.GetCondition())
			}
		}
		del := &fakeTxnFiler{}
		if code, settled := newS3a().routedDeleteNullVersion(startFakeFiler(t, del), "b", "obj", false, false, false, star); !settled || code != s3err.ErrNone {
			t.Fatalf("delete returned (%v, %v), want (%v, true)", code, settled, s3err.ErrNone)
		}
		objectKind(t, del.lastReq.Mutations[0])
		nested := &fakeNestedFiler{entry: &filer_pb.Entry{Name: "obj", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{Mime: "text/plain"}}}
		if code, settled := newS3a().routedDeleteNullVersion(startFakeFiler(t, nested), "b", "obj", false, false, false, star); !settled || code != s3err.ErrNone || len(nested.reqs) != 2 {
			t.Fatalf("nested delete returned (%v, %v) after %d transactions, want a demote", code, settled, len(nested.reqs))
		}
		objectKind(t, nested.reqs[0].Mutations[0])
		if star.Clauses[0].Kind != filer_pb.WriteCondition_IF_EXISTS {
			t.Fatalf("caller's condition changed to %v", star.Clauses[0].Kind)
		}
		// A filer that predates IF_OBJECT_EXISTS fails it closed; the delete is
		// then settled on the entry as read, under IF_EXISTS and that entry.
		guardFailed := &filer_pb.ObjectTransactionResponse{ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED, FailedMutation: &firstMutation}
		file := &filer_pb.Entry{Name: "obj", Attributes: &filer_pb.FuseAttributes{FileSize: 3}}
		for _, tc := range []struct {
			name  string
			entry *filer_pb.Entry
			want  s3err.ErrorCode
			kind  filer_pb.ObjectMutation_Type
		}{
			{"file", file, s3err.ErrNone, filer_pb.ObjectMutation_DELETE},
			{"nested object", nested.entry, s3err.ErrNone, filer_pb.ObjectMutation_PUT},
			{"plain directory", &filer_pb.Entry{Name: "obj", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}}, s3err.ErrPreconditionFailed, 0},
			{"absent", nil, s3err.ErrPreconditionFailed, 0},
		} {
			old := &fakeNestedFiler{entry: tc.entry, first: guardFailed}
			if code, settled := newS3a().routedDeleteNullVersion(startFakeFiler(t, old), "b", "obj", false, false, false, star); !settled || code != tc.want {
				t.Fatalf("older filer, %s: delete returned (%v, %v), want (%v, true)", tc.name, code, settled, tc.want)
			}
			if tc.want != s3err.ErrNone {
				if len(old.reqs) != 1 {
					t.Fatalf("older filer, %s: %d transactions, want 1", tc.name, len(old.reqs))
				}
				continue
			}
			m := old.reqs[1].Mutations[0]
			if c := m.GetCondition().GetClauses(); m.Type != tc.kind || len(c) != 2 || c[0].Kind != filer_pb.WriteCondition_IF_EXISTS ||
				c[1].Kind != filer_pb.WriteCondition_IF_ENTRY_EQUAL || !proto.Equal(c[1].ExpectedEntry, tc.entry) {
				t.Fatalf("older filer, %s: retry = %+v, want a %v under IF_EXISTS and the entry read", tc.name, m, tc.kind)
			}
		}
	})
}

// fakeNestedFiler answers the first transaction with first, else refuses its
// delete because other keys are nested under the entry, serves entry to
// lookups, and answers later transactions with demote.
type fakeNestedFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	entry  *filer_pb.Entry
	first  *filer_pb.ObjectTransactionResponse
	demote *filer_pb.ObjectTransactionResponse
	reqs   []*filer_pb.ObjectTransactionRequest
}

func (f *fakeNestedFiler) ObjectTransaction(ctx context.Context, req *filer_pb.ObjectTransactionRequest) (*filer_pb.ObjectTransactionResponse, error) {
	f.reqs = append(f.reqs, req)
	if len(f.reqs) == 1 && f.first != nil {
		return f.first, nil
	}
	if len(f.reqs) == 1 {
		failed := int32(0)
		return &filer_pb.ObjectTransactionResponse{Error: fmt.Sprintf("mutation 0: %v", fmt.Errorf("%w: %s", filer.ErrNonEmptyFolder, req.LockKey)), FailedMutation: &failed}, nil
	}
	if f.demote != nil {
		return f.demote, nil
	}
	return &filer_pb.ObjectTransactionResponse{}, nil
}

func (f *fakeNestedFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if f.entry == nil {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: f.entry}, nil
}

package s3_lifecycle

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type failoverFront struct {
	testFront
	dieOn int
	hold  bool
	stop  func()

	mu   sync.Mutex
	reqs []*s3_lifecycle_pb.LifecycleDeleteRequest
}

// LifecycleDelete applies the request; on the dieOn-th call the server then
// stops before replying, like a front lost after the delete landed.
func (f *failoverFront) LifecycleDelete(ctx context.Context, req *s3_lifecycle_pb.LifecycleDeleteRequest) (*s3_lifecycle_pb.LifecycleDeleteResponse, error) {
	if f.hold {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	resp, err := f.testFront.LifecycleDelete(ctx, req)
	f.mu.Lock()
	f.reqs = append(f.reqs, proto.Clone(req).(*s3_lifecycle_pb.LifecycleDeleteRequest))
	n := len(f.reqs)
	f.mu.Unlock()
	if n == f.dieOn {
		go f.stop()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return resp, err
}

func (f *failoverFront) received() []*s3_lifecycle_pb.LifecycleDeleteRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*s3_lifecycle_pb.LifecycleDeleteRequest(nil), f.reqs...)
}

func startFailoverFront(t *testing.T, f *failoverFront) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	s3_lifecycle_pb.RegisterSeaweedS3LifecycleInternalServer(s, f)
	f.stop = s.Stop
	go s.Serve(lis)
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func downAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

type reports struct {
	mu   sync.Mutex
	msgs []string
}

func (r *reports) add(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, msg)
}

func (r *reports) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.msgs...)
}

func newTestFailoverClient(t *testing.T, ctx context.Context, endpoints []string, r *reports) *failoverClient {
	t.Helper()
	h := newTestHandler()
	conn, addr, skipped := h.selectS3Front(ctx, endpoints)
	if conn == nil {
		t.Fatalf("no ready front: %v", skipped)
	}
	f := h.newFailoverClient(ctx, conn, addr, endpoints, r.add)
	t.Cleanup(f.Close)
	return f
}

func deleteReq(i int) *s3_lifecycle_pb.LifecycleDeleteRequest {
	return &s3_lifecycle_pb.LifecycleDeleteRequest{
		Bucket:           "bk",
		ObjectPath:       fmt.Sprintf("secret-key-%d", i),
		VersionId:        fmt.Sprintf("v%d", i),
		RuleHash:         []byte{1, 2, 3},
		ActionKind:       s3_lifecycle_pb.ActionKind_NONCURRENT_DAYS,
		ExpectedIdentity: &s3_lifecycle_pb.EntryIdentity{MtimeNs: int64(i), Size: 7, HeadFid: "3,01"},
	}
}

func TestSelectS3Front_FirstFrontDownUsesNext(t *testing.T) {
	setWorkerKey(t, frontKey)
	down := downAddr(t)
	goodAddr := startFront(t, &testFront{key: frontKey})

	conn, got, skipped := newTestHandler().selectS3Front(context.Background(), []string{down, goodAddr})
	if conn == nil {
		t.Fatalf("expected the second front, skipped: %v", skipped)
	}
	conn.Close()
	if got != goodAddr || len(skipped) != 1 || !strings.Contains(skipped[0].Error(), down) {
		t.Fatalf("got %q skipped %v, want %q after skipping %s", got, skipped, goodAddr, down)
	}
}

func TestFailoverClient_FrontLostMidPassResendsSameRequest(t *testing.T) {
	setWorkerKey(t, frontKey)
	a := &failoverFront{testFront: testFront{key: frontKey}, dieOn: 3}
	b := &failoverFront{testFront: testFront{key: frontKey}}
	aAddr, bAddr := startFailoverFront(t, a), startFailoverFront(t, b)
	r := &reports{}
	f := newTestFailoverClient(t, context.Background(), []string{aAddr, bAddr}, r)

	for i := 1; i <= 5; i++ {
		resp, err := f.LifecycleDelete(context.Background(), deleteReq(i))
		if err != nil || resp.Outcome != s3_lifecycle_pb.LifecycleDeleteOutcome_DONE {
			t.Fatalf("delete %d: resp %v err %v", i, resp, err)
		}
	}
	gotA, gotB := a.received(), b.received()
	if len(gotA) != 3 || len(gotB) != 3 {
		t.Fatalf("front a got %d, front b got %d deletes; want 3 and 3", len(gotA), len(gotB))
	}
	if !proto.Equal(gotA[2], gotB[0]) || !proto.Equal(gotB[0], deleteReq(3)) {
		t.Fatalf("resent request differs: a=%v b=%v", gotA[2], gotB[0])
	}
	msgs := r.all()
	if len(msgs) != 1 || !strings.Contains(msgs[0], "switched to s3 "+bAddr) || !strings.Contains(msgs[0], aAddr) || !strings.Contains(msgs[0], "Unavailable") {
		t.Fatalf("want one switch report naming both fronts, got %q", msgs)
	}
	for _, m := range msgs {
		assertNoSecrets(t, m)
		if strings.Contains(m, "secret-key") {
			t.Fatalf("report exposes an object key: %s", m)
		}
	}
}

func TestFailoverClient_ConcurrentCallersSwitchOnce(t *testing.T) {
	setWorkerKey(t, frontKey)
	a := &failoverFront{testFront: testFront{key: frontKey}, dieOn: 4}
	b := &failoverFront{testFront: testFront{key: frontKey}}
	c := &failoverFront{testFront: testFront{key: frontKey}}
	aAddr, bAddr, cAddr := startFailoverFront(t, a), startFailoverFront(t, b), startFailoverFront(t, c)
	r := &reports{}
	f := newTestFailoverClient(t, context.Background(), []string{aAddr, bAddr, cAddr}, r)

	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := f.LifecycleDelete(context.Background(), deleteReq(i)); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("delete failed: %v", err)
	}
	if msgs := r.all(); len(msgs) != 1 || !strings.Contains(msgs[0], "switched to s3 "+bAddr) {
		t.Fatalf("want one switch to %s, got %q", bAddr, msgs)
	}
	if n := len(c.received()); n != 0 {
		t.Fatalf("front c got %d deletes, want none", n)
	}
}

func TestFailoverClient_NoFrontLeftFailsFast(t *testing.T) {
	setWorkerKey(t, frontKey)
	a := &failoverFront{testFront: testFront{key: frontKey}, dieOn: 1}
	aAddr := startFailoverFront(t, a)
	down := downAddr(t)
	rejecting := &testFront{key: otherKey}
	rejectAddr := startFront(t, rejecting)
	r := &reports{}
	f := newTestFailoverClient(t, context.Background(), []string{aAddr, down, rejectAddr}, r)

	start := time.Now()
	_, err := f.LifecycleDelete(context.Background(), deleteReq(1))
	if err == nil {
		t.Fatal("want an error with no front left")
	}
	msg := err.Error()
	for _, want := range []string{"no s3 server is ready for lifecycle deletes", aAddr, down, rejectAddr, "rejected lifecycle credentials", "jwt.filer_signing.key"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error %q does not mention %q", msg, want)
		}
	}
	assertNoSecrets(t, msg)
	if strings.Contains(msg, "secret-key") {
		t.Fatalf("error exposes an object key: %s", msg)
	}

	if _, err2 := f.LifecycleDelete(context.Background(), deleteReq(2)); err2 == nil || err2.Error() != msg {
		t.Fatalf("second call: want the same error, got %v", err2)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("giving up took %v", elapsed)
	}
	if n := rejecting.readies.Load(); n != 1 {
		t.Fatalf("rejecting front checked %d times, want once", n)
	}
	if msgs := r.all(); len(msgs) != 1 || msgs[0] != msg {
		t.Fatalf("want the error reported once, got %q", msgs)
	}
}

func TestFailoverClient_ParentCancelDoesNotSwitch(t *testing.T) {
	setWorkerKey(t, frontKey)
	a := &failoverFront{testFront: testFront{key: frontKey}, hold: true}
	b := &failoverFront{testFront: testFront{key: frontKey}}
	aAddr, bAddr := startFailoverFront(t, a), startFailoverFront(t, b)
	r := &reports{}
	f := newTestFailoverClient(t, context.Background(), []string{aAddr, bAddr}, r)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	if _, err := f.LifecycleDelete(ctx, deleteReq(1)); err == nil {
		t.Fatal("want an error after cancellation")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("cancelled delete took %v", elapsed)
	}
	if b.readies.Load() != 0 || len(r.all()) != 0 {
		t.Fatalf("switched fronts after cancellation: %d readiness checks, reports %q", b.readies.Load(), r.all())
	}
	f.mu.Lock()
	current := f.addr
	f.mu.Unlock()
	if current != aAddr {
		t.Fatalf("current front %q, want %q", current, aAddr)
	}
}

func TestFailoverClient_SecondFrontLostMovesToThird(t *testing.T) {
	setWorkerKey(t, frontKey)
	a := &failoverFront{testFront: testFront{key: frontKey}, dieOn: 1}
	b := &failoverFront{testFront: testFront{key: frontKey}, dieOn: 1}
	c := &failoverFront{testFront: testFront{key: frontKey}}
	aAddr, bAddr, cAddr := startFailoverFront(t, a), startFailoverFront(t, b), startFailoverFront(t, c)
	r := &reports{}
	f := newTestFailoverClient(t, context.Background(), []string{aAddr, bAddr, cAddr}, r)

	resp, err := f.LifecycleDelete(context.Background(), deleteReq(1))
	if err != nil || resp.Outcome != s3_lifecycle_pb.LifecycleDeleteOutcome_DONE {
		t.Fatalf("resp %v err %v", resp, err)
	}
	for name, front := range map[string]*failoverFront{"a": a, "b": b, "c": c} {
		got := front.received()
		if len(got) != 1 || !proto.Equal(got[0], deleteReq(1)) {
			t.Fatalf("front %s got %v, want the one request", name, got)
		}
	}
	if msgs := r.all(); len(msgs) != 2 || !strings.Contains(msgs[1], "switched to s3 "+cAddr) {
		t.Fatalf("want switches to b then c, got %q", msgs)
	}
}

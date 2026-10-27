package cluster

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// The gateway must resolve a lock key to the same primary the filers do,
// otherwise it dials the wrong filer and the lock still gets forwarded. Both
// sides use the same HashRing over the same server set, so for every key the
// client's hostForKey must equal the filer ring's GetPrimary.
func TestLockClientHostMatchesFilerRing(t *testing.T) {
	servers := []pb.ServerAddress{
		"filer-a:8888", "filer-b:8888", "filer-c:8888", "filer-d:8888",
	}

	filerRing := lock_manager.NewHashRing(lock_manager.DefaultVnodeCount)
	filerRing.SetServers(servers)

	lc := NewLockClient(nil, "seed:8888")
	lc.SetRing(servers, 1)

	for _, key := range []string{
		"s3.object.write:/buckets/b/obj-0",
		"s3.object.write:/buckets/b/obj-1",
		"s3.object.write:/buckets/b/obj-2",
		"s3.object.write:/buckets/gosbench-0/w0obj-kilo-0877",
		"some/other/key",
	} {
		if got, want := lc.hostForKey(key), filerRing.GetPrimary(key); got != want {
			t.Errorf("key %q: client host %q != filer primary %q", key, got, want)
		}
	}
}

// Without a ring view, the client falls back to the seed filer (which the filer
// forwards from), preserving the pre-optimization behavior.
func TestLockClientHostFallsBackToSeed(t *testing.T) {
	lc := NewLockClient(nil, "seed:8888")
	if got := lc.hostForKey("any-key"); got != "seed:8888" {
		t.Errorf("expected seed fallback, got %q", got)
	}

	// An empty ring (no members yet) also falls back to the seed.
	lc.SetRing(nil, 1)
	if got := lc.hostForKey("any-key"); got != "seed:8888" {
		t.Errorf("expected seed fallback on empty ring, got %q", got)
	}
}

// A stale (older-version) update must not regress a newer ring view, while
// version 0 always applies as a bootstrap.
func TestLockClientSetRingVersionGuard(t *testing.T) {
	lc := NewLockClient(nil, "seed:8888")

	newer := []pb.ServerAddress{"filer-a:8888", "filer-b:8888"}
	lc.SetRing(newer, 10)
	primaryAt10 := lc.hostForKey("k")

	// Older version is ignored.
	lc.SetRing([]pb.ServerAddress{"filer-z:8888"}, 5)
	if got := lc.hostForKey("k"); got != primaryAt10 {
		t.Errorf("stale update applied: host changed to %q", got)
	}

	// version 0 is always accepted.
	lc.SetRing([]pb.ServerAddress{"filer-z:8888"}, 0)
	if got := lc.hostForKey("k"); got != "filer-z:8888" {
		t.Errorf("bootstrap update not applied, got %q", got)
	}
}

// PrimaryForKey returns "" before any ring is received (so a route-by-key
// caller falls back to the distributed lock) and the ring owner afterwards,
// unlike hostForKey which falls back to the seed.
func TestLockClientPrimaryForKey(t *testing.T) {
	lc := NewLockClient(nil, "seed:8888")
	if got := lc.PrimaryForKey("k"); got != "" {
		t.Errorf("expected empty before ring, got %q", got)
	}

	lc.SetRing([]pb.ServerAddress{"filer-a:8888", "filer-b:8888"}, 1)
	got := lc.PrimaryForKey("k")
	if got == "" {
		t.Fatal("expected an owner after ring set")
	}
	if got != lc.hostForKey("k") {
		t.Errorf("PrimaryForKey %q disagrees with hostForKey %q", got, lc.hostForKey("k"))
	}
}

// A moved key reports its previous owner within the cooling-off window; an unmoved
// key reports none.
func TestLockClientPriorOwnerForKey(t *testing.T) {
	lc := NewLockClient(nil, "seed:8888")

	setA := []pb.ServerAddress{"filer-a:8888", "filer-b:8888", "filer-c:8888"}
	lc.SetRing(setA, 1)
	// One ring: nothing to fall back to.
	if got := lc.PriorOwnerForKey("any"); got != "" {
		t.Fatalf("single ring should have no prior owner, got %q", got)
	}

	priorRing := lock_manager.NewHashRing(lock_manager.DefaultVnodeCount)
	priorRing.SetServers(setA)

	// Add a server so some keys' ownership moves.
	setB := []pb.ServerAddress{"filer-a:8888", "filer-b:8888", "filer-c:8888", "filer-d:8888"}
	lc.SetRing(setB, 2)

	var moved, stable string
	for i := 0; i < 2000 && (moved == "" || stable == ""); i++ {
		key := fmt.Sprintf("key-%d", i)
		if lc.PrimaryForKey(key) != priorRing.GetPrimary(key) {
			if moved == "" {
				moved = key
			}
		} else if stable == "" {
			stable = key
		}
	}
	if moved == "" || stable == "" {
		t.Skip("could not find both a moved and a stable key")
	}

	if got, want := lc.PriorOwnerForKey(moved), priorRing.GetPrimary(moved); got != want {
		t.Fatalf("PriorOwnerForKey(moved)=%q, want %q", got, want)
	}
	if got := lc.PriorOwnerForKey(stable); got != "" {
		t.Fatalf("unmoved key should have no prior owner, got %q", got)
	}
}

// The prior owner is only offered within the cooling-off window.
func TestLockClientPriorOwnerForKeyExpires(t *testing.T) {
	lc := NewLockClient(nil, "seed:8888")
	lc.priorWindow = 20 * time.Millisecond

	lc.SetRing([]pb.ServerAddress{"filer-a:8888", "filer-b:8888", "filer-c:8888"}, 1)
	lc.SetRing([]pb.ServerAddress{"filer-a:8888", "filer-b:8888", "filer-c:8888", "filer-d:8888"}, 2)

	time.Sleep(40 * time.Millisecond)
	for i := 0; i < 2000; i++ {
		if got := lc.PriorOwnerForKey(fmt.Sprintf("key-%d", i)); got != "" {
			t.Fatalf("prior owner should expire after the cooling window, got %q", got)
		}
	}
}

// A master change clears the version gate so the new leader's (lower-versioned)
// snapshot applies — versions are only comparable within one master's stream.
func TestLockClientResetRing(t *testing.T) {
	lc := NewLockClient(nil, "seed:8888")

	lc.SetRing([]pb.ServerAddress{"filer-a:8888", "filer-b:8888"}, 100)
	lc.ResetRing()

	// The last ring keeps routing during the gap; only version acceptance
	// is reset so the new leader's (lower-versioned) snapshot applies.
	if got := lc.hostForKey("k"); got == "seed:8888" {
		t.Fatal("expected the previous ring to keep routing after reset")
	}
	lc.SetRing([]pb.ServerAddress{"filer-z:8888"}, 50)
	if got := lc.hostForKey("k"); got != "filer-z:8888" {
		t.Fatalf("lower version from new master not applied, got %q", got)
	}
}

type noLockServerFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
}

func (s *noLockServerFiler) DistributedLock(ctx context.Context, req *filer_pb.LockRequest) (*filer_pb.LockResponse, error) {
	return &filer_pb.LockResponse{Error: lock_manager.NoLockServerError.Error()}, nil
}

// When every filer reports an empty lock ring, lock acquisition must fail
// after a bounded period instead of hanging the write forever.
func TestNewShortLivedLockFailsFastOnNoLockServer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, &noLockServerFiler{})
	go grpcServer.Serve(listener)
	defer grpcServer.Stop()

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	// "host:httpPort.grpcPort" dials the fake filer's port directly.
	lc := NewLockClient(dialOption, pb.ServerAddress(fmt.Sprintf("%s:0.%s", host, port)))
	lc.noLockServerRetryPeriod = 200 * time.Millisecond

	start := time.Now()
	lock := lc.NewShortLivedLock("test-key", "test-owner")
	elapsed := time.Since(start)

	if lock != nil {
		t.Fatal("expected nil lock when no lock server exists")
	}
	if elapsed > 10*time.Second {
		t.Fatalf("lock acquisition took %v, expected fail-fast", elapsed)
	}
}

type contendedLockFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
}

func (s *contendedLockFiler) DistributedLock(ctx context.Context, req *filer_pb.LockRequest) (*filer_pb.LockResponse, error) {
	return &filer_pb.LockResponse{Error: "lock already owned by someone"}, nil
}

// A lock held by another owner is ordinary contention: acquisition waits it
// out rather than failing on the unavailability bound.
func TestNewShortLivedLockWaitsOutContention(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, &contendedLockFiler{})
	go grpcServer.Serve(listener)
	defer grpcServer.Stop()

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	lc := NewLockClient(dialOption, pb.ServerAddress(fmt.Sprintf("%s:0.%s", host, port)))
	lc.noLockServerRetryPeriod = 200 * time.Millisecond

	done := make(chan *LiveLock, 1)
	go func() {
		done <- lc.NewShortLivedLock("test-key", "test-owner")
	}()
	select {
	case <-done:
		t.Fatal("ordinary lock contention must not hit the unavailability bound")
	case <-time.After(3 * lc.noLockServerRetryPeriod):
	}
}

// A ring member that refuses connections is unavailability, not contention:
// acquisition fails on the same bound as "no lock server found".
func TestNewShortLivedLockFailsFastOnUnreachableFiler(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	lc := NewLockClient(dialOption, pb.ServerAddress(fmt.Sprintf("%s:0.%s", host, port)))
	lc.noLockServerRetryPeriod = 200 * time.Millisecond

	start := time.Now()
	lock := lc.NewShortLivedLock("test-key", "test-owner")
	elapsed := time.Since(start)

	if lock != nil {
		t.Fatal("expected nil lock when the ring member is unreachable")
	}
	if elapsed > 10*time.Second {
		t.Fatalf("lock acquisition took %v, expected fail-fast", elapsed)
	}
}

type hangingLockFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
}

func (s *hangingLockFiler) DistributedLock(ctx context.Context, req *filer_pb.LockRequest) (*filer_pb.LockResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type grantingLockFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
}

func (s *grantingLockFiler) DistributedLock(ctx context.Context, req *filer_pb.LockRequest) (*filer_pb.LockResponse, error) {
	return &filer_pb.LockResponse{RenewToken: "tok"}, nil
}

// A ring member that accepts connections but never answers must not swallow
// the retry window: when its budget is spent the seed route still gets an
// attempt before acquisition gives up.
func TestNewShortLivedLockFallsBackToSeedOnHangingPrimary(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())

	hangListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	hangServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(hangServer, &hangingLockFiler{})
	go hangServer.Serve(hangListener)
	defer hangServer.Stop()

	seedListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	seedServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(seedServer, &grantingLockFiler{})
	go seedServer.Serve(seedListener)
	defer seedServer.Stop()

	hangHost, hangPort, err := net.SplitHostPort(hangListener.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	seedHost, seedPort, err := net.SplitHostPort(seedListener.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	// ServerAddress.Equals compares the http port, so the two fake filers
	// need distinct ones even though only the grpc port is dialed.
	seedAddr := pb.ServerAddress(fmt.Sprintf("%s:1.%s", seedHost, seedPort))
	primaryAddr := pb.ServerAddress(fmt.Sprintf("%s:2.%s", hangHost, hangPort))

	lc := NewLockClient(dialOption, seedAddr)
	lc.lockRpcTimeout = 50 * time.Millisecond
	lc.noLockServerRetryPeriod = 200 * time.Millisecond
	lc.SetRing([]pb.ServerAddress{primaryAddr}, 1)

	start := time.Now()
	lock := lc.NewShortLivedLock("test-key", "test-owner")
	elapsed := time.Since(start)

	if lock == nil {
		t.Fatal("expected the seed fallback to acquire the lock")
	}
	if !lock.hostFiler.Equals(seedAddr) {
		t.Fatalf("expected acquisition through the seed %s, got %s", seedAddr, lock.hostFiler)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("lock acquisition took %v, expected it bounded by the per-route window", elapsed)
	}
}

type redirectingLockFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	movedTo string
}

func (s *redirectingLockFiler) DistributedLock(ctx context.Context, req *filer_pb.LockRequest) (*filer_pb.LockResponse, error) {
	return &filer_pb.LockResponse{Error: "forward failed", LockHostMovedTo: s.movedTo}, nil
}

// The seed can redirect back to a dead primary via LockHostMovedTo. The seed
// fallback gets one fresh window per acquisition — letting every return to
// the seed refill it would make the primary->seed->primary cycle unbounded.
func TestNewShortLivedLockBoundsSeedRedirectCycle(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())

	hangListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	hangServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(hangServer, &hangingLockFiler{})
	go hangServer.Serve(hangListener)
	defer hangServer.Stop()

	hangHost, hangPort, err := net.SplitHostPort(hangListener.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	primaryAddr := pb.ServerAddress(fmt.Sprintf("%s:2.%s", hangHost, hangPort))

	seedListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	seedServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(seedServer, &redirectingLockFiler{movedTo: string(primaryAddr)})
	go seedServer.Serve(seedListener)
	defer seedServer.Stop()

	seedHost, seedPort, err := net.SplitHostPort(seedListener.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	seedAddr := pb.ServerAddress(fmt.Sprintf("%s:1.%s", seedHost, seedPort))

	lc := NewLockClient(dialOption, seedAddr)
	lc.lockRpcTimeout = 50 * time.Millisecond
	lc.noLockServerRetryPeriod = 200 * time.Millisecond

	start := time.Now()
	lock := lc.NewShortLivedLock("test-key", "test-owner")
	elapsed := time.Since(start)

	if lock != nil {
		t.Fatal("expected nil lock when the seed keeps redirecting to a dead primary")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("lock acquisition took %v, expected a bounded redirect cycle", elapsed)
	}
}

type cleanRedirectLockFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	movedTo string
}

func (s *cleanRedirectLockFiler) DistributedLock(ctx context.Context, req *filer_pb.LockRequest) (*filer_pb.LockResponse, error) {
	return &filer_pb.LockResponse{LockHostMovedTo: s.movedTo}, nil
}

// A second hop whose ring disagrees answers with a bare LockHostMovedTo and
// no error, so the first hop relays a clean redirect. Two filers pointing at
// each other then bounce the client forever if every redirect refills the
// window — tokenless redirects must count against it like failures do.
func TestNewShortLivedLockBoundsCleanRedirectCycle(t *testing.T) {
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())

	listenerA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	listenerB, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	hostA, portA, err := net.SplitHostPort(listenerA.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	hostB, portB, err := net.SplitHostPort(listenerB.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	addrA := pb.ServerAddress(fmt.Sprintf("%s:1.%s", hostA, portA))
	addrB := pb.ServerAddress(fmt.Sprintf("%s:2.%s", hostB, portB))

	serverA := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(serverA, &cleanRedirectLockFiler{movedTo: string(addrB)})
	go serverA.Serve(listenerA)
	defer serverA.Stop()
	serverB := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(serverB, &cleanRedirectLockFiler{movedTo: string(addrA)})
	go serverB.Serve(listenerB)
	defer serverB.Stop()

	lc := NewLockClient(dialOption, addrA)
	lc.lockRpcTimeout = 50 * time.Millisecond
	lc.noLockServerRetryPeriod = 200 * time.Millisecond

	done := make(chan *LiveLock, 1)
	go func() {
		done <- lc.NewShortLivedLock("test-key", "test-owner")
	}()
	select {
	case lock := <-done:
		if lock != nil {
			t.Fatal("expected nil lock when the ring keeps redirecting")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("clean redirect cycle was not bounded by the retry window")
	}
}

// A failed attempt keeps the token it sent: the request may have renewed the
// lease server-side before its response was lost, and a retry with the last
// token can still succeed. Only a definitive rejection may clear it.
func TestDoLockKeepsRenewTokenOnError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	filerAddr := pb.ServerAddress(fmt.Sprintf("%s:0.%s", host, port))
	lc := NewLockClient(dialOption, filerAddr)
	lock := &LiveLock{
		key:            "test-key",
		renewToken:     "tok",
		hostFiler:      filerAddr,
		grpcDialOption: dialOption,
		lc:             lc,
	}
	if _, _, err := lock.doLock(time.Second, 50*time.Millisecond); err == nil {
		t.Fatal("expected the attempt to fail against a dead member")
	}
	if lock.renewToken != "tok" {
		t.Errorf("renewToken = %q, want it kept for the next attempt", lock.renewToken)
	}
}

// A definitive rejection answers with an empty token, which clears the stale
// one — only transport failures keep it.
func TestDoLockClearsRenewTokenOnRejection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, &contendedLockFiler{})
	go grpcServer.Serve(listener)
	defer grpcServer.Stop()

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	filerAddr := pb.ServerAddress(fmt.Sprintf("%s:0.%s", host, port))
	lc := NewLockClient(dialOption, filerAddr)
	lock := &LiveLock{
		key:            "test-key",
		renewToken:     "tok",
		hostFiler:      filerAddr,
		grpcDialOption: dialOption,
		lc:             lc,
	}
	errorMessage, _, err := lock.doLock(time.Second, time.Second)
	if err != nil || errorMessage == "" {
		t.Fatalf("expected a contended attempt to report an error message, got errorMessage=%q err=%v", errorMessage, err)
	}
	if lock.renewToken != "" {
		t.Errorf("renewToken = %q, want it cleared by the rejection", lock.renewToken)
	}
}

// A bare LockHostMovedTo redirect is not a verdict on the token: the
// request may have committed a renewal before the ring moved, so the held
// token survives to renew against the new host.
func TestDoLockKeepsRenewTokenOnCleanRedirect(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, &cleanRedirectLockFiler{movedTo: "127.0.0.1:1.1"})
	go grpcServer.Serve(listener)
	defer grpcServer.Stop()

	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	filerAddr := pb.ServerAddress(fmt.Sprintf("%s:0.%s", host, port))
	lc := NewLockClient(dialOption, filerAddr)
	lock := &LiveLock{
		key:            "test-key",
		renewToken:     "tok",
		hostFiler:      filerAddr,
		grpcDialOption: dialOption,
		lc:             lc,
	}
	errorMessage, _, err := lock.doLock(time.Second, time.Second)
	if err != nil || errorMessage != "" {
		t.Fatalf("expected a clean redirect, got errorMessage=%q err=%v", errorMessage, err)
	}
	if lock.renewToken != "tok" {
		t.Errorf("renewToken = %q, want it kept across the redirect", lock.renewToken)
	}
}

// LiveLock.generation is a fencing token written and read with 64-bit atomic
// operations. On 32-bit platforms (GOARCH=386 and GOARCH=arm) a 64-bit atomic
// op requires an 8-byte-aligned address, which Go only guarantees for the
// first field of an allocated struct. generation must therefore stay first:
// a misaligned field makes StoreInt64 panic with "unaligned 64-bit atomic
// operation" the moment this test runs on a 32-bit target.
func TestLiveLockGenerationIsAligned(t *testing.T) {
	var lock LiveLock
	atomic.StoreInt64(&lock.generation, 42)
	if got := lock.Generation(); got != 42 {
		t.Fatalf("generation = %d, want 42", got)
	}
}

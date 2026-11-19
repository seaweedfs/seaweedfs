package wdclient

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// An embedded S3 gateway is seeded with its local filer as "ip:port" while the
// filer registers in the master as "ip:port.grpcPort". Both name the same filer,
// so discovery must keep it in place, with its health, as the current filer.
func TestDiscoveryKeepsSeededFilerUnderRegisteredForm(t *testing.T) {
	fc := newTestFilerClient("10.0.0.1:8888")
	seededHealth := fc.filerHealth[0]
	atomic.StoreInt32(&seededHealth.failureCount, 2)

	fc.applyDiscoveredFilers(map[pb.ServerAddress]struct{}{
		"10.0.0.1:8888.18888": {},
		"10.0.0.2:8888.18888": {},
		"10.0.0.3:8888.18888": {},
	})

	want := []pb.ServerAddress{"10.0.0.1:8888.18888", "10.0.0.2:8888.18888", "10.0.0.3:8888.18888"}
	if got := filerAddressList(fc); !reflect.DeepEqual(got, want) {
		t.Fatalf("filer list: want %v, got %v", want, got)
	}
	if fc.filerHealth[0] != seededHealth {
		t.Fatalf("seeded filer lost its health entry")
	}
	if got := atomic.LoadInt32(&fc.filerHealth[0].failureCount); got != 2 {
		t.Fatalf("seeded filer health reset: want failureCount 2, got %d", got)
	}
	if got := fc.GetCurrentFiler(); got != "10.0.0.1:8888.18888" {
		t.Fatalf("current filer: want the seeded local filer, got %s", got)
	}
}

func TestDiscoveryOrderIsDeterministic(t *testing.T) {
	discovered := map[pb.ServerAddress]struct{}{
		"10.0.0.1:8888.18888": {},
		"10.0.0.2:8888.18888": {},
		"10.0.0.3:8888.18888": {},
		"10.0.0.4:8888.18888": {},
	}
	var first []pb.ServerAddress
	for i := 0; i < 200; i++ {
		fc := newTestFilerClient("10.0.0.9:8888")
		fc.applyDiscoveredFilers(discovered)
		got := filerAddressList(fc)
		if first == nil {
			first = got
			continue
		}
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("run %d: list order changed: %v vs %v", i, got, first)
		}
	}
}

func TestDiscoveryCurrentGoneFallsToFirstSorted(t *testing.T) {
	a := pb.ServerAddress("10.0.0.1:18888")
	b := pb.ServerAddress("10.0.0.2:18888")
	c := pb.ServerAddress("10.0.0.3:18888")
	fc := newTestFilerClient(a, b)
	atomic.StoreInt32(&fc.filerIndex, 1)

	fc.applyDiscoveredFilers(map[pb.ServerAddress]struct{}{a: {}, c: {}})

	if got := fc.GetCurrentFiler(); got != a {
		t.Fatalf("current filer: want %s, got %s", a, got)
	}
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

const (
	homeA = pb.ServerAddress("10.0.0.1:8888.18888")
	peerB = pb.ServerAddress("10.0.0.2:8888.18888")
)

// newHomeTestClient returns a client on [home, peer] whose home is the first
// filer, with the default breaker (3 failures, 30s reset) and a fake clock.
func newHomeTestClient(clock *fakeClock) *FilerClient {
	fc := newTestFilerClient(homeA, peerB)
	fc.failureThreshold = 3
	fc.resetTimeout = 30 * time.Second
	fc.homeFiler = homeA
	fc.homeBackoff = fc.resetTimeout
	fc.now = clock.now
	return fc
}

// failHome trips the breaker on home and fails over to the peer.
func failHome(fc *FilerClient) {
	for i := 0; i < 3; i++ {
		fc.RecordFilerFailure(homeA)
	}
	fc.SetCurrentFiler(peerB)
}

func TestHomeReturnAfterBreakerReset(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	fc := newHomeTestClient(clock)

	failHome(fc)
	if got := fc.GetCurrentFiler(); got != peerB {
		t.Fatalf("after failover: want %s, got %s", peerB, got)
	}
	clock.advance(10 * time.Second)
	if got := fc.GetCurrentFiler(); got != peerB {
		t.Fatalf("home still in breaker: want %s, got %s", peerB, got)
	}
	clock.advance(21 * time.Second)
	if got := fc.GetCurrentFiler(); got != homeA {
		t.Fatalf("after breaker reset: want home %s, got %s", homeA, got)
	}
}

func TestHomeBackoffDoublesOnFlap(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	fc := newHomeTestClient(clock)

	failHome(fc)
	clock.advance(31 * time.Second)
	if got := fc.GetCurrentFiler(); got != homeA {
		t.Fatalf("first return: want home, got %s", got)
	}

	clock.advance(9 * time.Second) // +40s: home fails again soon after the return
	failHome(fc)
	if fc.homeBackoff != 60*time.Second {
		t.Fatalf("backoff after flap: want 60s, got %v", fc.homeBackoff)
	}
	clock.advance(55 * time.Second) // +95s: breaker reset, backoff not yet over
	if got := fc.GetCurrentFiler(); got != peerB {
		t.Fatalf("inside backoff: want %s, got %s", peerB, got)
	}
	clock.advance(6 * time.Second) // +101s
	if got := fc.GetCurrentFiler(); got != homeA {
		t.Fatalf("after backoff: want home, got %s", got)
	}
}

// flapUntilHome fails home over, waits until it is eligible again and returns.
func flapUntilHome(t *testing.T, fc *FilerClient, clock *fakeClock) {
	t.Helper()
	failHome(fc)
	wait := fc.homeBackoff
	if wait < fc.resetTimeout {
		wait = fc.resetTimeout
	}
	clock.advance(wait + time.Second)
	if got := fc.GetCurrentFiler(); got != homeA {
		t.Fatalf("expected a return home, got %s", got)
	}
	clock.advance(time.Second)
}

func TestHomeBackoffCapsAtFiveMinutes(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	fc := newHomeTestClient(clock)
	for i := 0; i < 6; i++ {
		flapUntilHome(t, fc, clock)
	}
	failHome(fc)
	if fc.homeBackoff != 5*time.Minute {
		t.Fatalf("backoff: want 5m cap, got %v", fc.homeBackoff)
	}
}

func TestHomeBackoffResetsAfterHold(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	fc := newHomeTestClient(clock)
	flapUntilHome(t, fc, clock)
	flapUntilHome(t, fc, clock)
	if fc.homeBackoff != 60*time.Second {
		t.Fatalf("setup: want backoff 60s, got %v", fc.homeBackoff)
	}

	clock.advance(10*time.Minute + time.Second)
	failHome(fc)
	if fc.homeBackoff != 30*time.Second {
		t.Fatalf("after a 10m hold: want backoff reset to 30s, got %v", fc.homeBackoff)
	}
}

func TestNoHomeKeepsStickyBehaviour(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	fc := newHomeTestClient(clock)
	fc.homeFiler = ""

	failHome(fc)
	clock.advance(time.Hour)
	if got := fc.GetCurrentFiler(); got != peerB {
		t.Fatalf("without a home the client must stay put: want %s, got %s", peerB, got)
	}
}

func TestDiscoveryPrefersHealthyHomeWhenCurrentGone(t *testing.T) {
	x := pb.ServerAddress("10.0.0.0:8888.18888") // sorts and stays first
	home := pb.ServerAddress("10.0.0.3:8888.18888")
	fc := newTestFilerClient(x, peerB, home)
	fc.homeFiler = home
	fc.failureThreshold = 3
	fc.resetTimeout = 30 * time.Second
	atomic.StoreInt32(&fc.filerIndex, 1) // current is the peer that leaves

	fc.applyDiscoveredFilers(map[pb.ServerAddress]struct{}{x: {}, home: {}})

	if got := fc.filerAddresses[atomic.LoadInt32(&fc.filerIndex)]; got != home {
		t.Fatalf("current after discovery: want home %s, got %s", home, got)
	}
}

// countingFiler answers LookupVolume with no locations and counts the calls.
type countingFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	calls    atomic.Int32
	onLookup func() // optional, runs inside each LookupVolume call
}

func (s *countingFiler) LookupVolume(context.Context, *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	s.calls.Add(1)
	if s.onLookup != nil {
		s.onLookup()
	}
	return &filer_pb.LookupVolumeResponse{}, nil
}

func startCountingFiler(t *testing.T) (pb.ServerAddress, *countingFiler) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &countingFiler{}
	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, srv)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)
	_, port, _ := net.SplitHostPort(lis.Addr().String())
	return pb.ServerAddress(fmt.Sprintf("127.0.0.1:0.%s", port)), srv
}

func TestLookupStartsFromEligibleHome(t *testing.T) {
	home, homeSrv := startCountingFiler(t)
	peer, peerSrv := startCountingFiler(t)
	fc := newTestFilerClient(home, peer)
	fc.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	fc.grpcTimeout = 5 * time.Second
	fc.maxRetries = 1
	fc.failureThreshold = 3
	fc.resetTimeout = 30 * time.Second
	fc.homeFiler = home
	atomic.StoreInt32(&fc.filerIndex, 1) // current is the peer

	if _, err := (&filerVolumeProvider{filerClient: fc}).LookupVolumeIds(context.Background(), []string{"4"}); err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if homeSrv.calls.Load() != 1 || peerSrv.calls.Load() != 0 {
		t.Fatalf("lookup calls: home=%d peer=%d, want home=1 peer=0", homeSrv.calls.Load(), peerSrv.calls.Load())
	}
}

// A lookup that started on the peer and finishes after the client has returned
// home must not drag the client back to the peer: home never failed.
func TestLookupInFlightDuringReturnDoesNotLeaveHome(t *testing.T) {
	home, _ := startCountingFiler(t)
	peer, peerSrv := startCountingFiler(t)
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	fc := newTestFilerClient(home, peer)
	fc.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	fc.grpcTimeout = 5 * time.Second
	fc.maxRetries = 1
	fc.failureThreshold = 3
	fc.resetTimeout = 30 * time.Second
	fc.homeFiler = home
	fc.homeBackoff = fc.resetTimeout
	fc.now = clock.now

	for i := 0; i < 3; i++ {
		fc.RecordFilerFailure(home)
	}
	fc.SetCurrentFiler(peer)
	clock.advance(10 * time.Second) // home still in breaker: the lookup starts on the peer

	peerSrv.onLookup = func() {
		clock.advance(25 * time.Second) // home becomes eligible mid-lookup
		if got := fc.GetCurrentFiler(); got != home {
			t.Errorf("mid-lookup return: want home %s, got %s", home, got)
		}
	}
	if _, err := (&filerVolumeProvider{filerClient: fc}).LookupVolumeIds(context.Background(), []string{"4"}); err != nil {
		t.Fatalf("lookup: %v", err)
	}

	if got := fc.filerAddresses[atomic.LoadInt32(&fc.filerIndex)]; got != home {
		t.Fatalf("after the lookup: want current home %s, got %s", home, got)
	}
	if fc.homeBackoff != 30*time.Second {
		t.Fatalf("backoff: want unchanged 30s, got %v", fc.homeBackoff)
	}
}

// A failover decided against a filer that is no longer current is stale and
// must not move the client.
func TestStaleFailoverDoesNotMoveClient(t *testing.T) {
	peerC := pb.ServerAddress("10.0.0.3:8888.18888")
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	fc := newTestFilerClient(homeA, peerB, peerC)
	fc.failureThreshold = 3
	fc.resetTimeout = 30 * time.Second
	fc.homeFiler = homeA
	fc.homeBackoff = fc.resetTimeout
	fc.now = clock.now

	fc.SwitchCurrentFiler(peerB, peerC) // the caller saw B as current; it is home

	if got := fc.filerAddresses[atomic.LoadInt32(&fc.filerIndex)]; got != homeA {
		t.Fatalf("stale failover moved the client to %s", got)
	}
	if !fc.homeEligibleAt.IsZero() {
		t.Fatalf("stale failover started the return backoff")
	}
}

// Many requests failing on home at once are one departure from home, not many.
func TestConcurrentFailoversFromHomeCountOnce(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	fc := newHomeTestClient(clock)
	failHome(fc)
	clock.advance(31 * time.Second)
	if got := fc.GetCurrentFiler(); got != homeA {
		t.Fatalf("setup: want home, got %s", got)
	}
	clock.advance(time.Second)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			fc.SwitchCurrentFiler(homeA, peerB)
		}()
	}
	close(start)
	wg.Wait()

	if fc.homeBackoff != 60*time.Second {
		t.Fatalf("backoff: want one doubling to 60s, got %v", fc.homeBackoff)
	}
}

// One failure leaves home's breaker closed, so only the backoff keeps the
// client on the peer. A request reading the current filer while another fails
// over from home must not see the peer before that backoff is in place, or it
// switches straight back home.
func TestFailoverFromHomeRacingReadKeepsBackoff(t *testing.T) {
	for iter := 0; iter < 200; iter++ {
		clock := &fakeClock{t: time.Unix(1_000_000, 0)}
		fc := newHomeTestClient(clock)
		fc.RecordFilerFailure(homeA)

		var stop atomic.Bool
		var ready, done sync.WaitGroup
		for g := 0; g < 4; g++ {
			ready.Add(1)
			done.Add(1)
			go func() {
				defer done.Done()
				fc.GetCurrentFiler()
				ready.Done()
				for !stop.Load() {
					fc.GetCurrentFiler()
				}
			}()
		}
		ready.Wait()
		fc.SwitchCurrentFiler(homeA, peerB)
		stop.Store(true)
		done.Wait()

		if got := fc.GetCurrentFiler(); got != peerB {
			t.Fatalf("iteration %d: returned home inside the backoff, current %s", iter, got)
		}
	}
}

// A home still inside its return backoff is not picked just because it is
// listed first when discovery removes the current filer.
func TestDiscoverySkipsBackingOffHomeWhenCurrentGone(t *testing.T) {
	peerC := pb.ServerAddress("10.0.0.3:8888.18888")
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	fc := newHomeTestClient(clock)
	fc.filerAddresses = []pb.ServerAddress{homeA, peerB, peerC}
	fc.filerHealth = []*filerHealth{{}, {}, {}}
	fc.RecordFilerFailure(homeA)
	fc.SwitchCurrentFiler(homeA, peerB)

	fc.applyDiscoveredFilers(map[pb.ServerAddress]struct{}{homeA: {}, peerC: {}})

	if got := fc.GetCurrentFiler(); got != peerC {
		t.Fatalf("current after discovery: want %s, got %s", peerC, got)
	}
}

// Discovery landing on home is a return too: failing again soon after it
// doubles the backoff like any other flap.
func TestReturnHomeByDiscoveryCountsForBackoff(t *testing.T) {
	peerC := pb.ServerAddress("10.0.0.3:8888.18888")
	clock := &fakeClock{t: time.Unix(1_000_000, 0)}
	fc := newHomeTestClient(clock)
	fc.filerAddresses = []pb.ServerAddress{homeA, peerB, peerC}
	fc.filerHealth = []*filerHealth{{}, {}, {}}
	fc.RecordFilerFailure(homeA)
	fc.SwitchCurrentFiler(homeA, peerB)

	clock.advance(31 * time.Second)
	fc.applyDiscoveredFilers(map[pb.ServerAddress]struct{}{homeA: {}, peerC: {}})
	if got := fc.filerAddresses[atomic.LoadInt32(&fc.filerIndex)]; got != homeA {
		t.Fatalf("setup: want discovery to return home, got %s", got)
	}

	clock.advance(9 * time.Second)
	fc.RecordFilerFailure(homeA)
	fc.SwitchCurrentFiler(homeA, peerC)
	if fc.homeBackoff != 60*time.Second {
		t.Fatalf("backoff after a flap following a discovery return: want 60s, got %v", fc.homeBackoff)
	}
}

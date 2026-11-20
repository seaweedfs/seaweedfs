package wdclient

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// lookupFiler answers LookupVolume the way a filer does: volumes it knows get
// their locations, any other volume fails the call with the master's error.
// While unavailable is set it fails every call as an unreachable filer would.
type lookupFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	known       map[string]string // volume id -> volume server url
	unavailable atomic.Bool
	calls       atomic.Int32
}

func (f *lookupFiler) LookupVolume(ctx context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	f.calls.Add(1)
	if f.unavailable.Load() {
		return nil, status.Error(codes.Unavailable, "filer restarting")
	}
	resp := &filer_pb.LookupVolumeResponse{LocationsMap: make(map[string]*filer_pb.Locations)}
	for _, vid := range req.VolumeIds {
		url, found := f.known[vid]
		if !found {
			return nil, fmt.Errorf("provider lookup failed: master volume lookup errors: volume %s: volume id %s not found", vid, vid)
		}
		resp.LocationsMap[vid] = &filer_pb.Locations{Locations: []*filer_pb.Location{{Url: url, PublicUrl: url}}}
	}
	return resp, nil
}

func startLookupFiler(t *testing.T, f *lookupFiler) pb.ServerAddress {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, f)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return pb.ServerAddress(fmt.Sprintf("127.0.0.1:1.%d", lis.Addr().(*net.TCPAddr).Port))
}

func newBreakerTestClient(t *testing.T, addr pb.ServerAddress) *FilerClient {
	t.Helper()
	fc := NewFilerClient([]pb.ServerAddress{addr}, grpc.WithTransportCredentials(insecure.NewCredentials()), "", &FilerClientOption{
		GrpcTimeout:      2 * time.Second,
		InitialRetryWait: time.Millisecond,
		ResetTimeout:     time.Minute, // longer than the test, so an open breaker stays open
	})
	t.Cleanup(fc.Close)
	return fc
}

// A volume the master has no location for is an answer from the filer, not a
// sign the filer is down. Before the fix the third such answer opened the
// breaker, and every lookup of the next 30 seconds failed without asking the
// filer, including lookups of volumes whose other replica was serving.
func TestFilerNotFoundAnswersDoNotOpenBreaker(t *testing.T) {
	filer := &lookupFiler{known: map[string]string{"8": "10.0.0.8:8080"}}
	fc := newBreakerTestClient(t, startLookupFiler(t, filer))
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := fc.LookupFileIdWithFallback(ctx, "7,0102"); err == nil {
			t.Fatalf("lookup %d of volume 7 succeeded, want not found", i)
		}
	}
	// Checked on its own: with the breaker open, the lookup below would
	// still succeed, because a lone filer in its cool-down is asked anyway.
	if n := atomic.LoadInt32(&fc.filerHealth[0].failureCount); n != 0 {
		t.Fatalf("not-found answers counted as %d filer failures", n)
	}

	urls, err := fc.LookupFileIdWithFallback(ctx, "8,0102")
	if err != nil {
		t.Fatalf("volume 8 after not-found answers for volume 7: %v", err)
	}
	if len(urls) != 1 || urls[0] != "http://10.0.0.8:8080/8,0102" {
		t.Fatalf("volume 8 urls = %v", urls)
	}
}

// When every filer is in its cool-down the breaker has nowhere to steer, so
// the lookup asks them anyway instead of failing without asking anyone.
func TestOnlyFilerInCooldownIsStillAsked(t *testing.T) {
	filer := &lookupFiler{known: map[string]string{"8": "10.0.0.8:8080"}}
	fc := newBreakerTestClient(t, startLookupFiler(t, filer))
	ctx := context.Background()

	filer.unavailable.Store(true)
	if _, err := fc.LookupFileIdWithFallback(ctx, "8,0102"); err == nil {
		t.Fatal("lookup succeeded while the filer was unavailable")
	}
	if !fc.allFilersUnhealthy() {
		t.Fatalf("breaker did not open after %d unavailable answers", filer.calls.Load())
	}

	filer.unavailable.Store(false)
	before := filer.calls.Load()
	if _, err := fc.LookupFileIdWithFallback(ctx, "8,0102"); err != nil {
		t.Fatalf("lookup after the filer recovered: %v", err)
	}
	if filer.calls.Load() == before {
		t.Fatal("the only filer was skipped instead of asked")
	}
	if fc.allFilersUnhealthy() {
		t.Fatal("a successful answer did not close the breaker")
	}
}

func TestFilerUnreachable(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"unavailable", status.Error(codes.Unavailable, "connection refused"), true},
		{"deadline", status.Error(codes.DeadlineExceeded, "deadline exceeded"), true},
		{"exhausted", status.Error(codes.ResourceExhausted, "too many requests"), true},
		{"wrapped unavailable", fmt.Errorf("filer.LookupVolume failed: %w", status.Error(codes.Unavailable, "x")), true},
		{"no status", fmt.Errorf("dial tcp: connection refused"), true},
		{"answer naming a connection", fmt.Errorf("filer.LookupVolume failed: %w", status.Error(codes.Unknown, "master: connection refused")), false},
		{"not found answer", status.Error(codes.Unknown, "volume id 7 not found"), false},
		{"not found code", status.Error(codes.NotFound, "volume id 7 not found"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := filerUnreachable(tc.err); got != tc.want {
				t.Fatalf("filerUnreachable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// A filer can trip its breaker after the pass decided to skip unhealthy
// filers but before the pass reaches it, as when a concurrent lookup records
// its failure in between. A pass that asked nobody must be retried, not
// reported as a non-retryable failure with no error.
func TestFilerTrippedDuringPassIsRetried(t *testing.T) {
	filer := &lookupFiler{known: map[string]string{"8": "10.0.0.8:8080"}}
	fc := newBreakerTestClient(t, startLookupFiler(t, filer))
	health := fc.filerHealth[0]
	now := time.Now()
	atomic.StoreInt32(&health.failureCount, fc.failureThreshold)
	atomic.StoreInt64(&health.lastFailureTimeNs, now.Add(-2*fc.resetTimeout).UnixNano())
	var trip sync.Once
	fc.now = func() time.Time {
		trip.Do(func() { atomic.StoreInt64(&health.lastFailureTimeNs, now.UnixNano()) })
		return now
	}

	locations, err := (&filerVolumeProvider{filerClient: fc}).LookupVolumeIds(context.Background(), []string{"8"})
	if err != nil {
		t.Fatalf("lookup with the filer tripped mid-pass: %v", err)
	}
	if len(locations["8"]) != 1 || filer.calls.Load() == 0 {
		t.Fatalf("locations = %v after %d filer calls", locations, filer.calls.Load())
	}
}

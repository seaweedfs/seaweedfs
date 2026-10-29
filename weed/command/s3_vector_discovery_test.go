package command

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// A worker that registers a moment after a failed lookup must surface inside
// a caller's retry budget: the no-client recheck window is much shorter than
// the live-client membership refresh.
func TestVectorDiscoveryRechecksRegistration(t *testing.T) {
	var calls int32
	d := &vectorDiscovery{
		discover: func(masters []pb.ServerAddress, opt grpc.DialOption) []string {
			atomic.AddInt32(&calls, 1)
			return nil
		},
	}

	if _, err := d.cli(); status.Code(err) != codes.Unavailable {
		t.Fatalf("no registered worker: expected Unavailable, got %v", err)
	}
	if _, err := d.cli(); status.Code(err) != codes.Unavailable {
		t.Fatalf("no registered worker: expected Unavailable, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("lookup inside the recheck window hit master again: %d lookups", got)
	}

	// Once the recheck interval passes the next call re-discovers.
	d.mu.Lock()
	d.nextResolve = time.Now().Add(-time.Second)
	d.mu.Unlock()
	d.discover = func(masters []pb.ServerAddress, opt grpc.DialOption) []string {
		atomic.AddInt32(&calls, 1)
		return []string{"127.0.0.1:9301"}
	}
	d.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())

	if _, err := d.cli(); err != nil {
		t.Fatalf("worker registered: expected a client, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expired recheck window did not rediscover: %d lookups", got)
	}

	// A live client rides out the refresh interval without rediscovery.
	if _, err := d.cli(); err != nil {
		t.Fatalf("cached client rejected: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("live client rediscovered inside the refresh window: %d lookups", got)
	}
}

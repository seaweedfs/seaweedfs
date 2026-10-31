package command

import (
	"fmt"
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

// A lookup slower than the throttle interval (an unreachable first master
// costs the full dial timeout) must not leave the next lookup already due,
// or every request queues behind back-to-back lookups.
func TestVectorDiscoveryThrottlesFromLookupEnd(t *testing.T) {
	const lookup = 600 * time.Millisecond
	var calls int32
	d := &vectorDiscovery{
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		discover: func(masters []pb.ServerAddress, opt grpc.DialOption) []string {
			atomic.AddInt32(&calls, 1)
			time.Sleep(lookup)
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
		t.Fatalf("slow lookup was repeated right away: %d lookups", got)
	}

	d.discover = func(masters []pb.ServerAddress, opt grpc.DialOption) []string {
		time.Sleep(lookup)
		return []string{"127.0.0.1:9301"}
	}
	for i := 0; i < 2; i++ {
		d.mu.Lock()
		d.nextResolve = time.Time{}
		d.mu.Unlock()
		if _, err := d.cli(); err != nil {
			t.Fatalf("worker registered: expected a client, got %v", err)
		}
		waitResolved(d)
	}
	d.mu.Lock()
	left := time.Until(d.nextResolve)
	d.mu.Unlock()
	if left < vectorResolveInterval-lookup/2 {
		t.Fatalf("refresh of a live worker is due in %v, want ~%v", left, vectorResolveInterval)
	}
}

func waitResolved(d *vectorDiscovery) {
	d.mu.Lock()
	resolving := d.resolving
	d.mu.Unlock()
	if resolving != nil {
		<-resolving
	}
}

// A lookup stuck on an unreachable master must not hold up requests that
// already have a live worker, and concurrent callers start only one lookup.
func TestVectorDiscoveryLookupDoesNotBlockCachedClient(t *testing.T) {
	d := &vectorDiscovery{
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		discover: func(masters []pb.ServerAddress, opt grpc.DialOption) []string {
			return []string{"127.0.0.1:9301"}
		},
	}
	cached, err := d.cli()
	if err != nil {
		t.Fatalf("worker registered: expected a client, got %v", err)
	}

	var calls int32
	entered, release := make(chan struct{}), make(chan struct{})
	d.discover = func(masters []pb.ServerAddress, opt grpc.DialOption) []string {
		if atomic.AddInt32(&calls, 1) == 1 {
			close(entered)
		}
		<-release
		return []string{"127.0.0.1:9301"}
	}
	d.mu.Lock()
	d.nextResolve = time.Time{}
	d.mu.Unlock()

	const callers = 8
	got := make(chan error, callers+1)
	call := func() {
		c, err := d.cli()
		if err == nil && c != cached {
			err = fmt.Errorf("got a different client")
		}
		got <- err
	}
	go call()
	<-entered
	for i := 0; i < callers; i++ {
		go call()
	}
	timeout := time.After(2 * time.Second)
	for i := 0; i <= callers; i++ {
		select {
		case err := <-got:
			if err != nil {
				close(release)
				t.Fatalf("cached client: %v", err)
			}
		case <-timeout:
			close(release)
			t.Fatalf("cached client blocked behind an in-flight lookup")
		}
	}
	close(release)
	waitResolved(d)
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("concurrent callers ran %d lookups, want 1", n)
	}
}

// Callers with no worker wait for the in-flight lookup and share its result
// rather than failing while it runs.
func TestVectorDiscoveryUncachedCallersShareLookup(t *testing.T) {
	var calls int32
	release := make(chan struct{})
	d := &vectorDiscovery{
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		discover: func(masters []pb.ServerAddress, opt grpc.DialOption) []string {
			atomic.AddInt32(&calls, 1)
			<-release
			return []string{"127.0.0.1:9301"}
		},
	}
	const callers = 8
	got := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			_, err := d.cli()
			got <- err
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	for i := 0; i < callers; i++ {
		if err := <-got; err != nil {
			t.Fatalf("caller waiting on the lookup: %v", err)
		}
	}
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("concurrent callers ran %d lookups, want 1", n)
	}
}

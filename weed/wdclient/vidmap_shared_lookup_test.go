package wdclient

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockingLookupProvider answers once release is closed, or fails with the
// context error if its context ends first, as the gRPC providers do.
type blockingLookupProvider struct {
	calls   atomic.Int32
	started chan struct{}
	once    sync.Once
	release chan struct{}
}

func (p *blockingLookupProvider) LookupVolumeIds(ctx context.Context, volumeIds []string) (map[string][]Location, error) {
	p.calls.Add(1)
	p.once.Do(func() { close(p.started) })
	select {
	case <-p.release:
		return map[string][]Location{"7": {{Url: "10.0.0.7:8080"}}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// joinSignalCtx closes joined the first time Done is read. doSharedLookup reads
// it only after DoChan has registered the caller with the flight.
type joinSignalCtx struct {
	context.Context
	once   sync.Once
	joined chan struct{}
}

func (c *joinSignalCtx) Done() <-chan struct{} {
	c.once.Do(func() { close(c.joined) })
	return c.Context.Done()
}

func TestSharedLookupSurvivesFirstCallerCancel(t *testing.T) {
	provider := &blockingLookupProvider{started: make(chan struct{}), release: make(chan struct{})}
	vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstErr := make(chan error, 1)
	go func() {
		_, err := vc.LookupVolumeIdsWithFallback(firstCtx, []string{"7"})
		firstErr <- err
	}()
	<-provider.started

	type lookupResult struct {
		locations map[string][]Location
		err       error
	}
	second := make(chan lookupResult, 1)
	secondCtx := &joinSignalCtx{Context: context.Background(), joined: make(chan struct{})}
	go func() {
		locations, err := vc.LookupVolumeIdsWithFallback(secondCtx, []string{"7"})
		second <- lookupResult{locations, err}
	}()
	select {
	case <-secondCtx.joined:
	case <-time.After(5 * time.Second):
		t.Fatal("second caller never joined the shared lookup on its own context")
	}

	cancelFirst()
	select {
	case err := <-firstErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled caller got %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled caller kept waiting on the shared lookup")
	}

	close(provider.release)
	select {
	case res := <-second:
		if res.err != nil || len(res.locations["7"]) != 1 {
			t.Fatalf("waiter got %v, err %v after another caller canceled", res.locations["7"], res.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter never got the shared lookup result")
	}
	if calls := provider.calls.Load(); calls != 1 {
		t.Fatalf("provider called %d times, want one shared lookup", calls)
	}
}

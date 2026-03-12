package weed_server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProxySemaphore_LimitsConcurrency(t *testing.T) {
	host := "test-volume:8080"

	var running atomic.Int32
	var maxSeen atomic.Int32
	var wg sync.WaitGroup

	// Launch more goroutines than the semaphore allows
	total := proxyReadConcurrencyPerVolumeServer * 3
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := acquireProxySemaphore(context.Background(), host); err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			defer releaseProxySemaphore(host)

			cur := running.Add(1)
			// Track peak concurrency
			for {
				old := maxSeen.Load()
				if cur <= old || maxSeen.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			running.Add(-1)
		}()
	}
	wg.Wait()

	peak := maxSeen.Load()
	if peak > int32(proxyReadConcurrencyPerVolumeServer) {
		t.Fatalf("peak concurrency %d exceeded limit %d", peak, proxyReadConcurrencyPerVolumeServer)
	}
	if peak == 0 {
		t.Fatal("no goroutines ran")
	}
}

func TestProxySemaphore_ContextCancellation(t *testing.T) {
	host := "test-cancel:8080"

	// Fill the semaphore
	for i := 0; i < proxyReadConcurrencyPerVolumeServer; i++ {
		if err := acquireProxySemaphore(context.Background(), host); err != nil {
			t.Fatalf("fill acquire: %v", err)
		}
	}

	// Try to acquire with a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := acquireProxySemaphore(ctx, host)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}

	// Clean up
	for i := 0; i < proxyReadConcurrencyPerVolumeServer; i++ {
		releaseProxySemaphore(host)
	}
}

func TestProxySemaphore_PerHostIsolation(t *testing.T) {
	hostA := "volume-a:8080"
	hostB := "volume-b:8080"

	// Fill hostA's semaphore
	for i := 0; i < proxyReadConcurrencyPerVolumeServer; i++ {
		if err := acquireProxySemaphore(context.Background(), hostA); err != nil {
			t.Fatalf("fill hostA: %v", err)
		}
	}

	// hostB should still be acquirable
	if err := acquireProxySemaphore(context.Background(), hostB); err != nil {
		t.Fatalf("hostB should not be blocked by hostA: %v", err)
	}
	releaseProxySemaphore(hostB)

	// Clean up hostA
	for i := 0; i < proxyReadConcurrencyPerVolumeServer; i++ {
		releaseProxySemaphore(hostA)
	}
}

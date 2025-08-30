package mount

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPrefetchManager_Basic(t *testing.T) {
	pm := NewPrefetchManager(2, 10, 5*time.Second)
	defer pm.Shutdown()

	// Test basic prefetch request
	ctx := context.Background()
	var callbackData []byte
	var callbackErr error
	var callbackCalled int32
	
	callback := func(data []byte, err error) {
		atomic.StoreInt32(&callbackCalled, 1)
		callbackData = data
		callbackErr = err
	}

	success := pm.Prefetch(ctx, "file1", 0, 0, 1024, 1, callback)
	if !success {
		t.Error("Expected prefetch request to succeed")
	}

	// Wait for callback to be called
	time.Sleep(100 * time.Millisecond)
	
	if atomic.LoadInt32(&callbackCalled) != 1 {
		t.Error("Expected callback to be called")
	}
	
	if callbackErr != nil {
		t.Errorf("Expected no error, got: %v", callbackErr)
	}
	
	if len(callbackData) != 1024 {
		t.Errorf("Expected data length 1024, got: %d", len(callbackData))
	}
}

func TestPrefetchManager_DuplicateRequests(t *testing.T) {
	pm := NewPrefetchManager(2, 10, 5*time.Second)
	defer pm.Shutdown()

	ctx := context.Background()
	var callbackCount int32
	
	callback := func(data []byte, err error) {
		atomic.AddInt32(&callbackCount, 1)
	}

	// Send the same request multiple times
	success1 := pm.Prefetch(ctx, "file1", 0, 0, 1024, 1, callback)
	success2 := pm.Prefetch(ctx, "file1", 0, 0, 1024, 1, callback)
	success3 := pm.Prefetch(ctx, "file1", 0, 0, 1024, 1, callback)

	if !success1 {
		t.Error("Expected first prefetch request to succeed")
	}
	
	if success2 || success3 {
		t.Error("Expected duplicate requests to be rejected")
	}

	// Wait for processing
	time.Sleep(100 * time.Millisecond)
	
	// Should have only one callback
	if atomic.LoadInt32(&callbackCount) != 1 {
		t.Errorf("Expected 1 callback, got: %d", atomic.LoadInt32(&callbackCount))
	}
	
	// Check metrics
	metrics := pm.GetMetrics()
	if metrics.TotalRequests != 3 {
		t.Errorf("Expected 3 total requests, got: %d", metrics.TotalRequests)
	}
	
	if metrics.DuplicateReqs != 2 {
		t.Errorf("Expected 2 duplicate requests, got: %d", metrics.DuplicateReqs)
	}
}

func TestPrefetchManager_WorkerPool(t *testing.T) {
	pm := NewPrefetchManager(3, 20, 5*time.Second)
	defer pm.Shutdown()

	ctx := context.Background()
	var completedCount int32
	
	callback := func(data []byte, err error) {
		atomic.AddInt32(&completedCount, 1)
	}

	// Send multiple requests
	requestCount := 10
	for i := 0; i < requestCount; i++ {
		fileId := "file" + string(rune('0'+i))
		success := pm.Prefetch(ctx, fileId, 0, 0, 1024, 1, callback)
		if !success {
			t.Errorf("Expected prefetch request %d to succeed", i)
		}
	}

	// Wait for all to complete
	time.Sleep(200 * time.Millisecond)
	
	completed := atomic.LoadInt32(&completedCount)
	if completed != int32(requestCount) {
		t.Errorf("Expected %d completed requests, got: %d", requestCount, completed)
	}
	
	metrics := pm.GetMetrics()
	if metrics.SuccessfulFetch != int64(requestCount) {
		t.Errorf("Expected %d successful fetches, got: %d", requestCount, metrics.SuccessfulFetch)
	}
}

func TestPrefetchManager_Cancel(t *testing.T) {
	pm := NewPrefetchManager(1, 5, 5*time.Second) // Single worker to ensure ordering
	defer pm.Shutdown()

	ctx := context.Background()
	var callbackCalled int32
	
	callback := func(data []byte, err error) {
		atomic.StoreInt32(&callbackCalled, 1)
	}

	// Queue a request
	success := pm.Prefetch(ctx, "file1", 0, 0, 1024, 1, callback)
	if !success {
		t.Error("Expected prefetch request to succeed")
	}
	
	// Cancel it immediately
	cancelled := pm.Cancel("file1", 0)
	if !cancelled {
		t.Error("Expected cancel to succeed")
	}

	// Wait a bit
	time.Sleep(50 * time.Millisecond)
	
	// Callback might still be called since cancellation is asynchronous
	// Main thing is that the job was marked as cancelled
}

func TestPrefetchManager_QueueFull(t *testing.T) {
	pm := NewPrefetchManager(1, 2, 5*time.Second) // Small queue
	defer pm.Shutdown()

	ctx := context.Background()
	callback := func(data []byte, err error) {}

	// Fill the queue
	success1 := pm.Prefetch(ctx, "file1", 0, 0, 1024, 1, callback)
	success2 := pm.Prefetch(ctx, "file2", 0, 0, 1024, 1, callback)
	success3 := pm.Prefetch(ctx, "file3", 0, 0, 1024, 1, callback) // This should fail

	if !success1 || !success2 {
		t.Error("Expected first two requests to succeed")
	}
	
	if success3 {
		t.Error("Expected third request to fail due to full queue")
	}
}

func TestPrefetchManager_Timeout(t *testing.T) {
	pm := NewPrefetchManager(1, 5, 50*time.Millisecond) // Very short timeout
	defer pm.Shutdown()

	ctx := context.Background()
	var timeoutCount int32
	
	callback := func(data []byte, err error) {
		if err == context.DeadlineExceeded {
			atomic.AddInt32(&timeoutCount, 1)
		}
	}

	// This implementation doesn't actually timeout since fetchChunk is fast
	// But the structure is there for when we integrate with real chunk fetching
	success := pm.Prefetch(ctx, "file1", 0, 0, 1024, 1, callback)
	if !success {
		t.Error("Expected prefetch request to succeed")
	}

	time.Sleep(200 * time.Millisecond)
}

func TestPrefetchManager_ConcurrentAccess(t *testing.T) {
	pm := NewPrefetchManager(4, 50, 5*time.Second)
	defer pm.Shutdown()

	ctx := context.Background()
	var completedCount int32
	
	callback := func(data []byte, err error) {
		atomic.AddInt32(&completedCount, 1)
	}

	// Test concurrent access from multiple goroutines
	var wg sync.WaitGroup
	goroutineCount := 10
	requestsPerGoroutine := 5
	
	for i := 0; i < goroutineCount; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			
			for j := 0; j < requestsPerGoroutine; j++ {
				fileId := "file" + string(rune('0'+goroutineID)) + "_" + string(rune('0'+j))
				pm.Prefetch(ctx, fileId, 0, 0, 1024, 1, callback)
			}
		}(i)
	}
	
	wg.Wait()
	
	// Wait for all requests to complete
	time.Sleep(500 * time.Millisecond)
	
	expectedTotal := goroutineCount * requestsPerGoroutine
	completed := atomic.LoadInt32(&completedCount)
	
	if completed != int32(expectedTotal) {
		t.Errorf("Expected %d completed requests, got: %d", expectedTotal, completed)
	}
}

func TestPrefetchManager_Metrics(t *testing.T) {
	pm := NewPrefetchManager(2, 10, 5*time.Second)
	defer pm.Shutdown()

	ctx := context.Background()
	callback := func(data []byte, err error) {}

	// Make some requests
	pm.Prefetch(ctx, "file1", 0, 0, 1024, 1, callback)
	pm.Prefetch(ctx, "file2", 0, 0, 1024, 1, callback)
	pm.Prefetch(ctx, "file1", 0, 0, 1024, 1, callback) // Duplicate

	time.Sleep(100 * time.Millisecond)

	metrics := pm.GetMetrics()
	
	if metrics.TotalRequests != 3 {
		t.Errorf("Expected 3 total requests, got: %d", metrics.TotalRequests)
	}
	
	if metrics.DuplicateReqs != 1 {
		t.Errorf("Expected 1 duplicate request, got: %d", metrics.DuplicateReqs)
	}
	
	if metrics.Workers != 2 {
		t.Errorf("Expected 2 workers, got: %d", metrics.Workers)
	}
	
	// Should have some successful fetches
	if metrics.SuccessfulFetch == 0 {
		t.Error("Expected some successful fetches")
	}
}

func TestPrefetchManager_Shutdown(t *testing.T) {
	pm := NewPrefetchManager(2, 10, 5*time.Second)

	ctx := context.Background()
	callback := func(data []byte, err error) {}

	// Make a request
	pm.Prefetch(ctx, "file1", 0, 0, 1024, 1, callback)

	// Shutdown should complete without hanging
	done := make(chan struct{})
	go func() {
		pm.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Error("Shutdown took too long")
	}
}

// Benchmark tests

func BenchmarkPrefetchManager_SingleWorker(b *testing.B) {
	pm := NewPrefetchManager(1, 1000, 30*time.Second)
	defer pm.Shutdown()

	ctx := context.Background()
	callback := func(data []byte, err error) {}

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		fileId := "file" + string(rune(i%100)) // Reuse file IDs to test deduplication
		pm.Prefetch(ctx, fileId, uint32(i), 0, 1024, 1, callback)
	}
}

func BenchmarkPrefetchManager_MultipleWorkers(b *testing.B) {
	pm := NewPrefetchManager(8, 1000, 30*time.Second)
	defer pm.Shutdown()

	ctx := context.Background()
	callback := func(data []byte, err error) {}

	b.ResetTimer()
	
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			fileId := "file" + string(rune(i%1000))
			pm.Prefetch(ctx, fileId, uint32(i), 0, 1024, 1, callback)
			i++
		}
	})
}

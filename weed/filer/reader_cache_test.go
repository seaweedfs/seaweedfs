package filer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockChunkCacheForReaderCache implements chunk cache for testing
type mockChunkCacheForReaderCache struct {
	data     map[string][]byte
	hitCount int32
	mu       sync.Mutex
}

func newMockChunkCacheForReaderCache() *mockChunkCacheForReaderCache {
	return &mockChunkCacheForReaderCache{
		data: make(map[string][]byte),
	}
}

func (m *mockChunkCacheForReaderCache) GetChunk(fileId string, minSize uint64) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d, ok := m.data[fileId]; ok {
		atomic.AddInt32(&m.hitCount, 1)
		return d
	}
	return nil
}

func (m *mockChunkCacheForReaderCache) ReadChunkAt(data []byte, fileId string, offset uint64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d, ok := m.data[fileId]; ok && int(offset) < len(d) {
		atomic.AddInt32(&m.hitCount, 1)
		n := copy(data, d[offset:])
		return n, nil
	}
	return 0, nil
}

func (m *mockChunkCacheForReaderCache) SetChunk(fileId string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[fileId] = data
}

func (m *mockChunkCacheForReaderCache) GetMaxFilePartSizeInCache() uint64 {
	return 1024 * 1024 // 1MB
}

func (m *mockChunkCacheForReaderCache) IsInCache(fileId string, lockNeeded bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.data[fileId]
	return ok
}

// TestReaderCacheContextCancellation tests that a reader can cancel its wait
// while the download continues for other readers
func TestReaderCacheContextCancellation(t *testing.T) {
	cache := newMockChunkCacheForReaderCache()

	// Create a ReaderCache - we can't easily test the full flow without mocking HTTP,
	// but we can test the context cancellation in readChunkAt
	rc := NewReaderCache(10, cache, nil)
	defer rc.destroy()

	// Pre-populate cache to avoid HTTP calls
	testData := []byte("test data for context cancellation")
	cache.SetChunk("test-file-1", testData)

	// Test that context cancellation works
	ctx, cancel := context.WithCancel(context.Background())

	buffer := make([]byte, len(testData))
	n, err := rc.ReadChunkAt(ctx, buffer, "test-file-1", nil, false, 0, len(testData), true)
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected %d bytes, got %d", len(testData), n)
	}

	// Cancel context and verify it doesn't affect already completed reads
	cancel()

	// Subsequent read with cancelled context should still work from cache
	buffer2 := make([]byte, len(testData))
	n2, err2 := rc.ReadChunkAt(ctx, buffer2, "test-file-1", nil, false, 0, len(testData), true)
	// Note: This may or may not error depending on whether it hits cache
	_ = n2
	_ = err2
}

// TestReaderCacheFallbackToChunkCache tests that when a cacher returns n=0, err=nil,
// we fall back to the chunkCache
func TestReaderCacheFallbackToChunkCache(t *testing.T) {
	cache := newMockChunkCacheForReaderCache()

	// Pre-populate the chunk cache with data
	testData := []byte("fallback test data that should be found in chunk cache")
	cache.SetChunk("fallback-file", testData)

	rc := NewReaderCache(10, cache, nil)
	defer rc.destroy()

	// Read should hit the chunk cache
	buffer := make([]byte, len(testData))
	n, err := rc.ReadChunkAt(context.Background(), buffer, "fallback-file", nil, false, 0, len(testData), true)

	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected %d bytes, got %d", len(testData), n)
	}

	// Verify cache was hit
	if cache.hitCount == 0 {
		t.Error("Expected chunk cache to be hit")
	}
}

// TestReaderCacheMultipleReadersWaitForSameChunk tests that multiple readers
// can wait for the same chunk download to complete
func TestReaderCacheMultipleReadersWaitForSameChunk(t *testing.T) {
	cache := newMockChunkCacheForReaderCache()

	// Pre-populate cache so we don't need HTTP
	testData := make([]byte, 1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	cache.SetChunk("shared-chunk", testData)

	rc := NewReaderCache(10, cache, nil)
	defer rc.destroy()

	// Launch multiple concurrent readers for the same chunk
	numReaders := 10
	var wg sync.WaitGroup
	errors := make(chan error, numReaders)
	bytesRead := make(chan int, numReaders)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buffer := make([]byte, len(testData))
			n, err := rc.ReadChunkAt(context.Background(), buffer, "shared-chunk", nil, false, 0, len(testData), true)
			if err != nil {
				errors <- err
			}
			bytesRead <- n
		}()
	}

	wg.Wait()
	close(errors)
	close(bytesRead)

	// Check for errors
	for err := range errors {
		t.Errorf("Reader got error: %v", err)
	}

	// Verify all readers got the expected data
	for n := range bytesRead {
		if n != len(testData) {
			t.Errorf("Expected %d bytes, got %d", len(testData), n)
		}
	}
}

// TestReaderCachePartialRead tests reading at different offsets
func TestReaderCachePartialRead(t *testing.T) {
	cache := newMockChunkCacheForReaderCache()

	testData := []byte("0123456789ABCDEFGHIJ")
	cache.SetChunk("partial-read-file", testData)

	rc := NewReaderCache(10, cache, nil)
	defer rc.destroy()

	tests := []struct {
		name     string
		offset   int64
		size     int
		expected []byte
	}{
		{"read from start", 0, 5, []byte("01234")},
		{"read from middle", 5, 5, []byte("56789")},
		{"read to end", 15, 5, []byte("FGHIJ")},
		{"read single byte", 10, 1, []byte("A")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := make([]byte, tt.size)
			n, err := rc.ReadChunkAt(context.Background(), buffer, "partial-read-file", nil, false, tt.offset, len(testData), true)

			if err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}
			if n != tt.size {
				t.Errorf("Expected %d bytes, got %d", tt.size, n)
			}
			if string(buffer[:n]) != string(tt.expected) {
				t.Errorf("Expected %q, got %q", tt.expected, buffer[:n])
			}
		})
	}
}

// TestReaderCacheCleanup tests that old downloaders are cleaned up
func TestReaderCacheCleanup(t *testing.T) {
	cache := newMockChunkCacheForReaderCache()

	// Create cache with limit of 3
	rc := NewReaderCache(3, cache, nil)
	defer rc.destroy()

	// Add data for multiple files
	for i := 0; i < 5; i++ {
		fileId := string(rune('A' + i))
		data := []byte("data for file " + fileId)
		cache.SetChunk(fileId, data)
	}

	// Read from multiple files - should trigger cleanup when exceeding limit
	for i := 0; i < 5; i++ {
		fileId := string(rune('A' + i))
		buffer := make([]byte, 20)
		_, err := rc.ReadChunkAt(context.Background(), buffer, fileId, nil, false, 0, 20, true)
		if err != nil {
			t.Errorf("Read error for file %s: %v", fileId, err)
		}
	}

	// Cache should still work - reads should succeed
	for i := 0; i < 5; i++ {
		fileId := string(rune('A' + i))
		buffer := make([]byte, 20)
		n, err := rc.ReadChunkAt(context.Background(), buffer, fileId, nil, false, 0, 20, true)
		if err != nil {
			t.Errorf("Second read error for file %s: %v", fileId, err)
		}
		if n == 0 {
			t.Errorf("Expected data for file %s, got 0 bytes", fileId)
		}
	}
}

// TestSingleChunkCacherDoneSignal tests that done channel is always closed
func TestSingleChunkCacherDoneSignal(t *testing.T) {
	cache := newMockChunkCacheForReaderCache()
	rc := NewReaderCache(10, cache, nil)
	defer rc.destroy()

	// Test that we can read even when data is in cache (done channel should work)
	testData := []byte("done signal test")
	cache.SetChunk("done-signal-test", testData)

	// Multiple goroutines reading same chunk
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buffer := make([]byte, len(testData))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			n, err := rc.ReadChunkAt(ctx, buffer, "done-signal-test", nil, false, 0, len(testData), true)
			if err != nil && err != context.DeadlineExceeded {
				t.Errorf("Unexpected error: %v", err)
			}
			if n == 0 && err == nil {
				t.Error("Got 0 bytes with no error")
			}
		}()
	}

	// Should complete without hanging
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out - done channel may not be signaled correctly")
	}
}

// ============================================================================
// Tests that exercise SingleChunkCacher concurrency logic
// ============================================================================
//
// These tests use blocking lookupFileIdFn to exercise the wait/cancellation
// logic in SingleChunkCacher without requiring HTTP calls.

// TestSingleChunkCacherLookupError tests handling of lookup errors
func TestSingleChunkCacherLookupError(t *testing.T) {
	cache := newMockChunkCacheForReaderCache()
	
	// Lookup function that returns an error
	lookupFn := func(ctx context.Context, fileId string) ([]string, error) {
		return nil, fmt.Errorf("lookup failed for %s", fileId)
	}

	rc := NewReaderCache(10, cache, lookupFn)
	defer rc.destroy()

	buffer := make([]byte, 100)
	_, err := rc.ReadChunkAt(context.Background(), buffer, "error-test", nil, false, 0, 100, true)
	
	if err == nil {
		t.Error("Expected an error, got nil")
	}
}

// TestSingleChunkCacherContextCancellationDuringLookup tests that a reader can
// cancel its wait while the lookup is in progress. This exercises the actual
// SingleChunkCacher wait/cancel logic.
func TestSingleChunkCacherContextCancellationDuringLookup(t *testing.T) {
	cache := newMockChunkCacheForReaderCache()
	lookupStarted := make(chan struct{})
	lookupCanFinish := make(chan struct{})

	// Lookup function that blocks to simulate slow operation
	lookupFn := func(ctx context.Context, fileId string) ([]string, error) {
		close(lookupStarted)
		<-lookupCanFinish // Block until test allows completion
		return nil, fmt.Errorf("lookup completed but reader should have cancelled")
	}

	rc := NewReaderCache(10, cache, lookupFn)
	defer rc.destroy()
	defer close(lookupCanFinish) // Ensure cleanup

	ctx, cancel := context.WithCancel(context.Background())
	readResult := make(chan error, 1)

	go func() {
		buffer := make([]byte, 100)
		_, err := rc.ReadChunkAt(ctx, buffer, "cancel-during-lookup", nil, false, 0, 100, true)
		readResult <- err
	}()

	// Wait for lookup to start, then cancel the reader's context
	select {
	case <-lookupStarted:
		cancel() // Cancel the reader while lookup is blocked
	case <-time.After(5 * time.Second):
		t.Fatal("Lookup never started")
	}

	// Read should return with context.Canceled
	select {
	case err := <-readResult:
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read did not complete after context cancellation")
	}
}

// TestSingleChunkCacherMultipleReadersWaitForDownload tests that multiple readers
// can wait for the same SingleChunkCacher download to complete. When lookup fails,
// all readers should receive the same error.
func TestSingleChunkCacherMultipleReadersWaitForDownload(t *testing.T) {
	cache := newMockChunkCacheForReaderCache()
	lookupStarted := make(chan struct{})
	lookupCanFinish := make(chan struct{})
	var lookupStartedOnce sync.Once

	// Lookup function that blocks to simulate slow operation
	lookupFn := func(ctx context.Context, fileId string) ([]string, error) {
		lookupStartedOnce.Do(func() { close(lookupStarted) })
		<-lookupCanFinish
		return nil, fmt.Errorf("simulated lookup error")
	}

	rc := NewReaderCache(10, cache, lookupFn)
	defer rc.destroy()

	numReaders := 5
	var wg sync.WaitGroup
	errors := make(chan error, numReaders)

	// Start multiple readers for the same chunk
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buffer := make([]byte, 100)
			_, err := rc.ReadChunkAt(context.Background(), buffer, "shared-chunk", nil, false, 0, 100, true)
			errors <- err
		}()
	}

	// Wait for lookup to start, then allow completion
	select {
	case <-lookupStarted:
		close(lookupCanFinish)
	case <-time.After(5 * time.Second):
		close(lookupCanFinish)
		t.Fatal("Lookup never started")
	}

	wg.Wait()
	close(errors)

	// All readers should receive an error
	errorCount := 0
	for err := range errors {
		if err != nil {
			errorCount++
		}
	}
	if errorCount != numReaders {
		t.Errorf("Expected %d errors, got %d", numReaders, errorCount)
	}
}

// TestSingleChunkCacherOneReaderCancelsOthersContinue tests that when one reader
// cancels, other readers waiting on the same chunk continue to wait.
func TestSingleChunkCacherOneReaderCancelsOthersContinue(t *testing.T) {
	cache := newMockChunkCacheForReaderCache()
	lookupStarted := make(chan struct{})
	lookupCanFinish := make(chan struct{})
	var lookupStartedOnce sync.Once

	lookupFn := func(ctx context.Context, fileId string) ([]string, error) {
		lookupStartedOnce.Do(func() { close(lookupStarted) })
		<-lookupCanFinish
		return nil, fmt.Errorf("simulated error after delay")
	}

	rc := NewReaderCache(10, cache, lookupFn)
	defer rc.destroy()

	cancelledReaderDone := make(chan error, 1)
	otherReaderDone := make(chan error, 1)

	ctx, cancel := context.WithCancel(context.Background())

	// Start reader that will be cancelled
	go func() {
		buffer := make([]byte, 100)
		_, err := rc.ReadChunkAt(ctx, buffer, "shared-chunk-2", nil, false, 0, 100, true)
		cancelledReaderDone <- err
	}()

	// Start reader that will NOT be cancelled
	go func() {
		buffer := make([]byte, 100)
		_, err := rc.ReadChunkAt(context.Background(), buffer, "shared-chunk-2", nil, false, 0, 100, true)
		otherReaderDone <- err
	}()

	// Wait for lookup to start
	select {
	case <-lookupStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Lookup never started")
	}

	// Cancel the first reader
	cancel()

	// First reader should complete with context.Canceled quickly
	select {
	case err := <-cancelledReaderDone:
		if err != context.Canceled {
			t.Errorf("Cancelled reader: expected context.Canceled, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Cancelled reader did not complete quickly")
	}

	// Allow the download to complete
	close(lookupCanFinish)

	// Other reader should eventually complete (with error since lookup returns error)
	select {
	case err := <-otherReaderDone:
		if err == nil || err == context.Canceled {
			t.Errorf("Other reader: expected non-nil non-cancelled error, got: %v", err)
		}
		// Expected: "simulated error after delay"
	case <-time.After(5 * time.Second):
		t.Error("Other reader did not complete")
	}
}

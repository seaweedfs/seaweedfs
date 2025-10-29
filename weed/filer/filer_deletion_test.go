package filer

import (
	"container/heap"
	"testing"
	"time"
)

func TestDeletionRetryQueue_AddAndRetrieve(t *testing.T) {
	queue := NewDeletionRetryQueue()

	// Add items
	queue.AddOrUpdate("file1", "is read only")
	queue.AddOrUpdate("file2", "connection reset")

	if queue.Size() != 2 {
		t.Errorf("Expected queue size 2, got %d", queue.Size())
	}

	// Items not ready yet (initial delay is 5 minutes)
	readyItems := queue.GetReadyItems(10)
	if len(readyItems) != 0 {
		t.Errorf("Expected 0 ready items, got %d", len(readyItems))
	}

	// Size should remain unchanged
	if queue.Size() != 2 {
		t.Errorf("Expected queue size 2 after checking ready items, got %d", queue.Size())
	}
}

func TestDeletionRetryQueue_ExponentialBackoff(t *testing.T) {
	queue := NewDeletionRetryQueue()

	// Create an item
	item := &DeletionRetryItem{
		FileId:      "test-file",
		RetryCount:  0,
		NextRetryAt: time.Now(),
		LastError:   "test error",
	}

	// Requeue multiple times to test backoff
	delays := []time.Duration{}

	for i := 0; i < 5; i++ {
		beforeTime := time.Now()
		queue.RequeueForRetry(item, "error")

		// Calculate expected delay for this retry count
		expectedDelay := InitialRetryDelay * time.Duration(1<<uint(i))
		if expectedDelay > MaxRetryDelay {
			expectedDelay = MaxRetryDelay
		}

		// Verify NextRetryAt is approximately correct
		actualDelay := item.NextRetryAt.Sub(beforeTime)
		delays = append(delays, actualDelay)

		// Allow small timing variance
		timeDiff := actualDelay - expectedDelay
		if timeDiff < 0 {
			timeDiff = -timeDiff
		}
		if timeDiff > 100*time.Millisecond {
			t.Errorf("Retry %d: expected delay ~%v, got %v (diff: %v)", i+1, expectedDelay, actualDelay, timeDiff)
		}

		// Verify retry count incremented
		if item.RetryCount != i+1 {
			t.Errorf("Expected RetryCount %d, got %d", i+1, item.RetryCount)
		}

		// Remove from queue for next iteration
		queue.lock.Lock()
		delete(queue.itemIndex, item.FileId)
		queue.heap = retryHeap{}
		queue.lock.Unlock()
	}

	t.Logf("Exponential backoff delays: %v", delays)
}

func TestDeletionRetryQueue_OverflowProtection(t *testing.T) {
	queue := NewDeletionRetryQueue()

	// Create an item with very high retry count
	item := &DeletionRetryItem{
		FileId:      "test-file",
		RetryCount:  60, // High count that would cause overflow without protection
		NextRetryAt: time.Now(),
		LastError:   "test error",
	}

	// Should not panic and should cap at MaxRetryDelay
	queue.RequeueForRetry(item, "error")

	delay := item.NextRetryAt.Sub(time.Now())
	if delay > MaxRetryDelay+time.Second {
		t.Errorf("Delay exceeded MaxRetryDelay: %v > %v", delay, MaxRetryDelay)
	}
}

func TestDeletionRetryQueue_MaxAttemptsReached(t *testing.T) {
	queue := NewDeletionRetryQueue()

	// Add item and set retry count near max
	queue.AddOrUpdate("file1", "error")

	// Manually set high retry count
	queue.lock.Lock()
	if item, exists := queue.itemIndex["file1"]; exists {
		item.RetryCount = MaxRetryAttempts
		item.NextRetryAt = time.Now().Add(-1 * time.Second) // Ready now
		queue.lock.Unlock()

		// Try to get ready items - should be discarded
		readyItems := queue.GetReadyItems(10)
		if len(readyItems) != 0 {
			t.Errorf("Expected 0 items (max attempts reached), got %d", len(readyItems))
		}

		// Should be removed from queue
		if queue.Size() != 0 {
			t.Errorf("Expected queue size 0 after max attempts, got %d", queue.Size())
		}
	} else {
		queue.lock.Unlock()
		t.Fatal("Item not found in queue")
	}
}

func TestIsRetryableError(t *testing.T) {
	testCases := []struct {
		error      string
		retryable  bool
		description string
	}{
		{"volume 123 is read only", true, "read-only volume"},
		{"connection reset by peer", true, "connection reset"},
		{"timeout exceeded", true, "timeout"},
		{"deadline exceeded", true, "deadline exceeded"},
		{"context canceled", true, "context canceled"},
		{"lookup error: volume not found", true, "lookup error"},
		{"connection refused", true, "connection refused"},
		{"too many requests", true, "rate limiting"},
		{"service unavailable", true, "service unavailable"},
		{"i/o timeout", true, "I/O timeout"},
		{"broken pipe", true, "broken pipe"},
		{"not found", false, "not found (not retryable)"},
		{"invalid file id", false, "invalid input (not retryable)"},
		{"", false, "empty error"},
	}

	for _, tc := range testCases {
		result := isRetryableError(tc.error)
		if result != tc.retryable {
			t.Errorf("%s: expected retryable=%v, got %v for error: %q",
				tc.description, tc.retryable, result, tc.error)
		}
	}
}

func TestDeletionRetryQueue_HeapOrdering(t *testing.T) {
	queue := NewDeletionRetryQueue()

	now := time.Now()

	// Add items with different retry times (out of order)
	items := []*DeletionRetryItem{
		{FileId: "file3", RetryCount: 1, NextRetryAt: now.Add(30 * time.Second), LastError: "error3"},
		{FileId: "file1", RetryCount: 1, NextRetryAt: now.Add(10 * time.Second), LastError: "error1"},
		{FileId: "file2", RetryCount: 1, NextRetryAt: now.Add(20 * time.Second), LastError: "error2"},
	}

	// Add items directly (simulating internal state)
	for _, item := range items {
		queue.lock.Lock()
		queue.itemIndex[item.FileId] = item
		queue.heap = append(queue.heap, item)
		queue.lock.Unlock()
	}

	// Use container/heap.Init to establish heap property
	queue.lock.Lock()
	heap.Init(&queue.heap)
	queue.lock.Unlock()

	// Verify heap maintains min-heap property (earliest time at top)
	queue.lock.Lock()
	if queue.heap[0].FileId != "file1" {
		t.Errorf("Expected file1 at heap top (earliest time), got %s", queue.heap[0].FileId)
	}
	queue.lock.Unlock()

	// Set all items to ready
	queue.lock.Lock()
	for _, item := range queue.itemIndex {
		item.NextRetryAt = now.Add(-1 * time.Second)
	}
	queue.lock.Unlock()

	// GetReadyItems should return in NextRetryAt order
	readyItems := queue.GetReadyItems(10)
	expectedOrder := []string{"file1", "file2", "file3"}

	if len(readyItems) != 3 {
		t.Fatalf("Expected 3 ready items, got %d", len(readyItems))
	}

	for i, item := range readyItems {
		if item.FileId != expectedOrder[i] {
			t.Errorf("Item %d: expected %s, got %s", i, expectedOrder[i], item.FileId)
		}
	}
}

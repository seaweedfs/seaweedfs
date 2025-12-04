package empty_folder_cleanup

import (
	"testing"
	"time"
)

func TestCleanupQueue_Add(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)
	now := time.Now()

	// Add first item
	if !q.Add("/buckets/b1/folder1", now) {
		t.Error("expected Add to return true for new item")
	}
	if q.Len() != 1 {
		t.Errorf("expected len 1, got %d", q.Len())
	}

	// Add second item with later time
	if !q.Add("/buckets/b1/folder2", now.Add(1*time.Second)) {
		t.Error("expected Add to return true for new item")
	}
	if q.Len() != 2 {
		t.Errorf("expected len 2, got %d", q.Len())
	}

	// Add duplicate with newer time - should update and reposition
	if q.Add("/buckets/b1/folder1", now.Add(2*time.Second)) {
		t.Error("expected Add to return false for existing item")
	}
	if q.Len() != 2 {
		t.Errorf("expected len 2 after duplicate, got %d", q.Len())
	}

	// folder1 should now be at the back (newer time) - verify by popping
	folder1, _ := q.Pop()
	folder2, _ := q.Pop()
	if folder1 != "/buckets/b1/folder2" || folder2 != "/buckets/b1/folder1" {
		t.Errorf("expected folder1 to be moved to back, got %s, %s", folder1, folder2)
	}
}

func TestCleanupQueue_Add_OutOfOrder(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)
	baseTime := time.Now()

	// Add items out of order
	q.Add("/buckets/b1/folder3", baseTime.Add(3*time.Second))
	q.Add("/buckets/b1/folder1", baseTime.Add(1*time.Second))
	q.Add("/buckets/b1/folder2", baseTime.Add(2*time.Second))

	// Items should be in time order (oldest first) - verify by popping
	expected := []string{"/buckets/b1/folder1", "/buckets/b1/folder2", "/buckets/b1/folder3"}
	for i, exp := range expected {
		folder, ok := q.Pop()
		if !ok || folder != exp {
			t.Errorf("at index %d: expected %s, got %s", i, exp, folder)
		}
	}
}

func TestCleanupQueue_Add_DuplicateWithOlderTime(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)
	baseTime := time.Now()

	// Add folder at t=5
	q.Add("/buckets/b1/folder1", baseTime.Add(5*time.Second))

	// Try to add same folder with older time - should NOT update
	q.Add("/buckets/b1/folder1", baseTime.Add(2*time.Second))

	// Time should remain at t=5
	_, queueTime, _ := q.Peek()
	if queueTime != baseTime.Add(5*time.Second) {
		t.Errorf("expected time to remain unchanged, got %v", queueTime)
	}
}

func TestCleanupQueue_Remove(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)
	now := time.Now()

	q.Add("/buckets/b1/folder1", now)
	q.Add("/buckets/b1/folder2", now.Add(1*time.Second))
	q.Add("/buckets/b1/folder3", now.Add(2*time.Second))

	// Remove middle item
	if !q.Remove("/buckets/b1/folder2") {
		t.Error("expected Remove to return true for existing item")
	}
	if q.Len() != 2 {
		t.Errorf("expected len 2, got %d", q.Len())
	}
	if q.Contains("/buckets/b1/folder2") {
		t.Error("removed item should not be in queue")
	}

	// Remove non-existent item
	if q.Remove("/buckets/b1/nonexistent") {
		t.Error("expected Remove to return false for non-existent item")
	}

	// Verify order is preserved by popping
	folder1, _ := q.Pop()
	folder3, _ := q.Pop()
	if folder1 != "/buckets/b1/folder1" || folder3 != "/buckets/b1/folder3" {
		t.Errorf("unexpected order: %s, %s", folder1, folder3)
	}
}

func TestCleanupQueue_Pop(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)
	now := time.Now()

	// Pop from empty queue
	folder, ok := q.Pop()
	if ok {
		t.Error("expected Pop to return false for empty queue")
	}
	if folder != "" {
		t.Errorf("expected empty folder, got %s", folder)
	}

	// Add items and pop in order
	q.Add("/buckets/b1/folder1", now)
	q.Add("/buckets/b1/folder2", now.Add(1*time.Second))
	q.Add("/buckets/b1/folder3", now.Add(2*time.Second))

	folder, ok = q.Pop()
	if !ok || folder != "/buckets/b1/folder1" {
		t.Errorf("expected folder1, got %s (ok=%v)", folder, ok)
	}

	folder, ok = q.Pop()
	if !ok || folder != "/buckets/b1/folder2" {
		t.Errorf("expected folder2, got %s (ok=%v)", folder, ok)
	}

	folder, ok = q.Pop()
	if !ok || folder != "/buckets/b1/folder3" {
		t.Errorf("expected folder3, got %s (ok=%v)", folder, ok)
	}

	// Queue should be empty now
	if q.Len() != 0 {
		t.Errorf("expected empty queue, got len %d", q.Len())
	}
}

func TestCleanupQueue_Peek(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)
	now := time.Now()

	// Peek empty queue
	folder, _, ok := q.Peek()
	if ok {
		t.Error("expected Peek to return false for empty queue")
	}

	// Add item and peek
	q.Add("/buckets/b1/folder1", now)
	folder, queueTime, ok := q.Peek()
	if !ok || folder != "/buckets/b1/folder1" {
		t.Errorf("expected folder1, got %s (ok=%v)", folder, ok)
	}
	if queueTime != now {
		t.Errorf("expected queue time %v, got %v", now, queueTime)
	}

	// Peek should not remove item
	if q.Len() != 1 {
		t.Errorf("Peek should not remove item, len=%d", q.Len())
	}
}

func TestCleanupQueue_Contains(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)
	now := time.Now()

	q.Add("/buckets/b1/folder1", now)

	if !q.Contains("/buckets/b1/folder1") {
		t.Error("expected Contains to return true")
	}
	if q.Contains("/buckets/b1/folder2") {
		t.Error("expected Contains to return false for non-existent")
	}
}

func TestCleanupQueue_ShouldProcess_MaxSize(t *testing.T) {
	q := NewCleanupQueue(3, 10*time.Minute)
	now := time.Now()

	// Empty queue
	if q.ShouldProcess() {
		t.Error("empty queue should not need processing")
	}

	// Add items below max
	q.Add("/buckets/b1/folder1", now)
	q.Add("/buckets/b1/folder2", now.Add(1*time.Second))
	if q.ShouldProcess() {
		t.Error("queue below max should not need processing")
	}

	// Add item to reach max
	q.Add("/buckets/b1/folder3", now.Add(2*time.Second))
	if !q.ShouldProcess() {
		t.Error("queue at max should need processing")
	}
}

func TestCleanupQueue_ShouldProcess_MaxAge(t *testing.T) {
	q := NewCleanupQueue(100, 100*time.Millisecond) // Short max age for testing

	// Add item with old event time
	oldTime := time.Now().Add(-1 * time.Second) // 1 second ago
	q.Add("/buckets/b1/folder1", oldTime)

	// Item is older than maxAge, should need processing
	if !q.ShouldProcess() {
		t.Error("old item should trigger processing")
	}

	// Clear and add fresh item
	q.Clear()
	q.Add("/buckets/b1/folder2", time.Now())

	// Fresh item should not trigger processing
	if q.ShouldProcess() {
		t.Error("fresh item should not trigger processing")
	}
}

func TestCleanupQueue_Clear(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)
	now := time.Now()

	q.Add("/buckets/b1/folder1", now)
	q.Add("/buckets/b1/folder2", now.Add(1*time.Second))
	q.Add("/buckets/b1/folder3", now.Add(2*time.Second))

	q.Clear()

	if q.Len() != 0 {
		t.Errorf("expected empty queue after Clear, got len %d", q.Len())
	}
	if q.Contains("/buckets/b1/folder1") {
		t.Error("queue should not contain items after Clear")
	}
}

func TestCleanupQueue_OldestAge(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)

	// Empty queue
	if q.OldestAge() != 0 {
		t.Error("empty queue should have zero oldest age")
	}

	// Add item with time in the past
	oldTime := time.Now().Add(-5 * time.Minute)
	q.Add("/buckets/b1/folder1", oldTime)

	// Age should be approximately 5 minutes
	age := q.OldestAge()
	if age < 4*time.Minute || age > 6*time.Minute {
		t.Errorf("expected ~5m age, got %v", age)
	}
}

func TestCleanupQueue_TimeOrder(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)
	baseTime := time.Now()

	// Add items in order
	items := []string{
		"/buckets/b1/a",
		"/buckets/b1/b",
		"/buckets/b1/c",
		"/buckets/b1/d",
		"/buckets/b1/e",
	}
	for i, item := range items {
		q.Add(item, baseTime.Add(time.Duration(i)*time.Second))
	}

	// Pop should return in time order
	for i, expected := range items {
		got, ok := q.Pop()
		if !ok {
			t.Errorf("Pop %d: expected item, got empty", i)
		}
		if got != expected {
			t.Errorf("Pop %d: expected %s, got %s", i, expected, got)
		}
	}
}

func TestCleanupQueue_DuplicateWithNewerTime(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)
	baseTime := time.Now()

	// Add items
	q.Add("/buckets/b1/folder1", baseTime)
	q.Add("/buckets/b1/folder2", baseTime.Add(1*time.Second))
	q.Add("/buckets/b1/folder3", baseTime.Add(2*time.Second))

	// Add duplicate with newer time - should update and reposition
	q.Add("/buckets/b1/folder1", baseTime.Add(3*time.Second))

	// folder1 should now be at the back (newest time) - verify by popping
	expected := []string{"/buckets/b1/folder2", "/buckets/b1/folder3", "/buckets/b1/folder1"}
	for i, exp := range expected {
		folder, ok := q.Pop()
		if !ok || folder != exp {
			t.Errorf("at index %d: expected %s, got %s", i, exp, folder)
		}
	}
}

func TestCleanupQueue_Concurrent(t *testing.T) {
	q := NewCleanupQueue(1000, 10*time.Minute)
	done := make(chan bool)
	now := time.Now()

	// Concurrent adds
	go func() {
		for i := 0; i < 100; i++ {
			q.Add("/buckets/b1/folder"+string(rune('A'+i%26)), now.Add(time.Duration(i)*time.Millisecond))
		}
		done <- true
	}()

	// Concurrent removes
	go func() {
		for i := 0; i < 50; i++ {
			q.Remove("/buckets/b1/folder" + string(rune('A'+i%26)))
		}
		done <- true
	}()

	// Concurrent pops
	go func() {
		for i := 0; i < 30; i++ {
			q.Pop()
		}
		done <- true
	}()

	// Concurrent reads
	go func() {
		for i := 0; i < 100; i++ {
			q.Len()
			q.Contains("/buckets/b1/folderA")
			q.ShouldProcess()
		}
		done <- true
	}()

	// Wait for all goroutines
	for i := 0; i < 4; i++ {
		<-done
	}

	// Just verify no panic occurred and queue is in consistent state
	_ = q.Len()
}



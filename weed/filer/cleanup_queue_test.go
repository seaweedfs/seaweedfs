package filer

import (
	"testing"
	"time"
)

func TestCleanupQueue_Add(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)

	// Add first item
	if !q.Add("/buckets/b1/folder1") {
		t.Error("expected Add to return true for new item")
	}
	if q.Len() != 1 {
		t.Errorf("expected len 1, got %d", q.Len())
	}

	// Add second item
	if !q.Add("/buckets/b1/folder2") {
		t.Error("expected Add to return true for new item")
	}
	if q.Len() != 2 {
		t.Errorf("expected len 2, got %d", q.Len())
	}

	// Add duplicate - should return false but move to back
	if q.Add("/buckets/b1/folder1") {
		t.Error("expected Add to return false for existing item")
	}
	if q.Len() != 2 {
		t.Errorf("expected len 2 after duplicate, got %d", q.Len())
	}

	// folder1 should now be at the back
	folders := q.GetAll()
	if folders[0] != "/buckets/b1/folder2" || folders[1] != "/buckets/b1/folder1" {
		t.Errorf("expected folder1 to be moved to back, got %v", folders)
	}
}

func TestCleanupQueue_Remove(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)

	q.Add("/buckets/b1/folder1")
	q.Add("/buckets/b1/folder2")
	q.Add("/buckets/b1/folder3")

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

	// Verify order is preserved
	folders := q.GetAll()
	if len(folders) != 2 {
		t.Errorf("expected 2 folders, got %d", len(folders))
	}
	if folders[0] != "/buckets/b1/folder1" || folders[1] != "/buckets/b1/folder3" {
		t.Errorf("unexpected order: %v", folders)
	}
}

func TestCleanupQueue_Pop(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)

	// Pop from empty queue
	folder, ok := q.Pop()
	if ok {
		t.Error("expected Pop to return false for empty queue")
	}
	if folder != "" {
		t.Errorf("expected empty folder, got %s", folder)
	}

	// Add items and pop in order
	q.Add("/buckets/b1/folder1")
	q.Add("/buckets/b1/folder2")
	q.Add("/buckets/b1/folder3")

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

	// Peek empty queue
	folder, _, ok := q.Peek()
	if ok {
		t.Error("expected Peek to return false for empty queue")
	}

	// Add item and peek
	q.Add("/buckets/b1/folder1")
	folder, queueTime, ok := q.Peek()
	if !ok || folder != "/buckets/b1/folder1" {
		t.Errorf("expected folder1, got %s (ok=%v)", folder, ok)
	}
	if queueTime.IsZero() {
		t.Error("expected non-zero queue time")
	}

	// Peek should not remove item
	if q.Len() != 1 {
		t.Errorf("Peek should not remove item, len=%d", q.Len())
	}
}

func TestCleanupQueue_Contains(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)

	q.Add("/buckets/b1/folder1")

	if !q.Contains("/buckets/b1/folder1") {
		t.Error("expected Contains to return true")
	}
	if q.Contains("/buckets/b1/folder2") {
		t.Error("expected Contains to return false for non-existent")
	}
}

func TestCleanupQueue_ShouldProcess_MaxSize(t *testing.T) {
	q := NewCleanupQueue(3, 10*time.Minute)

	// Empty queue
	if q.ShouldProcess() {
		t.Error("empty queue should not need processing")
	}

	// Add items below max
	q.Add("/buckets/b1/folder1")
	q.Add("/buckets/b1/folder2")
	if q.ShouldProcess() {
		t.Error("queue below max should not need processing")
	}

	// Add item to reach max
	q.Add("/buckets/b1/folder3")
	if !q.ShouldProcess() {
		t.Error("queue at max should need processing")
	}
}

func TestCleanupQueue_ShouldProcess_MaxAge(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)

	// Use mock time
	currentTime := time.Now()
	q.SetNowFunc(func() time.Time { return currentTime })

	q.Add("/buckets/b1/folder1")

	// Just added, should not need processing
	if q.ShouldProcess() {
		t.Error("fresh item should not trigger processing")
	}

	// Advance time past max age
	currentTime = currentTime.Add(11 * time.Minute)
	if !q.ShouldProcess() {
		t.Error("old item should trigger processing")
	}
}

func TestCleanupQueue_Clear(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)

	q.Add("/buckets/b1/folder1")
	q.Add("/buckets/b1/folder2")
	q.Add("/buckets/b1/folder3")

	q.Clear()

	if q.Len() != 0 {
		t.Errorf("expected empty queue after Clear, got len %d", q.Len())
	}
	if q.Contains("/buckets/b1/folder1") {
		t.Error("queue should not contain items after Clear")
	}
}

func TestCleanupQueue_GetAll(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)

	q.Add("/buckets/b1/folder1")
	q.Add("/buckets/b1/folder2")
	q.Add("/buckets/b1/folder3")

	folders := q.GetAll()
	expected := []string{"/buckets/b1/folder1", "/buckets/b1/folder2", "/buckets/b1/folder3"}

	if len(folders) != len(expected) {
		t.Errorf("expected %d folders, got %d", len(expected), len(folders))
	}
	for i, f := range folders {
		if f != expected[i] {
			t.Errorf("at index %d: expected %s, got %s", i, expected[i], f)
		}
	}

	// GetAll should not modify queue
	if q.Len() != 3 {
		t.Errorf("GetAll should not modify queue, len=%d", q.Len())
	}
}

func TestCleanupQueue_OldestAge(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)

	// Empty queue
	if q.OldestAge() != 0 {
		t.Error("empty queue should have zero oldest age")
	}

	// Use mock time
	baseTime := time.Now()
	currentTime := baseTime
	q.SetNowFunc(func() time.Time { return currentTime })

	q.Add("/buckets/b1/folder1")

	// Just added
	age := q.OldestAge()
	if age != 0 {
		t.Errorf("expected 0 age for fresh item, got %v", age)
	}

	// Advance time
	currentTime = baseTime.Add(5 * time.Minute)
	age = q.OldestAge()
	if age != 5*time.Minute {
		t.Errorf("expected 5m age, got %v", age)
	}
}

func TestCleanupQueue_FIFOOrder(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)

	// Add items
	items := []string{
		"/buckets/b1/a",
		"/buckets/b1/b",
		"/buckets/b1/c",
		"/buckets/b1/d",
		"/buckets/b1/e",
	}
	for _, item := range items {
		q.Add(item)
	}

	// Pop should return in FIFO order
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

func TestCleanupQueue_DuplicateMovesToBack(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)

	// Add items
	q.Add("/buckets/b1/folder1")
	q.Add("/buckets/b1/folder2")
	q.Add("/buckets/b1/folder3")

	// Add duplicate - should move folder1 to back with updated time
	q.Add("/buckets/b1/folder1")

	folders := q.GetAll()
	if len(folders) != 3 {
		t.Errorf("expected 3 folders, got %d", len(folders))
	}
	// folder1 should now be at the back
	expected := []string{"/buckets/b1/folder2", "/buckets/b1/folder3", "/buckets/b1/folder1"}
	for i, folder := range folders {
		if folder != expected[i] {
			t.Errorf("at index %d: expected %s, got %s", i, expected[i], folder)
		}
	}
}

func TestCleanupQueue_DuplicateUpdatesTime(t *testing.T) {
	q := NewCleanupQueue(100, 10*time.Minute)

	// Use mock time
	baseTime := time.Now()
	currentTime := baseTime
	q.SetNowFunc(func() time.Time { return currentTime })

	// Add folder at t=0
	q.Add("/buckets/b1/folder1")

	// Check initial time
	_, queueTime1, _ := q.Peek()
	if queueTime1 != baseTime {
		t.Errorf("expected queue time %v, got %v", baseTime, queueTime1)
	}

	// Advance time and re-add the same folder
	currentTime = baseTime.Add(5 * time.Minute)
	q.Add("/buckets/b1/folder1")

	// Time should be updated
	_, queueTime2, _ := q.Peek()
	if queueTime2 != currentTime {
		t.Errorf("expected updated queue time %v, got %v", currentTime, queueTime2)
	}
}

func TestCleanupQueue_Concurrent(t *testing.T) {
	q := NewCleanupQueue(1000, 10*time.Minute)
	done := make(chan bool)

	// Concurrent adds
	go func() {
		for i := 0; i < 100; i++ {
			q.Add("/buckets/b1/folder" + string(rune('A'+i%26)))
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
	_ = q.GetAll()
}


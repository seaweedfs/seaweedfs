package filer

import (
	"container/list"
	"sync"
	"time"
)

// CleanupQueue manages a deduplicated queue of folders pending cleanup.
// It uses a doubly-linked list for ordered insertion (FIFO) and a map for O(1) deduplication.
// Processing is triggered when:
// - Queue size reaches maxSize, OR
// - Oldest item exceeds maxAge
type CleanupQueue struct {
	mu       sync.Mutex
	items    *list.List                // Linked list of *queueItem (front = oldest)
	itemsMap map[string]*list.Element  // folder -> list element for O(1) lookup
	maxSize  int                       // Max queue size before triggering cleanup
	maxAge   time.Duration             // Max age before triggering cleanup
	nowFunc  func() time.Time          // For testing - defaults to time.Now
}

// queueItem represents an item in the cleanup queue
type queueItem struct {
	folder    string
	queueTime time.Time
}

// NewCleanupQueue creates a new CleanupQueue with the specified limits
func NewCleanupQueue(maxSize int, maxAge time.Duration) *CleanupQueue {
	return &CleanupQueue{
		items:    list.New(),
		itemsMap: make(map[string]*list.Element),
		maxSize:  maxSize,
		maxAge:   maxAge,
		nowFunc:  time.Now,
	}
}

// Add adds a folder to the queue. If already exists, it's a no-op (keeps original position/time).
// Returns true if the folder was added (not a duplicate).
func (q *CleanupQueue) Add(folder string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check for duplicate
	if _, exists := q.itemsMap[folder]; exists {
		return false
	}

	// Add to back of list
	item := &queueItem{
		folder:    folder,
		queueTime: q.nowFunc(),
	}
	elem := q.items.PushBack(item)
	q.itemsMap[folder] = elem
	return true
}

// Remove removes a specific folder from the queue (e.g., when a file is created).
// Returns true if the folder was found and removed.
func (q *CleanupQueue) Remove(folder string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	elem, exists := q.itemsMap[folder]
	if !exists {
		return false
	}

	q.items.Remove(elem)
	delete(q.itemsMap, folder)
	return true
}

// ShouldProcess returns true if the queue should be processed.
// This is true when:
// - Queue size >= maxSize, OR
// - Oldest item age > maxAge
func (q *CleanupQueue) ShouldProcess() bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.shouldProcessLocked()
}

// shouldProcessLocked checks if processing is needed (caller must hold lock)
func (q *CleanupQueue) shouldProcessLocked() bool {
	if q.items.Len() == 0 {
		return false
	}

	// Check if queue is full
	if q.items.Len() >= q.maxSize {
		return true
	}

	// Check if oldest item exceeds max age
	front := q.items.Front()
	if front != nil {
		item := front.Value.(*queueItem)
		if q.nowFunc().Sub(item.queueTime) > q.maxAge {
			return true
		}
	}

	return false
}

// Pop removes and returns the oldest folder from the queue.
// Returns the folder and true if an item was available, or empty string and false if queue is empty.
func (q *CleanupQueue) Pop() (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	front := q.items.Front()
	if front == nil {
		return "", false
	}

	item := front.Value.(*queueItem)
	q.items.Remove(front)
	delete(q.itemsMap, item.folder)

	return item.folder, true
}

// Peek returns the oldest folder without removing it.
// Returns the folder and queue time if available, or empty values if queue is empty.
func (q *CleanupQueue) Peek() (folder string, queueTime time.Time, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	front := q.items.Front()
	if front == nil {
		return "", time.Time{}, false
	}

	item := front.Value.(*queueItem)
	return item.folder, item.queueTime, true
}

// Len returns the current queue size.
func (q *CleanupQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.items.Len()
}

// Contains checks if a folder is in the queue.
func (q *CleanupQueue) Contains(folder string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, exists := q.itemsMap[folder]
	return exists
}

// Clear removes all items from the queue.
func (q *CleanupQueue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.items.Init()
	q.itemsMap = make(map[string]*list.Element)
}

// GetAll returns all folders in queue order (oldest first) without modifying the queue.
// Useful for testing and debugging.
func (q *CleanupQueue) GetAll() []string {
	q.mu.Lock()
	defer q.mu.Unlock()

	result := make([]string, 0, q.items.Len())
	for elem := q.items.Front(); elem != nil; elem = elem.Next() {
		item := elem.Value.(*queueItem)
		result = append(result, item.folder)
	}
	return result
}

// OldestAge returns the age of the oldest item in the queue, or 0 if empty.
func (q *CleanupQueue) OldestAge() time.Duration {
	q.mu.Lock()
	defer q.mu.Unlock()

	front := q.items.Front()
	if front == nil {
		return 0
	}

	item := front.Value.(*queueItem)
	return q.nowFunc().Sub(item.queueTime)
}

// SetNowFunc sets a custom time function (for testing)
func (q *CleanupQueue) SetNowFunc(fn func() time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.nowFunc = fn
}


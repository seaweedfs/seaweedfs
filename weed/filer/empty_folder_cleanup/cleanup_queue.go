package empty_folder_cleanup

import (
	"container/list"
	"sync"
	"time"
)

// CleanupQueue manages a deduplicated queue of folders pending cleanup.
// It uses a doubly-linked list ordered by event time (oldest at front) and a map for O(1) deduplication.
// Processing is triggered when:
// - Queue size reaches maxSize, OR
// - Oldest item exceeds maxAge
type CleanupQueue struct {
	mu       sync.Mutex
	items    *list.List               // Linked list of *queueItem ordered by time (front = oldest)
	itemsMap map[string]*list.Element // folder -> list element for O(1) lookup
	maxSize  int                      // Max queue size before triggering cleanup
	maxAge   time.Duration            // Max age before triggering cleanup
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
	}
}

// Add adds a folder to the queue with the specified event time.
// The item is inserted in time-sorted order (oldest at front) to handle out-of-order events.
// If folder already exists with an older time, the time is updated and position adjusted.
// Returns true if the folder was newly added, false if it was updated.
func (q *CleanupQueue) Add(folder string, eventTime time.Time) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check if folder already exists
	if elem, exists := q.itemsMap[folder]; exists {
		existingItem := elem.Value.(*queueItem)
		// Only update if new event is later
		if eventTime.After(existingItem.queueTime) {
			// Remove from current position
			q.items.Remove(elem)
			// Re-insert with new time in sorted position
			newElem := q.insertSorted(folder, eventTime)
			q.itemsMap[folder] = newElem
		}
		return false
	}

	// Insert new folder in sorted position
	elem := q.insertSorted(folder, eventTime)
	q.itemsMap[folder] = elem
	return true
}

// insertSorted inserts an item in the correct position to maintain time ordering (oldest at front)
func (q *CleanupQueue) insertSorted(folder string, eventTime time.Time) *list.Element {
	item := &queueItem{
		folder:    folder,
		queueTime: eventTime,
	}

	// Find the correct position (insert before the first item with a later time)
	for elem := q.items.Back(); elem != nil; elem = elem.Prev() {
		existingItem := elem.Value.(*queueItem)
		if !eventTime.Before(existingItem.queueTime) {
			// Insert after this element
			return q.items.InsertAfter(item, elem)
		}
	}

	// This item is the oldest, insert at front
	return q.items.PushFront(item)
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
		if time.Since(item.queueTime) > q.maxAge {
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

// OldestAge returns the age of the oldest item in the queue, or 0 if empty.
func (q *CleanupQueue) OldestAge() time.Duration {
	q.mu.Lock()
	defer q.mu.Unlock()

	front := q.items.Front()
	if front == nil {
		return 0
	}

	item := front.Value.(*queueItem)
	return time.Since(item.queueTime)
}


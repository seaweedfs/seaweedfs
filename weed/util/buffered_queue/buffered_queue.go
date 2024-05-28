package buffered_queue

import (
	"fmt"
	"sync"
)

// ItemChunkNode represents a node in the linked list of job chunks
type ItemChunkNode[T any] struct {
	items     []T
	headIndex int
	tailIndex int
	next      *ItemChunkNode[T]
	nodeId    int
}

// BufferedQueue implements a buffered queue using a linked list of job chunks
type BufferedQueue[T any] struct {
	chunkSize   int // Maximum number of items per chunk
	head        *ItemChunkNode[T]
	tail        *ItemChunkNode[T]
	last        *ItemChunkNode[T] // Pointer to the last chunk, for reclaiming memory
	count       int               // Total number of items in the queue
	mutex       sync.Mutex
	nodeCounter int
	waitCond    *sync.Cond
	isClosed    bool
}

// NewBufferedQueue creates a new buffered queue with the specified chunk size
func NewBufferedQueue[T any](chunkSize int) *BufferedQueue[T] {
	// Create an empty chunk to initialize head and tail
	chunk := &ItemChunkNode[T]{items: make([]T, chunkSize), nodeId: 0}
	bq := &BufferedQueue[T]{
		chunkSize: chunkSize,
		head:      chunk,
		tail:      chunk,
		last:      chunk,
		count:     0,
		mutex:     sync.Mutex{},
	}
	bq.waitCond = sync.NewCond(&bq.mutex)
	return bq
}

// Enqueue adds a job to the queue
func (q *BufferedQueue[T]) Enqueue(job T) error {

	if q.isClosed {
		return fmt.Errorf("queue is closed")
	}

	q.mutex.Lock()
	defer q.mutex.Unlock()

	// If the tail chunk is full, create a new chunk (reusing empty chunks if available)
	if q.tail.tailIndex == q.chunkSize {
		if q.tail == q.last {
			// Create a new chunk
			q.nodeCounter++
			newChunk := &ItemChunkNode[T]{items: make([]T, q.chunkSize), nodeId: q.nodeCounter}
			q.tail.next = newChunk
			q.tail = newChunk
			q.last = newChunk
		} else {
			// Reuse an empty chunk
			q.tail = q.tail.next
			q.tail.headIndex = 0
			q.tail.tailIndex = 0
			// println("tail moved to chunk", q.tail.nodeId)
		}
	}

	// Add the job to the tail chunk
	q.tail.items[q.tail.tailIndex] = job
	q.tail.tailIndex++
	q.count++
	if q.count == 1 {
		q.waitCond.Signal()
	}

	return nil
}

// Dequeue removes and returns a job from the queue
func (q *BufferedQueue[T]) Dequeue() (T, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	for q.count <= 0 && !q.isClosed {
		q.waitCond.Wait()
	}
	if q.count <= 0 && q.isClosed {
		var a T
		return a, false
	}

	q.maybeAdjustHeadIndex()

	job := q.head.items[q.head.headIndex]
	q.head.headIndex++
	q.count--

	return job, true
}

func (q *BufferedQueue[T]) maybeAdjustHeadIndex() {
	if q.head.headIndex == q.chunkSize {
		q.last.next = q.head
		q.head = q.head.next
		q.last = q.last.next
		q.last.next = nil
		//println("reusing chunk", q.last.nodeId)
		//fmt.Printf("head: %+v\n", q.head)
		//fmt.Printf("tail: %+v\n", q.tail)
		//fmt.Printf("last: %+v\n", q.last)
		//fmt.Printf("count: %d\n", q.count)
		//for p := q.head; p != nil ; p = p.next {
		//	fmt.Printf("Node: %+v\n", p)
		//}
	}
}

func (q *BufferedQueue[T]) PeekHead() (T, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.count <= 0 {
		var a T
		return a, false
	}

	q.maybeAdjustHeadIndex()

	job := q.head.items[q.head.headIndex]
	return job, true
}

// Size returns the number of items in the queue
func (q *BufferedQueue[T]) Size() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.count
}

// IsEmpty returns true if the queue is empty
func (q *BufferedQueue[T]) IsEmpty() bool {
	return q.Size() == 0
}

func (q *BufferedQueue[T]) CloseInput() {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.isClosed = true
	q.waitCond.Broadcast()
}

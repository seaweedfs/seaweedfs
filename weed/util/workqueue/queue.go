package workqueue

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/util/queue"
)

type Interface interface {
	Add(item queue.Item)
	Len() int
	Get() (item queue.Item, shutdown bool)
	Done(item queue.Item)
	ShutDown()
	ShuttingDown() bool
	Processing(item queue.Item) bool
}

type Queue struct {
	*queue.PriorityQueue
}

// Put adds an item to the queue.
func (q *Queue) Put(item queue.Item) error {
	return q.PriorityQueue.Put(item)
}

func (q *Queue) ShutDown() {
	q.PriorityQueue.Dispose()
}

// Get retrieves an item from the queue. If the queue is empty,
// this call blocks until the next item is added to the queue.
func (q *Queue) Get() (item queue.Item, closed bool) {
	res, err := q.PriorityQueue.Get(1)
	if err != nil {
		return nil, true
	}

	return res[0], false
}

// New constructs a new work queue.
func New() *Type {
	return newQueue()
}

func newQueue() *Type {
	t := &Type{
		processing: make(map[string]queue.Empty),
		cond:       sync.NewCond(&sync.Mutex{}),
		queue:      &Queue{PriorityQueue: queue.NewPriorityQueue(100, false)},
	}

	return t
}

// Type is a work queue (see the package comment).
type Type struct {
	// queue defines the order in which we will work on items. Every
	// element of queue should not in the processing set.
	queue *Queue

	// Things that are currently being processed are in the processing set.
	// These things may be simultaneously in the dirty set. When we finish
	// processing something and remove it from this set, we'll check if
	// it's in the dirty set, and if so, add it to the queue.
	processing map[string]queue.Empty

	cond *sync.Cond

	shuttingDown bool
}

// Add marks item as needing processing.
func (q *Type) Add(item queue.Item) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.shuttingDown {
		return
	}

	if _, ok := q.processing[item.GetUniqueID()]; ok {
		return
	}

	_ = q.queue.Put(item)

	q.cond.Signal()
}

// Len returns the current queue length, for informational purposes only. You
// shouldn't e.g. gate a call to Add() or Get() on Len() being a particular
// value, that can't be synchronized properly.
func (q *Type) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.queue.Len()
}

// Get blocks until it can return an item to be processed. If shutdown = true,
// the caller should end their goroutine. You must call Done with item when you
// have finished processing it.
func (q *Type) Get() (item queue.Item, shutdown bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for q.queue.Len() == 0 && !q.shuttingDown {
		q.cond.Wait()
	}
	if q.queue.Len() == 0 {
		// We must be shutting down.
		return nil, true
	}

	item, closed := q.queue.Get()
	if closed {
		return nil, true
	}

	q.processing[item.GetUniqueID()] = queue.Empty{}

	return item, false
}

// Done marks item as done processing.
func (q *Type) Done(item queue.Item) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	delete(q.processing, item.GetUniqueID())
	if len(q.processing) == 0 {
		q.cond.Signal()
	}
}

// ShutDown will cause q to ignore all new items added to it and
// immediately instruct the worker goroutines to exit.
func (q *Type) ShutDown() {
	q.shutdown()
}

func (q *Type) shutdown() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.shuttingDown = true
	q.cond.Broadcast()
}

func (q *Type) ShuttingDown() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	return q.shuttingDown
}

func (q *Type) Processing(item queue.Item) bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	_, ok := q.processing[item.GetUniqueID()]

	return ok
}

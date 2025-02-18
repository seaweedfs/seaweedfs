package util

import (
	"sync"
)

type node[T any] struct {
	data T
	next *node[T]
}

type Queue[T any] struct {
	head  *node[T]
	tail  *node[T]
	count int
	sync.RWMutex
}

func NewQueue[T any]() *Queue[T] {
	q := &Queue[T]{}
	return q
}

func (q *Queue[T]) Len() int {
	q.RLock()
	defer q.RUnlock()
	return q.count
}

func (q *Queue[T]) Enqueue(item T) {
	q.Lock()
	defer q.Unlock()

	n := &node[T]{data: item}

	if q.tail == nil {
		q.tail = n
		q.head = n
	} else {
		q.tail.next = n
		q.tail = n
	}
	q.count++
}

func (q *Queue[T]) Dequeue() (result T) {
	q.Lock()
	defer q.Unlock()

	if q.head == nil {
		return
	}

	n := q.head
	q.head = n.next

	if q.head == nil {
		q.tail = nil
	}
	q.count--

	return n.data
}

func (q *Queue[T]) Peek() (result T) {
	q.RLock()
	defer q.RUnlock()

	if q.head == nil {
		return
	}

	return q.head.data
}

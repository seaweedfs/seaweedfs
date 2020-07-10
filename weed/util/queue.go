package util

import "sync"

type node struct {
	data interface{}
	next *node
}

type Queue struct {
	head  *node
	tail  *node
	count int
	sync.RWMutex
}

func NewQueue() *Queue {
	q := &Queue{}
	return q
}

func (q *Queue) Len() int {
	q.RLock()
	defer q.RUnlock()
	return q.count
}

func (q *Queue) Enqueue(item interface{}) {
	q.Lock()
	defer q.Unlock()

	n := &node{data: item}

	if q.tail == nil {
		q.tail = n
		q.head = n
	} else {
		q.tail.next = n
		q.tail = n
	}
	q.count++
}

func (q *Queue) Dequeue() interface{} {
	q.Lock()
	defer q.Unlock()

	if q.head == nil {
		return nil
	}

	n := q.head
	q.head = n.next

	if q.head == nil {
		q.tail = nil
	}
	q.count--

	return n.data
}

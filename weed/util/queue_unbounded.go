package util

import "sync"

type UnboundedQueue struct {
	outbound     []string
	outboundLock sync.RWMutex
	inbound      []string
	inboundLock  sync.RWMutex
}

func NewUnboundedQueue() *UnboundedQueue {
	q := &UnboundedQueue{}
	return q
}

func (q *UnboundedQueue) EnQueue(items ...string) {
	q.inboundLock.Lock()
	defer q.inboundLock.Unlock()

	q.inbound = append(q.inbound, items...)

}

func (q *UnboundedQueue) Consume(fn func([]string)) {
	q.outboundLock.Lock()
	defer q.outboundLock.Unlock()

	if len(q.outbound) == 0 {
		q.inboundLock.Lock()
		inboundLen := len(q.inbound)
		if inboundLen > 0 {
			t := q.outbound
			q.outbound = q.inbound
			q.inbound = t
		}
		q.inboundLock.Unlock()
	}

	if len(q.outbound) > 0 {
		fn(q.outbound)
		q.outbound = q.outbound[:0]
	}

}

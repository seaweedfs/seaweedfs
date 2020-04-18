package broker

import (
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/util/log_buffer"
)

type TopicPartition struct {
	Namespace string
	Topic     string
	Partition int32
}
type TopicLock struct {
	sync.Mutex
	subscriberCount int
	publisherCount  int
	logBuffer       *log_buffer.LogBuffer
}

type TopicLocks struct {
	sync.Mutex
	locks map[TopicPartition]*TopicLock
}

func NewTopicLocks() *TopicLocks {
	return &TopicLocks{
		locks: make(map[TopicPartition]*TopicLock),
	}
}

func (tl *TopicLocks) RequestSubscriberLock(partition TopicPartition) *TopicLock {
	tl.Lock()
	defer tl.Unlock()

	lock, found := tl.locks[partition]
	if !found {
		lock = &TopicLock{}
		tl.locks[partition] = lock
	}
	lock.subscriberCount++

	return lock
}

func (tl *TopicLocks) RequestPublisherLock(partition TopicPartition, flushFn func(startTime, stopTime time.Time, buf []byte)) *log_buffer.LogBuffer {
	tl.Lock()
	defer tl.Unlock()

	lock, found := tl.locks[partition]
	if !found {
		lock = &TopicLock{}
		tl.locks[partition] = lock
	}
	lock.publisherCount++
	cond := sync.NewCond(&lock.Mutex)
	lock.logBuffer = log_buffer.NewLogBuffer(time.Minute, flushFn, func() {
		cond.Broadcast()
	})
	return lock.logBuffer
}

func (tl *TopicLocks) ReleaseLock(partition TopicPartition, isPublisher bool) {
	tl.Lock()
	defer tl.Unlock()

	lock, found := tl.locks[partition]
	if !found {
		return
	}
	if isPublisher {
		lock.publisherCount--
	} else {
		lock.subscriberCount--
	}
	if lock.subscriberCount <= 0 && lock.publisherCount <= 0 {
		delete(tl.locks, partition)
	}
}

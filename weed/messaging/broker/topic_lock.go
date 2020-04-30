package broker

import (
	"fmt"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
	"github.com/chrislusf/seaweedfs/weed/util/log_buffer"
)

type TopicPartition struct {
	Namespace string
	Topic     string
	Partition int32
}
type TopicLock struct {
	sync.Mutex
	cond            *sync.Cond
	subscriberCount int
	publisherCount  int
	logBuffer       *log_buffer.LogBuffer
}

type TopicLocks struct {
	sync.Mutex
	locks  map[TopicPartition]*TopicLock
	broker *MessageBroker
}

func NewTopicLocks(messageBroker *MessageBroker) *TopicLocks {
	return &TopicLocks{
		locks:  make(map[TopicPartition]*TopicLock),
		broker: messageBroker,
	}
}

func (locks *TopicLocks) buildLogBuffer(tl *TopicLock, tp TopicPartition, topicConfig *messaging_pb.TopicConfiguration) *log_buffer.LogBuffer {

	flushFn := func(startTime, stopTime time.Time, buf []byte) {

		if topicConfig.IsTransient {
			// return
		}

		// fmt.Printf("flushing with topic config %+v\n", topicConfig)

		targetFile := fmt.Sprintf(
			"%s/%s/%s/%04d-%02d-%02d/%02d-%02d.part%02d",
			filer2.TopicsDir, tp.Namespace, tp.Topic,
			startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour(), startTime.Minute(),
			tp.Partition,
		)

		if err := locks.broker.appendToFile(targetFile, topicConfig, buf); err != nil {
			glog.V(0).Infof("log write failed %s: %v", targetFile, err)
		}
	}
	logBuffer := log_buffer.NewLogBuffer(time.Minute, flushFn, func() {
		tl.cond.Broadcast()
	})

	return logBuffer
}

func (tl *TopicLocks) RequestLock(partition TopicPartition, topicConfig *messaging_pb.TopicConfiguration, isPublisher bool) *TopicLock {
	tl.Lock()
	defer tl.Unlock()

	lock, found := tl.locks[partition]
	if !found {
		lock = &TopicLock{}
		lock.cond = sync.NewCond(&lock.Mutex)
		tl.locks[partition] = lock
		lock.logBuffer = tl.buildLogBuffer(lock, partition, topicConfig)
	}
	if isPublisher {
		lock.publisherCount++
	} else {
		lock.subscriberCount++
	}
	return lock
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

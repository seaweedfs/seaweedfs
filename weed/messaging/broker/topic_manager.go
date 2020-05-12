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

const (
	TopicPartitionFmt = "%s/%s_%02d"
)

func (tp *TopicPartition) String() string {
	return fmt.Sprintf(TopicPartitionFmt, tp.Namespace, tp.Topic, tp.Partition)
}

type TopicControl struct {
	sync.Mutex
	cond            *sync.Cond
	subscriberCount int
	publisherCount  int
	logBuffer       *log_buffer.LogBuffer
}

type TopicManager struct {
	sync.Mutex
	topicControls map[TopicPartition]*TopicControl
	broker        *MessageBroker
}

func NewTopicManager(messageBroker *MessageBroker) *TopicManager {
	return &TopicManager{
		topicControls: make(map[TopicPartition]*TopicControl),
		broker:        messageBroker,
	}
}

func (tm *TopicManager) buildLogBuffer(tl *TopicControl, tp TopicPartition, topicConfig *messaging_pb.TopicConfiguration) *log_buffer.LogBuffer {

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

		if err := tm.broker.appendToFile(targetFile, topicConfig, buf); err != nil {
			glog.V(0).Infof("log write failed %s: %v", targetFile, err)
		}
	}
	logBuffer := log_buffer.NewLogBuffer(time.Minute, flushFn, func() {
		tl.cond.Broadcast()
	})

	return logBuffer
}

func (tm *TopicManager) RequestLock(partition TopicPartition, topicConfig *messaging_pb.TopicConfiguration, isPublisher bool) *TopicControl {
	tm.Lock()
	defer tm.Unlock()

	lock, found := tm.topicControls[partition]
	if !found {
		lock = &TopicControl{}
		lock.cond = sync.NewCond(&lock.Mutex)
		tm.topicControls[partition] = lock
		lock.logBuffer = tm.buildLogBuffer(lock, partition, topicConfig)
	}
	if isPublisher {
		lock.publisherCount++
	} else {
		lock.subscriberCount++
	}
	return lock
}

func (tm *TopicManager) ReleaseLock(partition TopicPartition, isPublisher bool) {
	tm.Lock()
	defer tm.Unlock()

	lock, found := tm.topicControls[partition]
	if !found {
		return
	}
	if isPublisher {
		lock.publisherCount--
	} else {
		lock.subscriberCount--
	}
	if lock.subscriberCount <= 0 && lock.publisherCount <= 0 {
		delete(tm.topicControls, partition)
		lock.logBuffer.Shutdown()
	}
}

func (tm *TopicManager) ListTopicPartitions() (tps []TopicPartition) {
	tm.Lock()
	defer tm.Unlock()

	for k := range tm.topicControls {
		tps = append(tps, k)
	}
	return
}

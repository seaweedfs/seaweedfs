package broker

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"sync/atomic"
	"time"
)

func (b *MessageQueueBroker) genLogFlushFunc(t topic.Topic, p topic.Partition) log_buffer.LogFlushFuncType {
	partitionDir := topic.PartitionDir(t, p)

	return func(logBuffer *log_buffer.LogBuffer, startTime, stopTime time.Time, buf []byte) {
		if len(buf) == 0 {
			return
		}

		startTime, stopTime = startTime.UTC(), stopTime.UTC()

		targetFile := fmt.Sprintf("%s/%s", partitionDir, startTime.Format(topic.TIME_FORMAT))

		// TODO append block with more metadata

		for {
			if err := b.appendToFile(targetFile, buf); err != nil {
				glog.V(0).Infof("metadata log write failed %s: %v", targetFile, err)
				time.Sleep(737 * time.Millisecond)
			} else {
				break
			}
		}

		atomic.StoreInt64(&logBuffer.LastFlushTsNs, stopTime.UnixNano())

		b.accessLock.Lock()
		defer b.accessLock.Unlock()
		if localPartition := b.localTopicManager.GetLocalPartition(t, p); localPartition != nil {
			localPartition.NotifyLogFlushed(logBuffer.LastFlushTsNs)
		}

		glog.V(0).Infof("flushing at %d to %s size %d", logBuffer.LastFlushTsNs, targetFile, len(buf))
	}
}

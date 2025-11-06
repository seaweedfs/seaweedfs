package broker

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

// LogBufferStart tracks the starting buffer offset for a live log file
// Buffer offsets are monotonically increasing, count = number of chunks
// Now stored in binary format for efficiency
type LogBufferStart struct {
	StartIndex int64 // Starting buffer offset (count = len(chunks))
}

func (b *MessageQueueBroker) genLogFlushFunc(t topic.Topic, p topic.Partition) log_buffer.LogFlushFuncType {
	partitionDir := topic.PartitionDir(t, p)

	return func(logBuffer *log_buffer.LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
		if len(buf) == 0 {
			return
		}

		startTime, stopTime = startTime.UTC(), stopTime.UTC()

		targetFile := fmt.Sprintf("%s/%s", partitionDir, startTime.Format(topic.TIME_FORMAT))

		// Get buffer offset (sequential: 0, 1, 2, 3...)
		bufferOffset := logBuffer.GetOffset()

		for {
			if err := b.appendToFileWithBufferIndex(targetFile, buf, bufferOffset, minOffset, maxOffset); err != nil {
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

		glog.V(0).Infof("flushing at %d to %s size %d from buffer %s (offset %d)", logBuffer.LastFlushTsNs, targetFile, len(buf), logBuffer.GetName(), bufferOffset)
	}
}

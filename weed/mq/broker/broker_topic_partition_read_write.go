package broker

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"google.golang.org/protobuf/proto"
	"math"
	"sync/atomic"
	"time"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func (b *MessageQueueBroker) genLogFlushFunc(t topic.Topic, partition *mq_pb.Partition) log_buffer.LogFlushFuncType {
	topicDir := fmt.Sprintf("%s/%s/%s", filer.TopicsDir, t.Namespace, t.Name)
	partitionGeneration := time.Unix(0, partition.UnixTimeNs).UTC().Format(topic.TIME_FORMAT)
	partitionDir := fmt.Sprintf("%s/%s/%04d-%04d", topicDir, partitionGeneration, partition.RangeStart, partition.RangeStop)

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
		p := topic.FromPbPartition(partition)
		if localPartition := b.localTopicManager.GetLocalPartition(t, p); localPartition != nil {
			localPartition.NotifyLogFlushed(logBuffer.LastFlushTsNs)
		}

		println("flushing at", logBuffer.LastFlushTsNs, "to", targetFile, "size", len(buf))

	}
}

func (b *MessageQueueBroker) genLogOnDiskReadFunc(t topic.Topic, partition *mq_pb.Partition) log_buffer.LogReadFromDiskFuncType {
	topicDir := fmt.Sprintf("%s/%s/%s", filer.TopicsDir, t.Namespace, t.Name)
	partitionGeneration := time.Unix(0, partition.UnixTimeNs).UTC().Format(topic.TIME_FORMAT)
	partitionDir := fmt.Sprintf("%s/%s/%04d-%04d", topicDir, partitionGeneration, partition.RangeStart, partition.RangeStop)

	lookupFileIdFn := func(fileId string) (targetUrls []string, err error) {
		return b.MasterClient.LookupFileId(fileId)
	}

	eachChunkFn := func(buf []byte, eachLogEntryFn log_buffer.EachLogEntryFuncType, starTsNs, stopTsNs int64) (processedTsNs int64, err error) {
		for pos := 0; pos+4 < len(buf); {

			size := util.BytesToUint32(buf[pos : pos+4])
			if pos+4+int(size) > len(buf) {
				err = fmt.Errorf("LogOnDiskReadFunc: read [%d,%d) from [0,%d)", pos, pos+int(size)+4, len(buf))
				return
			}
			entryData := buf[pos+4 : pos+4+int(size)]

			logEntry := &filer_pb.LogEntry{}
			if err = proto.Unmarshal(entryData, logEntry); err != nil {
				pos += 4 + int(size)
				err = fmt.Errorf("unexpected unmarshal mq_pb.Message: %v", err)
				return
			}
			if logEntry.TsNs < starTsNs {
				pos += 4 + int(size)
				continue
			}
			if stopTsNs != 0 && logEntry.TsNs > stopTsNs {
				println("stopTsNs", stopTsNs, "logEntry.TsNs", logEntry.TsNs)
				return
			}

			if _, err = eachLogEntryFn(logEntry); err != nil {
				err = fmt.Errorf("process log entry %v: %v", logEntry, err)
				return
			}

			processedTsNs = logEntry.TsNs

			pos += 4 + int(size)

		}

		return
	}

	eachFileFn := func(entry *filer_pb.Entry, eachLogEntryFn log_buffer.EachLogEntryFuncType, starTsNs, stopTsNs int64) (processedTsNs int64, err error) {
		if len(entry.Content) > 0 {
			glog.Warningf("this should not happen. unexpected content in %s/%s", partitionDir, entry.Name)
			return
		}
		var urlStrings []string
		for _, chunk := range entry.Chunks {
			if chunk.Size == 0 {
				continue
			}
			if chunk.IsChunkManifest {
				glog.Warningf("this should not happen. unexpected chunk manifest in %s/%s", partitionDir, entry.Name)
				return
			}
			urlStrings, err = lookupFileIdFn(chunk.FileId)
			if err != nil {
				err = fmt.Errorf("lookup %s: %v", chunk.FileId, err)
				return
			}
			if len(urlStrings) == 0 {
				err = fmt.Errorf("no url found for %s", chunk.FileId)
				return
			}

			// try one of the urlString until util.Get(urlString) succeeds
			var processed bool
			for _, urlString := range urlStrings {
				// TODO optimization opportunity: reuse the buffer
				var data []byte
				if data, _, err = util_http.Get(urlString); err == nil {
					processed = true
					if processedTsNs, err = eachChunkFn(data, eachLogEntryFn, starTsNs, stopTsNs); err != nil {
						return
					}
					break
				}
			}
			if !processed {
				err = fmt.Errorf("no data processed for %s %s", entry.Name, chunk.FileId)
				return
			}

		}
		return
	}

	return func(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (lastReadPosition log_buffer.MessagePosition, isDone bool, err error) {
		startFileName := startPosition.UTC().Format(topic.TIME_FORMAT)
		startTsNs := startPosition.Time.UnixNano()
		stopTime := time.Unix(0, stopTsNs)
		var processedTsNs int64
		err = b.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return filer_pb.SeaweedList(client, partitionDir, "", func(entry *filer_pb.Entry, isLast bool) error {
				if entry.IsDirectory {
					return nil
				}
				if stopTsNs != 0 && entry.Name > stopTime.UTC().Format(topic.TIME_FORMAT) {
					isDone = true
					return nil
				}
				if entry.Name < startPosition.UTC().Format(topic.TIME_FORMAT) {
					return nil
				}
				if processedTsNs, err = eachFileFn(entry, eachLogEntryFn, startTsNs, stopTsNs); err != nil {
					return err
				}
				return nil

			}, startFileName, true, math.MaxInt32)
		})
		lastReadPosition = log_buffer.NewMessagePosition(processedTsNs, -2)
		return
	}
}

package logstore

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"google.golang.org/protobuf/proto"
	"math"
	"strings"
	"time"
)

func GenLogOnDiskReadFunc(filerClient filer_pb.FilerClient, t topic.Topic, p topic.Partition) log_buffer.LogReadFromDiskFuncType {
	partitionDir := topic.PartitionDir(t, p)

	lookupFileIdFn := filer.LookupFn(filerClient)

	eachChunkFn := func(buf []byte, eachLogEntryFn log_buffer.EachLogEntryFuncType, starTsNs, stopTsNs int64) (processedTsNs int64, err error) {
		for pos := 0; pos+4 < len(buf); {

			size := util.BytesToUint32(buf[pos : pos+4])
			if pos+4+int(size) > len(buf) {
				err = fmt.Errorf("GenLogOnDiskReadFunc: read [%d,%d) from [0,%d)", pos, pos+int(size)+4, len(buf))
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

			// fmt.Printf(" read logEntry: %v, ts %v\n", string(logEntry.Key), time.Unix(0, logEntry.TsNs).UTC())
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
			// skip .offset files
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
				// fmt.Printf("reading %s/%s %s\n", partitionDir, entry.Name, urlString)
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
		err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return filer_pb.SeaweedList(client, partitionDir, "", func(entry *filer_pb.Entry, isLast bool) error {
				if entry.IsDirectory {
					return nil
				}
				if strings.HasSuffix(entry.Name, ".parquet") {
					return nil
				}
				// FIXME: this is a hack to skip the .offset files
				if strings.HasSuffix(entry.Name, ".offset") {
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

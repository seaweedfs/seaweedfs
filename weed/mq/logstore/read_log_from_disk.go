package logstore

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"google.golang.org/protobuf/proto"
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
				err = fmt.Errorf("unexpected unmarshal mq_pb.Message: %w", err)
				return
			}
			if logEntry.TsNs <= starTsNs {
				pos += 4 + int(size)
				continue
			}
			if stopTsNs != 0 && logEntry.TsNs > stopTsNs {
				println("stopTsNs", stopTsNs, "logEntry.TsNs", logEntry.TsNs)
				return
			}

			// fmt.Printf(" read logEntry: %v, ts %v\n", string(logEntry.Key), time.Unix(0, logEntry.TsNs).UTC())
			if _, err = eachLogEntryFn(logEntry); err != nil {
				err = fmt.Errorf("process log entry %v: %w", logEntry, err)
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
			urlStrings, err = lookupFileIdFn(context.Background(), chunk.FileId)
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
		err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			glog.V(1).Infof("🔍 Starting SeaweedList for partition directory: %s", partitionDir)

			entryCount := 0
			listErr := filer_pb.SeaweedList(context.Background(), client, partitionDir, "", func(entry *filer_pb.Entry, isLast bool) error {
				entryCount++
				if entryCount <= 10 { // Log first 10 entries for debugging
					glog.V(1).Infof("  📁 Found entry[%d]: %s (isDir=%t, size=%d)", entryCount, entry.Name, entry.IsDirectory, filer.TotalSize(entry.GetChunks()))
				}

				// Continue with original logic
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

			glog.V(1).Infof("🔍 SeaweedList completed for %s: found %d entries, err=%v", partitionDir, entryCount, listErr)
			return listErr
		})

		if err != nil {
			glog.V(0).Infof("⚠️  SeaweedList failed for partition %s: %v", partitionDir, err)

			// Debug: Try to list parent directories to understand structure
			parentPath := util.FullPath(partitionDir)
			if parentPath != "/" {
				lastSlash := strings.LastIndex(string(parentPath), "/")
				if lastSlash >= 0 {
					parentDir := string(parentPath[:lastSlash])
					if parentDir == "" {
						parentDir = "/"
					}
					glog.V(1).Infof("🔍 Attempting to list parent directory: %s", parentDir)
					_ = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
						parentErr := filer_pb.SeaweedList(context.Background(), client, parentDir, "", func(entry *filer_pb.Entry, isLast bool) error {
							glog.V(1).Infof("  📂 Parent entry: %s (isDir=%t)", entry.Name, entry.IsDirectory)
							return nil
						}, "", true, 50) // Limit to 50 entries

						if parentErr != nil {
							glog.V(1).Infof("  ❌ Parent directory listing also failed: %v", parentErr)
						}
						return nil // Don't propagate error, this is just debugging
					})
				}
			}
		}

		lastReadPosition = log_buffer.NewMessagePosition(processedTsNs, -2)
		return
	}
}

package logstore

import (
	"context"
	"encoding/binary"
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

	// Create a small cache for recently-read file chunks (3 files, 60s TTL)
	// This significantly reduces Filer load when multiple consumers are catching up
	fileCache := log_buffer.NewDiskBufferCache(3, 60*time.Second)

	lookupFileIdFn := filer.LookupFn(filerClient)

	eachChunkFn := func(buf []byte, eachLogEntryFn log_buffer.EachLogEntryFuncType, starTsNs, stopTsNs int64, startOffset int64, isOffsetBased bool) (processedTsNs int64, err error) {
		entriesSkipped := 0
		entriesProcessed := 0
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

			// Filter by offset if this is an offset-based subscription
			if isOffsetBased {
				if logEntry.Offset < startOffset {
					entriesSkipped++
					pos += 4 + int(size)
					continue
				}
			} else {
				// Filter by timestamp for timestamp-based subscriptions
				if logEntry.TsNs <= starTsNs {
					pos += 4 + int(size)
					continue
				}
				if stopTsNs != 0 && logEntry.TsNs > stopTsNs {
					println("stopTsNs", stopTsNs, "logEntry.TsNs", logEntry.TsNs)
					return
				}
			}

			// fmt.Printf(" read logEntry: %v, ts %v\n", string(logEntry.Key), time.Unix(0, logEntry.TsNs).UTC())
			if _, err = eachLogEntryFn(logEntry); err != nil {
				err = fmt.Errorf("process log entry %v: %w", logEntry, err)
				return
			}

			processedTsNs = logEntry.TsNs
			entriesProcessed++

			pos += 4 + int(size)

		}

		return
	}

	eachFileFn := func(entry *filer_pb.Entry, eachLogEntryFn log_buffer.EachLogEntryFuncType, starTsNs, stopTsNs int64, startOffset int64, isOffsetBased bool) (processedTsNs int64, err error) {
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
				glog.V(1).Infof("lookup %s failed: %v", chunk.FileId, err)
				err = fmt.Errorf("lookup %s: %v", chunk.FileId, err)
				return
			}
			if len(urlStrings) == 0 {
				glog.V(1).Infof("no url found for %s", chunk.FileId)
				err = fmt.Errorf("no url found for %s", chunk.FileId)
				return
			}
			glog.V(2).Infof("lookup %s returned %d URLs", chunk.FileId, len(urlStrings))

			// Try to get data from cache first
			cacheKey := fmt.Sprintf("%s/%s/%d/%s", t.Name, p.String(), p.RangeStart, chunk.FileId)
			if cachedData, _, found := fileCache.Get(cacheKey); found {
				if cachedData == nil {
					// Negative cache hit - data doesn't exist
					continue
				}
				// Positive cache hit - data exists
				if processedTsNs, err = eachChunkFn(cachedData, eachLogEntryFn, starTsNs, stopTsNs, startOffset, isOffsetBased); err != nil {
					glog.V(1).Infof("eachChunkFn failed on cached data: %v", err)
					return
				}
				continue
			}

			// Cache miss - try one of the urlString until util.Get(urlString) succeeds
			var processed bool
			for _, urlString := range urlStrings {
				// TODO optimization opportunity: reuse the buffer
				var data []byte
				glog.V(2).Infof("trying to fetch data from %s", urlString)
				if data, _, err = util_http.Get(urlString); err == nil {
					glog.V(2).Infof("successfully fetched %d bytes from %s", len(data), urlString)
					processed = true

					// Store in cache for future reads
					fileCache.Put(cacheKey, data, startOffset)

					if processedTsNs, err = eachChunkFn(data, eachLogEntryFn, starTsNs, stopTsNs, startOffset, isOffsetBased); err != nil {
						glog.V(1).Infof("eachChunkFn failed: %v", err)
						return
					}
					break
				} else {
					glog.V(2).Infof("failed to fetch from %s: %v", urlString, err)
				}
			}
			if !processed {
				// Store negative cache entry - data doesn't exist or all URLs failed
				fileCache.Put(cacheKey, nil, startOffset)
				glog.V(1).Infof("no data processed for %s %s - all URLs failed", entry.Name, chunk.FileId)
				err = fmt.Errorf("no data processed for %s %s", entry.Name, chunk.FileId)
				return
			}

		}
		return
	}

	return func(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (lastReadPosition log_buffer.MessagePosition, isDone bool, err error) {
		startFileName := startPosition.Time.UTC().Format(topic.TIME_FORMAT)
		startTsNs := startPosition.Time.UnixNano()
		stopTime := time.Unix(0, stopTsNs)
		var processedTsNs int64

		// Check if this is an offset-based subscription
		isOffsetBased := startPosition.IsOffsetBased
		var startOffset int64
		if isOffsetBased {
			startOffset = startPosition.Offset
			// CRITICAL FIX: For offset-based reads, ignore startFileName (which is based on Time)
			// and list all files from the beginning to find the right offset
			startFileName = ""
			glog.V(1).Infof("disk read start: topic=%s partition=%s startOffset=%d",
				t.Name, p, startOffset)
		}

		// OPTIMIZATION: For offset-based reads, collect all files with their offset ranges first
		// Then use binary search to find the right file, and skip files that don't contain the offset
		var candidateFiles []*filer_pb.Entry
		var foundStartFile bool

		err = filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			// First pass: collect all relevant files with their metadata
			glog.V(2).Infof("listing directory %s for offset %d startFileName=%q", partitionDir, startOffset, startFileName)
			return filer_pb.SeaweedList(context.Background(), client, partitionDir, "", func(entry *filer_pb.Entry, isLast bool) error {

				if entry.IsDirectory {
					return nil
				}
				if strings.HasSuffix(entry.Name, ".parquet") {
					return nil
				}
				if strings.HasSuffix(entry.Name, ".offset") {
					return nil
				}
				if stopTsNs != 0 && entry.Name > stopTime.UTC().Format(topic.TIME_FORMAT) {
					return nil
				}

				// OPTIMIZATION: For offset-based reads, check if this file contains the requested offset
				if isOffsetBased {
					glog.V(3).Infof("found file %s", entry.Name)
					// Check if file has offset range metadata
					if minOffsetBytes, hasMin := entry.Extended["offset_min"]; hasMin && len(minOffsetBytes) == 8 {
						if maxOffsetBytes, hasMax := entry.Extended["offset_max"]; hasMax && len(maxOffsetBytes) == 8 {
							fileMinOffset := int64(binary.BigEndian.Uint64(minOffsetBytes))
							fileMaxOffset := int64(binary.BigEndian.Uint64(maxOffsetBytes))

							// Skip files that don't contain our offset range
							if startOffset > fileMaxOffset {
								return nil
							}

							// If we haven't found the start file yet, check if this file contains it
							if !foundStartFile && startOffset >= fileMinOffset && startOffset <= fileMaxOffset {
								foundStartFile = true
							}
						}
					}
					// If file doesn't have offset metadata, include it (might be old format)
				} else {
					// Timestamp-based filtering
					topicName := t.Name
					if dotIndex := strings.LastIndex(topicName, "."); dotIndex != -1 {
						topicName = topicName[dotIndex+1:]
					}
					isSystemTopic := strings.HasPrefix(topicName, "_")
					if !isSystemTopic && startPosition.Time.Unix() > 86400 && entry.Name < startPosition.Time.UTC().Format(topic.TIME_FORMAT) {
						return nil
					}
				}

				// Add file to candidates for processing
				candidateFiles = append(candidateFiles, entry)
				glog.V(3).Infof("added candidate file %s (total=%d)", entry.Name, len(candidateFiles))
				return nil

			}, startFileName, true, math.MaxInt32)
		})

		if err != nil {
			glog.Errorf("failed to list directory %s: %v", partitionDir, err)
			return
		}

		glog.V(2).Infof("found %d candidate files for topic=%s partition=%s offset=%d",
			len(candidateFiles), t.Name, p, startOffset)

		if len(candidateFiles) == 0 {
			glog.V(2).Infof("no files found in %s", partitionDir)
			return startPosition, isDone, nil
		}

		// OPTIMIZATION: For offset-based reads with many files, use binary search to find start file
		if isOffsetBased && len(candidateFiles) > 10 {
			// Binary search to find the first file that might contain our offset
			left, right := 0, len(candidateFiles)-1
			startIdx := 0

			for left <= right {
				mid := (left + right) / 2
				entry := candidateFiles[mid]

				if minOffsetBytes, hasMin := entry.Extended["offset_min"]; hasMin && len(minOffsetBytes) == 8 {
					if maxOffsetBytes, hasMax := entry.Extended["offset_max"]; hasMax && len(maxOffsetBytes) == 8 {
						fileMinOffset := int64(binary.BigEndian.Uint64(minOffsetBytes))
						fileMaxOffset := int64(binary.BigEndian.Uint64(maxOffsetBytes))

						if startOffset < fileMinOffset {
							// Our offset is before this file, search left
							right = mid - 1
						} else if startOffset > fileMaxOffset {
							// Our offset is after this file, search right
							left = mid + 1
							startIdx = left
						} else {
							// Found the file containing our offset
							startIdx = mid
							break
						}
					} else {
						break
					}
				} else {
					break
				}
			}

			// Process files starting from the found index
			candidateFiles = candidateFiles[startIdx:]
		}

		// Second pass: process the filtered files
		// CRITICAL: For offset-based reads, process ALL candidate files in one call
		// This prevents multiple ReadFromDiskFn calls with 1.127s overhead each
		var filesProcessed int
		var lastProcessedOffset int64
		for _, entry := range candidateFiles {
			var fileTsNs int64
			if fileTsNs, err = eachFileFn(entry, eachLogEntryFn, startTsNs, stopTsNs, startOffset, isOffsetBased); err != nil {
				return lastReadPosition, isDone, err
			}
			if fileTsNs > 0 {
				processedTsNs = fileTsNs
				filesProcessed++
			}

			// For offset-based reads, track the last processed offset
			// We need to continue reading ALL files to avoid multiple disk read calls
			if isOffsetBased {
				// Extract the last offset from the file's extended attributes
				if maxOffsetBytes, hasMax := entry.Extended["offset_max"]; hasMax && len(maxOffsetBytes) == 8 {
					fileMaxOffset := int64(binary.BigEndian.Uint64(maxOffsetBytes))
					if fileMaxOffset > lastProcessedOffset {
						lastProcessedOffset = fileMaxOffset
					}
				}
			}
		}

		if isOffsetBased && filesProcessed > 0 {
			// Return a position that indicates we've read all disk data up to lastProcessedOffset
			// This prevents the subscription from calling ReadFromDiskFn again for these offsets
			lastReadPosition = log_buffer.NewMessagePositionFromOffset(lastProcessedOffset + 1)
		} else {
			// CRITICAL FIX: If no files were processed (e.g., all data already consumed),
			// return the requested offset to prevent busy loop
			if isOffsetBased {
				// For offset-based reads with no data, return the requested offset
				// This signals "I've checked, there's no data at this offset, move forward"
				lastReadPosition = log_buffer.NewMessagePositionFromOffset(startOffset)
			} else {
				// For timestamp-based reads, return error (-2)
				lastReadPosition = log_buffer.NewMessagePosition(processedTsNs, -2)
			}
		}
		return
	}
}

package broker

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

// BufferRange represents a range of buffer indexes that have been flushed to disk
type BufferRange struct {
	start int64
	end   int64
}

// GetUnflushedMessages returns messages from the broker's in-memory LogBuffer
// that haven't been flushed to disk yet, using buffer_start metadata for deduplication
// Now supports streaming responses and buffer index filtering for better performance
func (b *MessageQueueBroker) GetUnflushedMessages(req *mq_pb.GetUnflushedMessagesRequest, stream mq_pb.SeaweedMessaging_GetUnflushedMessagesServer) error {
	// Convert protobuf types to internal types
	t := topic.FromPbTopic(req.Topic)
	partition := topic.FromPbPartition(req.Partition)

	glog.V(2).Infof("GetUnflushedMessages request for %v %v", t, partition)

	// Get the local partition for this topic/partition
	b.accessLock.Lock()
	localPartition := b.localTopicManager.GetLocalPartition(t, partition)
	b.accessLock.Unlock()

	if localPartition == nil {
		return stream.Send(&mq_pb.GetUnflushedMessagesResponse{
			Error:       fmt.Sprintf("partition %v %v not found on this broker", t, partition),
			EndOfStream: true,
		})
	}

	// Build deduplication map from existing log files using buffer_start metadata
	partitionDir := topic.PartitionDir(t, partition)
	flushedBufferRanges, err := b.buildBufferStartDeduplicationMap(partitionDir)
	if err != nil {
		glog.Errorf("Failed to build deduplication map for %v %v: %v", t, partition, err)
		// Continue with empty map - better to potentially duplicate than to miss data
		flushedBufferRanges = make([]BufferRange, 0)
	}

	// Use buffer_start index for precise deduplication
	lastFlushTsNs := localPartition.LogBuffer.LastFlushTsNs
	startBufferIndex := req.StartBufferIndex
	startTimeNs := lastFlushTsNs // Still respect last flush time for safety

	glog.V(2).Infof("Streaming unflushed messages for %v %v, buffer >= %d, timestamp >= %d (safety), excluding %d flushed buffer ranges",
		t, partition, startBufferIndex, startTimeNs, len(flushedBufferRanges))

	// Stream messages from LogBuffer with filtering
	messageCount := 0
	startPosition := log_buffer.NewMessagePosition(startTimeNs, startBufferIndex)

	// Create a custom LoopProcessLogData function that captures the batch index
	// Since we can't modify the existing EachLogEntryFuncType signature,
	// we'll implement our own iteration logic based on LoopProcessLogData
	var lastReadPosition = startPosition
	var isDone bool

	for !isDone {
		// Use ReadFromBuffer to get the next batch with its correct batch index
		bytesBuf, batchIndex, err := localPartition.LogBuffer.ReadFromBuffer(lastReadPosition)
		if err == log_buffer.ResumeFromDiskError {
			break
		}
		if err != nil {
			return err
		}

		// If no more data in memory, we're done
		if bytesBuf == nil {
			break
		}

		// Process all messages in this batch
		buf := bytesBuf.Bytes()
		for pos := 0; pos+4 < len(buf); {
			size := util.BytesToUint32(buf[pos : pos+4])
			if pos+4+int(size) > len(buf) {
				break
			}
			entryData := buf[pos+4 : pos+4+int(size)]

			logEntry := &filer_pb.LogEntry{}
			if err = proto.Unmarshal(entryData, logEntry); err != nil {
				pos += 4 + int(size)
				continue
			}

			// Now we have the correct batchIndex for this message
			// Apply buffer index filtering if specified
			if startBufferIndex > 0 && batchIndex < startBufferIndex {
				glog.V(3).Infof("Skipping message from buffer index %d (< %d)", batchIndex, startBufferIndex)
				pos += 4 + int(size)
				continue
			}

			// Check if this message is from a buffer range that's already been flushed
			if b.isBufferIndexFlushed(batchIndex, flushedBufferRanges) {
				glog.V(3).Infof("Skipping message from flushed buffer index %d", batchIndex)
				pos += 4 + int(size)
				continue
			}

			// Stream this message
			err = stream.Send(&mq_pb.GetUnflushedMessagesResponse{
				Message: &mq_pb.LogEntry{
					TsNs:             logEntry.TsNs,
					Key:              logEntry.Key,
					Data:             logEntry.Data,
					PartitionKeyHash: uint32(logEntry.PartitionKeyHash),
				},
				EndOfStream: false,
			})

			if err != nil {
				glog.Errorf("Failed to stream message: %v", err)
				isDone = true
				break
			}

			messageCount++
			lastReadPosition = log_buffer.NewMessagePosition(logEntry.TsNs, batchIndex)
			pos += 4 + int(size)
		}

		// Release the buffer back to the pool
		localPartition.LogBuffer.ReleaseMemory(bytesBuf)
	}

	// Handle collection errors
	if err != nil && err != log_buffer.ResumeFromDiskError {
		streamErr := stream.Send(&mq_pb.GetUnflushedMessagesResponse{
			Error:       fmt.Sprintf("failed to stream unflushed messages: %v", err),
			EndOfStream: true,
		})
		if streamErr != nil {
			glog.Errorf("Failed to send error response: %v", streamErr)
		}
		return err
	}

	// Send end-of-stream marker
	err = stream.Send(&mq_pb.GetUnflushedMessagesResponse{
		EndOfStream: true,
	})

	if err != nil {
		glog.Errorf("Failed to send end-of-stream marker: %v", err)
		return err
	}

	glog.V(1).Infof("Streamed %d unflushed messages for %v %v", messageCount, t, partition)
	return nil
}

// buildBufferStartDeduplicationMap scans log files to build a map of buffer ranges
// that have been flushed to disk, using the buffer_start metadata
func (b *MessageQueueBroker) buildBufferStartDeduplicationMap(partitionDir string) ([]BufferRange, error) {
	var flushedRanges []BufferRange

	// List all files in the partition directory using filer client accessor
	err := b.fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.SeaweedList(context.Background(), client, partitionDir, "", func(entry *filer_pb.Entry, isLast bool) error {
			if entry.IsDirectory {
				return nil
			}

			// Skip Parquet files - they don't represent buffer ranges
			if strings.HasSuffix(entry.Name, ".parquet") {
				return nil
			}

			// Skip offset files
			if strings.HasSuffix(entry.Name, ".offset") {
				return nil
			}

			// Get buffer start for this file
			bufferStart, err := b.getLogBufferStartFromFile(entry)
			if err != nil {
				glog.V(2).Infof("Failed to get buffer start from file %s: %v", entry.Name, err)
				return nil // Continue with other files
			}

			if bufferStart == nil {
				// File has no buffer metadata - skip deduplication for this file
				glog.V(2).Infof("File %s has no buffer_start metadata", entry.Name)
				return nil
			}

			// Calculate the buffer range covered by this file
			chunkCount := int64(len(entry.GetChunks()))
			if chunkCount > 0 {
				fileRange := BufferRange{
					start: bufferStart.StartIndex,
					end:   bufferStart.StartIndex + chunkCount - 1,
				}
				flushedRanges = append(flushedRanges, fileRange)
				glog.V(3).Infof("File %s covers buffer range [%d-%d]", entry.Name, fileRange.start, fileRange.end)
			}

			return nil
		}, "", true, 1000)
	})

	if err != nil {
		return flushedRanges, fmt.Errorf("failed to list partition directory %s: %v", partitionDir, err)
	}

	return flushedRanges, nil
}

// getLogBufferStartFromFile extracts LogBufferStart metadata from a log file
func (b *MessageQueueBroker) getLogBufferStartFromFile(entry *filer_pb.Entry) (*LogBufferStart, error) {
	if entry.Extended == nil {
		return nil, nil
	}

	// Only support binary buffer_start format
	if startData, exists := entry.Extended["buffer_start"]; exists {
		if len(startData) == 8 {
			startIndex := int64(binary.BigEndian.Uint64(startData))
			if startIndex > 0 {
				return &LogBufferStart{StartIndex: startIndex}, nil
			}
		} else {
			return nil, fmt.Errorf("invalid buffer_start format: expected 8 bytes, got %d", len(startData))
		}
	}

	return nil, nil
}

// isBufferIndexFlushed checks if a buffer index is covered by any of the flushed ranges
func (b *MessageQueueBroker) isBufferIndexFlushed(bufferIndex int64, flushedRanges []BufferRange) bool {
	for _, flushedRange := range flushedRanges {
		if bufferIndex >= flushedRange.start && bufferIndex <= flushedRange.end {
			return true
		}
	}
	return false
}

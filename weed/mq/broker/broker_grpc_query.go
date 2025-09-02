package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
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

	// Determine filtering criteria based on oneof start_filter
	lastFlushTsNs := localPartition.LogBuffer.LastFlushTsNs
	var startTimeNs int64
	var startBufferIndex int64
	var filterType string

	// Handle oneof start_filter
	switch filter := req.StartFilter.(type) {
	case *mq_pb.GetUnflushedMessagesRequest_StartTimeNs:
		startTimeNs = filter.StartTimeNs
		filterType = "timestamp"
		// Use the more restrictive of lastFlushTsNs vs requested startTimeNs
		if lastFlushTsNs > startTimeNs {
			startTimeNs = lastFlushTsNs
		}
	case *mq_pb.GetUnflushedMessagesRequest_StartBufferIndex:
		startBufferIndex = filter.StartBufferIndex
		startTimeNs = lastFlushTsNs // Still respect last flush time
		filterType = "buffer_index"
	default:
		// No specific filter provided, use lastFlushTsNs as default
		startTimeNs = lastFlushTsNs
		filterType = "default"
	}

	glog.V(2).Infof("Streaming unflushed messages for %v %v, filter_type=%s, timestamp >= %d, buffer >= %d, excluding %d flushed buffer ranges",
		t, partition, filterType, startTimeNs, startBufferIndex, len(flushedBufferRanges))

	// Stream messages from LogBuffer with filtering
	messageCount := 0
	startPosition := log_buffer.NewMessagePosition(startTimeNs, startBufferIndex)
	_, _, err = localPartition.LogBuffer.LoopProcessLogData("sql_query_stream", startPosition, 0,
		func() bool { return false }, // Don't wait for more data
		func(logEntry *filer_pb.LogEntry) (isDone bool, err error) {
			// Apply buffer index filtering if specified
			currentBatchIndex := localPartition.LogBuffer.GetBatchIndex()
			if startBufferIndex > 0 && currentBatchIndex < startBufferIndex {
				glog.V(3).Infof("Skipping message from buffer index %d (< %d)", currentBatchIndex, startBufferIndex)
				return false, nil
			}

			// Check if this message is from a buffer range that's already been flushed
			if b.isBufferIndexFlushed(currentBatchIndex, flushedBufferRanges) {
				glog.V(3).Infof("Skipping message from flushed buffer index %d", currentBatchIndex)
				return false, nil
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
				return true, err // Stop streaming on error
			}

			messageCount++
			return false, nil
		})

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

	// Only support buffer_start format
	if startJson, exists := entry.Extended["buffer_start"]; exists {
		var bufferStart LogBufferStart
		if err := json.Unmarshal(startJson, &bufferStart); err != nil {
			return nil, fmt.Errorf("failed to parse buffer start: %v", err)
		}
		return &bufferStart, nil
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

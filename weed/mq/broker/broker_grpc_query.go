package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

// BufferRange represents a range of buffer offsets that have been flushed to disk
type BufferRange struct {
	start int64
	end   int64
}

// ErrNoPartitionAssignment indicates no broker assignment found for the partition.
// This is a normal case that means there are no unflushed messages for this partition.
var ErrNoPartitionAssignment = errors.New("no broker assignment found for partition")

// GetUnflushedMessages returns messages from the broker's in-memory LogBuffer
// that haven't been flushed to disk yet, using buffer_start metadata for deduplication
// Now supports streaming responses and buffer offset filtering for better performance
// Includes broker routing to redirect requests to the correct broker hosting the topic/partition
func (b *MessageQueueBroker) GetUnflushedMessages(req *mq_pb.GetUnflushedMessagesRequest, stream mq_pb.SeaweedMessaging_GetUnflushedMessagesServer) error {
	// Convert protobuf types to internal types
	t := topic.FromPbTopic(req.Topic)
	partition := topic.FromPbPartition(req.Partition)

	// Get or generate the local partition for this topic/partition (similar to subscriber flow)
	localPartition, getOrGenErr := b.GetOrGenerateLocalPartition(t, partition)
	if getOrGenErr != nil {
		// Fall back to the original logic for broker routing
		b.accessLock.Lock()
		localPartition = b.localTopicManager.GetLocalPartition(t, partition)
		b.accessLock.Unlock()
	} else {
	}

	if localPartition == nil {
		// Topic/partition not found locally, attempt to find the correct broker and redirect
		glog.V(1).Infof("Topic/partition %v %v not found locally, looking up broker", t, partition)

		// Look up which broker hosts this topic/partition
		brokerHost, err := b.findBrokerForTopicPartition(req.Topic, req.Partition)
		if err != nil {
			if errors.Is(err, ErrNoPartitionAssignment) {
				// Normal case: no broker assignment means no unflushed messages
				glog.V(2).Infof("No broker assignment for %v %v - no unflushed messages", t, partition)
				return stream.Send(&mq_pb.GetUnflushedMessagesResponse{
					EndOfStream: true,
				})
			}
			return stream.Send(&mq_pb.GetUnflushedMessagesResponse{
				Error:       fmt.Sprintf("failed to find broker for %v %v: %v", t, partition, err),
				EndOfStream: true,
			})
		}

		if brokerHost == "" {
			// This should not happen after ErrNoPartitionAssignment check, but keep for safety
			glog.V(2).Infof("Empty broker host for %v %v - no unflushed messages", t, partition)
			return stream.Send(&mq_pb.GetUnflushedMessagesResponse{
				EndOfStream: true,
			})
		}

		// Redirect to the correct broker
		glog.V(1).Infof("Redirecting GetUnflushedMessages request for %v %v to broker %s", t, partition, brokerHost)
		return b.redirectGetUnflushedMessages(brokerHost, req, stream)
	}

	// Build deduplication map from existing log files using buffer_start metadata
	partitionDir := topic.PartitionDir(t, partition)
	flushedBufferRanges, err := b.buildBufferStartDeduplicationMap(partitionDir)
	if err != nil {
		glog.Errorf("Failed to build deduplication map for %v %v: %v", t, partition, err)
		// Continue with empty map - better to potentially duplicate than to miss data
		flushedBufferRanges = make([]BufferRange, 0)
	}

	// Use buffer_start offset for precise deduplication
	lastFlushTsNs := localPartition.LogBuffer.GetLastFlushTsNs()
	startBufferOffset := req.StartBufferOffset
	startTimeNs := lastFlushTsNs // Still respect last flush time for safety

	// Stream messages from LogBuffer with filtering
	messageCount := 0
	startPosition := log_buffer.NewMessagePosition(startTimeNs, startBufferOffset)

	// Use the new LoopProcessLogDataWithOffset method to avoid code duplication
	_, _, err = localPartition.LogBuffer.LoopProcessLogDataWithOffset(
		"GetUnflushedMessages",
		startPosition,
		0,                            // stopTsNs = 0 means process all available data
		func() bool { return false }, // waitForDataFn = false means don't wait for new data
		func(logEntry *filer_pb.LogEntry, offset int64) (isDone bool, err error) {

			// Apply buffer offset filtering if specified
			if startBufferOffset > 0 && offset < startBufferOffset {
				return false, nil
			}

			// Check if this message is from a buffer range that's already been flushed
			if b.isBufferOffsetFlushed(offset, flushedBufferRanges) {
				return false, nil
			}

			// Stream this message
			err = stream.Send(&mq_pb.GetUnflushedMessagesResponse{
				Message:     logEntry,
				EndOfStream: false,
			})

			if err != nil {
				glog.Errorf("Failed to stream message: %v", err)
				return true, err // isDone = true to stop processing
			}

			messageCount++
			return false, nil // Continue processing
		},
	)

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

	return nil
}

// buildBufferStartDeduplicationMap scans log files to build a map of buffer ranges
// that have been flushed to disk, using the buffer_start metadata
func (b *MessageQueueBroker) buildBufferStartDeduplicationMap(partitionDir string) ([]BufferRange, error) {
	var flushedRanges []BufferRange

	// List all files in the partition directory using filer client accessor
	// Use pagination to handle directories with more than 1000 files
	err := b.fca.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var lastFileName string
		var hasMore = true

		for hasMore {
			var currentBatchProcessed int
			err := filer_pb.SeaweedList(context.Background(), client, partitionDir, "", func(entry *filer_pb.Entry, isLast bool) error {
				currentBatchProcessed++
				hasMore = !isLast // If this is the last entry of a full batch, there might be more
				lastFileName = entry.Name

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
			}, lastFileName, false, 1000) // Start from last processed file name for next batch

			if err != nil {
				return err
			}

			// If we processed fewer than 1000 entries, we've reached the end
			if currentBatchProcessed < 1000 {
				hasMore = false
			}
		}

		return nil
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

// isBufferOffsetFlushed checks if a buffer offset is covered by any of the flushed ranges
func (b *MessageQueueBroker) isBufferOffsetFlushed(bufferOffset int64, flushedRanges []BufferRange) bool {
	for _, flushedRange := range flushedRanges {
		if bufferOffset >= flushedRange.start && bufferOffset <= flushedRange.end {
			return true
		}
	}
	return false
}

// findBrokerForTopicPartition finds which broker hosts the specified topic/partition
func (b *MessageQueueBroker) findBrokerForTopicPartition(topic *schema_pb.Topic, partition *schema_pb.Partition) (string, error) {
	// Use LookupTopicBrokers to find which broker hosts this topic/partition
	ctx := context.Background()
	lookupReq := &mq_pb.LookupTopicBrokersRequest{
		Topic: topic,
	}

	// If we're not the lock owner (balancer), we need to redirect to the balancer first
	var lookupResp *mq_pb.LookupTopicBrokersResponse
	var err error

	if !b.isLockOwner() {
		// Redirect to balancer to get topic broker assignments
		balancerAddress := pb.ServerAddress(b.lockAsBalancer.LockOwner())
		err = b.withBrokerClient(false, balancerAddress, func(client mq_pb.SeaweedMessagingClient) error {
			lookupResp, err = client.LookupTopicBrokers(ctx, lookupReq)
			return err
		})
	} else {
		// We are the balancer, handle the lookup directly
		lookupResp, err = b.LookupTopicBrokers(ctx, lookupReq)
	}

	if err != nil {
		return "", fmt.Errorf("failed to lookup topic brokers: %v", err)
	}

	// Find the broker assignment that matches our partition
	for _, assignment := range lookupResp.BrokerPartitionAssignments {
		if b.partitionsMatch(partition, assignment.Partition) {
			if assignment.LeaderBroker != "" {
				return assignment.LeaderBroker, nil
			}
		}
	}

	return "", ErrNoPartitionAssignment
}

// partitionsMatch checks if two partitions represent the same partition
func (b *MessageQueueBroker) partitionsMatch(p1, p2 *schema_pb.Partition) bool {
	return p1.RingSize == p2.RingSize &&
		p1.RangeStart == p2.RangeStart &&
		p1.RangeStop == p2.RangeStop &&
		p1.UnixTimeNs == p2.UnixTimeNs
}

// redirectGetUnflushedMessages forwards the GetUnflushedMessages request to the correct broker
func (b *MessageQueueBroker) redirectGetUnflushedMessages(brokerHost string, req *mq_pb.GetUnflushedMessagesRequest, stream mq_pb.SeaweedMessaging_GetUnflushedMessagesServer) error {
	ctx := stream.Context()

	// Connect to the target broker and forward the request
	return b.withBrokerClient(false, pb.ServerAddress(brokerHost), func(client mq_pb.SeaweedMessagingClient) error {
		// Create a new stream to the target broker
		targetStream, err := client.GetUnflushedMessages(ctx, req)
		if err != nil {
			return fmt.Errorf("failed to create stream to broker %s: %v", brokerHost, err)
		}

		// Forward all responses from the target broker to our client
		for {
			response, err := targetStream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					// Normal end of stream
					return nil
				}
				return fmt.Errorf("error receiving from broker %s: %v", brokerHost, err)
			}

			// Forward the response to our client
			if sendErr := stream.Send(response); sendErr != nil {
				return fmt.Errorf("error forwarding response to client: %v", sendErr)
			}

			// Check if this is the end of stream
			if response.EndOfStream {
				return nil
			}
		}
	})
}

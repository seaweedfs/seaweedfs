package integration

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// CreateFreshSubscriber creates a new subscriber session without caching
// This ensures each fetch gets fresh data from the requested offset
// consumerGroup and consumerID are passed from Kafka client for proper tracking in SMQ
func (bc *BrokerClient) CreateFreshSubscriber(topic string, partition int32, startOffset int64, consumerGroup string, consumerID string) (*BrokerSubscriberSession, error) {
	// Create a dedicated context for this subscriber
	subscriberCtx := context.Background()

	stream, err := bc.client.SubscribeMessage(subscriberCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscribe stream: %v", err)
	}

	// Get the actual partition assignment from the broker
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual partition assignment for subscribe: %v", err)
	}

	// Convert Kafka offset to SeaweedMQ OffsetType
	var offsetType schema_pb.OffsetType
	var startTimestamp int64
	var startOffsetValue int64

	// Use EXACT_OFFSET to read from the specific offset
	offsetType = schema_pb.OffsetType_EXACT_OFFSET
	startTimestamp = 0
	startOffsetValue = startOffset

	// Send init message to start subscription with Kafka client's consumer group and ID
	initReq := &mq_pb.SubscribeMessageRequest{
		Message: &mq_pb.SubscribeMessageRequest_Init{
			Init: &mq_pb.SubscribeMessageRequest_InitMessage{
				ConsumerGroup: consumerGroup,
				ConsumerId:    consumerID,
				ClientId:      "kafka-gateway",
				Topic: &schema_pb.Topic{
					Namespace: "kafka",
					Name:      topic,
				},
				PartitionOffset: &schema_pb.PartitionOffset{
					Partition:   actualPartition,
					StartTsNs:   startTimestamp,
					StartOffset: startOffsetValue,
				},
				OffsetType:        offsetType,
				SlidingWindowSize: 10,
			},
		},
	}

	if err := stream.Send(initReq); err != nil {
		return nil, fmt.Errorf("failed to send subscribe init: %v", err)
	}

	// IMPORTANT: Don't wait for init response here!
	// The broker may send the first data record as the "init response"
	// If we call Recv() here, we'll consume that first record and ReadRecords will block
	// waiting for the second record, causing a 30-second timeout.
	// Instead, let ReadRecords handle all Recv() calls.

	session := &BrokerSubscriberSession{
		Stream:      stream,
		Topic:       topic,
		Partition:   partition,
		StartOffset: startOffset,
	}

	return session, nil
}

// GetOrCreateSubscriber gets or creates a subscriber for offset tracking
func (bc *BrokerClient) GetOrCreateSubscriber(topic string, partition int32, startOffset int64) (*BrokerSubscriberSession, error) {
	key := fmt.Sprintf("%s-%d", topic, partition)

	bc.subscribersLock.RLock()
	if session, exists := bc.subscribers[key]; exists {
		// If the existing session was created with a different start offset,
		// re-create the subscriber to honor the requested position.
		if session.StartOffset != startOffset {
			bc.subscribersLock.RUnlock()
			// Close and delete the old session before creating a new one
			bc.subscribersLock.Lock()
			if old, ok := bc.subscribers[key]; ok {
				if old.Stream != nil {
					_ = old.Stream.CloseSend()
				}
				if old.Cancel != nil {
					old.Cancel()
				}
				delete(bc.subscribers, key)
				glog.V(0).Infof("Closed old subscriber session for %s due to offset change", key)
			}
			bc.subscribersLock.Unlock()
		} else {
			bc.subscribersLock.RUnlock()
			return session, nil
		}
	} else {
		bc.subscribersLock.RUnlock()
	}

	// Create new subscriber stream
	bc.subscribersLock.Lock()
	defer bc.subscribersLock.Unlock()

	if session, exists := bc.subscribers[key]; exists {
		return session, nil
	}

	// CRITICAL FIX: Use background context for subscriber to prevent premature cancellation
	// Subscribers need to continue reading data even when the connection is closing,
	// otherwise Schema Registry and other clients can't read existing data.
	// The subscriber will be cleaned up when the stream is explicitly closed.
	subscriberCtx := context.Background()
	subscriberCancel := func() {} // No-op cancel

	stream, err := bc.client.SubscribeMessage(subscriberCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscribe stream: %v", err)
	}

	// Get the actual partition assignment from the broker instead of using Kafka partition mapping
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual partition assignment for subscribe: %v", err)
	}

	// Convert Kafka offset to appropriate SeaweedMQ OffsetType and parameters
	var offsetType schema_pb.OffsetType
	var startTimestamp int64
	var startOffsetValue int64

	if startOffset == -1 {
		// Kafka offset -1 typically means "latest"
		offsetType = schema_pb.OffsetType_RESET_TO_LATEST
		startTimestamp = 0   // Not used with RESET_TO_LATEST
		startOffsetValue = 0 // Not used with RESET_TO_LATEST
		glog.V(1).Infof("Using RESET_TO_LATEST for Kafka offset -1 (read latest)")
	} else {
		// CRITICAL FIX: Use EXACT_OFFSET to position subscriber at the exact Kafka offset
		// This allows the subscriber to read from both buffer and disk at the correct position
		offsetType = schema_pb.OffsetType_EXACT_OFFSET
		startTimestamp = 0             // Not used with EXACT_OFFSET
		startOffsetValue = startOffset // Use the exact Kafka offset
		glog.V(1).Infof("Using EXACT_OFFSET for Kafka offset %d (direct positioning)", startOffset)
	}

	glog.V(1).Infof("Creating subscriber for topic=%s partition=%d: Kafka offset %d -> SeaweedMQ %s (timestamp=%d)",
		topic, partition, startOffset, offsetType, startTimestamp)

	// Send init message using the actual partition structure that the broker allocated
	if err := stream.Send(&mq_pb.SubscribeMessageRequest{
		Message: &mq_pb.SubscribeMessageRequest_Init{
			Init: &mq_pb.SubscribeMessageRequest_InitMessage{
				ConsumerGroup: "kafka-gateway",
				ConsumerId:    fmt.Sprintf("kafka-gateway-%s-%d", topic, partition),
				ClientId:      "kafka-gateway",
				Topic: &schema_pb.Topic{
					Namespace: "kafka",
					Name:      topic,
				},
				PartitionOffset: &schema_pb.PartitionOffset{
					Partition:   actualPartition,
					StartTsNs:   startTimestamp,
					StartOffset: startOffsetValue,
				},
				OffsetType:        offsetType, // Use the correct offset type
				SlidingWindowSize: 10,
			},
		},
	}); err != nil {
		return nil, fmt.Errorf("failed to send subscribe init: %v", err)
	}

	session := &BrokerSubscriberSession{
		Topic:       topic,
		Partition:   partition,
		Stream:      stream,
		StartOffset: startOffset,
		Ctx:         subscriberCtx,
		Cancel:      subscriberCancel,
	}

	bc.subscribers[key] = session
	glog.V(0).Infof("Created subscriber session for %s with context cancellation support", key)
	return session, nil
}

// ReadRecords reads available records from the subscriber stream
// Uses a timeout-based approach to read multiple records without blocking indefinitely
func (bc *BrokerClient) ReadRecords(session *BrokerSubscriberSession, maxRecords int) ([]*SeaweedRecord, error) {
	if session == nil {
		return nil, fmt.Errorf("subscriber session cannot be nil")
	}

	if session.Stream == nil {
		return nil, fmt.Errorf("subscriber session stream cannot be nil")
	}

	// CRITICAL: Lock to prevent concurrent reads from the same stream
	// Multiple Fetch requests may try to read from the same subscriber concurrently,
	// causing the broker to return the same offset repeatedly
	session.mu.Lock()
	defer session.mu.Unlock()

	glog.V(2).Infof("[FETCH] ReadRecords: topic=%s partition=%d startOffset=%d maxRecords=%d",
		session.Topic, session.Partition, session.StartOffset, maxRecords)

	var records []*SeaweedRecord
	currentOffset := session.StartOffset

	// CRITICAL FIX: Return immediately if maxRecords is 0 or negative
	if maxRecords <= 0 {
		return records, nil
	}

	// CRITICAL FIX: Use cached records if available to avoid broker tight loop
	// If we've already consumed these records, return them from cache
	if len(session.consumedRecords) > 0 {
		cacheStartOffset := session.consumedRecords[0].Offset
		cacheEndOffset := session.consumedRecords[len(session.consumedRecords)-1].Offset

		if currentOffset >= cacheStartOffset && currentOffset <= cacheEndOffset {
			// Records are in cache
			glog.V(2).Infof("[FETCH] Returning cached records: requested offset %d is in cache [%d-%d]",
				currentOffset, cacheStartOffset, cacheEndOffset)

			// Find starting index in cache
			startIdx := int(currentOffset - cacheStartOffset)
			if startIdx < 0 || startIdx >= len(session.consumedRecords) {
				glog.Errorf("[FETCH] Cache index out of bounds: startIdx=%d, cache size=%d", startIdx, len(session.consumedRecords))
				return records, nil
			}

			// Return up to maxRecords from cache
			endIdx := startIdx + maxRecords
			if endIdx > len(session.consumedRecords) {
				endIdx = len(session.consumedRecords)
			}

			glog.V(2).Infof("[FETCH] Returning %d cached records from index %d to %d", endIdx-startIdx, startIdx, endIdx-1)
			return session.consumedRecords[startIdx:endIdx], nil
		}
	}

	// Read first record with timeout (important for empty topics)
	// Use reasonable timeout to balance consumer responsiveness and producer needs
	// Wait up to 2s for first record (allows Schema Registry producer to write+confirm)
	// Reduced from 10s to prevent excessive consumer lag (5x improvement)
	// With concurrent partition fetching, keep timeout low to prevent client timeouts
	// Must be less than client fetch_max_wait_ms (100ms) to allow all concurrent fetches to complete
	firstRecordTimeout := 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), firstRecordTimeout)
	defer cancel()

	type recvResult struct {
		resp *mq_pb.SubscribeMessageResponse
		err  error
	}
	recvChan := make(chan recvResult, 1)

	// Try to receive first record
	go func() {
		resp, err := session.Stream.Recv()
		select {
		case recvChan <- recvResult{resp: resp, err: err}:
		case <-ctx.Done():
			// Context cancelled, don't send (avoid blocking)
		}
	}()

	select {
	case result := <-recvChan:
		if result.err != nil {
			glog.V(2).Infof("[FETCH] Stream.Recv() error on first record: %v", result.err)
			return records, nil // Return empty - no error for empty topic
		}

		if dataMsg := result.resp.GetData(); dataMsg != nil {
			record := &SeaweedRecord{
				Key:       dataMsg.Key,
				Value:     dataMsg.Value,
				Timestamp: dataMsg.TsNs,
				Offset:    currentOffset,
			}
			records = append(records, record)
			currentOffset++
			glog.V(4).Infof("[FETCH] Received record: offset=%d, keyLen=%d, valueLen=%d",
				record.Offset, len(record.Key), len(record.Value))
		}

	case <-ctx.Done():
		// Timeout on first record - topic is empty or no data available
		glog.V(4).Infof("[FETCH] No data available (timeout on first record)")
		return records, nil
	}

	// If we got the first record, try to get more with adaptive timeout
	// CRITICAL: Schema Registry catch-up scenario - give generous timeout for the first batch
	// Schema Registry needs to read multiple records quickly when catching up (e.g., offsets 3-6)
	// The broker may be reading from disk, which introduces 10-20ms delay between records
	//
	// Strategy: Start with generous timeout (1 second) for first 5 records to allow broker
	// to read from disk, then switch to fast mode (100ms) for streaming in-memory data
	consecutiveReads := 0

	for len(records) < maxRecords {
		// Adaptive timeout based on how many records we've already read
		var currentTimeout time.Duration
		if consecutiveReads < 5 {
			// First 5 records: generous timeout for disk reads + network delays
			currentTimeout = 1 * time.Second
		} else {
			// After 5 records: assume we're streaming from memory, use faster timeout
			currentTimeout = 100 * time.Millisecond
		}

		readStart := time.Now()
		ctx2, cancel2 := context.WithTimeout(context.Background(), currentTimeout)
		recvChan2 := make(chan recvResult, 1)

		go func() {
			resp, err := session.Stream.Recv()
			select {
			case recvChan2 <- recvResult{resp: resp, err: err}:
			case <-ctx2.Done():
				// Context cancelled
			}
		}()

		select {
		case result := <-recvChan2:
			cancel2()
			readDuration := time.Since(readStart)

			if result.err != nil {
				glog.V(2).Infof("[FETCH] Stream.Recv() error after %d records: %v", len(records), result.err)
				// Update session offset before returning
				session.StartOffset = currentOffset
				return records, nil
			}

			if dataMsg := result.resp.GetData(); dataMsg != nil {
				record := &SeaweedRecord{
					Key:       dataMsg.Key,
					Value:     dataMsg.Value,
					Timestamp: dataMsg.TsNs,
					Offset:    currentOffset,
				}
				records = append(records, record)
				currentOffset++
				consecutiveReads++ // Track number of successful reads for adaptive timeout

				glog.V(4).Infof("[FETCH] Received record %d: offset=%d, keyLen=%d, valueLen=%d, readTime=%v",
					len(records), record.Offset, len(record.Key), len(record.Value), readDuration)
			}

		case <-ctx2.Done():
			cancel2()
			// Timeout - return what we have
			glog.V(4).Infof("[FETCH] Read timeout after %d records (waited %v), returning batch", len(records), time.Since(readStart))
			// CRITICAL: Update session offset so next fetch knows where we left off
			session.StartOffset = currentOffset
			return records, nil
		}
	}

	glog.V(2).Infof("[FETCH] ReadRecords returning %d records (maxRecords reached)", len(records))
	// Update session offset after successful read
	session.StartOffset = currentOffset

	// CRITICAL: Cache the consumed records to avoid broker tight loop
	// Append new records to cache (keep last 100 records max)
	session.consumedRecords = append(session.consumedRecords, records...)
	if len(session.consumedRecords) > 100 {
		// Keep only the most recent 100 records
		session.consumedRecords = session.consumedRecords[len(session.consumedRecords)-100:]
	}
	glog.V(2).Infof("[FETCH] Updated cache: now contains %d records", len(session.consumedRecords))

	return records, nil
}

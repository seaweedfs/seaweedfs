package integration

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// createSubscribeInitMessage creates a subscribe init message with the given parameters
func createSubscribeInitMessage(topic string, actualPartition *schema_pb.Partition, startOffset int64, offsetType schema_pb.OffsetType, consumerGroup string, consumerID string) *mq_pb.SubscribeMessageRequest {
	return &mq_pb.SubscribeMessageRequest{
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
					StartTsNs:   0,
					StartOffset: startOffset,
				},
				OffsetType:        offsetType,
				SlidingWindowSize: 10,
			},
		},
	}
}

// CreateFreshSubscriber creates a new subscriber session without caching
// This ensures each fetch gets fresh data from the requested offset
// consumerGroup and consumerID are passed from Kafka client for proper tracking in SMQ
func (bc *BrokerClient) CreateFreshSubscriber(topic string, partition int32, startOffset int64, consumerGroup string, consumerID string) (*BrokerSubscriberSession, error) {
	// Use BrokerClient's context so subscriber is cancelled when connection closes
	subscriberCtx, subscriberCancel := context.WithCancel(bc.ctx)

	stream, err := bc.client.SubscribeMessage(subscriberCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscribe stream: %v", err)
	}

	// Get the actual partition assignment from the broker
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual partition assignment for subscribe: %v", err)
	}

	// Use EXACT_OFFSET to read from the specific offset
	offsetType := schema_pb.OffsetType_EXACT_OFFSET

	// Send init message to start subscription with Kafka client's consumer group and ID
	initReq := createSubscribeInitMessage(topic, actualPartition, startOffset, offsetType, consumerGroup, consumerID)

	glog.V(2).Infof("[SUBSCRIBE-INIT] CreateFreshSubscriber sending init: topic=%s partition=%d startOffset=%d offsetType=%v consumerGroup=%s consumerID=%s",
		topic, partition, startOffset, offsetType, consumerGroup, consumerID)

	if err := stream.Send(initReq); err != nil {
		return nil, fmt.Errorf("failed to send subscribe init: %v", err)
	}

	// IMPORTANT: Don't wait for init response here!
	// The broker may send the first data record as the "init response"
	// If we call Recv() here, we'll consume that first record and ReadRecords will block
	// waiting for the second record, causing a 30-second timeout.
	// Instead, let ReadRecords handle all Recv() calls.

	session := &BrokerSubscriberSession{
		Stream:        stream,
		Topic:         topic,
		Partition:     partition,
		StartOffset:   startOffset,
		ConsumerGroup: consumerGroup,
		ConsumerID:    consumerID,
		Ctx:           subscriberCtx,
		Cancel:        subscriberCancel,
	}

	return session, nil
}

// GetOrCreateSubscriber gets or creates a subscriber for offset tracking
func (bc *BrokerClient) GetOrCreateSubscriber(topic string, partition int32, startOffset int64, consumerGroup string, consumerID string) (*BrokerSubscriberSession, error) {
	// Create a temporary session to generate the key
	tempSession := &BrokerSubscriberSession{
		Topic:         topic,
		Partition:     partition,
		ConsumerGroup: consumerGroup,
		ConsumerID:    consumerID,
	}
	key := tempSession.Key()

	bc.subscribersLock.RLock()
	if session, exists := bc.subscribers[key]; exists {
		// Check if we can reuse the existing session
		session.mu.Lock()
		currentOffset := session.StartOffset

		// Check cache to see what offsets are available
		canUseCache := false
		if len(session.consumedRecords) > 0 {
			cacheStartOffset := session.consumedRecords[0].Offset
			cacheEndOffset := session.consumedRecords[len(session.consumedRecords)-1].Offset
			if startOffset >= cacheStartOffset && startOffset <= cacheEndOffset {
				canUseCache = true
			}
		}
		session.mu.Unlock()

		// With seekable broker: Always reuse existing session
		// Any offset mismatch will be handled by FetchRecords via SeekMessage
		// This includes:
		// 1. Forward read: Natural continuation
		// 2. Backward read with cache hit: Serve from cache
		// 3. Backward read without cache: Send seek message to broker
		// No need for stream recreation - broker repositions internally

		bc.subscribersLock.RUnlock()
		if canUseCache {
			glog.V(2).Infof("[FETCH] Reusing session for %s: session at %d, requested %d (cached)",
				key, currentOffset, startOffset)
		} else if startOffset >= currentOffset {
			glog.V(2).Infof("[FETCH] Reusing session for %s: session at %d, requested %d (forward read)",
				key, currentOffset, startOffset)
		} else {
			glog.V(2).Infof("[FETCH] Reusing session for %s: session at %d, requested %d (will seek backward)",
				key, currentOffset, startOffset)
		}
		return session, nil
	}

	// Session doesn't exist - need to create one
	bc.subscribersLock.RUnlock()

	// Create new subscriber stream
	// Need to acquire write lock since we don't have it from the paths above
	bc.subscribersLock.Lock()
	defer bc.subscribersLock.Unlock()

	// Double-check if session was created by another thread while we were acquiring the lock
	if session, exists := bc.subscribers[key]; exists {
		// With seekable broker, always reuse existing session
		// FetchRecords will handle any offset mismatch via seek
		session.mu.Lock()
		existingOffset := session.StartOffset
		session.mu.Unlock()

		glog.V(1).Infof("[FETCH] Session created concurrently at offset %d (requested %d), reusing", existingOffset, startOffset)
		return session, nil
	}

	// Use BrokerClient's context so subscribers are automatically cancelled when connection closes
	// This ensures proper cleanup without artificial timeouts
	subscriberCtx, subscriberCancel := context.WithCancel(bc.ctx)

	stream, err := bc.client.SubscribeMessage(subscriberCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscribe stream: %v", err)
	}

	// Get the actual partition assignment from the broker instead of using Kafka partition mapping
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual partition assignment for subscribe: %v", err)
	}

	// Convert Kafka offset to appropriate SeaweedMQ OffsetType
	var offsetType schema_pb.OffsetType
	var offsetValue int64

	if startOffset == -1 {
		// Kafka offset -1 typically means "latest"
		offsetType = schema_pb.OffsetType_RESET_TO_LATEST
		offsetValue = 0 // Not used with RESET_TO_LATEST
		glog.V(2).Infof("Using RESET_TO_LATEST for Kafka offset -1 (read latest)")
	} else {
		// CRITICAL FIX: Use EXACT_OFFSET to position subscriber at the exact Kafka offset
		// This allows the subscriber to read from both buffer and disk at the correct position
		offsetType = schema_pb.OffsetType_EXACT_OFFSET
		offsetValue = startOffset // Use the exact Kafka offset
		glog.V(2).Infof("Using EXACT_OFFSET for Kafka offset %d (direct positioning)", startOffset)
	}

	glog.V(2).Infof("Creating subscriber for topic=%s partition=%d: Kafka offset %d -> SeaweedMQ %s",
		topic, partition, startOffset, offsetType)

	glog.V(2).Infof("[SUBSCRIBE-INIT] GetOrCreateSubscriber sending init: topic=%s partition=%d startOffset=%d offsetType=%v consumerGroup=%s consumerID=%s",
		topic, partition, offsetValue, offsetType, consumerGroup, consumerID)

	// Send init message using the actual partition structure that the broker allocated
	initReq := createSubscribeInitMessage(topic, actualPartition, offsetValue, offsetType, consumerGroup, consumerID)
	if err := stream.Send(initReq); err != nil {
		return nil, fmt.Errorf("failed to send subscribe init: %v", err)
	}

	session := &BrokerSubscriberSession{
		Topic:         topic,
		Partition:     partition,
		Stream:        stream,
		StartOffset:   startOffset,
		ConsumerGroup: consumerGroup,
		ConsumerID:    consumerID,
		Ctx:           subscriberCtx,
		Cancel:        subscriberCancel,
	}

	bc.subscribers[key] = session
	glog.V(2).Infof("Created subscriber session for %s with context cancellation support", key)
	return session, nil
}

// ReadRecordsFromOffset reads records starting from a specific offset
// If the offset is in cache, returns cached records; otherwise delegates to ReadRecords
// ctx controls the fetch timeout (should match Kafka fetch request's MaxWaitTime)
func (bc *BrokerClient) ReadRecordsFromOffset(ctx context.Context, session *BrokerSubscriberSession, requestedOffset int64, maxRecords int) ([]*SeaweedRecord, error) {
	if session == nil {
		return nil, fmt.Errorf("subscriber session cannot be nil")
	}

	session.mu.Lock()

	glog.V(2).Infof("[FETCH] ReadRecordsFromOffset: topic=%s partition=%d requestedOffset=%d sessionOffset=%d maxRecords=%d",
		session.Topic, session.Partition, requestedOffset, session.StartOffset, maxRecords)

	// Check cache first
	if len(session.consumedRecords) > 0 {
		cacheStartOffset := session.consumedRecords[0].Offset
		cacheEndOffset := session.consumedRecords[len(session.consumedRecords)-1].Offset

		if requestedOffset >= cacheStartOffset && requestedOffset <= cacheEndOffset {
			// Found in cache
			startIdx := int(requestedOffset - cacheStartOffset)
			// CRITICAL: Bounds check to prevent race condition where cache is modified between checks
			if startIdx < 0 || startIdx >= len(session.consumedRecords) {
				glog.V(2).Infof("[FETCH] Cache index out of bounds (race condition): startIdx=%d, cache size=%d, falling through to normal read",
					startIdx, len(session.consumedRecords))
				// Cache was modified, fall through to normal read path
			} else {
				endIdx := startIdx + maxRecords
				if endIdx > len(session.consumedRecords) {
					endIdx = len(session.consumedRecords)
				}
				glog.V(2).Infof("[FETCH] Returning %d cached records for %s at offset %d (cache: %d-%d)",
					endIdx-startIdx, session.Key(), requestedOffset, cacheStartOffset, cacheEndOffset)
				// CRITICAL: Capture slice while holding lock to prevent race condition
				// If we unlock before slicing, another goroutine could clear consumedRecords
				result := session.consumedRecords[startIdx:endIdx]
				session.mu.Unlock()
				return result, nil
			}
		} else {
			glog.V(2).Infof("[FETCH] Cache miss for %s: requested=%d, cache=[%d-%d]",
				session.Key(), requestedOffset, cacheStartOffset, cacheEndOffset)
		}
	}

	// Get the current offset atomically for comparison
	currentStartOffset := session.StartOffset
	session.mu.Unlock()

	// With seekable broker: Keep subscriber alive across all requests
	// Schema Registry and other clients expect persistent consumer connections
	//
	// Three scenarios, all handled via seek:
	// 1. requestedOffset < session.StartOffset: Send seek message (backward)
	// 2. requestedOffset == session.StartOffset: Continue reading (no seek needed)
	// 3. requestedOffset > session.StartOffset: Send seek message (forward)
	//
	// The stream persists for the entire consumer session - no recreation needed
	if requestedOffset != currentStartOffset {
		offsetDiff := requestedOffset - currentStartOffset
		seekDirection := "forward"
		if offsetDiff < 0 {
			seekDirection = "backward"
		}

		glog.V(2).Infof("[FETCH] Offset mismatch: %s seek from %d to %d (diff=%d)",
			seekDirection, currentStartOffset, requestedOffset, offsetDiff)

		// Send seek message to reposition stream
		seekMsg := &mq_pb.SubscribeMessageRequest{
			Message: &mq_pb.SubscribeMessageRequest_Seek{
				Seek: &mq_pb.SubscribeMessageRequest_SeekMessage{
					Offset:     requestedOffset,
					OffsetType: schema_pb.OffsetType_EXACT_OFFSET,
				},
			},
		}

		if err := session.Stream.Send(seekMsg); err != nil {
			// Handle graceful shutdown: EOF means stream is closing
			if err == io.EOF {
				glog.V(2).Infof("[FETCH] Stream closing during seek to offset %d, returning empty", requestedOffset)
				return []*SeaweedRecord{}, nil
			}
			return nil, fmt.Errorf("seek to offset %d failed: %v", requestedOffset, err)
		}

		// Update session state after successful seek
		session.mu.Lock()
		session.StartOffset = requestedOffset

		// CRITICAL: Only clear cache if seeking forward past cached data
		// For backward seeks, keep cache to avoid re-reading same data from broker
		shouldClearCache := true
		if len(session.consumedRecords) > 0 {
			cacheStartOffset := session.consumedRecords[0].Offset
			cacheEndOffset := session.consumedRecords[len(session.consumedRecords)-1].Offset
			// Keep cache if seeking to an offset within or before cached range
			if requestedOffset <= cacheEndOffset {
				shouldClearCache = false
				glog.V(2).Infof("[FETCH] Keeping cache after seek to %d (cache: [%d-%d])",
					requestedOffset, cacheStartOffset, cacheEndOffset)
			}
		}
		if shouldClearCache {
			session.consumedRecords = nil
			glog.V(2).Infof("[FETCH] Cleared cache after forward seek to %d", requestedOffset)
		}
		session.mu.Unlock()

		glog.V(2).Infof("[FETCH] Seek to offset %d successful", requestedOffset)
	} else {
		glog.V(2).Infof("[FETCH] Offset match: continuing from offset %d", requestedOffset)
	}

	// Read records from current position
	return bc.ReadRecords(ctx, session, maxRecords)
}

// ReadRecords reads available records from the subscriber stream
// Uses a timeout-based approach to read multiple records without blocking indefinitely
// ctx controls the fetch timeout (should match Kafka fetch request's MaxWaitTime)
func (bc *BrokerClient) ReadRecords(ctx context.Context, session *BrokerSubscriberSession, maxRecords int) ([]*SeaweedRecord, error) {
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
			// CRITICAL: Capture slice result while holding lock (defer will unlock after return)
			result := session.consumedRecords[startIdx:endIdx]
			return result, nil
		}
	}

	// Read first record with timeout (important for empty topics)
	// CRITICAL: For SMQ backend with consumer groups, we need adequate timeout for disk reads
	// When a consumer group resumes from a committed offset, the subscriber may need to:
	// 1. Connect to the broker (network latency)
	// 2. Seek to the correct offset in the log file (disk I/O)
	// 3. Read and deserialize the record (disk I/O)
	// Total latency can be 100-500ms for cold reads from disk
	//
	// CRITICAL: Use the context from the Kafka fetch request
	// The context timeout is set by the caller based on the Kafka fetch request's MaxWaitTime
	// This ensures we wait exactly as long as the client requested, not more or less
	// For in-memory reads (hot path), records arrive in <10ms
	// For low-volume topics (like _schemas), the caller sets longer timeout to keep subscriber alive
	// If no context provided, use a reasonable default timeout
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
	}

	// CRITICAL: Capture stream pointer while holding lock to prevent TOCTOU race
	// If we access session.Stream in the goroutine, it could become nil between check and use
	stream := session.Stream
	if stream == nil {
		glog.V(2).Infof("[FETCH] Stream is nil, cannot read")
		return records, nil
	}

	type recvResult struct {
		resp *mq_pb.SubscribeMessageResponse
		err  error
	}
	recvChan := make(chan recvResult, 1)

	// Try to receive first record using captured stream pointer
	go func() {
		// Recover from panics caused by stream being closed during Recv()
		defer func() {
			if r := recover(); r != nil {
				select {
				case recvChan <- recvResult{resp: nil, err: fmt.Errorf("stream recv panicked: %v", r)}:
				case <-ctx.Done():
				}
			}
		}()
		resp, err := stream.Recv()
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
			glog.V(2).Infof("[FETCH] Received record: offset=%d, keyLen=%d, valueLen=%d",
				record.Offset, len(record.Key), len(record.Value))

			// CRITICAL: Auto-acknowledge first message immediately for Kafka gateway
			// Kafka uses offset commits (not per-message acks) so we must ack to prevent
			// broker from blocking on in-flight messages waiting for acks that will never come
			ackMsg := &mq_pb.SubscribeMessageRequest{
				Message: &mq_pb.SubscribeMessageRequest_Ack{
					Ack: &mq_pb.SubscribeMessageRequest_AckMessage{
						Key:  dataMsg.Key,
						TsNs: dataMsg.TsNs,
					},
				},
			}
			if err := stream.Send(ackMsg); err != nil {
				glog.V(2).Infof("[FETCH] Failed to send ack for first record offset %d: %v (continuing)", record.Offset, err)
				// Don't fail the fetch if ack fails - continue reading
			}
		}

	case <-ctx.Done():
		// Timeout on first record - topic is empty or no data available
		glog.V(2).Infof("[FETCH] No data available (timeout on first record)")
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
		// CRITICAL: Use parent context (ctx) to respect client's MaxWaitTime deadline
		// The per-record timeout is combined with the overall fetch deadline
		ctx2, cancel2 := context.WithTimeout(ctx, currentTimeout)
		recvChan2 := make(chan recvResult, 1)

		go func() {
			// Recover from panics caused by stream being closed during Recv()
			defer func() {
				if r := recover(); r != nil {
					select {
					case recvChan2 <- recvResult{resp: nil, err: fmt.Errorf("stream recv panicked: %v", r)}:
					case <-ctx2.Done():
					}
				}
			}()
			// Use captured stream pointer to prevent TOCTOU race
			resp, err := stream.Recv()
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
				glog.V(2).Infof("[FETCH] Updating %s offset: %d -> %d (error case, read %d records)",
					session.Key(), session.StartOffset, currentOffset, len(records))
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

				glog.V(2).Infof("[FETCH] Received record %d: offset=%d, keyLen=%d, valueLen=%d, readTime=%v",
					len(records), record.Offset, len(record.Key), len(record.Value), readDuration)

				// CRITICAL: Auto-acknowledge message immediately for Kafka gateway
				// Kafka uses offset commits (not per-message acks) so we must ack to prevent
				// broker from blocking on in-flight messages waiting for acks that will never come
				ackMsg := &mq_pb.SubscribeMessageRequest{
					Message: &mq_pb.SubscribeMessageRequest_Ack{
						Ack: &mq_pb.SubscribeMessageRequest_AckMessage{
							Key:  dataMsg.Key,
							TsNs: dataMsg.TsNs,
						},
					},
				}
				if err := stream.Send(ackMsg); err != nil {
					glog.V(2).Infof("[FETCH] Failed to send ack for offset %d: %v (continuing)", record.Offset, err)
					// Don't fail the fetch if ack fails - continue reading
				}
			}

		case <-ctx2.Done():
			cancel2()
			// Timeout - return what we have
			glog.V(2).Infof("[FETCH] Read timeout after %d records (waited %v), returning batch", len(records), time.Since(readStart))
			// CRITICAL: Update session offset so next fetch knows where we left off
			glog.V(2).Infof("[FETCH] Updating %s offset: %d -> %d (timeout case, read %d records)",
				session.Key(), session.StartOffset, currentOffset, len(records))
			session.StartOffset = currentOffset
			return records, nil
		}
	}

	glog.V(2).Infof("[FETCH] ReadRecords returning %d records (maxRecords reached)", len(records))
	// Update session offset after successful read
	glog.V(2).Infof("[FETCH] Updating %s offset: %d -> %d (success case, read %d records)",
		session.Key(), session.StartOffset, currentOffset, len(records))
	session.StartOffset = currentOffset

	// CRITICAL: Cache the consumed records to avoid broker tight loop
	// Append new records to cache (keep last 1000 records max for better hit rate)
	session.consumedRecords = append(session.consumedRecords, records...)
	if len(session.consumedRecords) > 1000 {
		// Keep only the most recent 1000 records
		session.consumedRecords = session.consumedRecords[len(session.consumedRecords)-1000:]
	}
	glog.V(2).Infof("[FETCH] Updated cache: now contains %d records", len(session.consumedRecords))

	return records, nil
}

// CloseSubscriber closes and removes a subscriber session
func (bc *BrokerClient) CloseSubscriber(topic string, partition int32, consumerGroup string, consumerID string) {
	tempSession := &BrokerSubscriberSession{
		Topic:         topic,
		Partition:     partition,
		ConsumerGroup: consumerGroup,
		ConsumerID:    consumerID,
	}
	key := tempSession.Key()

	bc.subscribersLock.Lock()
	defer bc.subscribersLock.Unlock()

	if session, exists := bc.subscribers[key]; exists {
		// CRITICAL: Hold session lock while cancelling to prevent race with active Recv() calls
		session.mu.Lock()
		if session.Stream != nil {
			_ = session.Stream.CloseSend()
		}
		if session.Cancel != nil {
			session.Cancel()
		}
		session.mu.Unlock()
		delete(bc.subscribers, key)
		glog.V(2).Infof("[FETCH] Closed subscriber for %s", key)
	}
}

// NeedsRestart checks if the subscriber needs to restart to read from the given offset
// Returns true if:
// 1. Requested offset is before current position AND not in cache
// 2. Stream is closed/invalid
func (bc *BrokerClient) NeedsRestart(session *BrokerSubscriberSession, requestedOffset int64) bool {
	session.mu.Lock()
	defer session.mu.Unlock()

	// Check if stream is still valid
	if session.Stream == nil || session.Ctx == nil {
		return true
	}

	// Check if we can serve from cache
	if len(session.consumedRecords) > 0 {
		cacheStart := session.consumedRecords[0].Offset
		cacheEnd := session.consumedRecords[len(session.consumedRecords)-1].Offset
		if requestedOffset >= cacheStart && requestedOffset <= cacheEnd {
			// Can serve from cache, no restart needed
			return false
		}
	}

	// If requested offset is far behind current position, need restart
	if requestedOffset < session.StartOffset {
		return true
	}

	// Check if we're too far ahead (gap in cache)
	if requestedOffset > session.StartOffset+1000 {
		// Large gap - might be more efficient to restart
		return true
	}

	return false
}

// RestartSubscriber restarts an existing subscriber from a new offset
// This is more efficient than closing and recreating the session
func (bc *BrokerClient) RestartSubscriber(session *BrokerSubscriberSession, newOffset int64, consumerGroup string, consumerID string) error {
	session.mu.Lock()
	defer session.mu.Unlock()

	glog.V(2).Infof("[FETCH] Restarting subscriber for %s[%d]: from offset %d to %d",
		session.Topic, session.Partition, session.StartOffset, newOffset)

	// Close existing stream
	if session.Stream != nil {
		_ = session.Stream.CloseSend()
	}
	if session.Cancel != nil {
		session.Cancel()
	}

	// Clear cache since we're seeking to a different position
	session.consumedRecords = nil
	session.nextOffsetToRead = newOffset

	// Create new stream from new offset
	subscriberCtx, cancel := context.WithCancel(bc.ctx)

	stream, err := bc.client.SubscribeMessage(subscriberCtx)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to create subscribe stream for restart: %v", err)
	}

	// Get the actual partition assignment
	actualPartition, err := bc.getActualPartitionAssignment(session.Topic, session.Partition)
	if err != nil {
		cancel()
		_ = stream.CloseSend()
		return fmt.Errorf("failed to get actual partition assignment for restart: %v", err)
	}

	// Send init message with new offset
	initReq := createSubscribeInitMessage(session.Topic, actualPartition, newOffset, schema_pb.OffsetType_EXACT_OFFSET, consumerGroup, consumerID)

	if err := stream.Send(initReq); err != nil {
		cancel()
		_ = stream.CloseSend()
		return fmt.Errorf("failed to send subscribe init for restart: %v", err)
	}

	// Update session with new stream and offset
	session.Stream = stream
	session.Cancel = cancel
	session.Ctx = subscriberCtx
	session.StartOffset = newOffset

	glog.V(2).Infof("[FETCH] Successfully restarted subscriber for %s[%d] at offset %d",
		session.Topic, session.Partition, newOffset)

	return nil
}

// Seek helper methods for BrokerSubscriberSession

// SeekToOffset repositions the stream to read from a specific offset
func (session *BrokerSubscriberSession) SeekToOffset(offset int64) error {
	// Skip seek if already at the requested offset
	session.mu.Lock()
	currentOffset := session.StartOffset
	session.mu.Unlock()

	if currentOffset == offset {
		glog.V(2).Infof("[SEEK] Already at offset %d for %s[%d], skipping seek", offset, session.Topic, session.Partition)
		return nil
	}

	seekMsg := &mq_pb.SubscribeMessageRequest{
		Message: &mq_pb.SubscribeMessageRequest_Seek{
			Seek: &mq_pb.SubscribeMessageRequest_SeekMessage{
				Offset:     offset,
				OffsetType: schema_pb.OffsetType_EXACT_OFFSET,
			},
		},
	}

	if err := session.Stream.Send(seekMsg); err != nil {
		// Handle graceful shutdown
		if err == io.EOF {
			glog.V(2).Infof("[SEEK] Stream closing during seek to offset %d for %s[%d]", offset, session.Topic, session.Partition)
			return nil // Not an error during shutdown
		}
		return fmt.Errorf("seek to offset %d failed: %v", offset, err)
	}

	session.mu.Lock()
	session.StartOffset = offset
	// Only clear cache if seeking forward past cached data
	shouldClearCache := true
	if len(session.consumedRecords) > 0 {
		cacheEndOffset := session.consumedRecords[len(session.consumedRecords)-1].Offset
		if offset <= cacheEndOffset {
			shouldClearCache = false
		}
	}
	if shouldClearCache {
		session.consumedRecords = nil
	}
	session.mu.Unlock()

	glog.V(2).Infof("[SEEK] Seeked to offset %d for %s[%d]", offset, session.Topic, session.Partition)
	return nil
}

// SeekToTimestamp repositions the stream to read from messages at or after a specific timestamp
// timestamp is in nanoseconds since Unix epoch
// Note: We don't skip this operation even if we think we're at the right position because
// we can't easily determine the offset corresponding to a timestamp without querying the broker
func (session *BrokerSubscriberSession) SeekToTimestamp(timestampNs int64) error {
	seekMsg := &mq_pb.SubscribeMessageRequest{
		Message: &mq_pb.SubscribeMessageRequest_Seek{
			Seek: &mq_pb.SubscribeMessageRequest_SeekMessage{
				Offset:     timestampNs,
				OffsetType: schema_pb.OffsetType_EXACT_TS_NS,
			},
		},
	}

	if err := session.Stream.Send(seekMsg); err != nil {
		// Handle graceful shutdown
		if err == io.EOF {
			glog.V(2).Infof("[SEEK] Stream closing during seek to timestamp %d for %s[%d]", timestampNs, session.Topic, session.Partition)
			return nil // Not an error during shutdown
		}
		return fmt.Errorf("seek to timestamp %d failed: %v", timestampNs, err)
	}

	session.mu.Lock()
	// Note: We don't know the exact offset at this timestamp yet
	// It will be updated when we read the first message
	session.consumedRecords = nil
	session.mu.Unlock()

	glog.V(2).Infof("[SEEK] Seeked to timestamp %d for %s[%d]", timestampNs, session.Topic, session.Partition)
	return nil
}

// SeekToEarliest repositions the stream to the beginning of the partition
// Note: We don't skip this operation even if StartOffset == 0 because the broker
// may have a different notion of "earliest" (e.g., after compaction or retention)
func (session *BrokerSubscriberSession) SeekToEarliest() error {
	seekMsg := &mq_pb.SubscribeMessageRequest{
		Message: &mq_pb.SubscribeMessageRequest_Seek{
			Seek: &mq_pb.SubscribeMessageRequest_SeekMessage{
				Offset:     0,
				OffsetType: schema_pb.OffsetType_RESET_TO_EARLIEST,
			},
		},
	}

	if err := session.Stream.Send(seekMsg); err != nil {
		// Handle graceful shutdown
		if err == io.EOF {
			glog.V(2).Infof("[SEEK] Stream closing during seek to earliest for %s[%d]", session.Topic, session.Partition)
			return nil // Not an error during shutdown
		}
		return fmt.Errorf("seek to earliest failed: %v", err)
	}

	session.mu.Lock()
	session.StartOffset = 0
	session.consumedRecords = nil
	session.mu.Unlock()

	glog.V(2).Infof("[SEEK] Seeked to earliest for %s[%d]", session.Topic, session.Partition)
	return nil
}

// SeekToLatest repositions the stream to the end of the partition (next new message)
// Note: We don't skip this operation because "latest" is a moving target and we can't
// reliably determine if we're already at the latest position without querying the broker
func (session *BrokerSubscriberSession) SeekToLatest() error {
	seekMsg := &mq_pb.SubscribeMessageRequest{
		Message: &mq_pb.SubscribeMessageRequest_Seek{
			Seek: &mq_pb.SubscribeMessageRequest_SeekMessage{
				Offset:     0,
				OffsetType: schema_pb.OffsetType_RESET_TO_LATEST,
			},
		},
	}

	if err := session.Stream.Send(seekMsg); err != nil {
		// Handle graceful shutdown
		if err == io.EOF {
			glog.V(2).Infof("[SEEK] Stream closing during seek to latest for %s[%d]", session.Topic, session.Partition)
			return nil // Not an error during shutdown
		}
		return fmt.Errorf("seek to latest failed: %v", err)
	}

	session.mu.Lock()
	// Offset will be set when we read the first new message
	session.consumedRecords = nil
	session.mu.Unlock()

	glog.V(2).Infof("[SEEK] Seeked to latest for %s[%d]", session.Topic, session.Partition)
	return nil
}

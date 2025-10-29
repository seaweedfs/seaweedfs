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

	glog.V(4).Infof("[SUBSCRIBE-INIT] CreateFreshSubscriber sending init: topic=%s partition=%d startOffset=%d offsetType=%v consumerGroup=%s consumerID=%s",
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
			glog.V(4).Infof("[FETCH] Reusing session for %s: session at %d, requested %d (cached)",
				key, currentOffset, startOffset)
		} else if startOffset >= currentOffset {
			glog.V(4).Infof("[FETCH] Reusing session for %s: session at %d, requested %d (forward read)",
				key, currentOffset, startOffset)
		} else {
			glog.V(4).Infof("[FETCH] Reusing session for %s: session at %d, requested %d (will seek backward)",
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

		glog.V(3).Infof("[FETCH] Session created concurrently at offset %d (requested %d), reusing", existingOffset, startOffset)
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

	glog.V(4).Infof("[SUBSCRIBE-INIT] GetOrCreateSubscriber sending init: topic=%s partition=%d startOffset=%d offsetType=%v consumerGroup=%s consumerID=%s",
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

// createTemporarySubscriber creates a fresh subscriber for a single fetch operation
// This is used by the stateless fetch approach to eliminate concurrent access issues
// The subscriber is NOT stored in bc.subscribers and must be cleaned up by the caller
func (bc *BrokerClient) createTemporarySubscriber(topic string, partition int32, startOffset int64, consumerGroup string, consumerID string) (*BrokerSubscriberSession, error) {
	glog.V(4).Infof("[STATELESS] Creating temporary subscriber for %s-%d at offset %d", topic, partition, startOffset)

	// Create context for this temporary subscriber
	ctx, cancel := context.WithCancel(bc.ctx)

	// Create gRPC stream
	stream, err := bc.client.SubscribeMessage(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create subscribe stream: %v", err)
	}

	// Get the actual partition assignment from the broker
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to get actual partition assignment: %v", err)
	}

	// Convert Kafka offset to appropriate SeaweedMQ OffsetType
	var offsetType schema_pb.OffsetType
	var offsetValue int64

	if startOffset == -1 {
		offsetType = schema_pb.OffsetType_RESET_TO_LATEST
		offsetValue = 0
		glog.V(4).Infof("[STATELESS] Using RESET_TO_LATEST for Kafka offset -1")
	} else {
		offsetType = schema_pb.OffsetType_EXACT_OFFSET
		offsetValue = startOffset
		glog.V(4).Infof("[STATELESS] Using EXACT_OFFSET for Kafka offset %d", startOffset)
	}

	// Send init message
	initReq := createSubscribeInitMessage(topic, actualPartition, offsetValue, offsetType, consumerGroup, consumerID)
	if err := stream.Send(initReq); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to send subscribe init: %v", err)
	}

	// Create temporary session (not stored in bc.subscribers)
	session := &BrokerSubscriberSession{
		Topic:         topic,
		Partition:     partition,
		Stream:        stream,
		StartOffset:   startOffset,
		ConsumerGroup: consumerGroup,
		ConsumerID:    consumerID,
		Ctx:           ctx,
		Cancel:        cancel,
	}

	glog.V(4).Infof("[STATELESS] Created temporary subscriber for %s-%d starting at offset %d", topic, partition, startOffset)
	return session, nil
}

// createSubscriberSession creates a new subscriber session with proper initialization
// This is used by the hybrid approach for initial connections and backward seeks
func (bc *BrokerClient) createSubscriberSession(topic string, partition int32, startOffset int64, consumerGroup string, consumerID string) (*BrokerSubscriberSession, error) {
	glog.V(4).Infof("[HYBRID-SESSION] Creating subscriber session for %s-%d at offset %d", topic, partition, startOffset)

	// Create context for this subscriber
	ctx, cancel := context.WithCancel(bc.ctx)

	// Create gRPC stream
	stream, err := bc.client.SubscribeMessage(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create subscribe stream: %v", err)
	}

	// Get the actual partition assignment from the broker
	actualPartition, err := bc.getActualPartitionAssignment(topic, partition)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to get actual partition assignment: %v", err)
	}

	// Convert Kafka offset to appropriate SeaweedMQ OffsetType
	var offsetType schema_pb.OffsetType
	var offsetValue int64

	if startOffset == -1 {
		offsetType = schema_pb.OffsetType_RESET_TO_LATEST
		offsetValue = 0
		glog.V(4).Infof("[HYBRID-SESSION] Using RESET_TO_LATEST for Kafka offset -1")
	} else {
		offsetType = schema_pb.OffsetType_EXACT_OFFSET
		offsetValue = startOffset
		glog.V(4).Infof("[HYBRID-SESSION] Using EXACT_OFFSET for Kafka offset %d", startOffset)
	}

	// Send init message
	initReq := createSubscribeInitMessage(topic, actualPartition, offsetValue, offsetType, consumerGroup, consumerID)
	if err := stream.Send(initReq); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to send subscribe init: %v", err)
	}

	// Create session with proper initialization
	session := &BrokerSubscriberSession{
		Topic:            topic,
		Partition:        partition,
		Stream:           stream,
		StartOffset:      startOffset,
		ConsumerGroup:    consumerGroup,
		ConsumerID:       consumerID,
		Ctx:              ctx,
		Cancel:           cancel,
		consumedRecords:  nil,
		nextOffsetToRead: startOffset,
		lastReadOffset:   startOffset - 1, // Will be updated after first read
		initialized:      false,
	}

	glog.V(4).Infof("[HYBRID-SESSION] Created subscriber session for %s-%d starting at offset %d", topic, partition, startOffset)
	return session, nil
}

// serveFromCache serves records from the session's cache
func (bc *BrokerClient) serveFromCache(session *BrokerSubscriberSession, requestedOffset int64, maxRecords int) []*SeaweedRecord {
	// Find the start index in cache
	startIdx := -1
	for i, record := range session.consumedRecords {
		if record.Offset == requestedOffset {
			startIdx = i
			break
		}
	}

	if startIdx == -1 {
		// Offset not found in cache (shouldn't happen if caller checked properly)
		return nil
	}

	// Calculate end index
	endIdx := startIdx + maxRecords
	if endIdx > len(session.consumedRecords) {
		endIdx = len(session.consumedRecords)
	}

	// Return slice from cache
	result := session.consumedRecords[startIdx:endIdx]
	glog.V(4).Infof("[HYBRID-CACHE] Served %d records from cache (requested %d, offset %d)",
		len(result), maxRecords, requestedOffset)
	return result
}

// readRecordsFromSession reads records from the session's stream
func (bc *BrokerClient) readRecordsFromSession(ctx context.Context, session *BrokerSubscriberSession, startOffset int64, maxRecords int) ([]*SeaweedRecord, error) {
	glog.V(4).Infof("[HYBRID-READ] Reading from stream: offset=%d maxRecords=%d", startOffset, maxRecords)

	records := make([]*SeaweedRecord, 0, maxRecords)
	currentOffset := startOffset

	// Read until we have enough records or timeout
	for len(records) < maxRecords {
		// Check context timeout
		select {
		case <-ctx.Done():
			// Timeout or cancellation - return what we have
			glog.V(4).Infof("[HYBRID-READ] Context done, returning %d records", len(records))
			return records, nil
		default:
		}

		// Read from stream with timeout
		resp, err := session.Stream.Recv()
		if err != nil {
			if err == io.EOF {
				glog.V(4).Infof("[HYBRID-READ] Stream closed (EOF), returning %d records", len(records))
				return records, nil
			}
			return nil, fmt.Errorf("failed to receive from stream: %v", err)
		}

		// Handle data message
		if dataMsg := resp.GetData(); dataMsg != nil {
			record := &SeaweedRecord{
				Key:       dataMsg.Key,
				Value:     dataMsg.Value,
				Timestamp: dataMsg.TsNs,
				Offset:    currentOffset,
			}
			records = append(records, record)
			currentOffset++

			// Auto-acknowledge to prevent throttling
			ackReq := &mq_pb.SubscribeMessageRequest{
				Message: &mq_pb.SubscribeMessageRequest_Ack{
					Ack: &mq_pb.SubscribeMessageRequest_AckMessage{
						Key:  dataMsg.Key,
						TsNs: dataMsg.TsNs,
					},
				},
			}
			if err := session.Stream.Send(ackReq); err != nil {
				if err != io.EOF {
					glog.Warningf("[HYBRID-READ] Failed to send ack (non-critical): %v", err)
				}
			}
		}

		// Handle control messages
		if ctrlMsg := resp.GetCtrl(); ctrlMsg != nil {
			if ctrlMsg.Error != "" {
				// Error message from broker
				return nil, fmt.Errorf("broker error: %s", ctrlMsg.Error)
			}
			if ctrlMsg.IsEndOfStream {
				glog.V(4).Infof("[HYBRID-READ] End of stream, returning %d records", len(records))
				return records, nil
			}
			if ctrlMsg.IsEndOfTopic {
				glog.V(4).Infof("[HYBRID-READ] End of topic, returning %d records", len(records))
				return records, nil
			}
			// Empty control message (e.g., seek ack) - continue reading
			glog.V(4).Infof("[HYBRID-READ] Received control message (seek ack?), continuing")
			continue
		}
	}

	glog.V(4).Infof("[HYBRID-READ] Read %d records successfully", len(records))

	// Update cache
	session.consumedRecords = append(session.consumedRecords, records...)
	// Limit cache size to prevent unbounded growth
	const maxCacheSize = 10000
	if len(session.consumedRecords) > maxCacheSize {
		// Keep only the most recent records
		session.consumedRecords = session.consumedRecords[len(session.consumedRecords)-maxCacheSize:]
	}

	return records, nil
}

// FetchRecordsHybrid uses a hybrid approach: session reuse + proper offset tracking
// - Fast path (95%): Reuse session for sequential reads
// - Slow path (5%): Create new subscriber for backward seeks
// This combines performance (connection reuse) with correctness (proper tracking)
func (bc *BrokerClient) FetchRecordsHybrid(ctx context.Context, topic string, partition int32, requestedOffset int64, maxRecords int, consumerGroup string, consumerID string) ([]*SeaweedRecord, error) {
	glog.V(4).Infof("[FETCH-HYBRID] topic=%s partition=%d requestedOffset=%d maxRecords=%d",
		topic, partition, requestedOffset, maxRecords)

	// Get or create session for this (topic, partition, consumerGroup, consumerID)
	key := fmt.Sprintf("%s-%d-%s-%s", topic, partition, consumerGroup, consumerID)

	bc.subscribersLock.Lock()
	session, exists := bc.subscribers[key]
	if !exists {
		// No session - create one (this is initial fetch)
		glog.V(4).Infof("[FETCH-HYBRID] Creating initial session for %s at offset %d", key, requestedOffset)
		newSession, err := bc.createSubscriberSession(topic, partition, requestedOffset, consumerGroup, consumerID)
		if err != nil {
			bc.subscribersLock.Unlock()
			return nil, fmt.Errorf("failed to create initial session: %v", err)
		}
		bc.subscribers[key] = newSession
		session = newSession
	}
	bc.subscribersLock.Unlock()

	// CRITICAL: Lock the session for the entire operation to serialize requests
	// This prevents concurrent access to the same stream
	session.mu.Lock()
	defer session.mu.Unlock()

	// Check if we can serve from cache
	if len(session.consumedRecords) > 0 {
		cacheStart := session.consumedRecords[0].Offset
		cacheEnd := session.consumedRecords[len(session.consumedRecords)-1].Offset

		if requestedOffset >= cacheStart && requestedOffset <= cacheEnd {
			// Serve from cache
			glog.V(4).Infof("[FETCH-HYBRID] FAST: Serving from cache for %s offset %d (cache: %d-%d)",
				key, requestedOffset, cacheStart, cacheEnd)
			return bc.serveFromCache(session, requestedOffset, maxRecords), nil
		}
	}

	// Determine stream position
	// lastReadOffset tracks what we've actually read from the stream
	streamPosition := session.lastReadOffset + 1
	if !session.initialized {
		streamPosition = session.StartOffset
	}

	glog.V(4).Infof("[FETCH-HYBRID] requestedOffset=%d streamPosition=%d lastReadOffset=%d",
		requestedOffset, streamPosition, session.lastReadOffset)

	// Decision: Fast path or slow path?
	if requestedOffset < streamPosition {
		// SLOW PATH: Backward seek - need new subscriber
		glog.V(4).Infof("[FETCH-HYBRID] SLOW: Backward seek from %d to %d, creating new subscriber",
			streamPosition, requestedOffset)

		// Close old session
		if session.Stream != nil {
			session.Stream.CloseSend()
		}
		if session.Cancel != nil {
			session.Cancel()
		}

		// Create new subscriber at requested offset
		newSession, err := bc.createSubscriberSession(topic, partition, requestedOffset, consumerGroup, consumerID)
		if err != nil {
			return nil, fmt.Errorf("failed to create subscriber for backward seek: %v", err)
		}

		// Replace session in map
		bc.subscribersLock.Lock()
		bc.subscribers[key] = newSession
		bc.subscribersLock.Unlock()

		// Update local reference and lock the new session
		session.Stream = newSession.Stream
		session.Ctx = newSession.Ctx
		session.Cancel = newSession.Cancel
		session.StartOffset = requestedOffset
		session.lastReadOffset = requestedOffset - 1 // Will be updated after read
		session.initialized = false
		session.consumedRecords = nil

		streamPosition = requestedOffset
	} else if requestedOffset > streamPosition {
		// FAST PATH: Forward seek - use server-side seek
		seekOffset := requestedOffset
		glog.V(4).Infof("[FETCH-HYBRID] FAST: Forward seek from %d to %d using server-side seek",
			streamPosition, seekOffset)

		// Send seek message to broker
		seekReq := &mq_pb.SubscribeMessageRequest{
			Message: &mq_pb.SubscribeMessageRequest_Seek{
				Seek: &mq_pb.SubscribeMessageRequest_SeekMessage{
					Offset:     seekOffset,
					OffsetType: schema_pb.OffsetType_EXACT_OFFSET,
				},
			},
		}

		if err := session.Stream.Send(seekReq); err != nil {
			if err == io.EOF {
				glog.V(4).Infof("[FETCH-HYBRID] Stream closed during seek, ignoring")
				return nil, nil
			}
			return nil, fmt.Errorf("failed to send seek request: %v", err)
		}

		glog.V(4).Infof("[FETCH-HYBRID] Seek request sent, broker will reposition stream to offset %d", seekOffset)
		// NOTE: Don't wait for ack - the broker will restart Subscribe loop and send data
		// The ack will be handled inline with data messages in readRecordsFromSession

		// Clear cache since we've skipped ahead
		session.consumedRecords = nil
		streamPosition = seekOffset
	} else {
		// FAST PATH: Sequential read - continue from current position
		glog.V(4).Infof("[FETCH-HYBRID] FAST: Sequential read at offset %d", requestedOffset)
	}

	// Read records from stream
	records, err := bc.readRecordsFromSession(ctx, session, requestedOffset, maxRecords)
	if err != nil {
		return nil, err
	}

	// Update tracking
	if len(records) > 0 {
		session.lastReadOffset = records[len(records)-1].Offset
		session.initialized = true
		glog.V(4).Infof("[FETCH-HYBRID] Read %d records, lastReadOffset now %d",
			len(records), session.lastReadOffset)
	}

	return records, nil
}

// FetchRecordsWithDedup reads records with request deduplication to prevent duplicate concurrent fetches
// DEPRECATED: Use FetchRecordsHybrid instead for better performance
// ctx controls the fetch timeout (should match Kafka fetch request's MaxWaitTime)
func (bc *BrokerClient) FetchRecordsWithDedup(ctx context.Context, topic string, partition int32, startOffset int64, maxRecords int, consumerGroup string, consumerID string) ([]*SeaweedRecord, error) {
	// Create key for this fetch request
	key := fmt.Sprintf("%s-%d-%d", topic, partition, startOffset)

	glog.V(4).Infof("[FETCH-DEDUP] topic=%s partition=%d offset=%d maxRecords=%d key=%s",
		topic, partition, startOffset, maxRecords, key)

	// Check if there's already a fetch in progress for this exact request
	bc.fetchRequestsLock.Lock()

	if existing, exists := bc.fetchRequests[key]; exists {
		// Another fetch is in progress for this (topic, partition, offset)
		// Create a waiter channel and add it to the list
		waiter := make(chan FetchResult, 1)
		existing.mu.Lock()
		existing.waiters = append(existing.waiters, waiter)
		existing.mu.Unlock()
		bc.fetchRequestsLock.Unlock()

		glog.V(4).Infof("[FETCH-DEDUP] Waiting for in-progress fetch: %s", key)

		// Wait for the result from the in-progress fetch
		select {
		case result := <-waiter:
			glog.V(4).Infof("[FETCH-DEDUP] Received result from in-progress fetch: %s (records=%d, err=%v)",
				key, len(result.records), result.err)
			return result.records, result.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// No fetch in progress - this request will do the fetch
	fetchReq := &FetchRequest{
		topic:      topic,
		partition:  partition,
		offset:     startOffset,
		resultChan: make(chan FetchResult, 1),
		waiters:    []chan FetchResult{},
		inProgress: true,
	}
	bc.fetchRequests[key] = fetchReq
	bc.fetchRequestsLock.Unlock()

	glog.V(4).Infof("[FETCH-DEDUP] Starting new fetch: %s", key)

	// Perform the actual fetch
	records, err := bc.fetchRecordsStatelessInternal(ctx, topic, partition, startOffset, maxRecords, consumerGroup, consumerID)

	// Prepare result
	result := FetchResult{
		records: records,
		err:     err,
	}

	// Broadcast result to all waiters and clean up
	bc.fetchRequestsLock.Lock()
	fetchReq.mu.Lock()
	waiters := fetchReq.waiters
	fetchReq.mu.Unlock()
	delete(bc.fetchRequests, key)
	bc.fetchRequestsLock.Unlock()

	// Send result to all waiters
	glog.V(4).Infof("[FETCH-DEDUP] Broadcasting result to %d waiters: %s (records=%d, err=%v)",
		len(waiters), key, len(records), err)
	for _, waiter := range waiters {
		waiter <- result
		close(waiter)
	}

	return records, err
}

// fetchRecordsStatelessInternal is the internal implementation of stateless fetch
// This is called by FetchRecordsWithDedup and should not be called directly
func (bc *BrokerClient) fetchRecordsStatelessInternal(ctx context.Context, topic string, partition int32, startOffset int64, maxRecords int, consumerGroup string, consumerID string) ([]*SeaweedRecord, error) {
	glog.V(4).Infof("[FETCH-STATELESS] topic=%s partition=%d offset=%d maxRecords=%d",
		topic, partition, startOffset, maxRecords)

	// STATELESS APPROACH: Create a temporary subscriber just for this fetch
	// This eliminates concurrent access to shared offset state
	tempSubscriber, err := bc.createTemporarySubscriber(topic, partition, startOffset, consumerGroup, consumerID)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary subscriber: %v", err)
	}

	// Ensure cleanup even if read fails
	defer func() {
		if tempSubscriber.Stream != nil {
			// Send close message
			tempSubscriber.Stream.CloseSend()
		}
		if tempSubscriber.Cancel != nil {
			tempSubscriber.Cancel()
		}
	}()

	// Read records from the fresh subscriber (no seeking needed, it starts at startOffset)
	return bc.readRecordsFrom(ctx, tempSubscriber, startOffset, maxRecords)
}

// FetchRecordsStateless reads records using a stateless approach (creates fresh subscriber per fetch)
// DEPRECATED: Use FetchRecordsHybrid instead for better performance with session reuse
// This eliminates concurrent access to shared offset state
// ctx controls the fetch timeout (should match Kafka fetch request's MaxWaitTime)
func (bc *BrokerClient) FetchRecordsStateless(ctx context.Context, topic string, partition int32, startOffset int64, maxRecords int, consumerGroup string, consumerID string) ([]*SeaweedRecord, error) {
	return bc.FetchRecordsHybrid(ctx, topic, partition, startOffset, maxRecords, consumerGroup, consumerID)
}

// ReadRecordsFromOffset reads records starting from a specific offset using STATELESS approach
// Creates a fresh subscriber for each fetch to eliminate concurrent access issues
// ctx controls the fetch timeout (should match Kafka fetch request's MaxWaitTime)
// DEPRECATED: Use FetchRecordsStateless instead for better API clarity
func (bc *BrokerClient) ReadRecordsFromOffset(ctx context.Context, session *BrokerSubscriberSession, requestedOffset int64, maxRecords int) ([]*SeaweedRecord, error) {
	if session == nil {
		return nil, fmt.Errorf("subscriber session cannot be nil")
	}

	return bc.FetchRecordsStateless(ctx, session.Topic, session.Partition, requestedOffset, maxRecords, session.ConsumerGroup, session.ConsumerID)
}

// readRecordsFrom reads records from the stream, assigning offsets starting from startOffset
// Uses a timeout-based approach to read multiple records without blocking indefinitely
// ctx controls the fetch timeout (should match Kafka fetch request's MaxWaitTime)
func (bc *BrokerClient) readRecordsFrom(ctx context.Context, session *BrokerSubscriberSession, startOffset int64, maxRecords int) ([]*SeaweedRecord, error) {
	if session == nil {
		return nil, fmt.Errorf("subscriber session cannot be nil")
	}

	if session.Stream == nil {
		return nil, fmt.Errorf("subscriber session stream cannot be nil")
	}

	glog.V(4).Infof("[FETCH] readRecordsFrom: topic=%s partition=%d startOffset=%d maxRecords=%d",
		session.Topic, session.Partition, startOffset, maxRecords)

	var records []*SeaweedRecord
	currentOffset := startOffset

	// CRITICAL FIX: Return immediately if maxRecords is 0 or negative
	if maxRecords <= 0 {
		return records, nil
	}

	// Note: Cache checking is done in ReadRecordsFromOffset, not here
	// This function is called only when we need to read new data from the stream

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
		glog.V(4).Infof("[FETCH] Stream is nil, cannot read")
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
			glog.V(4).Infof("[FETCH] Stream.Recv() error on first record: %v", result.err)
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
			glog.V(4).Infof("[FETCH] Received first record: offset=%d, keyLen=%d, valueLen=%d",
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
				glog.V(4).Infof("[FETCH] Failed to send ack for first record offset %d: %v (continuing)", record.Offset, err)
				// Don't fail the fetch if ack fails - continue reading
			}
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
				glog.V(4).Infof("[FETCH] Stream.Recv() error after %d records: %v", len(records), result.err)
				// Return what we have - cache will be updated at the end
				break
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

				// DEBUG: Log received message with value preview for GitHub Actions debugging
				valuePreview := ""
				if len(dataMsg.Value) > 0 {
					if len(dataMsg.Value) <= 50 {
						valuePreview = string(dataMsg.Value)
					} else {
						valuePreview = fmt.Sprintf("%s...(total %d bytes)", string(dataMsg.Value[:50]), len(dataMsg.Value))
					}
				} else {
					valuePreview = "<empty>"
				}
				glog.V(1).Infof("[FETCH_RECORD] offset=%d keyLen=%d valueLen=%d valuePreview=%q readTime=%v",
					record.Offset, len(record.Key), len(record.Value), valuePreview, readDuration)

				glog.V(4).Infof("[FETCH] Received record %d: offset=%d, keyLen=%d, valueLen=%d, readTime=%v",
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
					glog.V(4).Infof("[FETCH] Failed to send ack for offset %d: %v (continuing)", record.Offset, err)
					// Don't fail the fetch if ack fails - continue reading
				}
			}

		case <-ctx2.Done():
			cancel2()
			// Timeout - return what we have
			glog.V(4).Infof("[FETCH] Read timeout after %d records (waited %v), returning batch", len(records), time.Since(readStart))
			return records, nil
		}
	}

	glog.V(4).Infof("[FETCH] Returning %d records (maxRecords reached)", len(records))
	return records, nil
}

// ReadRecords is a simplified version for deprecated code paths
// It reads from wherever the stream currently is
func (bc *BrokerClient) ReadRecords(ctx context.Context, session *BrokerSubscriberSession, maxRecords int) ([]*SeaweedRecord, error) {
	// Determine where stream is based on cache
	session.mu.Lock()
	var streamOffset int64
	if len(session.consumedRecords) > 0 {
		streamOffset = session.consumedRecords[len(session.consumedRecords)-1].Offset + 1
	} else {
		streamOffset = session.StartOffset
	}
	session.mu.Unlock()

	return bc.readRecordsFrom(ctx, session, streamOffset, maxRecords)
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
		glog.V(4).Infof("[FETCH] Closed subscriber for %s", key)
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

	glog.V(4).Infof("[FETCH] Restarting subscriber for %s[%d]: from offset %d to %d",
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

	glog.V(4).Infof("[FETCH] Successfully restarted subscriber for %s[%d] at offset %d",
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
		glog.V(4).Infof("[SEEK] Already at offset %d for %s[%d], skipping seek", offset, session.Topic, session.Partition)
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
			glog.V(4).Infof("[SEEK] Stream closing during seek to offset %d for %s[%d]", offset, session.Topic, session.Partition)
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

	glog.V(4).Infof("[SEEK] Seeked to offset %d for %s[%d]", offset, session.Topic, session.Partition)
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
			glog.V(4).Infof("[SEEK] Stream closing during seek to timestamp %d for %s[%d]", timestampNs, session.Topic, session.Partition)
			return nil // Not an error during shutdown
		}
		return fmt.Errorf("seek to timestamp %d failed: %v", timestampNs, err)
	}

	session.mu.Lock()
	// Note: We don't know the exact offset at this timestamp yet
	// It will be updated when we read the first message
	session.consumedRecords = nil
	session.mu.Unlock()

	glog.V(4).Infof("[SEEK] Seeked to timestamp %d for %s[%d]", timestampNs, session.Topic, session.Partition)
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
			glog.V(4).Infof("[SEEK] Stream closing during seek to earliest for %s[%d]", session.Topic, session.Partition)
			return nil // Not an error during shutdown
		}
		return fmt.Errorf("seek to earliest failed: %v", err)
	}

	session.mu.Lock()
	session.StartOffset = 0
	session.consumedRecords = nil
	session.mu.Unlock()

	glog.V(4).Infof("[SEEK] Seeked to earliest for %s[%d]", session.Topic, session.Partition)
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
			glog.V(4).Infof("[SEEK] Stream closing during seek to latest for %s[%d]", session.Topic, session.Partition)
			return nil // Not an error during shutdown
		}
		return fmt.Errorf("seek to latest failed: %v", err)
	}

	session.mu.Lock()
	// Offset will be set when we read the first new message
	session.consumedRecords = nil
	session.mu.Unlock()

	glog.V(4).Infof("[SEEK] Seeked to latest for %s[%d]", session.Topic, session.Partition)
	return nil
}

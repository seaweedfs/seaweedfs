package integration

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// GetStoredRecords retrieves records from SeaweedMQ using the proper subscriber API
func (h *SeaweedMQHandler) GetStoredRecords(topic string, partition int32, fromOffset int64, maxRecords int) ([]SMQRecord, error) {
	glog.Infof("[FETCH] GetStoredRecords: topic=%s partition=%d fromOffset=%d maxRecords=%d", topic, partition, fromOffset, maxRecords)

	// Verify topic exists
	if !h.TopicExists(topic) {
		return nil, fmt.Errorf("topic %s does not exist", topic)
	}

	// CRITICAL: Use per-connection BrokerClient to prevent gRPC stream interference
	// Each Kafka connection has its own isolated BrokerClient instance
	var brokerClient *BrokerClient
	consumerGroup := "kafka-fetch-consumer"                            // default
	consumerID := fmt.Sprintf("kafka-fetch-%d", time.Now().UnixNano()) // default

	// Get the per-connection broker client from connection context
	if h.protocolHandler != nil {
		connCtx := h.protocolHandler.GetConnectionContext()
		if connCtx != nil {
			// Extract per-connection broker client
			if connCtx.BrokerClient != nil {
				if bc, ok := connCtx.BrokerClient.(*BrokerClient); ok {
					brokerClient = bc
					glog.Infof("[FETCH] Using per-connection BrokerClient for topic=%s partition=%d", topic, partition)
				}
			}

			// Extract consumer group and client ID
			if connCtx.ConsumerGroup != "" {
				consumerGroup = connCtx.ConsumerGroup
				glog.Infof("[FETCH] Using actual consumer group from context: %s", consumerGroup)
			}
			if connCtx.MemberID != "" {
				consumerID = connCtx.MemberID
				glog.Infof("[FETCH] Using actual member ID from context: %s", consumerID)
			} else if connCtx.ClientID != "" {
				// Fallback to client ID if member ID not set (for clients not using consumer groups)
				consumerID = connCtx.ClientID
				glog.Infof("[FETCH] Using client ID from context: %s", consumerID)
			}
		}
	}

	// Fallback to shared broker client if per-connection client not available
	if brokerClient == nil {
		glog.Warningf("[FETCH] No per-connection BrokerClient, falling back to shared client")
		brokerClient = h.brokerClient
		if brokerClient == nil {
			return nil, fmt.Errorf("no broker client available")
		}
	}

	// CRITICAL FIX: Reuse existing subscriber if offset matches to avoid concurrent subscriber storm
	// Creating too many concurrent subscribers to the same offset causes the broker to return
	// the same data repeatedly, creating an infinite loop.
	glog.Infof("[FETCH] Getting or creating subscriber for topic=%s partition=%d fromOffset=%d", topic, partition, fromOffset)

	brokerSubscriber, err := brokerClient.GetOrCreateSubscriber(topic, partition, fromOffset)
	if err != nil {
		glog.Errorf("[FETCH] Failed to get/create subscriber: %v", err)
		return nil, fmt.Errorf("failed to get/create subscriber: %v", err)
	}
	glog.Infof("[FETCH] Subscriber ready")

	// CRITICAL FIX: If the subscriber has already consumed past the requested offset,
	// close it and create a fresh one to avoid broker tight loop
	if brokerSubscriber.StartOffset > fromOffset {
		glog.Infof("[FETCH] Subscriber already at offset %d (requested %d < current), closing and recreating",
			brokerSubscriber.StartOffset, fromOffset)

		// Close the old subscriber
		if brokerSubscriber.Stream != nil {
			_ = brokerSubscriber.Stream.CloseSend()
		}

		// Remove from cache
		key := fmt.Sprintf("%s-%d", topic, partition)
		brokerClient.subscribersLock.Lock()
		delete(brokerClient.subscribers, key)
		brokerClient.subscribersLock.Unlock()

		// Create a fresh subscriber at the requested offset
		brokerSubscriber, err = brokerClient.CreateFreshSubscriber(topic, partition, fromOffset, consumerGroup, consumerID)
		if err != nil {
			glog.Errorf("[FETCH] Failed to create fresh subscriber: %v", err)
			return nil, fmt.Errorf("failed to create fresh subscriber: %v", err)
		}
		glog.Infof("[FETCH] Created fresh subscriber at offset %d", fromOffset)
	}

	// NOTE: We DON'T close the subscriber here because we're reusing it across Fetch requests
	// The subscriber will be closed when the connection closes or when a different offset is requested

	// Read records using the subscriber
	glog.Infof("[FETCH] Calling ReadRecords for topic=%s partition=%d maxRecords=%d", topic, partition, maxRecords)
	seaweedRecords, err := brokerClient.ReadRecords(brokerSubscriber, maxRecords)
	if err != nil {
		glog.Errorf("[FETCH] ReadRecords failed: %v", err)
		return nil, fmt.Errorf("failed to read records: %v", err)
	}
	glog.Infof("[FETCH] ReadRecords returned %d records", len(seaweedRecords))

	// CRITICAL FIX: If ReadRecords returns 0 but data should exist (check HWM), recreate subscriber
	// This handles the case where subscriber was created before data was published and flushed
	if len(seaweedRecords) == 0 && brokerClient != nil {
		// Check if data actually exists
		hwm, hwmErr := brokerClient.GetHighWaterMark(topic, partition)
		if hwmErr == nil && hwm > fromOffset {
			glog.Warningf("[FETCH] No records returned but HWM=%d > fromOffset=%d, recreating subscriber to force disk read", hwm, fromOffset)

			// Close the stuck subscriber
			if brokerSubscriber.Stream != nil {
				_ = brokerSubscriber.Stream.CloseSend()
			}

			// Remove from cache
			key := fmt.Sprintf("%s-%d", topic, partition)
			brokerClient.subscribersLock.Lock()
			delete(brokerClient.subscribers, key)
			brokerClient.subscribersLock.Unlock()

			// Create fresh subscriber and try again ONE more time
			brokerSubscriber, err = brokerClient.CreateFreshSubscriber(topic, partition, fromOffset, consumerGroup, consumerID)
			if err != nil {
				glog.Errorf("[FETCH] Failed to recreate subscriber: %v", err)
				return nil, fmt.Errorf("failed to recreate subscriber: %v", err)
			}
			glog.Infof("[FETCH] Recreated subscriber, retrying ReadRecords")

			seaweedRecords, err = brokerClient.ReadRecords(brokerSubscriber, maxRecords)
			if err != nil {
				glog.Errorf("[FETCH] ReadRecords failed after recreation: %v", err)
				return nil, fmt.Errorf("failed to read records after recreation: %v", err)
			}
			glog.Infof("[FETCH] After recreation: ReadRecords returned %d records", len(seaweedRecords))
		}
	}

	// Convert SeaweedMQ records to SMQRecord interface with proper Kafka offsets
	smqRecords := make([]SMQRecord, 0, len(seaweedRecords))
	for i, seaweedRecord := range seaweedRecords {
		// CRITICAL FIX: Use the actual offset from SeaweedMQ
		// The SeaweedRecord.Offset field now contains the correct offset from the subscriber
		kafkaOffset := seaweedRecord.Offset

		// Validate that the offset makes sense
		expectedOffset := fromOffset + int64(i)
		if kafkaOffset != expectedOffset {
			glog.Warningf("[FETCH] Offset mismatch for record %d: got=%d, expected=%d", i, kafkaOffset, expectedOffset)
		}

		smqRecord := &SeaweedSMQRecord{
			key:       seaweedRecord.Key,
			value:     seaweedRecord.Value,
			timestamp: seaweedRecord.Timestamp,
			offset:    kafkaOffset,
		}
		smqRecords = append(smqRecords, smqRecord)

		glog.Infof("[FETCH] Record %d: offset=%d, keyLen=%d, valueLen=%d", i, kafkaOffset, len(seaweedRecord.Key), len(seaweedRecord.Value))
	}

	glog.Infof("[FETCH] Successfully read %d records from SMQ", len(smqRecords))
	return smqRecords, nil
}

// GetEarliestOffset returns the earliest available offset for a topic partition
// ALWAYS queries SMQ broker directly - no ledger involved
func (h *SeaweedMQHandler) GetEarliestOffset(topic string, partition int32) (int64, error) {

	// Check if topic exists
	if !h.TopicExists(topic) {
		return 0, nil // Empty topic starts at offset 0
	}

	// ALWAYS query SMQ broker directly for earliest offset
	if h.brokerClient != nil {
		earliestOffset, err := h.brokerClient.GetEarliestOffset(topic, partition)
		if err != nil {
			return 0, err
		}
		return earliestOffset, nil
	}

	// No broker client - this shouldn't happen in production
	return 0, fmt.Errorf("broker client not available")
}

// GetLatestOffset returns the latest available offset for a topic partition
// ALWAYS queries SMQ broker directly - no ledger involved
func (h *SeaweedMQHandler) GetLatestOffset(topic string, partition int32) (int64, error) {

	// Check if topic exists
	if !h.TopicExists(topic) {
		return 0, nil // Empty topic
	}

	// Check cache first
	cacheKey := fmt.Sprintf("%s:%d", topic, partition)
	h.hwmCacheMu.RLock()
	if entry, exists := h.hwmCache[cacheKey]; exists {
		if time.Now().Before(entry.expiresAt) {
			// Cache hit - return cached value
			h.hwmCacheMu.RUnlock()
			return entry.value, nil
		}
	}
	h.hwmCacheMu.RUnlock()

	// Cache miss or expired - query SMQ broker
	if h.brokerClient != nil {
		latestOffset, err := h.brokerClient.GetHighWaterMark(topic, partition)
		if err != nil {
			return 0, err
		}

		// Update cache
		h.hwmCacheMu.Lock()
		h.hwmCache[cacheKey] = &hwmCacheEntry{
			value:     latestOffset,
			expiresAt: time.Now().Add(h.hwmCacheTTL),
		}
		h.hwmCacheMu.Unlock()

		return latestOffset, nil
	}

	// No broker client - this shouldn't happen in production
	return 0, fmt.Errorf("broker client not available")
}

// WithFilerClient executes a function with a filer client
func (h *SeaweedMQHandler) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	if h.brokerClient == nil {
		return fmt.Errorf("no broker client available")
	}
	return h.brokerClient.WithFilerClient(streamingMode, fn)
}

// GetFilerAddress returns the filer address used by this handler
func (h *SeaweedMQHandler) GetFilerAddress() string {
	if h.brokerClient != nil {
		return h.brokerClient.GetFilerAddress()
	}
	return ""
}

// ProduceRecord publishes a record to SeaweedMQ and lets SMQ generate the offset
func (h *SeaweedMQHandler) ProduceRecord(topic string, partition int32, key []byte, value []byte) (int64, error) {
	if len(key) > 0 {
	}
	if len(value) > 0 {
	} else {
	}

	// Verify topic exists
	if !h.TopicExists(topic) {
		return 0, fmt.Errorf("topic %s does not exist", topic)
	}

	// Get current timestamp
	timestamp := time.Now().UnixNano()

	// Publish to SeaweedMQ and let SMQ generate the offset
	var smqOffset int64
	var publishErr error
	if h.brokerClient == nil {
		publishErr = fmt.Errorf("no broker client available")
	} else {
		smqOffset, publishErr = h.brokerClient.PublishRecord(topic, partition, key, value, timestamp)
	}

	if publishErr != nil {
		return 0, fmt.Errorf("failed to publish to SeaweedMQ: %v", publishErr)
	}

	// SMQ should have generated and returned the offset - use it directly as the Kafka offset

	// Invalidate HWM cache for this partition to ensure fresh reads
	// This is critical for read-your-own-write scenarios (e.g., Schema Registry)
	cacheKey := fmt.Sprintf("%s:%d", topic, partition)
	h.hwmCacheMu.Lock()
	delete(h.hwmCache, cacheKey)
	h.hwmCacheMu.Unlock()

	return smqOffset, nil
}

// ProduceRecordValue produces a record using RecordValue format to SeaweedMQ
// ALWAYS uses broker's assigned offset - no ledger involved
func (h *SeaweedMQHandler) ProduceRecordValue(topic string, partition int32, key []byte, recordValueBytes []byte) (int64, error) {
	// Verify topic exists
	if !h.TopicExists(topic) {
		return 0, fmt.Errorf("topic %s does not exist", topic)
	}

	// Get current timestamp
	timestamp := time.Now().UnixNano()

	// Publish RecordValue to SeaweedMQ and get the broker-assigned offset
	var smqOffset int64
	var publishErr error
	if h.brokerClient == nil {
		publishErr = fmt.Errorf("no broker client available")
	} else {
		smqOffset, publishErr = h.brokerClient.PublishRecordValue(topic, partition, key, recordValueBytes, timestamp)
	}

	if publishErr != nil {
		return 0, fmt.Errorf("failed to publish RecordValue to SeaweedMQ: %v", publishErr)
	}

	// SMQ broker has assigned the offset - use it directly as the Kafka offset

	// Invalidate HWM cache for this partition to ensure fresh reads
	// This is critical for read-your-own-write scenarios (e.g., Schema Registry)
	cacheKey := fmt.Sprintf("%s:%d", topic, partition)
	h.hwmCacheMu.Lock()
	delete(h.hwmCache, cacheKey)
	h.hwmCacheMu.Unlock()

	return smqOffset, nil
}

// Ledger methods removed - SMQ broker handles all offset management directly

// FetchRecords DEPRECATED - only used in old tests
func (h *SeaweedMQHandler) FetchRecords(topic string, partition int32, fetchOffset int64, maxBytes int32) ([]byte, error) {
	// Verify topic exists
	if !h.TopicExists(topic) {
		return nil, fmt.Errorf("topic %s does not exist", topic)
	}

	// DEPRECATED: This function only used in old tests
	// Get HWM directly from broker
	highWaterMark, err := h.GetLatestOffset(topic, partition)
	if err != nil {
		return nil, err
	}

	// If fetch offset is at or beyond high water mark, no records to return
	if fetchOffset >= highWaterMark {
		return []byte{}, nil
	}

	// Get or create subscriber session for this topic/partition
	var seaweedRecords []*SeaweedRecord

	// Calculate how many records to fetch
	recordsToFetch := int(highWaterMark - fetchOffset)
	if recordsToFetch > 100 {
		recordsToFetch = 100 // Limit batch size
	}

	// Read records using broker client
	if h.brokerClient == nil {
		return nil, fmt.Errorf("no broker client available")
	}
	brokerSubscriber, subErr := h.brokerClient.GetOrCreateSubscriber(topic, partition, fetchOffset)
	if subErr != nil {
		return nil, fmt.Errorf("failed to get broker subscriber: %v", subErr)
	}
	seaweedRecords, err = h.brokerClient.ReadRecords(brokerSubscriber, recordsToFetch)

	if err != nil {
		// If no records available, return empty batch instead of error
		return []byte{}, nil
	}

	// Map SeaweedMQ records to Kafka offsets and update ledger
	kafkaRecords, err := h.mapSeaweedToKafkaOffsets(topic, partition, seaweedRecords, fetchOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to map offsets: %v", err)
	}

	// Convert mapped records to Kafka record batch format
	return h.convertSeaweedToKafkaRecordBatch(kafkaRecords, fetchOffset, maxBytes)
}

// mapSeaweedToKafkaOffsets maps SeaweedMQ records to proper Kafka offsets
func (h *SeaweedMQHandler) mapSeaweedToKafkaOffsets(topic string, partition int32, seaweedRecords []*SeaweedRecord, startOffset int64) ([]*SeaweedRecord, error) {
	if len(seaweedRecords) == 0 {
		return seaweedRecords, nil
	}

	// DEPRECATED: This function only used in old tests
	// Just map offsets sequentially
	mappedRecords := make([]*SeaweedRecord, 0, len(seaweedRecords))

	for i, seaweedRecord := range seaweedRecords {
		currentKafkaOffset := startOffset + int64(i)

		// Create a copy of the record with proper Kafka offset assignment
		mappedRecord := &SeaweedRecord{
			Key:       seaweedRecord.Key,
			Value:     seaweedRecord.Value,
			Timestamp: seaweedRecord.Timestamp,
			Offset:    currentKafkaOffset,
		}

		// Just skip any error handling since this is deprecated
		{
			// Log warning but continue processing
		}

		mappedRecords = append(mappedRecords, mappedRecord)
	}

	return mappedRecords, nil
}

// convertSeaweedToKafkaRecordBatch converts SeaweedMQ records to Kafka record batch format
func (h *SeaweedMQHandler) convertSeaweedToKafkaRecordBatch(seaweedRecords []*SeaweedRecord, fetchOffset int64, maxBytes int32) ([]byte, error) {
	if len(seaweedRecords) == 0 {
		return []byte{}, nil
	}

	batch := make([]byte, 0, 512)

	// Record batch header
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(fetchOffset))
	batch = append(batch, baseOffsetBytes...) // base offset

	// Batch length (placeholder, will be filled at end)
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	batch = append(batch, 0, 0, 0, 0) // partition leader epoch
	batch = append(batch, 2)          // magic byte (version 2)

	// CRC placeholder
	batch = append(batch, 0, 0, 0, 0)

	// Batch attributes
	batch = append(batch, 0, 0)

	// Last offset delta
	lastOffsetDelta := uint32(len(seaweedRecords) - 1)
	lastOffsetDeltaBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lastOffsetDeltaBytes, lastOffsetDelta)
	batch = append(batch, lastOffsetDeltaBytes...)

	// Timestamps - use actual timestamps from SeaweedMQ records
	var firstTimestamp, maxTimestamp int64
	if len(seaweedRecords) > 0 {
		firstTimestamp = seaweedRecords[0].Timestamp
		maxTimestamp = firstTimestamp
		for _, record := range seaweedRecords {
			if record.Timestamp > maxTimestamp {
				maxTimestamp = record.Timestamp
			}
		}
	}

	firstTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(firstTimestampBytes, uint64(firstTimestamp))
	batch = append(batch, firstTimestampBytes...)

	maxTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTimestampBytes, uint64(maxTimestamp))
	batch = append(batch, maxTimestampBytes...)

	// Producer info (simplified)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF) // producer ID (-1)
	batch = append(batch, 0xFF, 0xFF)                                     // producer epoch (-1)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)                         // base sequence (-1)

	// Record count
	recordCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordCountBytes, uint32(len(seaweedRecords)))
	batch = append(batch, recordCountBytes...)

	// Add actual records from SeaweedMQ
	for i, seaweedRecord := range seaweedRecords {
		record := h.convertSingleSeaweedRecord(seaweedRecord, int64(i), fetchOffset)
		recordLength := byte(len(record))
		batch = append(batch, recordLength)
		batch = append(batch, record...)

		// Check if we're approaching maxBytes limit
		if int32(len(batch)) > maxBytes*3/4 {
			// Leave room for remaining headers and stop adding records
			break
		}
	}

	// Fill in the batch length
	batchLength := uint32(len(batch) - batchLengthPos - 4)
	binary.BigEndian.PutUint32(batch[batchLengthPos:batchLengthPos+4], batchLength)

	return batch, nil
}

// convertSingleSeaweedRecord converts a single SeaweedMQ record to Kafka format
func (h *SeaweedMQHandler) convertSingleSeaweedRecord(seaweedRecord *SeaweedRecord, index, baseOffset int64) []byte {
	record := make([]byte, 0, 64)

	// Record attributes
	record = append(record, 0)

	// Timestamp delta (varint - simplified)
	timestampDelta := seaweedRecord.Timestamp - baseOffset // Simple delta calculation
	if timestampDelta < 0 {
		timestampDelta = 0
	}
	record = append(record, byte(timestampDelta&0xFF)) // Simplified varint encoding

	// Offset delta (varint - simplified)
	record = append(record, byte(index))

	// Key length and key
	if len(seaweedRecord.Key) > 0 {
		record = append(record, byte(len(seaweedRecord.Key)))
		record = append(record, seaweedRecord.Key...)
	} else {
		// Null key
		record = append(record, 0xFF)
	}

	// Value length and value
	if len(seaweedRecord.Value) > 0 {
		record = append(record, byte(len(seaweedRecord.Value)))
		record = append(record, seaweedRecord.Value...)
	} else {
		// Empty value
		record = append(record, 0)
	}

	// Headers count (0)
	record = append(record, 0)

	return record
}

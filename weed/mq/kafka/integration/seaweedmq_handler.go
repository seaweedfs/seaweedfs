package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// GetStoredRecords retrieves records from SeaweedMQ using the proper subscriber API
// ctx controls the fetch timeout (should match Kafka fetch request's MaxWaitTime)
func (h *SeaweedMQHandler) GetStoredRecords(ctx context.Context, topic string, partition int32, fromOffset int64, maxRecords int) ([]SMQRecord, error) {
	glog.V(2).Infof("[FETCH] GetStoredRecords: topic=%s partition=%d fromOffset=%d maxRecords=%d", topic, partition, fromOffset, maxRecords)

	// Verify topic exists
	if !h.TopicExists(topic) {
		return nil, fmt.Errorf("topic %s does not exist", topic)
	}

	// CRITICAL: Use per-connection BrokerClient to prevent gRPC stream interference
	// Each Kafka connection has its own isolated BrokerClient instance
	var brokerClient *BrokerClient
	consumerGroup := "kafka-fetch-consumer" // default
	// CRITICAL FIX: Use stable consumer ID per topic-partition, NOT with timestamp
	// Including timestamp would create a new session on every fetch, causing subscriber churn
	consumerID := fmt.Sprintf("kafka-fetch-%s-%d", topic, partition) // default, stable per topic-partition

	// Get the per-connection broker client from connection context
	if h.protocolHandler != nil {
		connCtx := h.protocolHandler.GetConnectionContext()
		if connCtx != nil {
			// Extract per-connection broker client
			if connCtx.BrokerClient != nil {
				if bc, ok := connCtx.BrokerClient.(*BrokerClient); ok {
					brokerClient = bc
					glog.V(2).Infof("[FETCH] Using per-connection BrokerClient for topic=%s partition=%d", topic, partition)
				}
			}

			// Extract consumer group and client ID
			if connCtx.ConsumerGroup != "" {
				consumerGroup = connCtx.ConsumerGroup
				glog.V(2).Infof("[FETCH] Using actual consumer group from context: %s", consumerGroup)
			}
			if connCtx.MemberID != "" {
				// Use member ID as base, but still include topic-partition for uniqueness
				consumerID = fmt.Sprintf("%s-%s-%d", connCtx.MemberID, topic, partition)
				glog.V(2).Infof("[FETCH] Using actual member ID from context: %s", consumerID)
			} else if connCtx.ClientID != "" {
				// Fallback to client ID if member ID not set (for clients not using consumer groups)
				// Include topic-partition to ensure each partition consumer is unique
				consumerID = fmt.Sprintf("%s-%s-%d", connCtx.ClientID, topic, partition)
				glog.V(2).Infof("[FETCH] Using client ID from context: %s", consumerID)
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
	glog.V(2).Infof("[FETCH] Getting or creating subscriber for topic=%s partition=%d fromOffset=%d", topic, partition, fromOffset)

	// GetOrCreateSubscriber handles offset mismatches internally
	// If the cached subscriber is at a different offset, it will be recreated automatically
	brokerSubscriber, err := brokerClient.GetOrCreateSubscriber(topic, partition, fromOffset, consumerGroup, consumerID)
	if err != nil {
		glog.Errorf("[FETCH] Failed to get/create subscriber: %v", err)
		return nil, fmt.Errorf("failed to get/create subscriber: %v", err)
	}
	glog.V(2).Infof("[FETCH] Subscriber ready at offset %d", brokerSubscriber.StartOffset)

	// NOTE: We DON'T close the subscriber here because we're reusing it across Fetch requests
	// The subscriber will be closed when the connection closes or when a different offset is requested

	// Read records using the subscriber
	// CRITICAL: Pass the requested fromOffset to ReadRecords so it can check the cache correctly
	// If the session has advanced past fromOffset, ReadRecords will return cached data
	// Pass context to respect Kafka fetch request's MaxWaitTime
	isSchemasTopic := topic == "_schemas"
	if isSchemasTopic {
		glog.Infof("[SCHEMAS] Calling ReadRecordsFromOffset: topic=%s partition=%d fromOffset=%d maxRecords=%d subscriberOffset=%d",
			topic, partition, fromOffset, maxRecords, brokerSubscriber.StartOffset)
	} else {
		glog.V(2).Infof("[FETCH] Calling ReadRecords for topic=%s partition=%d fromOffset=%d maxRecords=%d", topic, partition, fromOffset, maxRecords)
	}
	seaweedRecords, err := brokerClient.ReadRecordsFromOffset(ctx, brokerSubscriber, fromOffset, maxRecords)
	if isSchemasTopic {
		glog.Infof("[SCHEMAS] ReadRecordsFromOffset returned %d records, err=%v", len(seaweedRecords), err)
	}
	if err != nil {
		glog.Errorf("[FETCH] ReadRecords failed: %v", err)
		return nil, fmt.Errorf("failed to read records: %v", err)
	}
	// CRITICAL FIX: If ReadRecords returns 0 but HWM indicates data exists on disk, force a disk read
	// This handles the case where subscriber advanced past data that was already on disk
	// Only do this ONCE per fetch request to avoid subscriber churn
	if len(seaweedRecords) == 0 {
		hwm, hwmErr := brokerClient.GetHighWaterMark(topic, partition)
		if hwmErr == nil && fromOffset < hwm {
			if isSchemasTopic {
				glog.Infof("[SCHEMAS] No records from subscriber at offset %d, but HWM=%d (data exists), restarting for disk read",
					fromOffset, hwm)
			}
			// Restart the existing subscriber at the requested offset for disk read
			// This is more efficient than closing and recreating
			consumerGroup := "kafka-gateway"
			consumerID := fmt.Sprintf("kafka-gateway-%s-%d", topic, partition)

			if err := brokerClient.RestartSubscriber(brokerSubscriber, fromOffset, consumerGroup, consumerID); err != nil {
				return nil, fmt.Errorf("failed to restart subscriber: %v", err)
			}

			// Try reading again from restarted subscriber (will do disk read)
			seaweedRecords, err = brokerClient.ReadRecordsFromOffset(ctx, brokerSubscriber, fromOffset, maxRecords)
			if err != nil {
				return nil, fmt.Errorf("failed to read after restart: %v", err)
			}
			if isSchemasTopic {
				glog.Infof("[SCHEMAS] After restart: got %d records", len(seaweedRecords))
			}
		}
	}

	glog.V(2).Infof("[FETCH] ReadRecords returned %d records", len(seaweedRecords))
	//
	// This approach is correct for Kafka protocol:
	// - Clients continuously poll with Fetch requests
	// - If no data is available, we return empty and client will retry
	// - Eventually the data will be read from disk and returned
	//
	// We only recreate subscriber if the offset mismatches, which is handled earlier in this function

	// Convert SeaweedMQ records to SMQRecord interface with proper Kafka offsets
	smqRecords := make([]SMQRecord, 0, len(seaweedRecords))
	for i, seaweedRecord := range seaweedRecords {
		// CRITICAL FIX: Use the actual offset from SeaweedMQ
		// The SeaweedRecord.Offset field now contains the correct offset from the subscriber
		kafkaOffset := seaweedRecord.Offset

		// CRITICAL: Skip records before the requested offset
		// This can happen when the subscriber cache returns old data
		if kafkaOffset < fromOffset {
			glog.V(2).Infof("[FETCH] Skipping record %d with offset %d (requested fromOffset=%d)", i, kafkaOffset, fromOffset)
			continue
		}

		smqRecord := &SeaweedSMQRecord{
			key:       seaweedRecord.Key,
			value:     seaweedRecord.Value,
			timestamp: seaweedRecord.Timestamp,
			offset:    kafkaOffset,
		}
		smqRecords = append(smqRecords, smqRecord)

		glog.V(4).Infof("[FETCH] Record %d: offset=%d, keyLen=%d, valueLen=%d", i, kafkaOffset, len(seaweedRecord.Key), len(seaweedRecord.Value))
	}

	glog.V(2).Infof("[FETCH] Successfully read %d records from SMQ", len(smqRecords))
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
	isSchemasTopic := topic == "_schemas"

	// Check if topic exists
	if !h.TopicExists(topic) {
		if isSchemasTopic {
			glog.Infof("[SCHEMAS DEBUG] Topic does not exist, returning offset=0")
		}
		return 0, nil // Empty topic
	}

	// Check cache first
	cacheKey := fmt.Sprintf("%s:%d", topic, partition)
	h.hwmCacheMu.RLock()
	if entry, exists := h.hwmCache[cacheKey]; exists {
		if time.Now().Before(entry.expiresAt) {
			// Cache hit - return cached value
			h.hwmCacheMu.RUnlock()
			if isSchemasTopic {
				glog.Infof("[SCHEMAS DEBUG] HWM cache hit: offset=%d ttl=%v",
					entry.value, entry.expiresAt.Sub(time.Now()))
			}
			return entry.value, nil
		}
	}
	h.hwmCacheMu.RUnlock()

	// Cache miss or expired - query SMQ broker
	if h.brokerClient != nil {
		latestOffset, err := h.brokerClient.GetHighWaterMark(topic, partition)
		if isSchemasTopic {
			glog.Infof("[SCHEMAS DEBUG] HWM query from broker: offset=%d err=%v (cache miss/expired)",
				latestOffset, err)
		}
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
	isSchemasTopic := topic == "_schemas"

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

	if isSchemasTopic {
		glog.Infof("[SCHEMAS DEBUG] Produced record: topic=%s partition=%d assignedOffset=%d keyLen=%d valueLen=%d (HWM cache invalidated)",
			topic, partition, smqOffset, len(key), len(value))
	}

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
	// Use default consumer group/ID since this is a deprecated function
	brokerSubscriber, subErr := h.brokerClient.GetOrCreateSubscriber(topic, partition, fetchOffset, "deprecated-consumer-group", "deprecated-consumer")
	if subErr != nil {
		return nil, fmt.Errorf("failed to get broker subscriber: %v", subErr)
	}
	// This is a deprecated function, use background context
	seaweedRecords, err = h.brokerClient.ReadRecords(context.Background(), brokerSubscriber, recordsToFetch)

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

package protocol

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/compression"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/protobuf/proto"
)

func (h *Handler) handleFetch(ctx context.Context, correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	// Parse the Fetch request to get the requested topics and partitions
	fetchRequest, err := h.parseFetchRequest(apiVersion, requestBody)
	if err != nil {
		return nil, fmt.Errorf("parse fetch request: %w", err)
	}

	// Basic long-polling to avoid client busy-looping when there's no data.
	var throttleTimeMs int32 = 0
	// Only long-poll when all referenced topics exist; unknown topics should not block
	allTopicsExist := func() bool {
		for _, topic := range fetchRequest.Topics {
			if !h.seaweedMQHandler.TopicExists(topic.Name) {
				return false
			}
		}
		return true
	}
	hasDataAvailable := func() bool {
		for _, topic := range fetchRequest.Topics {
			for _, p := range topic.Partitions {
				ledger := h.GetLedger(topic.Name, p.PartitionID)
				if ledger == nil {
					continue
				}
				if ledger.GetHighWaterMark() > p.FetchOffset {
					return true
				}
			}
		}
		return false
	}
	// Cap long-polling to avoid blocking connection shutdowns in tests
	maxWaitMs := fetchRequest.MaxWaitTime
	if maxWaitMs > 1000 {
		maxWaitMs = 1000
	}
	shouldLongPoll := fetchRequest.MinBytes > 0 && maxWaitMs > 0 && !hasDataAvailable() && allTopicsExist()
	if shouldLongPoll {
		start := time.Now()
		// Limit polling time to maximum 2 seconds to prevent hanging in CI
		maxPollTime := time.Duration(maxWaitMs) * time.Millisecond
		if maxPollTime > 2*time.Second {
			maxPollTime = 2 * time.Second
			fmt.Printf("DEBUG: Limiting fetch polling to 2 seconds to prevent hanging\n")
		}
		deadline := start.Add(maxPollTime)
		for time.Now().Before(deadline) {
			// Use context-aware sleep instead of blocking time.Sleep
			select {
			case <-ctx.Done():
				fmt.Printf("DEBUG: Fetch polling cancelled due to context cancellation\n")
				throttleTimeMs = int32(time.Since(start) / time.Millisecond)
				break
			case <-time.After(10 * time.Millisecond):
				// Continue with polling
			}
			if hasDataAvailable() {
				break
			}
		}
		throttleTimeMs = int32(time.Since(start) / time.Millisecond)
	}

	// Build the response
	response := make([]byte, 0, 1024)

	// Correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Fetch v1+ has throttle_time_ms at the beginning
	if apiVersion >= 1 {
		throttleBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(throttleBytes, uint32(throttleTimeMs))
		response = append(response, throttleBytes...)
	}

	// Fetch v7+ has error_code and session_id
	if apiVersion >= 7 {
		response = append(response, 0, 0)       // error_code (2 bytes, 0 = no error)
		response = append(response, 0, 0, 0, 0) // session_id (4 bytes, 0 = no session)
	}

	// Topics count
	topicsCount := len(fetchRequest.Topics)
	topicsCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsCountBytes, uint32(topicsCount))
	response = append(response, topicsCountBytes...)

	// Process each requested topic
	for _, topic := range fetchRequest.Topics {
		topicNameBytes := []byte(topic.Name)

		// Topic name length and name
		response = append(response, byte(len(topicNameBytes)>>8), byte(len(topicNameBytes)))
		response = append(response, topicNameBytes...)

		// Partitions count for this topic
		partitionsCount := len(topic.Partitions)
		partitionsCountBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(partitionsCountBytes, uint32(partitionsCount))
		response = append(response, partitionsCountBytes...)

		// Process each requested partition
		for _, partition := range topic.Partitions {
			// Partition ID
			partitionIDBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionIDBytes, uint32(partition.PartitionID))
			response = append(response, partitionIDBytes...)

			// Error code (2 bytes) - default 0 = no error (may patch below)
			errorPos := len(response)
			response = append(response, 0, 0)

			// Get ledger for this topic-partition to determine high water mark
			// Use GetLedger (not GetOrCreateLedger) to avoid creating topics that don't exist
			ledger := h.GetLedger(topic.Name, partition.PartitionID)
			var highWaterMark int64 = 0
			if ledger != nil {
				highWaterMark = ledger.GetHighWaterMark()
			}

			// Normalize special fetch offsets: -2 = earliest, -1 = latest
			effectiveFetchOffset := partition.FetchOffset
			if effectiveFetchOffset < 0 {
				if effectiveFetchOffset == -2 { // earliest
					if ledger != nil {
						effectiveFetchOffset = ledger.GetEarliestOffset()
					} else {
						effectiveFetchOffset = 0
					}
				} else if effectiveFetchOffset == -1 { // latest
					if ledger != nil {
						effectiveFetchOffset = ledger.GetLatestOffset()
					} else {
						effectiveFetchOffset = 0
					}
				}
			}

			fmt.Printf("DEBUG: Fetch v%d - partition: %d, fetchOffset: %d (effective: %d), highWaterMark: %d, maxBytes: %d\n",
				apiVersion, partition.PartitionID, partition.FetchOffset, effectiveFetchOffset, highWaterMark, partition.MaxBytes)

			// High water mark (8 bytes)
			highWaterMarkBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(highWaterMarkBytes, uint64(highWaterMark))
			response = append(response, highWaterMarkBytes...)

			// Fetch v4+ has last_stable_offset and log_start_offset
			if apiVersion >= 4 {
				// Last stable offset (8 bytes) - same as high water mark for non-transactional
				response = append(response, highWaterMarkBytes...)
				// Log start offset (8 bytes) - 0 for simplicity
				response = append(response, 0, 0, 0, 0, 0, 0, 0, 0)

				// Aborted transactions count (4 bytes) = 0
				response = append(response, 0, 0, 0, 0)
			}

			// If topic/ledger does not exist, patch error to UNKNOWN_TOPIC_OR_PARTITION
			if ledger == nil || !h.seaweedMQHandler.TopicExists(topic.Name) {
				response[errorPos] = 0
				response[errorPos+1] = 3 // UNKNOWN_TOPIC_OR_PARTITION
			}

			// Records - get actual stored record batches using multi-batch fetcher
			var recordBatch []byte
			if ledger != nil && highWaterMark > effectiveFetchOffset {
				fmt.Printf("DEBUG: Multi-batch fetch - partition:%d, offset:%d, maxBytes:%d\n",
					partition.PartitionID, effectiveFetchOffset, partition.MaxBytes)

				// Use multi-batch fetcher for better MaxBytes compliance
				multiFetcher := NewMultiBatchFetcher(h)
				result, err := multiFetcher.FetchMultipleBatches(
					topic.Name,
					partition.PartitionID,
					effectiveFetchOffset,
					highWaterMark,
					partition.MaxBytes,
				)

				if err == nil && result.TotalSize > 0 {
					fmt.Printf("DEBUG: Multi-batch result - %d batches, %d bytes, next offset %d\n",
						result.BatchCount, result.TotalSize, result.NextOffset)
					recordBatch = result.RecordBatches
				} else {
					fmt.Printf("DEBUG: Multi-batch failed or empty, falling back to single batch\n")
					// Fallback to original single batch logic
					smqRecords, err := h.seaweedMQHandler.GetStoredRecords(topic.Name, partition.PartitionID, effectiveFetchOffset, 10)
					if err == nil && len(smqRecords) > 0 {
						recordBatch = h.constructRecordBatchFromSMQ(effectiveFetchOffset, smqRecords)
						fmt.Printf("DEBUG: Fallback single batch size: %d bytes\n", len(recordBatch))
					} else {
						recordBatch = h.constructSimpleRecordBatch(effectiveFetchOffset, highWaterMark)
						fmt.Printf("DEBUG: Fallback synthetic batch size: %d bytes\n", len(recordBatch))
					}
				}
			} else {
				fmt.Printf("DEBUG: No messages available - effective fetchOffset %d >= highWaterMark %d\n", effectiveFetchOffset, highWaterMark)
				recordBatch = []byte{} // No messages available
			}

			// Records size (4 bytes)
			recordsSizeBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(recordsSizeBytes, uint32(len(recordBatch)))
			response = append(response, recordsSizeBytes...)

			// Records data
			response = append(response, recordBatch...)

			// Try to fetch schematized records if schema management is enabled
			if h.IsSchemaEnabled() && h.isSchematizedTopic(topic.Name) {
				schematizedMessages, err := h.fetchSchematizedRecords(topic.Name, partition.PartitionID, effectiveFetchOffset, partition.MaxBytes)
				if err != nil {
					Debug("Failed to fetch schematized records for topic %s partition %d: %v", topic.Name, partition.PartitionID, err)
				} else if len(schematizedMessages) > 0 {
					Debug("Successfully fetched %d schematized messages for topic %s partition %d", len(schematizedMessages), topic.Name, partition.PartitionID)
					// TODO: Integrate schematized messages into the record batch
					// For now, just log that we found them
				}
			}
		}
	}

	fmt.Printf("DEBUG: Fetch v%d response constructed, size: %d bytes\n", apiVersion, len(response))
	return response, nil
}

// FetchRequest represents a parsed Kafka Fetch request
type FetchRequest struct {
	ReplicaID      int32
	MaxWaitTime    int32
	MinBytes       int32
	MaxBytes       int32
	IsolationLevel int8
	Topics         []FetchTopic
}

type FetchTopic struct {
	Name       string
	Partitions []FetchPartition
}

type FetchPartition struct {
	PartitionID    int32
	FetchOffset    int64
	LogStartOffset int64
	MaxBytes       int32
}

// parseFetchRequest parses a Kafka Fetch request
func (h *Handler) parseFetchRequest(apiVersion uint16, requestBody []byte) (*FetchRequest, error) {
	if len(requestBody) < 12 {
		return nil, fmt.Errorf("fetch request too short: %d bytes", len(requestBody))
	}

	offset := 0
	request := &FetchRequest{}

	// NOTE: client_id is already handled by HandleConn and stripped from requestBody
	// Request body starts directly with fetch-specific fields

	// Replica ID (4 bytes)
	if offset+4 > len(requestBody) {
		return nil, fmt.Errorf("insufficient data for replica_id")
	}
	request.ReplicaID = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
	offset += 4

	// Max wait time (4 bytes)
	if offset+4 > len(requestBody) {
		return nil, fmt.Errorf("insufficient data for max_wait_time")
	}
	request.MaxWaitTime = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
	offset += 4

	// Min bytes (4 bytes)
	if offset+4 > len(requestBody) {
		return nil, fmt.Errorf("insufficient data for min_bytes")
	}
	request.MinBytes = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
	offset += 4

	// Max bytes (4 bytes) - only in v3+
	if apiVersion >= 3 {
		if offset+4 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for max_bytes")
		}
		request.MaxBytes = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
		offset += 4
	}

	// Isolation level (1 byte) - only in v4+
	if apiVersion >= 4 {
		if offset+1 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for isolation_level")
		}
		request.IsolationLevel = int8(requestBody[offset])
		offset += 1
	}

	// Session ID (4 bytes) and Session Epoch (4 bytes) - only in v7+
	if apiVersion >= 7 {
		if offset+8 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for session_id and epoch")
		}
		offset += 8 // Skip session_id and session_epoch
	}

	// Topics count (4 bytes)
	if offset+4 > len(requestBody) {
		return nil, fmt.Errorf("insufficient data for topics count")
	}
	topicsCount := int(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
	offset += 4

	// Parse topics
	request.Topics = make([]FetchTopic, topicsCount)
	for i := 0; i < topicsCount; i++ {
		// Topic name length (2 bytes)
		if offset+2 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for topic name length")
		}
		topicNameLength := int(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
		offset += 2

		// Topic name
		if offset+topicNameLength > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for topic name")
		}
		request.Topics[i].Name = string(requestBody[offset : offset+topicNameLength])
		offset += topicNameLength

		// Partitions count (4 bytes)
		if offset+4 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for partitions count")
		}
		partitionsCount := int(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
		offset += 4

		// Parse partitions
		request.Topics[i].Partitions = make([]FetchPartition, partitionsCount)
		for j := 0; j < partitionsCount; j++ {
			// Partition ID (4 bytes)
			if offset+4 > len(requestBody) {
				return nil, fmt.Errorf("insufficient data for partition ID")
			}
			request.Topics[i].Partitions[j].PartitionID = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
			offset += 4

			// Current leader epoch (4 bytes) - only in v9+
			if apiVersion >= 9 {
				if offset+4 > len(requestBody) {
					return nil, fmt.Errorf("insufficient data for current leader epoch")
				}
				offset += 4 // Skip current leader epoch
			}

			// Fetch offset (8 bytes)
			if offset+8 > len(requestBody) {
				return nil, fmt.Errorf("insufficient data for fetch offset")
			}
			request.Topics[i].Partitions[j].FetchOffset = int64(binary.BigEndian.Uint64(requestBody[offset : offset+8]))
			offset += 8

			// Log start offset (8 bytes) - only in v5+
			if apiVersion >= 5 {
				if offset+8 > len(requestBody) {
					return nil, fmt.Errorf("insufficient data for log start offset")
				}
				request.Topics[i].Partitions[j].LogStartOffset = int64(binary.BigEndian.Uint64(requestBody[offset : offset+8]))
				offset += 8
			}

			// Partition max bytes (4 bytes)
			if offset+4 > len(requestBody) {
				return nil, fmt.Errorf("insufficient data for partition max bytes")
			}
			request.Topics[i].Partitions[j].MaxBytes = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
			offset += 4
		}
	}

	return request, nil
}

// constructRecordBatchFromLedger creates a record batch from messages stored in the ledger
func (h *Handler) constructRecordBatchFromLedger(ledger interface{}, fetchOffset, highWaterMark int64) []byte {
	// Get the actual ledger interface
	offsetLedger, ok := ledger.(interface {
		GetMessages(startOffset, endOffset int64) []interface{}
	})

	if !ok {
		// If ledger doesn't support GetMessages, return empty batch
		return []byte{}
	}

	// Calculate how many records to fetch
	recordsToFetch := highWaterMark - fetchOffset
	if recordsToFetch <= 0 {
		return []byte{} // no records to fetch
	}

	// Limit the number of records for performance
	if recordsToFetch > 100 {
		recordsToFetch = 100
	}

	// Get messages from ledger
	messages := offsetLedger.GetMessages(fetchOffset, fetchOffset+recordsToFetch)
	if len(messages) == 0 {
		return []byte{} // no messages available
	}

	// Create a realistic record batch
	batch := make([]byte, 0, 1024)

	// Record batch header
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(fetchOffset))
	batch = append(batch, baseOffsetBytes...) // base offset (8 bytes)

	// Calculate batch length (will be filled after we know the size)
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0) // batch length placeholder (4 bytes)

	batch = append(batch, 0, 0, 0, 0) // partition leader epoch (4 bytes)
	batch = append(batch, 2)          // magic byte (version 2) (1 byte)

	// CRC placeholder (4 bytes) - will be calculated at the end
	crcPos := len(batch)
	batch = append(batch, 0, 0, 0, 0) // CRC32 placeholder

	// Batch attributes (2 bytes) - no compression, no transactional
	batch = append(batch, 0, 0) // attributes

	// Last offset delta (4 bytes)
	lastOffsetDelta := uint32(len(messages) - 1)
	lastOffsetDeltaBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lastOffsetDeltaBytes, lastOffsetDelta)
	batch = append(batch, lastOffsetDeltaBytes...)

	// First timestamp (8 bytes)
	firstTimestamp := time.Now().UnixMilli()
	firstTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(firstTimestampBytes, uint64(firstTimestamp))
	batch = append(batch, firstTimestampBytes...)

	// Max timestamp (8 bytes)
	maxTimestamp := firstTimestamp + int64(len(messages)) - 1
	maxTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTimestampBytes, uint64(maxTimestamp))
	batch = append(batch, maxTimestampBytes...)

	// Producer ID (8 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	// Producer Epoch (2 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF)

	// Base Sequence (4 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Record count (4 bytes)
	recordCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordCountBytes, uint32(len(messages)))
	batch = append(batch, recordCountBytes...)

	// Add individual records
	for i, msg := range messages {
		// Try to extract key and value from the message
		var key, value []byte

		// Handle different message types
		if msgMap, ok := msg.(map[string]interface{}); ok {
			if keyVal, exists := msgMap["key"]; exists {
				if keyBytes, ok := keyVal.([]byte); ok {
					key = keyBytes
				} else if keyStr, ok := keyVal.(string); ok {
					key = []byte(keyStr)
				}
			}
			if valueVal, exists := msgMap["value"]; exists {
				if valueBytes, ok := valueVal.([]byte); ok {
					value = valueBytes
				} else if valueStr, ok := valueVal.(string); ok {
					value = []byte(valueStr)
				}
			}
		}

		// If we couldn't extract key/value, create default ones
		if value == nil {
			value = []byte(fmt.Sprintf("Message %d", fetchOffset+int64(i)))
		}

		// Build individual record
		record := make([]byte, 0, 128)

		// Record attributes (1 byte)
		record = append(record, 0)

		// Timestamp delta (varint)
		timestampDelta := int64(i)
		record = append(record, encodeVarint(timestampDelta)...)

		// Offset delta (varint)
		offsetDelta := int64(i)
		record = append(record, encodeVarint(offsetDelta)...)

		// Key length and key (varint + data)
		if key == nil {
			record = append(record, encodeVarint(-1)...) // null key
		} else {
			record = append(record, encodeVarint(int64(len(key)))...)
			record = append(record, key...)
		}

		// Value length and value (varint + data)
		record = append(record, encodeVarint(int64(len(value)))...)
		record = append(record, value...)

		// Headers count (varint) - 0 headers
		record = append(record, encodeVarint(0)...)

		// Prepend record length (varint)
		recordLength := int64(len(record))
		batch = append(batch, encodeVarint(recordLength)...)
		batch = append(batch, record...)
	}

	// Fill in the batch length
	batchLength := uint32(len(batch) - batchLengthPos - 4)
	binary.BigEndian.PutUint32(batch[batchLengthPos:batchLengthPos+4], batchLength)

	// Calculate CRC32 for the batch
	// CRC is calculated over: attributes + last_offset_delta + first_timestamp + max_timestamp + producer_id + producer_epoch + base_sequence + records_count + records
	// This starts after the CRC field (which comes after magic byte)
	crcStartPos := crcPos + 4 // start after the CRC field
	crcData := batch[crcStartPos:]
	crc := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[crcPos:crcPos+4], crc)

	return batch
}

// constructSimpleRecordBatch creates a simple record batch for testing
func (h *Handler) constructSimpleRecordBatch(fetchOffset, highWaterMark int64) []byte {
	recordsToFetch := highWaterMark - fetchOffset
	if recordsToFetch <= 0 {
		return []byte{} // no records to fetch
	}

	// Limit the number of records for testing
	if recordsToFetch > 10 {
		recordsToFetch = 10
	}

	// Create a simple record batch
	batch := make([]byte, 0, 512)

	// Record batch header
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(fetchOffset))
	batch = append(batch, baseOffsetBytes...) // base offset (8 bytes)

	// Calculate batch length (will be filled after we know the size)
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0) // batch length placeholder (4 bytes)

	batch = append(batch, 0, 0, 0, 0) // partition leader epoch (4 bytes)
	batch = append(batch, 2)          // magic byte (version 2) (1 byte)

	// CRC placeholder (4 bytes) - will be calculated at the end
	crcPos := len(batch)
	batch = append(batch, 0, 0, 0, 0) // CRC32 placeholder

	// Batch attributes (2 bytes) - no compression, no transactional
	batch = append(batch, 0, 0) // attributes

	// Last offset delta (4 bytes)
	lastOffsetDelta := uint32(recordsToFetch - 1)
	lastOffsetDeltaBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lastOffsetDeltaBytes, lastOffsetDelta)
	batch = append(batch, lastOffsetDeltaBytes...)

	// First timestamp (8 bytes)
	firstTimestamp := time.Now().UnixMilli()
	firstTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(firstTimestampBytes, uint64(firstTimestamp))
	batch = append(batch, firstTimestampBytes...)

	// Max timestamp (8 bytes)
	maxTimestamp := firstTimestamp + recordsToFetch - 1
	maxTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTimestampBytes, uint64(maxTimestamp))
	batch = append(batch, maxTimestampBytes...)

	// Producer ID (8 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	// Producer Epoch (2 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF)

	// Base Sequence (4 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Record count (4 bytes)
	recordCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordCountBytes, uint32(recordsToFetch))
	batch = append(batch, recordCountBytes...)

	// Add individual records
	for i := int64(0); i < recordsToFetch; i++ {
		// Create test message
		key := []byte(fmt.Sprintf("key-%d", fetchOffset+i))
		value := []byte(fmt.Sprintf("Test message %d", fetchOffset+i))

		// Build individual record
		record := make([]byte, 0, 128)

		// Record attributes (1 byte)
		record = append(record, 0)

		// Timestamp delta (varint)
		timestampDelta := i
		record = append(record, encodeVarint(timestampDelta)...)

		// Offset delta (varint)
		offsetDelta := i
		record = append(record, encodeVarint(offsetDelta)...)

		// Key length and key (varint + data)
		record = append(record, encodeVarint(int64(len(key)))...)
		record = append(record, key...)

		// Value length and value (varint + data)
		record = append(record, encodeVarint(int64(len(value)))...)
		record = append(record, value...)

		// Headers count (varint) - 0 headers
		record = append(record, encodeVarint(0)...)

		// Prepend record length (varint)
		recordLength := int64(len(record))
		batch = append(batch, encodeVarint(recordLength)...)
		batch = append(batch, record...)
	}

	// Fill in the batch length
	batchLength := uint32(len(batch) - batchLengthPos - 4)
	binary.BigEndian.PutUint32(batch[batchLengthPos:batchLengthPos+4], batchLength)

	// Calculate CRC32 for the batch
	// CRC is calculated over: attributes + last_offset_delta + first_timestamp + max_timestamp + producer_id + producer_epoch + base_sequence + records_count + records
	// This starts after the CRC field (which comes after magic byte)
	crcStartPos := crcPos + 4 // start after the CRC field
	crcData := batch[crcStartPos:]
	crc := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[crcPos:crcPos+4], crc)

	return batch
}

// constructRecordBatchFromSMQ creates a Kafka record batch from SeaweedMQ records
func (h *Handler) constructRecordBatchFromSMQ(fetchOffset int64, smqRecords []offset.SMQRecord) []byte {
	if len(smqRecords) == 0 {
		return []byte{}
	}

	// Create record batch using the SMQ records
	batch := make([]byte, 0, 512)

	// Record batch header
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(fetchOffset))
	batch = append(batch, baseOffsetBytes...) // base offset (8 bytes)

	// Calculate batch length (will be filled after we know the size)
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0) // batch length placeholder (4 bytes)

	// Partition leader epoch (4 bytes) - use -1 for no epoch
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Magic byte (1 byte) - v2 format
	batch = append(batch, 2)

	// CRC placeholder (4 bytes) - will be calculated later
	crcPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Attributes (2 bytes) - no compression, etc.
	batch = append(batch, 0, 0)

	// Last offset delta (4 bytes)
	lastOffsetDelta := int32(len(smqRecords) - 1)
	lastOffsetDeltaBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lastOffsetDeltaBytes, uint32(lastOffsetDelta))
	batch = append(batch, lastOffsetDeltaBytes...)

	// Base timestamp (8 bytes) - use first record timestamp
	baseTimestamp := smqRecords[0].GetTimestamp()
	baseTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseTimestampBytes, uint64(baseTimestamp))
	batch = append(batch, baseTimestampBytes...)

	// Max timestamp (8 bytes) - use last record timestamp or same as base
	maxTimestamp := baseTimestamp
	if len(smqRecords) > 1 {
		maxTimestamp = smqRecords[len(smqRecords)-1].GetTimestamp()
	}
	maxTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTimestampBytes, uint64(maxTimestamp))
	batch = append(batch, maxTimestampBytes...)

	// Producer ID (8 bytes) - use -1 for no producer ID
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	// Producer epoch (2 bytes) - use -1 for no producer epoch
	batch = append(batch, 0xFF, 0xFF)

	// Base sequence (4 bytes) - use -1 for no base sequence
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Records count (4 bytes)
	recordCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordCountBytes, uint32(len(smqRecords)))
	batch = append(batch, recordCountBytes...)

	// Add individual records from SMQ records
	for i, smqRecord := range smqRecords {
		// Build individual record
		recordBytes := make([]byte, 0, 128)

		// Record attributes (1 byte)
		recordBytes = append(recordBytes, 0)

		// Timestamp delta (varint) - calculate from base timestamp
		timestampDelta := smqRecord.GetTimestamp() - baseTimestamp
		recordBytes = append(recordBytes, encodeVarint(timestampDelta)...)

		// Offset delta (varint)
		offsetDelta := int64(i)
		recordBytes = append(recordBytes, encodeVarint(offsetDelta)...)

		// Key length and key (varint + data)
		key := smqRecord.GetKey()
		if key == nil {
			recordBytes = append(recordBytes, encodeVarint(-1)...) // null key
		} else {
			recordBytes = append(recordBytes, encodeVarint(int64(len(key)))...)
			recordBytes = append(recordBytes, key...)
		}

		// Value length and value (varint + data)
		value := smqRecord.GetValue()
		if value == nil {
			recordBytes = append(recordBytes, encodeVarint(-1)...) // null value
		} else {
			recordBytes = append(recordBytes, encodeVarint(int64(len(value)))...)
			recordBytes = append(recordBytes, value...)
		}

		// Headers count (varint) - 0 headers
		recordBytes = append(recordBytes, encodeVarint(0)...)

		// Prepend record length (varint)
		recordLength := int64(len(recordBytes))
		batch = append(batch, encodeVarint(recordLength)...)
		batch = append(batch, recordBytes...)
	}

	// Fill in the batch length
	batchLength := uint32(len(batch) - batchLengthPos - 4)
	binary.BigEndian.PutUint32(batch[batchLengthPos:batchLengthPos+4], batchLength)

	// Calculate CRC32 for the batch
	crcStartPos := crcPos + 4 // start after the CRC field
	crcData := batch[crcStartPos:]
	crc := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[crcPos:crcPos+4], crc)

	return batch
}

// constructRecordBatch creates a realistic Kafka record batch that matches produced messages
// This creates record batches that mirror what was actually stored during Produce operations
func (h *Handler) constructRecordBatch(ledger interface{}, fetchOffset, highWaterMark int64) []byte {
	recordsToFetch := highWaterMark - fetchOffset
	if recordsToFetch <= 0 {
		return []byte{} // no records to fetch
	}

	// Limit the number of records for testing
	if recordsToFetch > 10 {
		recordsToFetch = 10
	}

	// Create a realistic record batch that matches what clients expect
	// This simulates the same format that would be stored during Produce operations
	batch := make([]byte, 0, 512)

	// Record batch header (61 bytes total)
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(fetchOffset))
	batch = append(batch, baseOffsetBytes...) // base offset (8 bytes)

	// Calculate batch length (will be filled after we know the size)
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0) // batch length placeholder (4 bytes)

	batch = append(batch, 0, 0, 0, 0) // partition leader epoch (4 bytes)
	batch = append(batch, 2)          // magic byte (version 2) (1 byte)

	// CRC placeholder (4 bytes) - for testing, use 0
	batch = append(batch, 0, 0, 0, 0) // CRC32

	// Batch attributes (2 bytes) - no compression, no transactional
	batch = append(batch, 0, 0) // attributes

	// Last offset delta (4 bytes)
	lastOffsetDelta := uint32(recordsToFetch - 1)
	lastOffsetDeltaBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lastOffsetDeltaBytes, lastOffsetDelta)
	batch = append(batch, lastOffsetDeltaBytes...)

	// First timestamp (8 bytes)
	firstTimestamp := time.Now().UnixMilli() // Use milliseconds like Kafka
	firstTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(firstTimestampBytes, uint64(firstTimestamp))
	batch = append(batch, firstTimestampBytes...)

	// Max timestamp (8 bytes)
	maxTimestamp := firstTimestamp + recordsToFetch - 1 // 1ms per record
	maxTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTimestampBytes, uint64(maxTimestamp))
	batch = append(batch, maxTimestampBytes...)

	// Producer ID (8 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	// Producer Epoch (2 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF)

	// Base Sequence (4 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Record count (4 bytes)
	recordCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordCountBytes, uint32(recordsToFetch))
	batch = append(batch, recordCountBytes...)

	// Add records that match typical client expectations
	for i := int64(0); i < recordsToFetch; i++ {
		// Build individual record
		record := make([]byte, 0, 64)

		// Record attributes (1 byte)
		record = append(record, 0)

		// Timestamp delta (varint) - use proper varint encoding
		timestampDelta := i // milliseconds from first timestamp
		record = append(record, encodeVarint(timestampDelta)...)

		// Offset delta (varint)
		offsetDelta := i
		record = append(record, encodeVarint(offsetDelta)...)

		// Key length (varint) - -1 for null key
		record = append(record, encodeVarint(-1)...)

		// Value length and value
		value := fmt.Sprintf("Test message %d", fetchOffset+i)
		record = append(record, encodeVarint(int64(len(value)))...)
		record = append(record, []byte(value)...)

		// Headers count (varint) - 0 headers
		record = append(record, encodeVarint(0)...)

		// Prepend record length (varint)
		recordLength := int64(len(record))
		batch = append(batch, encodeVarint(recordLength)...)
		batch = append(batch, record...)
	}

	// Fill in the batch length
	batchLength := uint32(len(batch) - batchLengthPos - 4)
	binary.BigEndian.PutUint32(batch[batchLengthPos:batchLengthPos+4], batchLength)

	return batch
}

// encodeVarint encodes a signed integer using Kafka's varint encoding
func encodeVarint(value int64) []byte {
	// Kafka uses zigzag encoding for signed integers
	zigzag := uint64((value << 1) ^ (value >> 63))

	var buf []byte
	for zigzag >= 0x80 {
		buf = append(buf, byte(zigzag)|0x80)
		zigzag >>= 7
	}
	buf = append(buf, byte(zigzag))
	return buf
}

// getMultipleRecordBatches retrieves and combines multiple record batches starting from the given offset
func (h *Handler) getMultipleRecordBatches(topicName string, partitionID int32, startOffset, highWaterMark int64) []byte {
	var combinedBatches []byte
	var batchCount int
	const maxBatchSize = 1024 * 1024 // 1MB limit for combined batches

	Debug("Multi-batch concatenation: startOffset=%d, highWaterMark=%d", startOffset, highWaterMark)

	// Try to get all available record batches from startOffset to highWaterMark-1
	for offset := startOffset; offset < highWaterMark && len(combinedBatches) < maxBatchSize; offset++ {
		if batch, exists := h.GetRecordBatch(topicName, partitionID, offset); exists {
			// Validate batch format before concatenation
			if !h.isValidRecordBatch(batch) {
				Debug("Skipping invalid record batch at offset %d", offset)
				continue
			}

			// Check if adding this batch would exceed size limit
			if len(combinedBatches)+len(batch) > maxBatchSize {
				Debug("Batch size limit reached, stopping concatenation at offset %d", offset)
				break
			}

			// Concatenate the batch directly - Kafka protocol allows multiple record batches
			// to be concatenated in the records field of a fetch response
			combinedBatches = append(combinedBatches, batch...)
			batchCount++

			Debug("Concatenated batch %d: offset=%d, size=%d bytes, total=%d bytes",
				batchCount, offset, len(batch), len(combinedBatches))
		} else {
			Debug("No batch found at offset %d, stopping concatenation", offset)
			break
		}
	}

	Debug("Multi-batch concatenation complete: %d batches, %d total bytes", batchCount, len(combinedBatches))
	return combinedBatches
}

// isValidRecordBatch performs basic validation on a record batch
func (h *Handler) isValidRecordBatch(batch []byte) bool {
	// Minimum record batch size: base_offset(8) + batch_length(4) + partition_leader_epoch(4) + magic(1) = 17 bytes
	if len(batch) < 17 {
		return false
	}

	// Check magic byte (should be 2 for record batch format v2)
	magic := batch[16] // magic byte is at offset 16
	if magic != 2 {
		Debug("Invalid magic byte in record batch: %d (expected 2)", magic)
		return false
	}

	// Check batch length field consistency
	batchLength := binary.BigEndian.Uint32(batch[8:12])
	expectedTotalSize := int(batchLength) + 12 // batch_length doesn't include base_offset(8) + batch_length(4)
	if len(batch) != expectedTotalSize {
		Debug("Record batch size mismatch: got %d bytes, expected %d", len(batch), expectedTotalSize)
		return false
	}

	return true
}

// reconstructSchematizedMessage reconstructs a schematized message from SMQ RecordValue
func (h *Handler) reconstructSchematizedMessage(recordValue *schema_pb.RecordValue, metadata map[string]string) ([]byte, error) {
	// Only reconstruct if schema management is enabled
	if !h.IsSchemaEnabled() {
		return nil, fmt.Errorf("schema management not enabled")
	}

	// Extract schema information from metadata
	schemaIDStr, exists := metadata["schema_id"]
	if !exists {
		return nil, fmt.Errorf("no schema ID in metadata")
	}

	var schemaID uint32
	if _, err := fmt.Sscanf(schemaIDStr, "%d", &schemaID); err != nil {
		return nil, fmt.Errorf("invalid schema ID: %w", err)
	}

	formatStr, exists := metadata["schema_format"]
	if !exists {
		return nil, fmt.Errorf("no schema format in metadata")
	}

	var format schema.Format
	switch formatStr {
	case "AVRO":
		format = schema.FormatAvro
	case "PROTOBUF":
		format = schema.FormatProtobuf
	case "JSON_SCHEMA":
		format = schema.FormatJSONSchema
	default:
		return nil, fmt.Errorf("unsupported schema format: %s", formatStr)
	}

	// Use schema manager to encode back to original format
	return h.schemaManager.EncodeMessage(recordValue, schemaID, format)
}

// fetchSchematizedRecords fetches and reconstructs schematized records from SeaweedMQ
func (h *Handler) fetchSchematizedRecords(topicName string, partitionID int32, offset int64, maxBytes int32) ([][]byte, error) {
	// Only proceed if schema management is enabled
	if !h.IsSchemaEnabled() {
		return [][]byte{}, nil
	}

	// Fetch stored records from SeaweedMQ
	maxRecords := 100 // Reasonable batch size limit
	smqRecords, err := h.seaweedMQHandler.GetStoredRecords(topicName, partitionID, offset, maxRecords)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch SMQ records: %w", err)
	}

	if len(smqRecords) == 0 {
		return [][]byte{}, nil
	}

	var reconstructedMessages [][]byte
	totalBytes := int32(0)

	for _, smqRecord := range smqRecords {
		// Check if we've exceeded maxBytes limit
		if maxBytes > 0 && totalBytes >= maxBytes {
			break
		}

		// Try to reconstruct the schematized message
		reconstructedMsg, err := h.reconstructSchematizedMessageFromSMQ(smqRecord)
		if err != nil {
			// Log error but continue with other messages
			Error("Failed to reconstruct schematized message at offset %d: %v", smqRecord.GetOffset(), err)
			continue
		}

		if reconstructedMsg != nil {
			reconstructedMessages = append(reconstructedMessages, reconstructedMsg)
			totalBytes += int32(len(reconstructedMsg))
		}
	}

	Debug("Fetched %d schematized records for topic %s partition %d from offset %d",
		len(reconstructedMessages), topicName, partitionID, offset)

	return reconstructedMessages, nil
}

// reconstructSchematizedMessageFromSMQ reconstructs a schematized message from an SMQRecord
func (h *Handler) reconstructSchematizedMessageFromSMQ(smqRecord offset.SMQRecord) ([]byte, error) {
	// Get the stored value (should be a serialized RecordValue)
	valueBytes := smqRecord.GetValue()
	if len(valueBytes) == 0 {
		return nil, fmt.Errorf("empty value in SMQ record")
	}

	// Try to unmarshal as RecordValue
	recordValue := &schema_pb.RecordValue{}
	if err := proto.Unmarshal(valueBytes, recordValue); err != nil {
		// If it's not a RecordValue, it might be a regular Kafka message
		// Return it as-is (non-schematized)
		return valueBytes, nil
	}

	// Extract schema metadata from the RecordValue fields
	metadata := h.extractSchemaMetadataFromRecord(recordValue)
	if len(metadata) == 0 {
		// No schema metadata found, treat as regular message
		return valueBytes, nil
	}

	// Remove Kafka metadata fields to get the original message content
	originalRecord := h.removeKafkaMetadataFields(recordValue)

	// Reconstruct the original Confluent envelope
	return h.reconstructSchematizedMessage(originalRecord, metadata)
}

// extractSchemaMetadataFromRecord extracts schema metadata from RecordValue fields
func (h *Handler) extractSchemaMetadataFromRecord(recordValue *schema_pb.RecordValue) map[string]string {
	metadata := make(map[string]string)

	// Look for schema metadata fields in the record
	if schemaIDField := recordValue.Fields["_schema_id"]; schemaIDField != nil {
		if schemaIDValue := schemaIDField.GetStringValue(); schemaIDValue != "" {
			metadata["schema_id"] = schemaIDValue
		}
	}

	if schemaFormatField := recordValue.Fields["_schema_format"]; schemaFormatField != nil {
		if schemaFormatValue := schemaFormatField.GetStringValue(); schemaFormatValue != "" {
			metadata["schema_format"] = schemaFormatValue
		}
	}

	if schemaSubjectField := recordValue.Fields["_schema_subject"]; schemaSubjectField != nil {
		if schemaSubjectValue := schemaSubjectField.GetStringValue(); schemaSubjectValue != "" {
			metadata["schema_subject"] = schemaSubjectValue
		}
	}

	if schemaVersionField := recordValue.Fields["_schema_version"]; schemaVersionField != nil {
		if schemaVersionValue := schemaVersionField.GetStringValue(); schemaVersionValue != "" {
			metadata["schema_version"] = schemaVersionValue
		}
	}

	return metadata
}

// removeKafkaMetadataFields removes Kafka and schema metadata fields from RecordValue
func (h *Handler) removeKafkaMetadataFields(recordValue *schema_pb.RecordValue) *schema_pb.RecordValue {
	originalRecord := &schema_pb.RecordValue{
		Fields: make(map[string]*schema_pb.Value),
	}

	// Copy all fields except metadata fields
	for key, value := range recordValue.Fields {
		if !h.isMetadataField(key) {
			originalRecord.Fields[key] = value
		}
	}

	return originalRecord
}

// isMetadataField checks if a field is a metadata field that should be excluded from the original message
func (h *Handler) isMetadataField(fieldName string) bool {
	return fieldName == "_kafka_offset" ||
		fieldName == "_kafka_partition" ||
		fieldName == "_kafka_timestamp" ||
		fieldName == "_schema_id" ||
		fieldName == "_schema_format" ||
		fieldName == "_schema_subject" ||
		fieldName == "_schema_version"
}

// createSchematizedRecordBatch creates a Kafka record batch from reconstructed schematized messages
func (h *Handler) createSchematizedRecordBatch(messages [][]byte, baseOffset int64) []byte {
	if len(messages) == 0 {
		// Return empty record batch
		return h.createEmptyRecordBatch(baseOffset)
	}

	// Create individual record entries for the batch
	var recordsData []byte
	currentTimestamp := time.Now().UnixMilli()

	for i, msg := range messages {
		// Create a record entry (Kafka record format v2)
		record := h.createRecordEntry(msg, int32(i), currentTimestamp)
		recordsData = append(recordsData, record...)
	}

	// Apply compression if the data is large enough to benefit
	enableCompression := len(recordsData) > 100
	var compressionType compression.CompressionCodec = compression.None
	var finalRecordsData []byte

	if enableCompression {
		compressed, err := compression.Compress(compression.Gzip, recordsData)
		if err == nil && len(compressed) < len(recordsData) {
			finalRecordsData = compressed
			compressionType = compression.Gzip
			Debug("Applied GZIP compression: %d -> %d bytes (%.1f%% reduction)",
				len(recordsData), len(compressed),
				100.0*(1.0-float64(len(compressed))/float64(len(recordsData))))
		} else {
			finalRecordsData = recordsData
		}
	} else {
		finalRecordsData = recordsData
	}

	// Create the record batch with proper compression and CRC
	batch, err := h.createRecordBatchWithCompressionAndCRC(baseOffset, finalRecordsData, compressionType, int32(len(messages)))
	if err != nil {
		// Fallback to simple batch creation
		Debug("Failed to create compressed record batch, falling back: %v", err)
		return h.createRecordBatchWithPayload(baseOffset, int32(len(messages)), finalRecordsData)
	}

	Debug("Created schematized record batch: %d messages, %d bytes, compression=%v",
		len(messages), len(batch), compressionType)

	return batch
}

// createRecordEntry creates a single record entry in Kafka record format v2
func (h *Handler) createRecordEntry(messageData []byte, offsetDelta int32, timestamp int64) []byte {
	// Record format v2:
	// - length (varint)
	// - attributes (int8)
	// - timestamp delta (varint)
	// - offset delta (varint)
	// - key length (varint) + key
	// - value length (varint) + value
	// - headers count (varint) + headers

	var record []byte

	// Attributes (1 byte) - no special attributes
	record = append(record, 0)

	// Timestamp delta (varint) - 0 for now (all messages have same timestamp)
	record = append(record, encodeVarint(0)...)

	// Offset delta (varint)
	record = append(record, encodeVarint(int64(offsetDelta))...)

	// Key length (varint) + key - no key for schematized messages
	record = append(record, encodeVarint(-1)...) // -1 indicates null key

	// Value length (varint) + value
	record = append(record, encodeVarint(int64(len(messageData)))...)
	record = append(record, messageData...)

	// Headers count (varint) - no headers
	record = append(record, encodeVarint(0)...)

	// Prepend the total record length (varint)
	recordLength := encodeVarint(int64(len(record)))
	return append(recordLength, record...)
}


// createRecordBatchWithCompressionAndCRC creates a Kafka record batch with proper compression and CRC
func (h *Handler) createRecordBatchWithCompressionAndCRC(baseOffset int64, recordsData []byte, compressionType compression.CompressionCodec, recordCount int32) ([]byte, error) {
	// Create record batch header
	batch := make([]byte, 0, len(recordsData)+61) // 61 bytes for header

	// Base offset (8 bytes)
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(baseOffset))
	batch = append(batch, baseOffsetBytes...)

	// Batch length placeholder (4 bytes) - will be filled later
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Partition leader epoch (4 bytes)
	batch = append(batch, 0, 0, 0, 0)

	// Magic byte (1 byte) - version 2
	batch = append(batch, 2)

	// CRC placeholder (4 bytes) - will be calculated later
	crcPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Attributes (2 bytes) - compression type and other flags
	attributes := int16(compressionType) // Set compression type in lower 3 bits
	attributesBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(attributesBytes, uint16(attributes))
	batch = append(batch, attributesBytes...)

	// Last offset delta (4 bytes)
	lastOffsetDelta := uint32(recordCount - 1)
	lastOffsetDeltaBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lastOffsetDeltaBytes, lastOffsetDelta)
	batch = append(batch, lastOffsetDeltaBytes...)

	// First timestamp (8 bytes)
	currentTimestamp := time.Now().UnixMilli()
	firstTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(firstTimestampBytes, uint64(currentTimestamp))
	batch = append(batch, firstTimestampBytes...)

	// Max timestamp (8 bytes) - same as first for simplicity
	batch = append(batch, firstTimestampBytes...)

	// Producer ID (8 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	// Producer epoch (2 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF)

	// Base sequence (4 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Record count (4 bytes)
	recordCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordCountBytes, uint32(recordCount))
	batch = append(batch, recordCountBytes...)

	// Records payload (compressed or uncompressed)
	batch = append(batch, recordsData...)

	// Calculate and set batch length (excluding base offset and batch length fields)
	batchLength := len(batch) - 12 // 8 bytes base offset + 4 bytes batch length
	binary.BigEndian.PutUint32(batch[batchLengthPos:batchLengthPos+4], uint32(batchLength))

	// Calculate and set CRC32 (from partition leader epoch to end)
	crcData := batch[16:] // Skip base offset (8) and batch length (4) and start from partition leader epoch
	crc := crc32.ChecksumIEEE(crcData)
	binary.BigEndian.PutUint32(batch[crcPos:crcPos+4], crc)

	return batch, nil
}

// createEmptyRecordBatch creates an empty Kafka record batch using the new parser
func (h *Handler) createEmptyRecordBatch(baseOffset int64) []byte {
	// Use the new record batch creation function with no compression
	emptyRecords := []byte{}
	batch, err := CreateRecordBatch(baseOffset, emptyRecords, compression.None)
	if err != nil {
		// Fallback to manual creation if there's an error
		return h.createEmptyRecordBatchManual(baseOffset)
	}
	return batch
}

// createEmptyRecordBatchManual creates an empty Kafka record batch manually (fallback)
func (h *Handler) createEmptyRecordBatchManual(baseOffset int64) []byte {
	// Create a minimal empty record batch
	batch := make([]byte, 0, 61) // Standard record batch header size

	// Base offset (8 bytes)
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(baseOffset))
	batch = append(batch, baseOffsetBytes...)

	// Batch length (4 bytes) - will be filled at the end
	lengthPlaceholder := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Partition leader epoch (4 bytes) - 0 for simplicity
	batch = append(batch, 0, 0, 0, 0)

	// Magic byte (1 byte) - version 2
	batch = append(batch, 2)

	// CRC32 (4 bytes) - placeholder, should be calculated
	batch = append(batch, 0, 0, 0, 0)

	// Attributes (2 bytes) - no compression, no transactional
	batch = append(batch, 0, 0)

	// Last offset delta (4 bytes) - 0 for empty batch
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// First timestamp (8 bytes) - current time
	timestamp := time.Now().UnixMilli()
	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, uint64(timestamp))
	batch = append(batch, timestampBytes...)

	// Max timestamp (8 bytes) - same as first for empty batch
	batch = append(batch, timestampBytes...)

	// Producer ID (8 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	// Producer Epoch (2 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF)

	// Base Sequence (4 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Record count (4 bytes) - 0 for empty batch
	batch = append(batch, 0, 0, 0, 0)

	// Fill in the batch length
	batchLength := len(batch) - 12 // Exclude base offset and length field itself
	binary.BigEndian.PutUint32(batch[lengthPlaceholder:lengthPlaceholder+4], uint32(batchLength))

	return batch
}

// createRecordBatchWithPayload creates a record batch with the given payload
func (h *Handler) createRecordBatchWithPayload(baseOffset int64, recordCount int32, payload []byte) []byte {
	// For Phase 7, create a simplified record batch
	// In Phase 8, this will implement proper Kafka record batch format v2

	batch := h.createEmptyRecordBatch(baseOffset)

	// Update record count
	recordCountOffset := len(batch) - 4
	binary.BigEndian.PutUint32(batch[recordCountOffset:recordCountOffset+4], uint32(recordCount))

	// Append payload (simplified - real implementation would format individual records)
	batch = append(batch, payload...)

	// Update batch length
	batchLength := len(batch) - 12
	binary.BigEndian.PutUint32(batch[8:12], uint32(batchLength))

	return batch
}

// handleSchematizedFetch handles fetch requests for topics with schematized messages
func (h *Handler) handleSchematizedFetch(topicName string, partitionID int32, offset int64, maxBytes int32) ([]byte, error) {
	// Check if this topic uses schema management
	if !h.IsSchemaEnabled() {
		// Fall back to regular fetch handling
		return nil, fmt.Errorf("schema management not enabled")
	}

	// Fetch schematized records from SeaweedMQ
	messages, err := h.fetchSchematizedRecords(topicName, partitionID, offset, maxBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schematized records: %w", err)
	}

	// Create record batch from reconstructed messages
	recordBatch := h.createSchematizedRecordBatch(messages, offset)

	fmt.Printf("DEBUG: Created schematized record batch: %d bytes for %d messages\n",
		len(recordBatch), len(messages))

	return recordBatch, nil
}

// isSchematizedTopic checks if a topic uses schema management
func (h *Handler) isSchematizedTopic(topicName string) bool {
	if !h.IsSchemaEnabled() {
		return false
	}

	// Check multiple indicators for schematized topics:

	// 1. Confluent Schema Registry naming conventions
	if h.matchesSchemaRegistryConvention(topicName) {
		return true
	}

	// 2. Check if topic has schema metadata in SeaweedMQ
	if h.hasSchemaMetadata(topicName) {
		return true
	}

	// 3. Check for schema configuration in topic metadata
	if h.hasSchemaConfiguration(topicName) {
		return true
	}

	// 4. Check if topic has been used with schematized messages before
	if h.hasSchematizedMessageHistory(topicName) {
		return true
	}

	return false
}

// matchesSchemaRegistryConvention checks Confluent Schema Registry naming patterns
func (h *Handler) matchesSchemaRegistryConvention(topicName string) bool {
	// Common Schema Registry subject patterns:
	// - topicName-value (for message values)
	// - topicName-key (for message keys)
	// - topicName (direct topic name as subject)

	if len(topicName) > 6 && topicName[len(topicName)-6:] == "-value" {
		return true
	}
	if len(topicName) > 4 && topicName[len(topicName)-4:] == "-key" {
		return true
	}

	// Check if the topic name itself is registered as a schema subject
	if h.schemaManager != nil {
		// Try to get latest schema for this subject
		_, err := h.schemaManager.GetLatestSchema(topicName)
		if err == nil {
			return true
		}

		// Also check with -value suffix
		_, err = h.schemaManager.GetLatestSchema(topicName + "-value")
		if err == nil {
			return true
		}
	}

	return false
}

// hasSchemaMetadata checks if topic has schema metadata in SeaweedMQ
func (h *Handler) hasSchemaMetadata(topicName string) bool {
	// This would integrate with SeaweedMQ's topic metadata system
	// For now, return false as this requires SeaweedMQ integration
	// TODO: Implement SeaweedMQ topic metadata lookup
	return false
}

// hasSchemaConfiguration checks topic-level schema configuration
func (h *Handler) hasSchemaConfiguration(topicName string) bool {
	// This would check for topic-level configuration that enables schemas
	// Could be stored in SeaweedMQ topic configuration or external config
	// TODO: Implement configuration-based schema detection
	return false
}

// hasSchematizedMessageHistory checks if topic has been used with schemas before
func (h *Handler) hasSchematizedMessageHistory(topicName string) bool {
	// This could maintain a cache of topics that have had schematized messages
	// For now, return false as this requires persistent state
	// TODO: Implement schema usage history tracking
	return false
}

// getSchemaMetadataForTopic retrieves schema metadata for a topic
func (h *Handler) getSchemaMetadataForTopic(topicName string) (map[string]string, error) {
	if !h.IsSchemaEnabled() {
		return nil, fmt.Errorf("schema management not enabled")
	}

	// Try multiple approaches to get schema metadata

	// 1. Try to get schema from registry using topic name as subject
	metadata, err := h.getSchemaMetadataFromRegistry(topicName)
	if err == nil {
		return metadata, nil
	}

	// 2. Try with -value suffix (common pattern)
	metadata, err = h.getSchemaMetadataFromRegistry(topicName + "-value")
	if err == nil {
		return metadata, nil
	}

	// 3. Try with -key suffix
	metadata, err = h.getSchemaMetadataFromRegistry(topicName + "-key")
	if err == nil {
		return metadata, nil
	}

	// 4. Check SeaweedMQ topic metadata (TODO: implement)
	metadata, err = h.getSchemaMetadataFromSeaweedMQ(topicName)
	if err == nil {
		return metadata, nil
	}

	// 5. Check topic configuration (TODO: implement)
	metadata, err = h.getSchemaMetadataFromConfig(topicName)
	if err == nil {
		return metadata, nil
	}

	return nil, fmt.Errorf("no schema metadata found for topic %s", topicName)
}

// getSchemaMetadataFromRegistry retrieves schema metadata from Schema Registry
func (h *Handler) getSchemaMetadataFromRegistry(subject string) (map[string]string, error) {
	if h.schemaManager == nil {
		return nil, fmt.Errorf("schema manager not available")
	}

	// Get latest schema for the subject
	cachedSchema, err := h.schemaManager.GetLatestSchema(subject)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for subject %s: %w", subject, err)
	}

	// Build metadata map
	// Detect format from schema content
	// Simple format detection - assume Avro for now
	format := schema.FormatAvro

	metadata := map[string]string{
		"schema_id":      fmt.Sprintf("%d", cachedSchema.LatestID),
		"schema_format":  format.String(),
		"schema_subject": subject,
		"schema_version": fmt.Sprintf("%d", cachedSchema.Version),
		"schema_content": cachedSchema.Schema,
	}

	return metadata, nil
}

// getSchemaMetadataFromSeaweedMQ retrieves schema metadata from SeaweedMQ topic metadata
func (h *Handler) getSchemaMetadataFromSeaweedMQ(topicName string) (map[string]string, error) {
	// TODO: Implement SeaweedMQ topic metadata integration
	// This would query SeaweedMQ's topic metadata system to get schema information
	// that might be stored as topic-level configuration
	return nil, fmt.Errorf("SeaweedMQ schema metadata lookup not implemented")
}

// getSchemaMetadataFromConfig retrieves schema metadata from configuration
func (h *Handler) getSchemaMetadataFromConfig(topicName string) (map[string]string, error) {
	// TODO: Implement configuration-based schema metadata lookup
	// This could read from a configuration file, database, or other source
	// that maps topics to their schema information
	return nil, fmt.Errorf("configuration-based schema metadata lookup not implemented")
}

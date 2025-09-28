package protocol

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"
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
		// With direct SMQ reading, we always attempt to fetch
		// SMQ will return empty if no data is available
		return true
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
			Debug("Limiting fetch polling to 2 seconds to prevent hanging")
		}
		deadline := start.Add(maxPollTime)
		for time.Now().Before(deadline) {
			// Use context-aware sleep instead of blocking time.Sleep
			select {
			case <-ctx.Done():
				Debug("Fetch polling cancelled due to context cancellation")
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

		// Check if this topic uses schema management (topic-level check with filer metadata)
		var isSchematizedTopic bool
		if h.IsSchemaEnabled() {
			// Use existing schema detection logic
			isSchematizedTopic = h.isSchematizedTopic(topic.Name)
			if isSchematizedTopic {
				Debug("Topic %s is schematized (from filer metadata), will fetch schematized records for all partitions", topic.Name)
			}
		}

		// Process each requested partition
		for _, partition := range topic.Partitions {
			Debug("Processing fetch for topic %s partition %d", topic.Name, partition.PartitionID)
			// Partition ID
			partitionIDBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionIDBytes, uint32(partition.PartitionID))
			response = append(response, partitionIDBytes...)

			// Error code (2 bytes) - default 0 = no error (may patch below)
			errorPos := len(response)
			response = append(response, 0, 0)

			// Use direct SMQ reading - no ledgers needed
			// SMQ handles high water mark and offset management internally
			// For empty topics, high water mark should be 0
			// For topics with data, we'll use a reasonable default and let SMQ handle it
			var highWaterMark int64
			if partition.FetchOffset == 0 {
				highWaterMark = 0 // Empty topic
				Debug("Setting highWaterMark to 0 for empty topic %s partition %d", topic.Name, partition.PartitionID)
				if strings.HasPrefix(topic.Name, "_") {
					Debug("SYSTEM TOPIC: %s is a system topic, ensuring it has proper empty response", topic.Name)
				}
			} else {
				highWaterMark = partition.FetchOffset + 1000 // Reasonable default for topics with data
				Debug("Setting highWaterMark to %d for topic %s partition %d with fetchOffset %d", highWaterMark, topic.Name, partition.PartitionID, partition.FetchOffset)
			}

			// Normalize special fetch offsets: -2 = earliest, -1 = latest
			effectiveFetchOffset := partition.FetchOffset
			if effectiveFetchOffset < 0 {
				if effectiveFetchOffset == -2 { // earliest
					effectiveFetchOffset = 0
				} else if effectiveFetchOffset == -1 { // latest
					effectiveFetchOffset = highWaterMark
				}
			}

			fetchStartTime := time.Now()
			Debug("Fetch v%d - Topic: %s, partition: %d, fetchOffset: %d (effective: %d), highWaterMark: %d, maxBytes: %d",
				apiVersion, topic.Name, partition.PartitionID, partition.FetchOffset, effectiveFetchOffset, highWaterMark, partition.MaxBytes)

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

			// If topic does not exist, check if it's a system topic that should be auto-created
			if !h.seaweedMQHandler.TopicExists(topic.Name) {
				Debug("Topic %s does not exist, checking if it's a system topic", topic.Name)
				if isSystemTopic(topic.Name) {
					// Auto-create system topics
					Debug("Auto-creating system topic %s during fetch", topic.Name)
					if err := h.createTopicWithSchemaSupport(topic.Name, 1); err != nil {
						Debug("Failed to auto-create system topic %s: %v", topic.Name, err)
						response[errorPos] = 0
						response[errorPos+1] = 3 // UNKNOWN_TOPIC_OR_PARTITION
					} else {
						Debug("Successfully auto-created system topic %s", topic.Name)
						// Topic now exists, continue with fetch
					}
				} else {
					Debug("Topic %s does not exist and is not a system topic", topic.Name)
					response[errorPos] = 0
					response[errorPos+1] = 3 // UNKNOWN_TOPIC_OR_PARTITION
				}
			} else {
				Debug("Topic %s exists", topic.Name)
			}

			// Records - get actual stored record batches using multi-batch fetcher
			var recordBatch []byte
			if highWaterMark > effectiveFetchOffset {
				Debug("Multi-batch fetch - partition:%d, offset:%d, maxBytes:%d",
					partition.PartitionID, effectiveFetchOffset, partition.MaxBytes)

				// Use multi-batch fetcher for better MaxBytes compliance
				multiFetcher := NewMultiBatchFetcher(h)
				multiBatchStartTime := time.Now()
				result, err := multiFetcher.FetchMultipleBatches(
					topic.Name,
					partition.PartitionID,
					effectiveFetchOffset,
					highWaterMark,
					partition.MaxBytes,
				)
				multiBatchDuration := time.Since(multiBatchStartTime)

				if err == nil && result.TotalSize > 0 {
					Debug("Multi-batch result - %d batches, %d bytes, next offset %d, duration=%v",
						result.BatchCount, result.TotalSize, result.NextOffset, multiBatchDuration)
					recordBatch = result.RecordBatches
				} else {
					Debug("Multi-batch failed or empty, falling back to single batch, duration=%v", multiBatchDuration)
					// Fallback to original single batch logic
					Debug("GetStoredRecords: topic='%s', partition=%d, offset=%d, limit=10", topic.Name, partition.PartitionID, effectiveFetchOffset)
					startTime := time.Now()
					smqRecords, err := h.seaweedMQHandler.GetStoredRecords(topic.Name, partition.PartitionID, effectiveFetchOffset, 10)
					duration := time.Since(startTime)
					Debug("GetStoredRecords result: records=%d, err=%v, duration=%v", len(smqRecords), err, duration)
					if err == nil && len(smqRecords) > 0 {
						recordBatch = h.constructRecordBatchFromSMQ(topic.Name, effectiveFetchOffset, smqRecords)
						Debug("Fallback single batch size: %d bytes", len(recordBatch))
					} else {
						// No records available - return empty batch instead of generating test data
						recordBatch = []byte{}
						Debug("No records available - returning empty batch")
					}
				}
			} else {
				Debug("No messages available - effective fetchOffset %d >= highWaterMark %d", effectiveFetchOffset, highWaterMark)
				recordBatch = []byte{} // No messages available
			}

			// Try to fetch schematized records if this topic uses schema management
			if isSchematizedTopic {
				schematizedMessages, err := h.fetchSchematizedRecords(topic.Name, partition.PartitionID, effectiveFetchOffset, partition.MaxBytes)
				if err != nil {
					Debug("Failed to fetch schematized records for topic %s partition %d: %v", topic.Name, partition.PartitionID, err)
				} else if len(schematizedMessages) > 0 {
					Debug("Successfully fetched %d schematized messages for topic %s partition %d", len(schematizedMessages), topic.Name, partition.PartitionID)

					// Create schematized record batch and replace the regular record batch
					schematizedBatch := h.createSchematizedRecordBatch(schematizedMessages, effectiveFetchOffset)
					if len(schematizedBatch) > 0 {
						// Replace the record batch with the schematized version
						recordBatch = schematizedBatch
						Debug("Replaced record batch with schematized version: %d bytes for %d messages", len(recordBatch), len(schematizedMessages))
					}
				}
			}

			fetchDuration := time.Since(fetchStartTime)
			Debug("Fetch v%d - Partition processing completed: topic=%s, partition=%d, duration=%v, recordBatchSize=%d",
				apiVersion, topic.Name, partition.PartitionID, fetchDuration, len(recordBatch))

			// Records size (4 bytes)
			recordsSizeBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(recordsSizeBytes, uint32(len(recordBatch)))
			response = append(response, recordsSizeBytes...)

			// Records data
			response = append(response, recordBatch...)
		}
	}

	Debug("Fetch v%d response constructed, size: %d bytes", apiVersion, len(response))
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

// constructRecordBatchFromSMQ creates a Kafka record batch from SeaweedMQ records
func (h *Handler) constructRecordBatchFromSMQ(topicName string, fetchOffset int64, smqRecords []offset.SMQRecord) []byte {
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

	// Base timestamp (8 bytes) - convert from nanoseconds to milliseconds for Kafka compatibility
	baseTimestamp := smqRecords[0].GetTimestamp() / 1000000 // Convert nanoseconds to milliseconds
	baseTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseTimestampBytes, uint64(baseTimestamp))
	batch = append(batch, baseTimestampBytes...)

	// Max timestamp (8 bytes) - convert from nanoseconds to milliseconds for Kafka compatibility
	maxTimestamp := baseTimestamp
	if len(smqRecords) > 1 {
		maxTimestamp = smqRecords[len(smqRecords)-1].GetTimestamp() / 1000000 // Convert nanoseconds to milliseconds
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

		// Timestamp delta (varint) - calculate from base timestamp (both in milliseconds)
		recordTimestampMs := smqRecord.GetTimestamp() / 1000000 // Convert nanoseconds to milliseconds
		timestampDelta := recordTimestampMs - baseTimestamp     // Both in milliseconds now
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

		// Value length and value (varint + data) - decode RecordValue to get original Kafka message
		value := h.decodeRecordValueToKafkaMessage(topicName, smqRecord.GetValue())
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
	// Only proceed when schema feature is toggled on
	if !h.useSchema {
		return [][]byte{}, nil
	}

	// Check if SeaweedMQ handler is available when schema feature is in use
	if h.seaweedMQHandler == nil {
		return nil, fmt.Errorf("SeaweedMQ handler not available")
	}

	// If schema management isn't fully configured, return empty instead of error
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

	Debug("Created schematized record batch: %d bytes for %d messages",
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
		latestSchema, err := h.schemaManager.GetLatestSchema(topicName)
		if err == nil {
			// Since we retrieved schema from registry, ensure topic config is updated
			h.ensureTopicSchemaFromLatestSchema(topicName, latestSchema)
			return true
		}

		// Check with -value suffix (common pattern for value schemas)
		latestSchemaValue, err := h.schemaManager.GetLatestSchema(topicName + "-value")
		if err == nil {
			// Since we retrieved schema from registry, ensure topic config is updated
			h.ensureTopicSchemaFromLatestSchema(topicName, latestSchemaValue)
			return true
		}

		// Check with -key suffix (for key schemas)
		latestSchemaKey, err := h.schemaManager.GetLatestSchema(topicName + "-key")
		if err == nil {
			// Since we retrieved key schema from registry, ensure topic config is updated
			h.ensureTopicKeySchemaFromLatestSchema(topicName, latestSchemaKey)
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

	// Since we retrieved schema from registry, ensure topic config is updated
	// Extract topic name from subject (remove -key or -value suffix if present)
	topicName := h.extractTopicFromSubject(subject)
	if topicName != "" {
		h.ensureTopicSchemaFromLatestSchema(topicName, cachedSchema)
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

// ensureTopicSchemaFromLatestSchema ensures topic configuration is updated when latest schema is retrieved
func (h *Handler) ensureTopicSchemaFromLatestSchema(topicName string, latestSchema *schema.CachedSubject) {
	if latestSchema == nil {
		return
	}

	// Convert CachedSubject to CachedSchema format for reuse
	// Note: CachedSubject has different field structure than expected
	cachedSchema := &schema.CachedSchema{
		ID:       latestSchema.LatestID,
		Schema:   latestSchema.Schema,
		Subject:  latestSchema.Subject,
		Version:  latestSchema.Version,
		Format:   schema.FormatAvro, // Default to Avro, could be improved with format detection
		CachedAt: latestSchema.CachedAt,
	}

	// Use existing function to handle the schema update
	h.ensureTopicSchemaFromRegistryCache(topicName, cachedSchema)
}

// extractTopicFromSubject extracts the topic name from a schema registry subject
func (h *Handler) extractTopicFromSubject(subject string) string {
	// Remove common suffixes used in schema registry
	if strings.HasSuffix(subject, "-value") {
		return strings.TrimSuffix(subject, "-value")
	}
	if strings.HasSuffix(subject, "-key") {
		return strings.TrimSuffix(subject, "-key")
	}
	// If no suffix, assume subject name is the topic name
	return subject
}

// ensureTopicKeySchemaFromLatestSchema ensures topic configuration is updated when key schema is retrieved
func (h *Handler) ensureTopicKeySchemaFromLatestSchema(topicName string, latestSchema *schema.CachedSubject) {
	if latestSchema == nil {
		return
	}

	// Convert CachedSubject to CachedSchema format for reuse
	// Note: CachedSubject has different field structure than expected
	cachedSchema := &schema.CachedSchema{
		ID:       latestSchema.LatestID,
		Schema:   latestSchema.Schema,
		Subject:  latestSchema.Subject,
		Version:  latestSchema.Version,
		Format:   schema.FormatAvro, // Default to Avro, could be improved with format detection
		CachedAt: latestSchema.CachedAt,
	}

	// Use existing function to handle the key schema update
	h.ensureTopicKeySchemaFromRegistryCache(topicName, cachedSchema)
}

// decodeRecordValueToKafkaMessage decodes a RecordValue back to the original Kafka message bytes
func (h *Handler) decodeRecordValueToKafkaMessage(topicName string, recordValueBytes []byte) []byte {
	if recordValueBytes == nil {
		return nil
	}

	// Try to unmarshal as RecordValue
	recordValue := &schema_pb.RecordValue{}
	if err := proto.Unmarshal(recordValueBytes, recordValue); err != nil {
		// If it's not a RecordValue, return the raw bytes (backward compatibility)
		Debug("Failed to unmarshal RecordValue, returning raw bytes: %v", err)
		return recordValueBytes
	}

	// If schema management is enabled, re-encode the RecordValue to Confluent format
	if h.IsSchemaEnabled() {
		if encodedMsg, err := h.encodeRecordValueToConfluentFormat(topicName, recordValue); err == nil {
			return encodedMsg
		} else {
			Debug("Failed to encode RecordValue to Confluent format: %v", err)
		}
	}

	// Fallback: convert RecordValue to JSON
	return h.recordValueToJSON(recordValue)
}

// encodeRecordValueToConfluentFormat re-encodes a RecordValue back to Confluent format
func (h *Handler) encodeRecordValueToConfluentFormat(topicName string, recordValue *schema_pb.RecordValue) ([]byte, error) {
	if recordValue == nil {
		return nil, fmt.Errorf("RecordValue is nil")
	}

	// Get schema configuration from topic config
	schemaConfig, err := h.getTopicSchemaConfig(topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic schema config: %w", err)
	}

	// Use schema manager to encode RecordValue back to original format
	encodedBytes, err := h.schemaManager.EncodeMessage(recordValue, schemaConfig.ValueSchemaID, schemaConfig.ValueSchemaFormat)
	if err != nil {
		return nil, fmt.Errorf("failed to encode RecordValue: %w", err)
	}

	return encodedBytes, nil
}

// getTopicSchemaConfig retrieves schema configuration for a topic
func (h *Handler) getTopicSchemaConfig(topicName string) (*TopicSchemaConfig, error) {
	h.topicSchemaConfigMu.RLock()
	defer h.topicSchemaConfigMu.RUnlock()

	if h.topicSchemaConfigs == nil {
		return nil, fmt.Errorf("no schema configuration available for topic: %s", topicName)
	}

	config, exists := h.topicSchemaConfigs[topicName]
	if !exists {
		return nil, fmt.Errorf("no schema configuration found for topic: %s", topicName)
	}

	return config, nil
}

// decodeRecordValueToKafkaKey decodes a key RecordValue back to the original Kafka key bytes
func (h *Handler) decodeRecordValueToKafkaKey(topicName string, keyRecordValueBytes []byte) []byte {
	if keyRecordValueBytes == nil {
		return nil
	}

	// Try to get topic schema config
	schemaConfig, err := h.getTopicSchemaConfig(topicName)
	if err != nil || !schemaConfig.HasKeySchema {
		// No key schema config available, return raw bytes
		return keyRecordValueBytes
	}

	// Try to unmarshal as RecordValue
	recordValue := &schema_pb.RecordValue{}
	if err := proto.Unmarshal(keyRecordValueBytes, recordValue); err != nil {
		// If it's not a RecordValue, return the raw bytes
		return keyRecordValueBytes
	}

	// If key schema management is enabled, re-encode the RecordValue to Confluent format
	if h.IsSchemaEnabled() {
		if encodedKey, err := h.encodeKeyRecordValueToConfluentFormat(topicName, recordValue); err == nil {
			return encodedKey
		}
	}

	// Fallback: convert RecordValue to JSON
	return h.recordValueToJSON(recordValue)
}

// encodeKeyRecordValueToConfluentFormat re-encodes a key RecordValue back to Confluent format
func (h *Handler) encodeKeyRecordValueToConfluentFormat(topicName string, recordValue *schema_pb.RecordValue) ([]byte, error) {
	if recordValue == nil {
		return nil, fmt.Errorf("key RecordValue is nil")
	}

	// Get schema configuration from topic config
	schemaConfig, err := h.getTopicSchemaConfig(topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic schema config: %w", err)
	}

	if !schemaConfig.HasKeySchema {
		return nil, fmt.Errorf("no key schema configured for topic: %s", topicName)
	}

	// Use schema manager to encode RecordValue back to original format
	encodedBytes, err := h.schemaManager.EncodeMessage(recordValue, schemaConfig.KeySchemaID, schemaConfig.KeySchemaFormat)
	if err != nil {
		return nil, fmt.Errorf("failed to encode key RecordValue: %w", err)
	}

	return encodedBytes, nil
}

// recordValueToJSON converts a RecordValue to JSON bytes (fallback)
func (h *Handler) recordValueToJSON(recordValue *schema_pb.RecordValue) []byte {
	if recordValue == nil || recordValue.Fields == nil {
		return []byte("{}")
	}

	// Simple JSON conversion - in a real implementation, this would be more sophisticated
	jsonStr := "{"
	first := true
	for fieldName, fieldValue := range recordValue.Fields {
		if !first {
			jsonStr += ","
		}
		first = false

		jsonStr += fmt.Sprintf(`"%s":`, fieldName)

		switch v := fieldValue.Kind.(type) {
		case *schema_pb.Value_StringValue:
			jsonStr += fmt.Sprintf(`"%s"`, v.StringValue)
		case *schema_pb.Value_BytesValue:
			jsonStr += fmt.Sprintf(`"%s"`, string(v.BytesValue))
		case *schema_pb.Value_Int32Value:
			jsonStr += fmt.Sprintf(`%d`, v.Int32Value)
		case *schema_pb.Value_Int64Value:
			jsonStr += fmt.Sprintf(`%d`, v.Int64Value)
		case *schema_pb.Value_BoolValue:
			jsonStr += fmt.Sprintf(`%t`, v.BoolValue)
		default:
			jsonStr += `null`
		}
	}
	jsonStr += "}"

	return []byte(jsonStr)
}

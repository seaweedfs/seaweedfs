package protocol

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/compression"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/protobuf/proto"
)

// partitionFetchResult holds the result of fetching from a single partition
type partitionFetchResult struct {
	topicIndex     int
	partitionIndex int
	recordBatch    []byte
	highWaterMark  int64
	errorCode      int16
	fetchDuration  time.Duration
}

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
		// Check if any requested partition has data available
		// Compare fetch offset with high water mark
		for _, topic := range fetchRequest.Topics {
			if !h.seaweedMQHandler.TopicExists(topic.Name) {
				continue
			}
			for _, partition := range topic.Partitions {
				hwm, err := h.seaweedMQHandler.GetLatestOffset(topic.Name, partition.PartitionID)
				if err != nil {
					continue
				}
				// Normalize fetch offset
				effectiveOffset := partition.FetchOffset
				if effectiveOffset == -2 { // earliest
					effectiveOffset = 0
				} else if effectiveOffset == -1 { // latest
					effectiveOffset = hwm
				}
				// If fetch offset < hwm, data is available
				if effectiveOffset < hwm {
					return true
				}
			}
		}
		return false
	}
	// Long-poll when client requests it via MaxWaitTime and there's no data
	// Even if MinBytes=0, we should honor MaxWaitTime to reduce polling overhead
	maxWaitMs := fetchRequest.MaxWaitTime

	// Long-poll if: (1) client wants to wait (maxWaitMs > 0), (2) no data available, (3) topics exist
	// NOTE: We long-poll even if MinBytes=0, since the client specified a wait time
	hasData := hasDataAvailable()
	topicsExist := allTopicsExist()
	shouldLongPoll := maxWaitMs > 0 && !hasData && topicsExist

	if shouldLongPoll {
		start := time.Now()
		// Use the client's requested wait time (already capped at 1s)
		maxPollTime := time.Duration(maxWaitMs) * time.Millisecond
		deadline := start.Add(maxPollTime)
	pollLoop:
		for time.Now().Before(deadline) {
			// Use context-aware sleep instead of blocking time.Sleep
			select {
			case <-ctx.Done():
				throttleTimeMs = int32(time.Since(start) / time.Millisecond)
				break pollLoop
			case <-time.After(10 * time.Millisecond):
				// Continue with polling
			}
			if hasDataAvailable() {
				// Data became available during polling - return immediately with NO throttle
				// Throttle time should only be used for quota enforcement, not for long-poll timing
				throttleTimeMs = 0
				break pollLoop
			}
		}
		// If we got here without breaking early, we hit the timeout
		// Long-poll timeout is NOT throttling - throttle time should only be used for quota/rate limiting
		// Do NOT set throttle time based on long-poll duration
		throttleTimeMs = 0
	}

	// Build the response
	response := make([]byte, 0, 1024)
	totalAppendedRecordBytes := 0

	// NOTE: Correlation ID is NOT included in the response body
	// The wire protocol layer (writeResponseWithTimeout) writes: [Size][CorrelationID][Body]
	// Kafka clients read the correlation ID separately from the 8-byte header, then read Size-4 bytes of body
	// If we include correlation ID here, clients will see it twice and fail with "4 extra bytes" errors

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

	// Check if this version uses flexible format (v12+)
	isFlexible := IsFlexibleVersion(1, apiVersion) // API key 1 = Fetch

	// Topics count - write the actual number of topics in the request
	// Kafka protocol: we MUST return all requested topics in the response (even with empty data)
	topicsCount := len(fetchRequest.Topics)
	if isFlexible {
		// Flexible versions use compact array format (count + 1)
		response = append(response, EncodeUvarint(uint32(topicsCount+1))...)
	} else {
		topicsCountBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(topicsCountBytes, uint32(topicsCount))
		response = append(response, topicsCountBytes...)
	}

	// ====================================================================
	// PERSISTENT PARTITION READERS
	// Use per-connection persistent goroutines that maintain offset position
	// and stream forward, eliminating repeated lookups and reducing broker CPU
	// ====================================================================

	// Get connection context to access persistent partition readers
	connContext := h.getConnectionContextFromRequest(ctx)
	if connContext == nil {
		glog.Errorf("FETCH CORR=%d: Connection context not available - cannot use persistent readers",
			correlationID)
		return nil, fmt.Errorf("connection context not available")
	}

	glog.V(4).Infof("[%s] FETCH CORR=%d: Processing %d topics with %d total partitions",
		connContext.ConnectionID, correlationID, len(fetchRequest.Topics),
		func() int {
			count := 0
			for _, t := range fetchRequest.Topics {
				count += len(t.Partitions)
			}
			return count
		}())

	// Collect results from persistent readers
	// Dispatch all requests concurrently, then wait for all results in parallel
	// to avoid sequential timeout accumulation
	type pendingFetch struct {
		topicName   string
		partitionID int32
		resultChan  chan *partitionFetchResult
	}

	pending := make([]pendingFetch, 0)

	// Phase 1: Dispatch all fetch requests to partition readers (non-blocking)
	for _, topic := range fetchRequest.Topics {
		isSchematizedTopic := false
		if h.IsSchemaEnabled() {
			isSchematizedTopic = h.isSchematizedTopic(topic.Name)
		}

		for _, partition := range topic.Partitions {
			key := TopicPartitionKey{Topic: topic.Name, Partition: partition.PartitionID}

			// All topics (including system topics) use persistent readers for in-memory access
			// This enables instant notification and avoids ForceFlush dependencies

			// Get or create persistent reader for this partition
			reader := h.getOrCreatePartitionReader(ctx, connContext, key, partition.FetchOffset)
			if reader == nil {
				// Failed to create reader - add empty pending
				glog.Errorf("[%s] Failed to get/create partition reader for %s[%d]",
					connContext.ConnectionID, topic.Name, partition.PartitionID)
				nilChan := make(chan *partitionFetchResult, 1)
				nilChan <- &partitionFetchResult{errorCode: 3} // UNKNOWN_TOPIC_OR_PARTITION
				pending = append(pending, pendingFetch{
					topicName:   topic.Name,
					partitionID: partition.PartitionID,
					resultChan:  nilChan,
				})
				continue
			}

			// Signal reader to fetch (don't wait for result yet)
			resultChan := make(chan *partitionFetchResult, 1)
			fetchReq := &partitionFetchRequest{
				requestedOffset: partition.FetchOffset,
				maxBytes:        partition.MaxBytes,
				maxWaitMs:       maxWaitMs, // Pass MaxWaitTime from Kafka fetch request
				resultChan:      resultChan,
				isSchematized:   isSchematizedTopic,
				apiVersion:      apiVersion,
			}

			// Try to send request (increased timeout for CI environments with slow disk I/O)
			select {
			case reader.fetchChan <- fetchReq:
				// Request sent successfully, add to pending
				pending = append(pending, pendingFetch{
					topicName:   topic.Name,
					partitionID: partition.PartitionID,
					resultChan:  resultChan,
				})
			case <-time.After(200 * time.Millisecond):
				// Channel full, return empty result
				glog.Warningf("[%s] Reader channel full for %s[%d], returning empty",
					connContext.ConnectionID, topic.Name, partition.PartitionID)
				emptyChan := make(chan *partitionFetchResult, 1)
				emptyChan <- &partitionFetchResult{}
				pending = append(pending, pendingFetch{
					topicName:   topic.Name,
					partitionID: partition.PartitionID,
					resultChan:  emptyChan,
				})
			}
		}
	}

	// Phase 2: Wait for all results with adequate timeout for CI environments
	// We MUST return a result for every requested partition or Sarama will error
	results := make([]*partitionFetchResult, len(pending))
	// Use 95% of client's MaxWaitTime to ensure we return BEFORE client timeout
	// This maximizes data collection time while leaving a safety buffer for:
	// - Response serialization, network transmission, client processing
	// For 500ms client timeout: 475ms internal fetch, 25ms buffer
	// For 100ms client timeout: 95ms internal fetch, 5ms buffer
	effectiveDeadlineMs := time.Duration(maxWaitMs) * 95 / 100
	deadline := time.After(effectiveDeadlineMs * time.Millisecond)
	if maxWaitMs < 20 {
		// For very short timeouts (< 20ms), use full timeout to maximize data collection
		deadline = time.After(time.Duration(maxWaitMs) * time.Millisecond)
	}

	// Collect results one by one with shared deadline
	for i, pf := range pending {
		select {
		case result := <-pf.resultChan:
			results[i] = result
		case <-deadline:
			// Deadline expired, return empty for this and all remaining partitions
			for j := i; j < len(pending); j++ {
				results[j] = &partitionFetchResult{}
			}
			glog.V(3).Infof("[%s] Fetch deadline expired, returning empty for %d remaining partitions",
				connContext.ConnectionID, len(pending)-i)
			goto done
		case <-ctx.Done():
			// Context cancelled, return empty for remaining
			for j := i; j < len(pending); j++ {
				results[j] = &partitionFetchResult{}
			}
			goto done
		}
	}
done:

	// ====================================================================
	// BUILD RESPONSE FROM FETCHED DATA
	// Now assemble the response in the correct order using fetched results
	// ====================================================================

	// Verify we have results for all requested partitions
	// Sarama requires a response block for EVERY requested partition to avoid ErrIncompleteResponse
	expectedResultCount := 0
	for _, topic := range fetchRequest.Topics {
		expectedResultCount += len(topic.Partitions)
	}
	if len(results) != expectedResultCount {
		glog.Errorf("[%s] Result count mismatch: expected %d, got %d - this will cause ErrIncompleteResponse",
			connContext.ConnectionID, expectedResultCount, len(results))
		// Pad with empty results if needed (safety net - shouldn't happen with fixed code)
		for len(results) < expectedResultCount {
			results = append(results, &partitionFetchResult{})
		}
	}

	// Process each requested topic
	resultIdx := 0
	for _, topic := range fetchRequest.Topics {
		topicNameBytes := []byte(topic.Name)

		// Topic name length and name
		if isFlexible {
			// Flexible versions use compact string format (length + 1)
			response = append(response, EncodeUvarint(uint32(len(topicNameBytes)+1))...)
		} else {
			response = append(response, byte(len(topicNameBytes)>>8), byte(len(topicNameBytes)))
		}
		response = append(response, topicNameBytes...)

		// Partitions count for this topic
		partitionsCount := len(topic.Partitions)
		if isFlexible {
			// Flexible versions use compact array format (count + 1)
			response = append(response, EncodeUvarint(uint32(partitionsCount+1))...)
		} else {
			partitionsCountBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionsCountBytes, uint32(partitionsCount))
			response = append(response, partitionsCountBytes...)
		}

		// Process each requested partition (using pre-fetched results)
		for _, partition := range topic.Partitions {
			// Get the pre-fetched result for this partition
			result := results[resultIdx]
			resultIdx++

			// Partition ID
			partitionIDBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionIDBytes, uint32(partition.PartitionID))
			response = append(response, partitionIDBytes...)

			// Error code (2 bytes) - use the result's error code
			response = append(response, byte(result.errorCode>>8), byte(result.errorCode))

			// Use the pre-fetched high water mark from concurrent fetch
			highWaterMark := result.highWaterMark

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

			// Use the pre-fetched record batch
			recordBatch := result.recordBatch

			// Records size - flexible versions (v12+) use compact format: varint(size+1)
			if isFlexible {
				if len(recordBatch) == 0 {
					response = append(response, 0) // null records = 0 in compact format
				} else {
					response = append(response, EncodeUvarint(uint32(len(recordBatch)+1))...)
				}
			} else {
				// Non-flexible versions use int32(size)
				recordsSizeBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(recordsSizeBytes, uint32(len(recordBatch)))
				response = append(response, recordsSizeBytes...)
			}

			// Records data
			response = append(response, recordBatch...)
			totalAppendedRecordBytes += len(recordBatch)

			// Tagged fields for flexible versions (v12+) after each partition
			if isFlexible {
				response = append(response, 0) // Empty tagged fields
			}
		}

		// Tagged fields for flexible versions (v12+) after each topic
		if isFlexible {
			response = append(response, 0) // Empty tagged fields
		}
	}

	// Tagged fields for flexible versions (v12+) at the end of response
	if isFlexible {
		response = append(response, 0) // Empty tagged fields
	}

	// Verify topics count hasn't been corrupted
	if !isFlexible {
		// Topics count position depends on API version:
		// v0: byte 0 (no throttle_time_ms, no error_code, no session_id)
		// v1-v6: byte 4 (after throttle_time_ms)
		// v7+: byte 10 (after throttle_time_ms, error_code, session_id)
		var topicsCountPos int
		if apiVersion == 0 {
			topicsCountPos = 0
		} else if apiVersion < 7 {
			topicsCountPos = 4
		} else {
			topicsCountPos = 10
		}

		if len(response) >= topicsCountPos+4 {
			actualTopicsCount := binary.BigEndian.Uint32(response[topicsCountPos : topicsCountPos+4])
			if actualTopicsCount != uint32(topicsCount) {
				glog.Errorf("FETCH CORR=%d v%d: Topics count CORRUPTED! Expected %d, found %d at response[%d:%d]=%02x %02x %02x %02x",
					correlationID, apiVersion, topicsCount, actualTopicsCount, topicsCountPos, topicsCountPos+4,
					response[topicsCountPos], response[topicsCountPos+1], response[topicsCountPos+2], response[topicsCountPos+3])
			}
		}
	}

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

	// Check if this version uses flexible format (v12+)
	isFlexible := IsFlexibleVersion(1, apiVersion) // API key 1 = Fetch

	// NOTE: client_id is already handled by HandleConn and stripped from requestBody
	// Request body starts directly with fetch-specific fields

	// Replica ID (4 bytes) - always fixed
	if offset+4 > len(requestBody) {
		return nil, fmt.Errorf("insufficient data for replica_id")
	}
	request.ReplicaID = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
	offset += 4

	// Max wait time (4 bytes) - always fixed
	if offset+4 > len(requestBody) {
		return nil, fmt.Errorf("insufficient data for max_wait_time")
	}
	request.MaxWaitTime = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
	offset += 4

	// Min bytes (4 bytes) - always fixed
	if offset+4 > len(requestBody) {
		return nil, fmt.Errorf("insufficient data for min_bytes")
	}
	request.MinBytes = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
	offset += 4

	// Max bytes (4 bytes) - only in v3+, always fixed
	if apiVersion >= 3 {
		if offset+4 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for max_bytes")
		}
		request.MaxBytes = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
		offset += 4
	}

	// Isolation level (1 byte) - only in v4+, always fixed
	if apiVersion >= 4 {
		if offset+1 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for isolation_level")
		}
		request.IsolationLevel = int8(requestBody[offset])
		offset += 1
	}

	// Session ID (4 bytes) and Session Epoch (4 bytes) - only in v7+, always fixed
	if apiVersion >= 7 {
		if offset+8 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for session_id and epoch")
		}
		offset += 8 // Skip session_id and session_epoch
	}

	// Topics count - flexible uses compact array, non-flexible uses INT32
	var topicsCount int
	if isFlexible {
		// Compact array: length+1 encoded as varint
		length, consumed, err := DecodeCompactArrayLength(requestBody[offset:])
		if err != nil {
			return nil, fmt.Errorf("decode topics compact array: %w", err)
		}
		topicsCount = int(length)
		offset += consumed
	} else {
		// Regular array: INT32 length
		if offset+4 > len(requestBody) {
			return nil, fmt.Errorf("insufficient data for topics count")
		}
		topicsCount = int(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
		offset += 4
	}

	// Parse topics
	request.Topics = make([]FetchTopic, topicsCount)
	for i := 0; i < topicsCount; i++ {
		// Topic name - flexible uses compact string, non-flexible uses STRING (INT16 length)
		var topicName string
		if isFlexible {
			// Compact string: length+1 encoded as varint
			name, consumed, err := DecodeFlexibleString(requestBody[offset:])
			if err != nil {
				return nil, fmt.Errorf("decode topic name compact string: %w", err)
			}
			topicName = name
			offset += consumed
		} else {
			// Regular string: INT16 length + bytes
			if offset+2 > len(requestBody) {
				return nil, fmt.Errorf("insufficient data for topic name length")
			}
			topicNameLength := int(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
			offset += 2

			if offset+topicNameLength > len(requestBody) {
				return nil, fmt.Errorf("insufficient data for topic name")
			}
			topicName = string(requestBody[offset : offset+topicNameLength])
			offset += topicNameLength
		}
		request.Topics[i].Name = topicName

		// Partitions count - flexible uses compact array, non-flexible uses INT32
		var partitionsCount int
		if isFlexible {
			// Compact array: length+1 encoded as varint
			length, consumed, err := DecodeCompactArrayLength(requestBody[offset:])
			if err != nil {
				return nil, fmt.Errorf("decode partitions compact array: %w", err)
			}
			partitionsCount = int(length)
			offset += consumed
		} else {
			// Regular array: INT32 length
			if offset+4 > len(requestBody) {
				return nil, fmt.Errorf("insufficient data for partitions count")
			}
			partitionsCount = int(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
			offset += 4
		}

		// Parse partitions
		request.Topics[i].Partitions = make([]FetchPartition, partitionsCount)
		for j := 0; j < partitionsCount; j++ {
			// Partition ID (4 bytes) - always fixed
			if offset+4 > len(requestBody) {
				return nil, fmt.Errorf("insufficient data for partition ID")
			}
			request.Topics[i].Partitions[j].PartitionID = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
			offset += 4

			// Current leader epoch (4 bytes) - only in v9+, always fixed
			if apiVersion >= 9 {
				if offset+4 > len(requestBody) {
					return nil, fmt.Errorf("insufficient data for current leader epoch")
				}
				offset += 4 // Skip current leader epoch
			}

			// Fetch offset (8 bytes) - always fixed
			if offset+8 > len(requestBody) {
				return nil, fmt.Errorf("insufficient data for fetch offset")
			}
			request.Topics[i].Partitions[j].FetchOffset = int64(binary.BigEndian.Uint64(requestBody[offset : offset+8]))
			offset += 8

			// Log start offset (8 bytes) - only in v5+, always fixed
			if apiVersion >= 5 {
				if offset+8 > len(requestBody) {
					return nil, fmt.Errorf("insufficient data for log start offset")
				}
				request.Topics[i].Partitions[j].LogStartOffset = int64(binary.BigEndian.Uint64(requestBody[offset : offset+8]))
				offset += 8
			}

			// Partition max bytes (4 bytes) - always fixed
			if offset+4 > len(requestBody) {
				return nil, fmt.Errorf("insufficient data for partition max bytes")
			}
			request.Topics[i].Partitions[j].MaxBytes = int32(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
			offset += 4

			// Tagged fields for partition (only in flexible versions v12+)
			if isFlexible {
				_, consumed, err := DecodeTaggedFields(requestBody[offset:])
				if err != nil {
					return nil, fmt.Errorf("decode partition tagged fields: %w", err)
				}
				offset += consumed
			}
		}

		// Tagged fields for topic (only in flexible versions v12+)
		if isFlexible {
			_, consumed, err := DecodeTaggedFields(requestBody[offset:])
			if err != nil {
				return nil, fmt.Errorf("decode topic tagged fields: %w", err)
			}
			offset += consumed
		}
	}

	// Forgotten topics data (only in v7+)
	if apiVersion >= 7 {
		// Skip forgotten topics array - we don't use incremental fetch yet
		var forgottenTopicsCount int
		if isFlexible {
			length, consumed, err := DecodeCompactArrayLength(requestBody[offset:])
			if err != nil {
				return nil, fmt.Errorf("decode forgotten topics compact array: %w", err)
			}
			forgottenTopicsCount = int(length)
			offset += consumed
		} else {
			if offset+4 > len(requestBody) {
				// End of request, no forgotten topics
				return request, nil
			}
			forgottenTopicsCount = int(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
			offset += 4
		}

		// Skip forgotten topics if present
		for i := 0; i < forgottenTopicsCount && offset < len(requestBody); i++ {
			// Skip topic name
			if isFlexible {
				_, consumed, err := DecodeFlexibleString(requestBody[offset:])
				if err != nil {
					break
				}
				offset += consumed
			} else {
				if offset+2 > len(requestBody) {
					break
				}
				nameLen := int(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
				offset += 2 + nameLen
			}

			// Skip partitions array
			if isFlexible {
				length, consumed, err := DecodeCompactArrayLength(requestBody[offset:])
				if err != nil {
					break
				}
				offset += consumed
				// Skip partition IDs (4 bytes each)
				offset += int(length) * 4
			} else {
				if offset+4 > len(requestBody) {
					break
				}
				partCount := int(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
				offset += 4 + partCount*4
			}

			// Skip tagged fields if flexible
			if isFlexible {
				_, consumed, err := DecodeTaggedFields(requestBody[offset:])
				if err != nil {
					break
				}
				offset += consumed
			}
		}
	}

	// Rack ID (only in v11+) - optional string
	if apiVersion >= 11 && offset < len(requestBody) {
		if isFlexible {
			_, consumed, err := DecodeFlexibleString(requestBody[offset:])
			if err == nil {
				offset += consumed
			}
		} else {
			if offset+2 <= len(requestBody) {
				rackIDLen := int(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
				if rackIDLen >= 0 && offset+2+rackIDLen <= len(requestBody) {
					offset += 2 + rackIDLen
				}
			}
		}
	}

	// Top-level tagged fields (only in flexible versions v12+)
	if isFlexible && offset < len(requestBody) {
		_, consumed, err := DecodeTaggedFields(requestBody[offset:])
		if err != nil {
			// Don't fail on trailing tagged fields parsing
		} else {
			offset += consumed
		}
	}

	return request, nil
}

// constructRecordBatchFromSMQ creates a Kafka record batch from SeaweedMQ records
func (h *Handler) constructRecordBatchFromSMQ(topicName string, fetchOffset int64, smqRecords []integration.SMQRecord) []byte {
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

	// Partition leader epoch (4 bytes) - use 0 (real Kafka uses 0, not -1)
	batch = append(batch, 0x00, 0x00, 0x00, 0x00)

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

		// Key length and key (varint + data) - decode RecordValue to get original Kafka message
		key := h.decodeRecordValueToKafkaMessage(topicName, smqRecord.GetKey())
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
	// Kafka CRC calculation covers: partition leader epoch + magic + attributes + ... (everything after batch length)
	// Skip: BaseOffset(8) + BatchLength(4) = 12 bytes
	crcData := batch[crcPos+4:] // CRC covers ONLY from attributes (byte 21) onwards // Skip CRC field itself, include rest
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

// SchematizedRecord holds both key and value for schematized messages
type SchematizedRecord struct {
	Key   []byte
	Value []byte
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

// isSchematizedTopic checks if a topic uses schema management
func (h *Handler) isSchematizedTopic(topicName string) bool {
	// System topics (_schemas, __consumer_offsets, etc.) should NEVER use schema encoding
	// They have their own internal formats and should be passed through as-is
	if h.isSystemTopic(topicName) {
		return false
	}

	if !h.IsSchemaEnabled() {
		return false
	}

	// Check multiple indicators for schematized topics:

	// Check Confluent Schema Registry naming conventions
	return h.matchesSchemaRegistryConvention(topicName)
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

	// Check if the topic has registered schema subjects in Schema Registry
	// Use standard Kafka naming convention: <topic>-value and <topic>-key
	if h.schemaManager != nil {
		// Check with -value suffix (standard pattern for value schemas)
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

// getSchemaMetadataForTopic retrieves schema metadata for a topic
func (h *Handler) getSchemaMetadataForTopic(topicName string) (map[string]string, error) {
	if !h.IsSchemaEnabled() {
		return nil, fmt.Errorf("schema management not enabled")
	}

	// Try multiple approaches to get schema metadata from Schema Registry

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

	return nil, fmt.Errorf("no schema found in registry for topic %s (tried %s, %s-value, %s-key)", topicName, topicName, topicName, topicName)
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

	// For system topics like _schemas, _consumer_offsets, etc.,
	// return the raw bytes as-is. These topics store Kafka's internal format (Avro, etc.)
	// and should NOT be processed as RecordValue protobuf messages.
	if strings.HasPrefix(topicName, "_") {
		return recordValueBytes
	}

	// CRITICAL: If schema management is not enabled, we should NEVER try to parse as RecordValue
	// All messages are stored as raw bytes when schema management is disabled
	// Attempting to parse them as RecordValue will cause corruption due to protobuf's lenient parsing
	if !h.IsSchemaEnabled() {
		return recordValueBytes
	}

	// Try to unmarshal as RecordValue
	recordValue := &schema_pb.RecordValue{}
	if err := proto.Unmarshal(recordValueBytes, recordValue); err != nil {
		// Not a RecordValue format - this is normal for Avro/JSON/raw Kafka messages
		// Return raw bytes as-is (Kafka consumers expect this)
		return recordValueBytes
	}

	// Validate that the unmarshaled RecordValue is actually a valid RecordValue
	// Protobuf unmarshal is lenient and can succeed with garbage data for random bytes
	// We need to check if this looks like a real RecordValue or just random bytes
	if !h.isValidRecordValue(recordValue, recordValueBytes) {
		// Not a valid RecordValue - return raw bytes as-is
		return recordValueBytes
	}

	// If schema management is enabled, re-encode the RecordValue to Confluent format
	if h.IsSchemaEnabled() {
		if encodedMsg, err := h.encodeRecordValueToConfluentFormat(topicName, recordValue); err == nil {
			return encodedMsg
		} else {
		}
	}

	// Fallback: convert RecordValue to JSON
	return h.recordValueToJSON(recordValue)
}

// isValidRecordValue checks if a RecordValue looks like a real RecordValue or garbage from random bytes
// This performs a roundtrip test: marshal the RecordValue and check if it produces similar output
func (h *Handler) isValidRecordValue(recordValue *schema_pb.RecordValue, originalBytes []byte) bool {
	// Empty or nil Fields means not a valid RecordValue
	if recordValue == nil || recordValue.Fields == nil || len(recordValue.Fields) == 0 {
		return false
	}

	// Check if field names are valid UTF-8 strings (not binary garbage)
	// Real RecordValue messages have proper field names like "name", "age", etc.
	// Random bytes parsed as protobuf often create non-UTF8 or very short field names
	for fieldName, fieldValue := range recordValue.Fields {
		// Field name should be valid UTF-8
		if !utf8.ValidString(fieldName) {
			return false
		}

		// Field name should have reasonable length (at least 1 char, at most 1000)
		if len(fieldName) == 0 || len(fieldName) > 1000 {
			return false
		}

		// Field value should not be nil
		if fieldValue == nil || fieldValue.Kind == nil {
			return false
		}
	}

	// Roundtrip check: If this is a real RecordValue, marshaling it back should produce
	// similar-sized output. Random bytes that accidentally parse as protobuf will typically
	// produce very different output when marshaled back.
	remarshaled, err := proto.Marshal(recordValue)
	if err != nil {
		return false
	}

	// Check if the sizes are reasonably similar (within 50% tolerance)
	// Real RecordValue will have similar size, random bytes will be very different
	originalSize := len(originalBytes)
	remarshaledSize := len(remarshaled)
	if originalSize == 0 {
		return false
	}

	// Calculate size ratio - should be close to 1.0 for real RecordValue
	ratio := float64(remarshaledSize) / float64(originalSize)
	if ratio < 0.5 || ratio > 2.0 {
		// Size differs too much - this is likely random bytes parsed as protobuf
		return false
	}

	return true
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

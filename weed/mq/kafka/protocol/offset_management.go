package protocol

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
)

// ConsumerOffsetKey uniquely identifies a consumer offset
type ConsumerOffsetKey struct {
	ConsumerGroup         string
	Topic                 string
	Partition             int32
	ConsumerGroupInstance string // Optional - for static group membership
}

// OffsetCommit API (key 8) - Commit consumer group offsets
// This API allows consumers to persist their current position in topic partitions

// OffsetCommitRequest represents an OffsetCommit request from a Kafka client
type OffsetCommitRequest struct {
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupInstanceID string // Optional static membership ID
	RetentionTime   int64  // Offset retention time (-1 for broker default)
	Topics          []OffsetCommitTopic
}

// OffsetCommitTopic represents topic-level offset commit data
type OffsetCommitTopic struct {
	Name       string
	Partitions []OffsetCommitPartition
}

// OffsetCommitPartition represents partition-level offset commit data
type OffsetCommitPartition struct {
	Index       int32  // Partition index
	Offset      int64  // Offset to commit
	LeaderEpoch int32  // Leader epoch (-1 if not available)
	Metadata    string // Optional metadata
}

// OffsetCommitResponse represents an OffsetCommit response to a Kafka client
type OffsetCommitResponse struct {
	CorrelationID uint32
	Topics        []OffsetCommitTopicResponse
}

// OffsetCommitTopicResponse represents topic-level offset commit response
type OffsetCommitTopicResponse struct {
	Name       string
	Partitions []OffsetCommitPartitionResponse
}

// OffsetCommitPartitionResponse represents partition-level offset commit response
type OffsetCommitPartitionResponse struct {
	Index     int32
	ErrorCode int16
}

// OffsetFetch API (key 9) - Fetch consumer group committed offsets
// This API allows consumers to retrieve their last committed positions

// OffsetFetchRequest represents an OffsetFetch request from a Kafka client
type OffsetFetchRequest struct {
	GroupID         string
	GroupInstanceID string // Optional static membership ID
	Topics          []OffsetFetchTopic
	RequireStable   bool // Only fetch stable offsets
}

// OffsetFetchTopic represents topic-level offset fetch data
type OffsetFetchTopic struct {
	Name       string
	Partitions []int32 // Partition indices to fetch (empty = all partitions)
}

// OffsetFetchResponse represents an OffsetFetch response to a Kafka client
type OffsetFetchResponse struct {
	CorrelationID uint32
	Topics        []OffsetFetchTopicResponse
	ErrorCode     int16 // Group-level error
}

// OffsetFetchTopicResponse represents topic-level offset fetch response
type OffsetFetchTopicResponse struct {
	Name       string
	Partitions []OffsetFetchPartitionResponse
}

// OffsetFetchPartitionResponse represents partition-level offset fetch response
type OffsetFetchPartitionResponse struct {
	Index       int32
	Offset      int64  // Committed offset (-1 if no offset)
	LeaderEpoch int32  // Leader epoch (-1 if not available)
	Metadata    string // Optional metadata
	ErrorCode   int16  // Partition-level error
}

// Error codes specific to offset management are imported from errors.go

func (h *Handler) handleOffsetCommit(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	// Parse OffsetCommit request
	req, err := h.parseOffsetCommitRequest(requestBody, apiVersion)
	if err != nil {
		return h.buildOffsetCommitErrorResponse(correlationID, ErrorCodeInvalidCommitOffsetSize, apiVersion), nil
	}

	// Validate request
	if req.GroupID == "" || req.MemberID == "" {
		return h.buildOffsetCommitErrorResponse(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}

	// Get or create consumer group
	// Some Kafka clients (like kafka-go Reader) commit offsets without formally joining
	// the group via JoinGroup/SyncGroup. We need to support these "simple consumer" use cases.
	group := h.groupCoordinator.GetOrCreateGroup(req.GroupID)

	group.Mu.Lock()
	defer group.Mu.Unlock()

	// Update group's last activity
	group.LastActivity = time.Now()

	// Check generation compatibility
	// Allow commits for empty groups (no active members) to support simple consumers
	// that commit offsets without formal group membership
	groupIsEmpty := len(group.Members) == 0
	generationMatches := groupIsEmpty || (req.GenerationID == group.Generation)

	glog.V(3).Infof("[OFFSET_COMMIT] Group check: id=%s reqGen=%d groupGen=%d members=%d empty=%v matches=%v",
		req.GroupID, req.GenerationID, group.Generation, len(group.Members), groupIsEmpty, generationMatches)

	// Process offset commits
	resp := OffsetCommitResponse{
		CorrelationID: correlationID,
		Topics:        make([]OffsetCommitTopicResponse, 0, len(req.Topics)),
	}

	for _, t := range req.Topics {
		topicResp := OffsetCommitTopicResponse{
			Name:       t.Name,
			Partitions: make([]OffsetCommitPartitionResponse, 0, len(t.Partitions)),
		}

		for _, p := range t.Partitions {

			// Create consumer offset key for SMQ storage (not used immediately)
			key := ConsumerOffsetKey{
				Topic:                 t.Name,
				Partition:             p.Index,
				ConsumerGroup:         req.GroupID,
				ConsumerGroupInstance: req.GroupInstanceID,
			}

			// Commit offset synchronously for immediate consistency
			var errCode int16 = ErrorCodeNone
			if generationMatches {
				// Store in in-memory map for immediate response
				// This is the primary committed offset position for consumers
				if err := h.commitOffset(group, t.Name, p.Index, p.Offset, p.Metadata); err != nil {
					errCode = ErrorCodeOffsetMetadataTooLarge
					glog.V(2).Infof("[OFFSET_COMMIT] Failed to commit offset: group=%s topic=%s partition=%d offset=%d err=%v",
						req.GroupID, t.Name, p.Index, p.Offset, err)
				} else {
					// Also persist to SMQ storage for durability across broker restarts
					// This is done synchronously to ensure offset is not lost
					if err := h.commitOffsetToSMQ(key, p.Offset, p.Metadata); err != nil {
						// Log the error but don't fail the commit
						// In-memory commit is the source of truth for active consumers
						// SMQ persistence is best-effort for crash recovery
						glog.V(3).Infof("[OFFSET_COMMIT] SMQ persist failed (non-fatal): group=%s topic=%s partition=%d offset=%d err=%v",
							req.GroupID, t.Name, p.Index, p.Offset, err)
					}
					glog.V(3).Infof("[OFFSET_COMMIT] Committed: group=%s topic=%s partition=%d offset=%d gen=%d",
						req.GroupID, t.Name, p.Index, p.Offset, group.Generation)
				}
			} else {
				// Do not store commit if generation mismatch
				errCode = 22 // IllegalGeneration
				glog.V(2).Infof("[OFFSET_COMMIT] Rejected - generation mismatch: group=%s expected=%d got=%d members=%d",
					req.GroupID, group.Generation, req.GenerationID, len(group.Members))
			}

			topicResp.Partitions = append(topicResp.Partitions, OffsetCommitPartitionResponse{
				Index:     p.Index,
				ErrorCode: errCode,
			})
		}

		resp.Topics = append(resp.Topics, topicResp)
	}

	return h.buildOffsetCommitResponse(resp, apiVersion), nil
}

func (h *Handler) handleOffsetFetch(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	// Parse OffsetFetch request
	request, err := h.parseOffsetFetchRequest(requestBody)
	if err != nil {
		return h.buildOffsetFetchErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}

	// Validate request
	if request.GroupID == "" {
		return h.buildOffsetFetchErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}

	// Get or create consumer group
	// IMPORTANT: Use GetOrCreateGroup (not GetGroup) to allow fetching persisted offsets
	// even if the group doesn't exist in memory yet. This is critical for consumer restarts.
	// Kafka allows offset fetches for groups that haven't joined yet (e.g., simple consumers).
	group := h.groupCoordinator.GetOrCreateGroup(request.GroupID)

	group.Mu.RLock()
	defer group.Mu.RUnlock()

	glog.V(4).Infof("[OFFSET_FETCH] Request: group=%s topics=%d", request.GroupID, len(request.Topics))

	// Build response
	response := OffsetFetchResponse{
		CorrelationID: correlationID,
		Topics:        make([]OffsetFetchTopicResponse, 0, len(request.Topics)),
		ErrorCode:     ErrorCodeNone,
	}

	for _, topic := range request.Topics {
		topicResponse := OffsetFetchTopicResponse{
			Name:       topic.Name,
			Partitions: make([]OffsetFetchPartitionResponse, 0),
		}

		// If no partitions specified, fetch all partitions for the topic
		partitionsToFetch := topic.Partitions
		if len(partitionsToFetch) == 0 {
			// Get all partitions for this topic from group's offset commits
			if topicOffsets, exists := group.OffsetCommits[topic.Name]; exists {
				for partition := range topicOffsets {
					partitionsToFetch = append(partitionsToFetch, partition)
				}
			}
		}

		// Fetch offsets for requested partitions
		for _, partition := range partitionsToFetch {
			var fetchedOffset int64 = -1
			var metadata string = ""
			var errorCode int16 = ErrorCodeNone

			// Try fetching from in-memory cache first (works for both mock and SMQ backends)
			if off, meta, err := h.fetchOffset(group, topic.Name, partition); err == nil && off >= 0 {
				fetchedOffset = off
				metadata = meta
				glog.V(4).Infof("[OFFSET_FETCH] Found in memory: group=%s topic=%s partition=%d offset=%d",
					request.GroupID, topic.Name, partition, off)
			} else {
				// Fallback: try fetching from SMQ persistent storage
				// This handles cases where offsets are stored in SMQ but not yet loaded into memory
				key := ConsumerOffsetKey{
					Topic:                 topic.Name,
					Partition:             partition,
					ConsumerGroup:         request.GroupID,
					ConsumerGroupInstance: request.GroupInstanceID,
				}
				if off, meta, err := h.fetchOffsetFromSMQ(key); err == nil && off >= 0 {
					fetchedOffset = off
					metadata = meta
					glog.V(3).Infof("[OFFSET_FETCH] Found in storage: group=%s topic=%s partition=%d offset=%d",
						request.GroupID, topic.Name, partition, off)
				} else {
					glog.V(3).Infof("[OFFSET_FETCH] No offset found: group=%s topic=%s partition=%d (will start from auto.offset.reset)",
						request.GroupID, topic.Name, partition)
				}
				// No offset found in either location (-1 indicates no committed offset)
			}

			partitionResponse := OffsetFetchPartitionResponse{
				Index:       partition,
				Offset:      fetchedOffset,
				LeaderEpoch: 0, // Default epoch for SeaweedMQ (single leader model)
				Metadata:    metadata,
				ErrorCode:   errorCode,
			}
			topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
		}

		response.Topics = append(response.Topics, topicResponse)
	}

	return h.buildOffsetFetchResponse(response, apiVersion), nil
}

func (h *Handler) parseOffsetCommitRequest(data []byte, apiVersion uint16) (*OffsetCommitRequest, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("request too short")
	}

	offset := 0

	// GroupID (string)
	groupIDLength := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+groupIDLength > len(data) {
		return nil, fmt.Errorf("invalid group ID length")
	}
	groupID := string(data[offset : offset+groupIDLength])
	offset += groupIDLength

	// Generation ID (4 bytes)
	if offset+4 > len(data) {
		return nil, fmt.Errorf("missing generation ID")
	}
	generationID := int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// MemberID (string)
	if offset+2 > len(data) {
		return nil, fmt.Errorf("missing member ID length")
	}
	memberIDLength := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+memberIDLength > len(data) {
		return nil, fmt.Errorf("invalid member ID length")
	}
	memberID := string(data[offset : offset+memberIDLength])
	offset += memberIDLength

	// RetentionTime (8 bytes) - exists in v0-v4, removed in v5+
	var retentionTime int64 = -1
	if apiVersion <= 4 {
		if len(data) < offset+8 {
			return nil, fmt.Errorf("missing retention time for v%d", apiVersion)
		}
		retentionTime = int64(binary.BigEndian.Uint64(data[offset : offset+8]))
		offset += 8
	}

	// GroupInstanceID (nullable string) - ONLY in version 3+
	var groupInstanceID string
	if apiVersion >= 3 {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("missing group instance ID length")
		}
		groupInstanceIDLength := int(int16(binary.BigEndian.Uint16(data[offset:])))
		offset += 2
		if groupInstanceIDLength == -1 {
			// Null string
			groupInstanceID = ""
		} else if groupInstanceIDLength > 0 {
			if offset+groupInstanceIDLength > len(data) {
				return nil, fmt.Errorf("invalid group instance ID length")
			}
			groupInstanceID = string(data[offset : offset+groupInstanceIDLength])
			offset += groupInstanceIDLength
		}
	}

	// Topics array
	var topicsCount uint32
	if len(data) >= offset+4 {
		topicsCount = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
	}

	topics := make([]OffsetCommitTopic, 0, topicsCount)

	for i := uint32(0); i < topicsCount && offset < len(data); i++ {
		// Parse topic name
		if len(data) < offset+2 {
			break
		}
		topicNameLength := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		if len(data) < offset+int(topicNameLength) {
			break
		}
		topicName := string(data[offset : offset+int(topicNameLength)])
		offset += int(topicNameLength)

		// Parse partitions array
		if len(data) < offset+4 {
			break
		}
		partitionsCount := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		partitions := make([]OffsetCommitPartition, 0, partitionsCount)

		for j := uint32(0); j < partitionsCount && offset < len(data); j++ {
			// Parse partition index (4 bytes)
			if len(data) < offset+4 {
				break
			}
			partitionIndex := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4

			// Parse committed offset (8 bytes)
			if len(data) < offset+8 {
				break
			}
			committedOffset := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
			offset += 8

			// Parse leader epoch (4 bytes) - ONLY in version 6+
			var leaderEpoch int32 = -1
			if apiVersion >= 6 {
				if len(data) < offset+4 {
					break
				}
				leaderEpoch = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
				offset += 4
			}

			// Parse metadata (string)
			var metadata string = ""
			if len(data) >= offset+2 {
				metadataLength := int16(binary.BigEndian.Uint16(data[offset : offset+2]))
				offset += 2
				if metadataLength == -1 {
					metadata = ""
				} else if metadataLength >= 0 && len(data) >= offset+int(metadataLength) {
					metadata = string(data[offset : offset+int(metadataLength)])
					offset += int(metadataLength)
				}
			}

			partitions = append(partitions, OffsetCommitPartition{
				Index:       partitionIndex,
				Offset:      committedOffset,
				LeaderEpoch: leaderEpoch,
				Metadata:    metadata,
			})
		}
		topics = append(topics, OffsetCommitTopic{
			Name:       topicName,
			Partitions: partitions,
		})
	}

	return &OffsetCommitRequest{
		GroupID:         groupID,
		GenerationID:    generationID,
		MemberID:        memberID,
		GroupInstanceID: groupInstanceID,
		RetentionTime:   retentionTime,
		Topics:          topics,
	}, nil
}

func (h *Handler) parseOffsetFetchRequest(data []byte) (*OffsetFetchRequest, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("request too short")
	}

	offset := 0

	// GroupID (string)
	groupIDLength := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+groupIDLength > len(data) {
		return nil, fmt.Errorf("invalid group ID length")
	}
	groupID := string(data[offset : offset+groupIDLength])
	offset += groupIDLength

	// Parse Topics array - classic encoding (INT32 count) for v0-v5
	if len(data) < offset+4 {
		return nil, fmt.Errorf("OffsetFetch request missing topics array")
	}
	topicsCount := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	topics := make([]OffsetFetchTopic, 0, topicsCount)

	for i := uint32(0); i < topicsCount && offset < len(data); i++ {
		// Parse topic name (STRING: INT16 length + bytes)
		if len(data) < offset+2 {
			break
		}
		topicNameLength := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		if len(data) < offset+int(topicNameLength) {
			break
		}
		topicName := string(data[offset : offset+int(topicNameLength)])
		offset += int(topicNameLength)

		// Parse partitions array (ARRAY: INT32 count)
		if len(data) < offset+4 {
			break
		}
		partitionsCount := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		partitions := make([]int32, 0, partitionsCount)

		// If partitionsCount is 0, it means "fetch all partitions"
		if partitionsCount == 0 {
			partitions = nil // nil means all partitions
		} else {
			for j := uint32(0); j < partitionsCount && offset < len(data); j++ {
				// Parse partition index (4 bytes)
				if len(data) < offset+4 {
					break
				}
				partitionIndex := int32(binary.BigEndian.Uint32(data[offset : offset+4]))
				offset += 4

				partitions = append(partitions, partitionIndex)
			}
		}

		topics = append(topics, OffsetFetchTopic{
			Name:       topicName,
			Partitions: partitions,
		})
	}

	// Parse RequireStable flag (1 byte) - for transactional consistency
	var requireStable bool
	if len(data) >= offset+1 {
		requireStable = data[offset] != 0
		offset += 1
	}

	return &OffsetFetchRequest{
		GroupID:       groupID,
		Topics:        topics,
		RequireStable: requireStable,
	}, nil
}

func (h *Handler) commitOffset(group *consumer.ConsumerGroup, topic string, partition int32, offset int64, metadata string) error {
	// Initialize topic offsets if needed
	if group.OffsetCommits == nil {
		group.OffsetCommits = make(map[string]map[int32]consumer.OffsetCommit)
	}

	if group.OffsetCommits[topic] == nil {
		group.OffsetCommits[topic] = make(map[int32]consumer.OffsetCommit)
	}

	// Store the offset commit
	group.OffsetCommits[topic][partition] = consumer.OffsetCommit{
		Offset:    offset,
		Metadata:  metadata,
		Timestamp: time.Now(),
	}

	return nil
}

func (h *Handler) fetchOffset(group *consumer.ConsumerGroup, topic string, partition int32) (int64, string, error) {
	// Check if topic exists in offset commits
	if group.OffsetCommits == nil {
		return -1, "", nil // No committed offset
	}

	topicOffsets, exists := group.OffsetCommits[topic]
	if !exists {
		return -1, "", nil // No committed offset for topic
	}

	offsetCommit, exists := topicOffsets[partition]
	if !exists {
		return -1, "", nil // No committed offset for partition
	}

	return offsetCommit.Offset, offsetCommit.Metadata, nil
}

func (h *Handler) buildOffsetCommitResponse(response OffsetCommitResponse, apiVersion uint16) []byte {
	estimatedSize := 16
	for _, topic := range response.Topics {
		estimatedSize += len(topic.Name) + 8 + len(topic.Partitions)*8
	}

	result := make([]byte, 0, estimatedSize)

	// NOTE: Correlation ID is handled by writeResponseWithCorrelationID
	// Do NOT include it in the response body

	// Throttle time (4 bytes) - ONLY for version 3+, and it goes at the BEGINNING
	if apiVersion >= 3 {
		result = append(result, 0, 0, 0, 0) // throttle_time_ms = 0
	}

	// Topics array length (4 bytes)
	topicsLengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsLengthBytes, uint32(len(response.Topics)))
	result = append(result, topicsLengthBytes...)

	// Topics
	for _, topic := range response.Topics {
		// Topic name length (2 bytes)
		nameLength := make([]byte, 2)
		binary.BigEndian.PutUint16(nameLength, uint16(len(topic.Name)))
		result = append(result, nameLength...)

		// Topic name
		result = append(result, []byte(topic.Name)...)

		// Partitions array length (4 bytes)
		partitionsLength := make([]byte, 4)
		binary.BigEndian.PutUint32(partitionsLength, uint32(len(topic.Partitions)))
		result = append(result, partitionsLength...)

		// Partitions
		for _, partition := range topic.Partitions {
			// Partition index (4 bytes)
			indexBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(indexBytes, uint32(partition.Index))
			result = append(result, indexBytes...)

			// Error code (2 bytes)
			errorBytes := make([]byte, 2)
			binary.BigEndian.PutUint16(errorBytes, uint16(partition.ErrorCode))
			result = append(result, errorBytes...)
		}
	}

	return result
}

func (h *Handler) buildOffsetFetchResponse(response OffsetFetchResponse, apiVersion uint16) []byte {
	estimatedSize := 32
	for _, topic := range response.Topics {
		estimatedSize += len(topic.Name) + 16 + len(topic.Partitions)*32
		for _, partition := range topic.Partitions {
			estimatedSize += len(partition.Metadata)
		}
	}

	result := make([]byte, 0, estimatedSize)

	// NOTE: Correlation ID is handled by writeResponseWithCorrelationID
	// Do NOT include it in the response body

	// Throttle time (4 bytes) - for version 3+ this appears immediately after correlation ID
	if apiVersion >= 3 {
		result = append(result, 0, 0, 0, 0) // throttle_time_ms = 0
	}

	// Topics array length (4 bytes)
	topicsLengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsLengthBytes, uint32(len(response.Topics)))
	result = append(result, topicsLengthBytes...)

	// Topics
	for _, topic := range response.Topics {
		// Topic name length (2 bytes)
		nameLength := make([]byte, 2)
		binary.BigEndian.PutUint16(nameLength, uint16(len(topic.Name)))
		result = append(result, nameLength...)

		// Topic name
		result = append(result, []byte(topic.Name)...)

		// Partitions array length (4 bytes)
		partitionsLength := make([]byte, 4)
		binary.BigEndian.PutUint32(partitionsLength, uint32(len(topic.Partitions)))
		result = append(result, partitionsLength...)

		// Partitions
		for _, partition := range topic.Partitions {
			// Partition index (4 bytes)
			indexBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(indexBytes, uint32(partition.Index))
			result = append(result, indexBytes...)

			// Committed offset (8 bytes)
			offsetBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(offsetBytes, uint64(partition.Offset))
			result = append(result, offsetBytes...)

			// Leader epoch (4 bytes) - only included in version 5+
			if apiVersion >= 5 {
				epochBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(epochBytes, uint32(partition.LeaderEpoch))
				result = append(result, epochBytes...)
			}

			// Metadata length (2 bytes)
			metadataLength := make([]byte, 2)
			binary.BigEndian.PutUint16(metadataLength, uint16(len(partition.Metadata)))
			result = append(result, metadataLength...)

			// Metadata
			result = append(result, []byte(partition.Metadata)...)

			// Error code (2 bytes)
			errorBytes := make([]byte, 2)
			binary.BigEndian.PutUint16(errorBytes, uint16(partition.ErrorCode))
			result = append(result, errorBytes...)
		}
	}

	// Group-level error code (2 bytes) - only included in version 2+
	if apiVersion >= 2 {
		groupErrorBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(groupErrorBytes, uint16(response.ErrorCode))
		result = append(result, groupErrorBytes...)
	}

	return result
}

func (h *Handler) buildOffsetCommitErrorResponse(correlationID uint32, errorCode int16, apiVersion uint16) []byte {
	response := OffsetCommitResponse{
		CorrelationID: correlationID,
		Topics: []OffsetCommitTopicResponse{
			{
				Name: "",
				Partitions: []OffsetCommitPartitionResponse{
					{Index: 0, ErrorCode: errorCode},
				},
			},
		},
	}

	return h.buildOffsetCommitResponse(response, apiVersion)
}

func (h *Handler) buildOffsetFetchErrorResponse(correlationID uint32, errorCode int16) []byte {
	response := OffsetFetchResponse{
		CorrelationID: correlationID,
		Topics:        []OffsetFetchTopicResponse{},
		ErrorCode:     errorCode,
	}

	return h.buildOffsetFetchResponse(response, 0)
}

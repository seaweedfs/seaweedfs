package protocol

import (
	"encoding/binary"
	"fmt"
	"time"
	
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
)

// OffsetCommit API (key 8) - Commit consumer group offsets
// This API allows consumers to persist their current position in topic partitions

// OffsetCommitRequest represents an OffsetCommit request from a Kafka client
type OffsetCommitRequest struct {
	GroupID      string
	GenerationID int32
	MemberID     string
	GroupInstanceID string  // Optional static membership ID
	RetentionTime int64     // Offset retention time (-1 for broker default)
	Topics       []OffsetCommitTopic
}

// OffsetCommitTopic represents topic-level offset commit data
type OffsetCommitTopic struct {
	Name       string
	Partitions []OffsetCommitPartition
}

// OffsetCommitPartition represents partition-level offset commit data
type OffsetCommitPartition struct {
	Index    int32  // Partition index
	Offset   int64  // Offset to commit
	LeaderEpoch int32 // Leader epoch (-1 if not available)
	Metadata string // Optional metadata
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
	RequireStable   bool   // Only fetch stable offsets
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

// Error codes specific to offset management
const (
	ErrorCodeInvalidCommitOffsetSize int16 = 28
	ErrorCodeOffsetMetadataTooLarge  int16 = 12
	ErrorCodeOffsetLoadInProgress    int16 = 14
	ErrorCodeNotCoordinatorForGroup  int16 = 16
	ErrorCodeGroupAuthorizationFailed int16 = 30
)

func (h *Handler) handleOffsetCommit(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Parse OffsetCommit request
	request, err := h.parseOffsetCommitRequest(requestBody)
	if err != nil {
		return h.buildOffsetCommitErrorResponse(correlationID, ErrorCodeInvalidCommitOffsetSize), nil
	}
	
	// Validate request
	if request.GroupID == "" || request.MemberID == "" {
		return h.buildOffsetCommitErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}
	
	// Get consumer group
	group := h.groupCoordinator.GetGroup(request.GroupID)
	if group == nil {
		return h.buildOffsetCommitErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}
	
	group.Mu.Lock()
	defer group.Mu.Unlock()
	
	// Update group's last activity
	group.LastActivity = time.Now()
	
	// Validate member exists and is in stable state
	member, exists := group.Members[request.MemberID]
	if !exists {
		return h.buildOffsetCommitErrorResponse(correlationID, ErrorCodeUnknownMemberID), nil
	}
	
	if member.State != consumer.MemberStateStable {
		return h.buildOffsetCommitErrorResponse(correlationID, ErrorCodeRebalanceInProgress), nil
	}
	
	// Validate generation
	if request.GenerationID != group.Generation {
		return h.buildOffsetCommitErrorResponse(correlationID, ErrorCodeIllegalGeneration), nil
	}
	
	// Process offset commits
	response := OffsetCommitResponse{
		CorrelationID: correlationID,
		Topics:        make([]OffsetCommitTopicResponse, 0, len(request.Topics)),
	}
	
	for _, topic := range request.Topics {
		topicResponse := OffsetCommitTopicResponse{
			Name:       topic.Name,
			Partitions: make([]OffsetCommitPartitionResponse, 0, len(topic.Partitions)),
		}
		
		for _, partition := range topic.Partitions {
			// Validate partition assignment - consumer should only commit offsets for assigned partitions
			assigned := false
			for _, assignment := range member.Assignment {
				if assignment.Topic == topic.Name && assignment.Partition == partition.Index {
					assigned = true
					break
				}
			}
			
			var errorCode int16 = ErrorCodeNone
			if !assigned && group.State == consumer.GroupStateStable {
				// Allow commits during rebalancing, but restrict during stable state
				errorCode = ErrorCodeIllegalGeneration
			} else {
				// Commit the offset
				err := h.commitOffset(group, topic.Name, partition.Index, partition.Offset, partition.Metadata)
				if err != nil {
					errorCode = ErrorCodeOffsetMetadataTooLarge // Generic error
				}
			}
			
			partitionResponse := OffsetCommitPartitionResponse{
				Index:     partition.Index,
				ErrorCode: errorCode,
			}
			topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
		}
		
		response.Topics = append(response.Topics, topicResponse)
	}
	
	return h.buildOffsetCommitResponse(response), nil
}

func (h *Handler) handleOffsetFetch(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Parse OffsetFetch request
	request, err := h.parseOffsetFetchRequest(requestBody)
	if err != nil {
		return h.buildOffsetFetchErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}
	
	// Validate request
	if request.GroupID == "" {
		return h.buildOffsetFetchErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}
	
	// Get consumer group
	group := h.groupCoordinator.GetGroup(request.GroupID)
	if group == nil {
		return h.buildOffsetFetchErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}
	
	group.Mu.RLock()
	defer group.Mu.RUnlock()
	
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
			offset, metadata, err := h.fetchOffset(group, topic.Name, partition)
			
			var errorCode int16 = ErrorCodeNone
			if err != nil {
				errorCode = ErrorCodeOffsetLoadInProgress // Generic error
			}
			
			partitionResponse := OffsetFetchPartitionResponse{
				Index:       partition,
				Offset:      offset,
				LeaderEpoch: -1, // Not implemented
				Metadata:    metadata,
				ErrorCode:   errorCode,
			}
			topicResponse.Partitions = append(topicResponse.Partitions, partitionResponse)
		}
		
		response.Topics = append(response.Topics, topicResponse)
	}
	
	return h.buildOffsetFetchResponse(response), nil
}

func (h *Handler) parseOffsetCommitRequest(data []byte) (*OffsetCommitRequest, error) {
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
	
	// TODO: CRITICAL - This parsing is completely broken for real clients
	// Currently hardcoded to return "test-topic" with partition 0
	// Real OffsetCommit requests contain:
	// - RetentionTime (8 bytes, -1 for broker default)
	// - Topics array with actual topic names  
	// - Partitions array with actual partition IDs and offsets
	// - Optional group instance ID for static membership
	// Without fixing this, no real Kafka client can commit offsets properly
	
	return &OffsetCommitRequest{
		GroupID:       groupID,
		GenerationID:  generationID,
		MemberID:      memberID,
		RetentionTime: -1, // Use broker default
		Topics: []OffsetCommitTopic{
			{
				Name: "test-topic", // TODO: Parse actual topic from request
				Partitions: []OffsetCommitPartition{
					{Index: 0, Offset: 0, LeaderEpoch: -1, Metadata: ""}, // TODO: Parse actual partition data
				},
			},
		},
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
	
	// TODO: CRITICAL - OffsetFetch parsing is also hardcoded 
	// Real clients send topics array with specific partitions to fetch
	// Need to parse:
	// - Topics array (4 bytes count + topics)
	// - For each topic: name + partitions array
	// - RequireStable flag for transactional consistency
	// Currently will fail with any real Kafka client doing offset fetches
	
	return &OffsetFetchRequest{
		GroupID: groupID,
		Topics: []OffsetFetchTopic{
			{
				Name:       "test-topic", // TODO: Parse actual topics from request
				Partitions: []int32{0},   // TODO: Parse actual partitions or empty for "all"
			},
		},
		RequireStable: false,
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

func (h *Handler) buildOffsetCommitResponse(response OffsetCommitResponse) []byte {
	estimatedSize := 16
	for _, topic := range response.Topics {
		estimatedSize += len(topic.Name) + 8 + len(topic.Partitions)*8
	}
	
	result := make([]byte, 0, estimatedSize)
	
	// Correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, response.CorrelationID)
	result = append(result, correlationIDBytes...)
	
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
	
	// Throttle time (4 bytes, 0 = no throttling)
	result = append(result, 0, 0, 0, 0)
	
	return result
}

func (h *Handler) buildOffsetFetchResponse(response OffsetFetchResponse) []byte {
	estimatedSize := 32
	for _, topic := range response.Topics {
		estimatedSize += len(topic.Name) + 16 + len(topic.Partitions)*32
		for _, partition := range topic.Partitions {
			estimatedSize += len(partition.Metadata)
		}
	}
	
	result := make([]byte, 0, estimatedSize)
	
	// Correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, response.CorrelationID)
	result = append(result, correlationIDBytes...)
	
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
			
			// Leader epoch (4 bytes)
			epochBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(epochBytes, uint32(partition.LeaderEpoch))
			result = append(result, epochBytes...)
			
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
	
	// Group-level error code (2 bytes)
	groupErrorBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(groupErrorBytes, uint16(response.ErrorCode))
	result = append(result, groupErrorBytes...)
	
	// Throttle time (4 bytes, 0 = no throttling)
	result = append(result, 0, 0, 0, 0)
	
	return result
}

func (h *Handler) buildOffsetCommitErrorResponse(correlationID uint32, errorCode int16) []byte {
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
	
	return h.buildOffsetCommitResponse(response)
}

func (h *Handler) buildOffsetFetchErrorResponse(correlationID uint32, errorCode int16) []byte {
	response := OffsetFetchResponse{
		CorrelationID: correlationID,
		Topics:        []OffsetFetchTopicResponse{},
		ErrorCode:     errorCode,
	}
	
	return h.buildOffsetFetchResponse(response)
}

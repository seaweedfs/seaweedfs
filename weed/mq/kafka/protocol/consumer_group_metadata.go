package protocol

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
)

// ConsumerProtocolMetadata represents parsed consumer protocol metadata
type ConsumerProtocolMetadata struct {
	Version            int16    // Protocol metadata version
	Topics             []string // Subscribed topic names
	UserData           []byte   // Optional user data
	AssignmentStrategy string   // Preferred assignment strategy
}

// ConnectionContext holds connection-specific information for requests
type ConnectionContext struct {
	RemoteAddr   net.Addr // Client's remote address
	LocalAddr    net.Addr // Server's local address
	ConnectionID string   // Connection identifier
}

// ExtractClientHost extracts the client hostname/IP from connection context
func ExtractClientHost(connCtx *ConnectionContext) string {
	if connCtx == nil || connCtx.RemoteAddr == nil {
		return "unknown"
	}

	// Extract host portion from address
	if tcpAddr, ok := connCtx.RemoteAddr.(*net.TCPAddr); ok {
		return tcpAddr.IP.String()
	}

	// Fallback: parse string representation
	addrStr := connCtx.RemoteAddr.String()
	if host, _, err := net.SplitHostPort(addrStr); err == nil {
		return host
	}

	// Last resort: return full address
	return addrStr
}

// ParseConsumerProtocolMetadata parses consumer protocol metadata with enhanced error handling
func ParseConsumerProtocolMetadata(metadata []byte, strategyName string) (*ConsumerProtocolMetadata, error) {
	if len(metadata) < 2 {
		return &ConsumerProtocolMetadata{
			Version:            0,
			Topics:             []string{},
			UserData:           []byte{},
			AssignmentStrategy: strategyName,
		}, nil
	}

	result := &ConsumerProtocolMetadata{
		AssignmentStrategy: strategyName,
	}

	offset := 0

	// Parse version (2 bytes)
	if len(metadata) < offset+2 {
		return nil, fmt.Errorf("metadata too short for version field")
	}
	result.Version = int16(binary.BigEndian.Uint16(metadata[offset : offset+2]))
	offset += 2

	// Parse topics array
	if len(metadata) < offset+4 {
		return nil, fmt.Errorf("metadata too short for topics count")
	}
	topicsCount := binary.BigEndian.Uint32(metadata[offset : offset+4])
	offset += 4

	// Validate topics count (reasonable limit)
	if topicsCount > 10000 {
		return nil, fmt.Errorf("unreasonable topics count: %d", topicsCount)
	}

	result.Topics = make([]string, 0, topicsCount)

	for i := uint32(0); i < topicsCount && offset < len(metadata); i++ {
		// Parse topic name length
		if len(metadata) < offset+2 {
			return nil, fmt.Errorf("metadata too short for topic %d name length", i)
		}
		topicNameLength := binary.BigEndian.Uint16(metadata[offset : offset+2])
		offset += 2

		// Validate topic name length
		if topicNameLength > 1000 {
			return nil, fmt.Errorf("unreasonable topic name length: %d", topicNameLength)
		}

		if len(metadata) < offset+int(topicNameLength) {
			return nil, fmt.Errorf("metadata too short for topic %d name data", i)
		}

		topicName := string(metadata[offset : offset+int(topicNameLength)])
		offset += int(topicNameLength)

		// Validate topic name (basic validation)
		if len(topicName) == 0 {
			continue // Skip empty topic names
		}

		result.Topics = append(result.Topics, topicName)
	}

	// Parse user data if remaining bytes exist
	if len(metadata) >= offset+4 {
		userDataLength := binary.BigEndian.Uint32(metadata[offset : offset+4])
		offset += 4

		// Handle -1 (0xFFFFFFFF) as null/empty user data (Kafka protocol convention)
		if userDataLength == 0xFFFFFFFF {
			result.UserData = []byte{}
			return result, nil
		}

		// Validate user data length
		if userDataLength > 100000 { // 100KB limit
			return nil, fmt.Errorf("unreasonable user data length: %d", userDataLength)
		}

		if len(metadata) >= offset+int(userDataLength) {
			result.UserData = make([]byte, userDataLength)
			copy(result.UserData, metadata[offset:offset+int(userDataLength)])
		}
	}

	return result, nil
}

// GenerateConsumerProtocolMetadata creates protocol metadata for a consumer subscription
func GenerateConsumerProtocolMetadata(topics []string, userData []byte) []byte {
	// Calculate total size needed
	size := 2 + 4 + 4 // version + topics_count + user_data_length
	for _, topic := range topics {
		size += 2 + len(topic) // topic_name_length + topic_name
	}
	size += len(userData)

	metadata := make([]byte, 0, size)

	// Version (2 bytes) - use version 1
	metadata = append(metadata, 0, 1)

	// Topics count (4 bytes)
	topicsCount := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsCount, uint32(len(topics)))
	metadata = append(metadata, topicsCount...)

	// Topics (string array)
	for _, topic := range topics {
		topicLen := make([]byte, 2)
		binary.BigEndian.PutUint16(topicLen, uint16(len(topic)))
		metadata = append(metadata, topicLen...)
		metadata = append(metadata, []byte(topic)...)
	}

	// UserData length and data (4 bytes + data)
	userDataLen := make([]byte, 4)
	binary.BigEndian.PutUint32(userDataLen, uint32(len(userData)))
	metadata = append(metadata, userDataLen...)
	metadata = append(metadata, userData...)

	return metadata
}

// ValidateAssignmentStrategy checks if an assignment strategy is supported
func ValidateAssignmentStrategy(strategy string) bool {
	supportedStrategies := map[string]bool{
		"range":              true,
		"roundrobin":         true,
		"sticky":             true,
		"cooperative-sticky": false, // Not yet implemented
	}

	return supportedStrategies[strategy]
}

// ExtractTopicsFromMetadata extracts topic list from protocol metadata with fallback
func ExtractTopicsFromMetadata(protocols []GroupProtocol, fallbackTopics []string) []string {
	for _, protocol := range protocols {
		if ValidateAssignmentStrategy(protocol.Name) {
			parsed, err := ParseConsumerProtocolMetadata(protocol.Metadata, protocol.Name)
			if err != nil {
				fmt.Printf("DEBUG: Failed to parse protocol metadata: %v\n", err)
				continue
			}

			if len(parsed.Topics) > 0 {
				fmt.Printf("DEBUG: Extracted %d topics from protocol\n", len(parsed.Topics))
				return parsed.Topics
			}
		}
	}

	// Fallback to provided topics or default
	if len(fallbackTopics) > 0 {
		fmt.Printf("DEBUG: Using fallback topics (%d topics)\n", len(fallbackTopics))
		return fallbackTopics
	}

	fmt.Printf("DEBUG: No topics found, using default test topic\n")
	return []string{"test-topic"}
}

// SelectBestProtocol chooses the best assignment protocol from available options
func SelectBestProtocol(protocols []GroupProtocol, groupProtocols []string) string {
	// Priority order: sticky > roundrobin > range
	protocolPriority := []string{"sticky", "roundrobin", "range"}

	// Find supported protocols in client's list
	clientProtocols := make(map[string]bool)
	for _, protocol := range protocols {
		if ValidateAssignmentStrategy(protocol.Name) {
			clientProtocols[protocol.Name] = true
		}
	}

	// Find supported protocols in group's list
	groupProtocolSet := make(map[string]bool)
	for _, protocol := range groupProtocols {
		groupProtocolSet[protocol] = true
	}

	// Select highest priority protocol that both client and group support
	for _, preferred := range protocolPriority {
		if clientProtocols[preferred] && (len(groupProtocols) == 0 || groupProtocolSet[preferred]) {
			return preferred
		}
	}

	// Fallback to first supported protocol from client
	for _, protocol := range protocols {
		if ValidateAssignmentStrategy(protocol.Name) {
			return protocol.Name
		}
	}

	// Last resort
	return "range"
}

// SanitizeConsumerGroupID validates and sanitizes consumer group ID
func SanitizeConsumerGroupID(groupID string) (string, error) {
	if len(groupID) == 0 {
		return "", fmt.Errorf("empty group ID")
	}

	if len(groupID) > 255 {
		return "", fmt.Errorf("group ID too long: %d characters (max 255)", len(groupID))
	}

	// Basic validation: no control characters
	for _, char := range groupID {
		if char < 32 || char == 127 {
			return "", fmt.Errorf("group ID contains invalid characters")
		}
	}

	return strings.TrimSpace(groupID), nil
}

// ProtocolMetadataDebugInfo returns debug information about protocol metadata
type ProtocolMetadataDebugInfo struct {
	Strategy     string
	Version      int16
	TopicCount   int
	Topics       []string
	UserDataSize int
	ParsedOK     bool
	ParseError   string
}

// AnalyzeProtocolMetadata provides detailed debug information about protocol metadata
func AnalyzeProtocolMetadata(protocols []GroupProtocol) []ProtocolMetadataDebugInfo {
	result := make([]ProtocolMetadataDebugInfo, 0, len(protocols))

	for _, protocol := range protocols {
		info := ProtocolMetadataDebugInfo{
			Strategy: protocol.Name,
		}

		parsed, err := ParseConsumerProtocolMetadata(protocol.Metadata, protocol.Name)
		if err != nil {
			info.ParsedOK = false
			info.ParseError = err.Error()
		} else {
			info.ParsedOK = true
			info.Version = parsed.Version
			info.TopicCount = len(parsed.Topics)
			info.Topics = parsed.Topics
			info.UserDataSize = len(parsed.UserData)
		}

		result = append(result, info)
	}

	return result
}

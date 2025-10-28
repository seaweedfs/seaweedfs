package protocol

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
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
	RemoteAddr    net.Addr // Client's remote address
	LocalAddr     net.Addr // Server's local address
	ConnectionID  string   // Connection identifier
	ClientID      string   // Kafka client ID from request headers
	ConsumerGroup string   // Consumer group (set by JoinGroup)
	MemberID      string   // Consumer group member ID (set by JoinGroup)
	// Per-connection broker client for isolated gRPC streams
	// Each Kafka connection MUST have its own gRPC streams to avoid interference
	// when multiple consumers or requests are active on different connections
	BrokerClient interface{} // Will be set to *integration.BrokerClient

	// Persistent partition readers - one goroutine per topic-partition that maintains position
	// and streams forward, eliminating repeated offset lookups and reducing broker CPU load
	partitionReaders sync.Map // map[TopicPartitionKey]*partitionReader
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

// ValidateAssignmentStrategy checks if an assignment strategy is supported
func ValidateAssignmentStrategy(strategy string) bool {
	supportedStrategies := map[string]bool{
		consumer.ProtocolNameRange:             true,
		consumer.ProtocolNameRoundRobin:        true,
		consumer.ProtocolNameSticky:            true,
		consumer.ProtocolNameCooperativeSticky: true, // Incremental cooperative rebalancing (Kafka 2.4+)
	}

	return supportedStrategies[strategy]
}

// ExtractTopicsFromMetadata extracts topic list from protocol metadata with fallback
func ExtractTopicsFromMetadata(protocols []GroupProtocol, fallbackTopics []string) []string {
	for _, protocol := range protocols {
		if ValidateAssignmentStrategy(protocol.Name) {
			parsed, err := ParseConsumerProtocolMetadata(protocol.Metadata, protocol.Name)
			if err != nil {
				continue
			}

			if len(parsed.Topics) > 0 {
				return parsed.Topics
			}
		}
	}

	// Fallback to provided topics or empty list
	if len(fallbackTopics) > 0 {
		return fallbackTopics
	}

	// Return empty slice if no topics found - consumer may be using pattern subscription
	return []string{}
}

// SelectBestProtocol chooses the best assignment protocol from available options
func SelectBestProtocol(protocols []GroupProtocol, groupProtocols []string) string {
	// Priority order: sticky > roundrobin > range
	protocolPriority := []string{consumer.ProtocolNameSticky, consumer.ProtocolNameRoundRobin, consumer.ProtocolNameRange}

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

	// If group has existing protocols, find a protocol supported by both client and group
	if len(groupProtocols) > 0 {
		// Try to find a protocol that both client and group support
		for _, preferred := range protocolPriority {
			if clientProtocols[preferred] && groupProtocolSet[preferred] {
				return preferred
			}
		}

		// No common protocol found - handle special fallback case
		// If client supports nothing we validate, but group supports "range", use "range"
		if len(clientProtocols) == 0 && groupProtocolSet[consumer.ProtocolNameRange] {
			return consumer.ProtocolNameRange
		}

		// Return empty string to indicate no compatible protocol found
		return ""
	}

	// Fallback to first supported protocol from client (only when group has no existing protocols)
	for _, protocol := range protocols {
		if ValidateAssignmentStrategy(protocol.Name) {
			return protocol.Name
		}
	}

	// Last resort
	return consumer.ProtocolNameRange
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

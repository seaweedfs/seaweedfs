package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// Handler processes Kafka protocol requests from clients
type Handler struct {
}

func NewHandler() *Handler {
	return &Handler{}
}

// HandleConn processes a single client connection
func (h *Handler) HandleConn(conn net.Conn) error {
	defer conn.Close()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	defer w.Flush()

	for {
		// Read message size (4 bytes)
		var sizeBytes [4]byte
		if _, err := io.ReadFull(r, sizeBytes[:]); err != nil {
			if err == io.EOF {
				return nil // clean disconnect
			}
			return fmt.Errorf("read size: %w", err)
		}

		size := binary.BigEndian.Uint32(sizeBytes[:])
		if size == 0 || size > 1024*1024 { // 1MB limit
			return fmt.Errorf("invalid message size: %d", size)
		}

		// Read the message
		messageBuf := make([]byte, size)
		if _, err := io.ReadFull(r, messageBuf); err != nil {
			return fmt.Errorf("read message: %w", err)
		}

		// Parse at least the basic header to get API key and correlation ID
		if len(messageBuf) < 8 {
			return fmt.Errorf("message too short")
		}

		apiKey := binary.BigEndian.Uint16(messageBuf[0:2])
		apiVersion := binary.BigEndian.Uint16(messageBuf[2:4])
		correlationID := binary.BigEndian.Uint32(messageBuf[4:8])

		// Handle the request based on API key
		var response []byte
		var err error

		switch apiKey {
		case 18: // ApiVersions
			response, err = h.handleApiVersions(correlationID)
		case 3: // Metadata
			response, err = h.handleMetadata(correlationID, messageBuf[8:]) // skip header
		case 2: // ListOffsets
			response, err = h.handleListOffsets(correlationID, messageBuf[8:]) // skip header
		default:
			err = fmt.Errorf("unsupported API key: %d (version %d)", apiKey, apiVersion)
		}

		if err != nil {
			return fmt.Errorf("handle request: %w", err)
		}

		// Write response size and data
		responseSizeBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(responseSizeBytes, uint32(len(response)))

		if _, err := w.Write(responseSizeBytes); err != nil {
			return fmt.Errorf("write response size: %w", err)
		}
		if _, err := w.Write(response); err != nil {
			return fmt.Errorf("write response: %w", err)
		}

		if err := w.Flush(); err != nil {
			return fmt.Errorf("flush response: %w", err)
		}
	}
}

func (h *Handler) handleApiVersions(correlationID uint32) ([]byte, error) {
	// Build ApiVersions response manually
	// Response format: correlation_id(4) + error_code(2) + num_api_keys(4) + api_keys + throttle_time(4)
	
	response := make([]byte, 0, 64)
	
	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)
	
	// Error code (0 = no error)
	response = append(response, 0, 0)
	
	// Number of API keys (compact array format in newer versions, but using basic format for simplicity)
	response = append(response, 0, 0, 0, 3) // 3 API keys
	
	// API Key 18 (ApiVersions): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 18) // API key 18
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 3)  // max version 3
	
	// API Key 3 (Metadata): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 3)  // API key 3
	response = append(response, 0, 0)  // min version 0  
	response = append(response, 0, 7)  // max version 7
	
	// API Key 2 (ListOffsets): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 2)  // API key 2
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 5)  // max version 5
	
	// Throttle time (4 bytes, 0 = no throttling)
	response = append(response, 0, 0, 0, 0)
	
	return response, nil
}

func (h *Handler) handleMetadata(correlationID uint32, requestBody []byte) ([]byte, error) {
	// For now, ignore the request body content (topics filter, etc.)
	// Build minimal Metadata response
	// Response format: correlation_id(4) + throttle_time(4) + brokers + cluster_id + controller_id + topics
	
	response := make([]byte, 0, 256)
	
	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)
	
	// Throttle time (4 bytes, 0 = no throttling)
	response = append(response, 0, 0, 0, 0)
	
	// Brokers array length (4 bytes) - 1 broker (this gateway)
	response = append(response, 0, 0, 0, 1)
	
	// Broker 0: node_id(4) + host + port(4) + rack
	response = append(response, 0, 0, 0, 0) // node_id = 0
	
	// Host string: length(2) + "localhost"
	host := "localhost"
	response = append(response, 0, byte(len(host)))
	response = append(response, []byte(host)...)
	
	// Port (4 bytes) - 9092 (standard Kafka port)
	response = append(response, 0, 0, 0x23, 0x84) // 9092 in big-endian
	
	// Rack - nullable string, using null (-1 length)
	response = append(response, 0xFF, 0xFF) // null rack
	
	// Cluster ID - nullable string, using null
	response = append(response, 0xFF, 0xFF) // null cluster_id
	
	// Controller ID (4 bytes) - -1 (no controller)
	response = append(response, 0xFF, 0xFF, 0xFF, 0xFF)
	
	// Topics array length (4 bytes) - 0 topics for now
	response = append(response, 0, 0, 0, 0)
	
	return response, nil
}

func (h *Handler) handleListOffsets(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Parse minimal request to understand what's being asked
	// For this stub, we'll just return stub responses for any requested topic/partition
	// Request format after client_id: topics_array
	
	if len(requestBody) < 6 { // at minimum need client_id_size(2) + topics_count(4)
		return nil, fmt.Errorf("ListOffsets request too short")
	}
	
	// Skip client_id: client_id_size(2) + client_id_data
	clientIDSize := binary.BigEndian.Uint16(requestBody[0:2])
	offset := 2 + int(clientIDSize)
	
	if len(requestBody) < offset+4 {
		return nil, fmt.Errorf("ListOffsets request missing topics count")
	}
	
	topicsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
	offset += 4
	
	response := make([]byte, 0, 256)
	
	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)
	
	// Throttle time (4 bytes, 0 = no throttling)
	response = append(response, 0, 0, 0, 0)
	
	// Topics count (same as request)
	topicsCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsCountBytes, topicsCount)
	response = append(response, topicsCountBytes...)
	
	// Process each requested topic
	for i := uint32(0); i < topicsCount && offset < len(requestBody); i++ {
		if len(requestBody) < offset+2 {
			break
		}
		
		// Parse topic name
		topicNameSize := binary.BigEndian.Uint16(requestBody[offset : offset+2])
		offset += 2
		
		if len(requestBody) < offset+int(topicNameSize)+4 {
			break
		}
		
		topicName := requestBody[offset : offset+int(topicNameSize)]
		offset += int(topicNameSize)
		
		// Parse partitions count for this topic
		partitionsCount := binary.BigEndian.Uint32(requestBody[offset : offset+4])
		offset += 4
		
		// Response: topic_name_size(2) + topic_name + partitions_array
		response = append(response, byte(topicNameSize>>8), byte(topicNameSize))
		response = append(response, topicName...)
		
		partitionsCountBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(partitionsCountBytes, partitionsCount)
		response = append(response, partitionsCountBytes...)
		
		// Process each partition
		for j := uint32(0); j < partitionsCount && offset+12 <= len(requestBody); j++ {
			// Parse partition request: partition_id(4) + timestamp(8)
			partitionID := binary.BigEndian.Uint32(requestBody[offset : offset+4])
			timestamp := int64(binary.BigEndian.Uint64(requestBody[offset+4 : offset+12]))
			offset += 12
			
			// Response: partition_id(4) + error_code(2) + timestamp(8) + offset(8)
			partitionIDBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionIDBytes, partitionID)
			response = append(response, partitionIDBytes...)
			
			// Error code (0 = no error)
			response = append(response, 0, 0)
			
			// For stub: return the original timestamp for timestamp queries, or current time for earliest/latest
			var responseTimestamp int64
			var responseOffset int64
			
			switch timestamp {
			case -2: // earliest offset
				responseTimestamp = 0
				responseOffset = 0
			case -1: // latest offset  
				responseTimestamp = 1000000000 // some timestamp
				responseOffset = 0             // stub: no messages yet
			default: // specific timestamp
				responseTimestamp = timestamp
				responseOffset = 0 // stub: no messages at any timestamp
			}
			
			timestampBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(timestampBytes, uint64(responseTimestamp))
			response = append(response, timestampBytes...)
			
			offsetBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(offsetBytes, uint64(responseOffset))
			response = append(response, offsetBytes...)
		}
	}
	
	return response, nil
}

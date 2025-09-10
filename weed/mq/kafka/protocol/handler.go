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
	response = append(response, 0, 0, 0, 1) // 1 API key
	
	// API Key 18 (ApiVersions): api_key(2) + min_version(2) + max_version(2)
	response = append(response, 0, 18) // API key 18
	response = append(response, 0, 0)  // min version 0
	response = append(response, 0, 3)  // max version 3
	
	// Throttle time (4 bytes, 0 = no throttling)
	response = append(response, 0, 0, 0, 0)
	
	return response, nil
}

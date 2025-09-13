package kafka

import (
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

func TestDebugProduceV7Response(t *testing.T) {
	// Start gateway
	gatewayServer := gateway.NewTestServer(gateway.Options{
		Listen: "127.0.0.1:0",
	})

	go func() {
		if err := gatewayServer.Start(); err != nil {
			t.Errorf("Failed to start gateway: %v", err)
		}
	}()
	defer gatewayServer.Close()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	host, port := gatewayServer.GetListenerAddr()
	addr := fmt.Sprintf("%s:%d", host, port)
	t.Logf("Gateway running on %s", addr)

	// Add test topic
	handler := gatewayServer.GetHandler()
	topicName := "response-debug-topic"
	handler.AddTopicForTesting(topicName, 1)
	t.Logf("Added topic: %s", topicName)

	// Create raw TCP connection
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	t.Logf("=== Sending raw Produce v7 request ===")

	// Build a minimal Produce v7 request manually
	request := buildRawProduceV7Request(topicName)

	t.Logf("Sending Produce v7 request (%d bytes)", len(request))

	// Send request
	_, err = conn.Write(request)
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}

	// Read response
	responseSize := make([]byte, 4)
	_, err = conn.Read(responseSize)
	if err != nil {
		t.Fatalf("Failed to read response size: %v", err)
	}

	size := binary.BigEndian.Uint32(responseSize)
	t.Logf("Response size: %d bytes", size)

	responseData := make([]byte, size)
	_, err = conn.Read(responseData)
	if err != nil {
		t.Fatalf("Failed to read response data: %v", err)
	}

	t.Logf("=== Analyzing Produce v7 response ===")
	t.Logf("Raw response hex (%d bytes): %x", len(responseData), responseData)

	// Parse response manually
	analyzeProduceV7Response(t, responseData)
}

func buildRawProduceV7Request(topicName string) []byte {
	request := make([]byte, 0, 200)

	// Message size (placeholder, will be filled at end)
	sizePos := len(request)
	request = append(request, 0, 0, 0, 0)

	// Request header: api_key(2) + api_version(2) + correlation_id(4) + client_id(STRING)
	request = append(request, 0, 0)       // api_key = 0 (Produce)
	request = append(request, 0, 7)       // api_version = 7
	request = append(request, 0, 0, 0, 1) // correlation_id = 1

	// client_id (STRING: 2 bytes length + data)
	clientID := "test-client"
	request = append(request, 0, byte(len(clientID)))
	request = append(request, []byte(clientID)...)

	// Produce v7 request body: transactional_id(NULLABLE_STRING) + acks(2) + timeout_ms(4) + topics(ARRAY)

	// transactional_id (NULLABLE_STRING: -1 = null)
	request = append(request, 0xFF, 0xFF)

	// acks (-1 = all)
	request = append(request, 0xFF, 0xFF)

	// timeout_ms (10000)
	request = append(request, 0, 0, 0x27, 0x10)

	// topics array (1 topic)
	request = append(request, 0, 0, 0, 1)

	// topic name (STRING)
	request = append(request, 0, byte(len(topicName)))
	request = append(request, []byte(topicName)...)

	// partitions array (1 partition)
	request = append(request, 0, 0, 0, 1)

	// partition 0: partition_id(4) + record_set_size(4) + record_set_data
	request = append(request, 0, 0, 0, 0) // partition_id = 0

	// Simple record set (minimal)
	recordSet := []byte{
		0, 0, 0, 0, 0, 0, 0, 0, // base_offset
		0, 0, 0, 20, // batch_length
		0, 0, 0, 0, // partition_leader_epoch
		2,          // magic
		0, 0, 0, 0, // crc32
		0, 0, // attributes
		0, 0, 0, 0, // last_offset_delta
		// ... minimal record batch
	}

	request = append(request, 0, 0, 0, byte(len(recordSet))) // record_set_size
	request = append(request, recordSet...)

	// Fill in the message size
	messageSize := uint32(len(request) - 4)
	binary.BigEndian.PutUint32(request[sizePos:sizePos+4], messageSize)

	return request
}

func analyzeProduceV7Response(t *testing.T, data []byte) {
	if len(data) < 4 {
		t.Fatalf("Response too short: %d bytes", len(data))
	}

	offset := 0

	// correlation_id (4 bytes)
	correlationID := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	t.Logf("correlation_id: %d", correlationID)

	if len(data) < offset+4 {
		t.Fatalf("Response missing throttle_time_ms")
	}

	// throttle_time_ms (4 bytes)
	throttleTime := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	t.Logf("throttle_time_ms: %d", throttleTime)

	if len(data) < offset+4 {
		t.Fatalf("Response missing topics count")
	}

	// topics count (4 bytes)
	topicsCount := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	t.Logf("topics_count: %d", topicsCount)

	for i := uint32(0); i < topicsCount; i++ {
		if len(data) < offset+2 {
			t.Fatalf("Response missing topic name length")
		}

		// topic name (STRING: 2 bytes length + data)
		topicNameLen := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		if len(data) < offset+int(topicNameLen) {
			t.Fatalf("Response missing topic name data")
		}

		topicName := string(data[offset : offset+int(topicNameLen)])
		offset += int(topicNameLen)
		t.Logf("topic[%d]: %s", i, topicName)

		if len(data) < offset+4 {
			t.Fatalf("Response missing partitions count")
		}

		// partitions count (4 bytes)
		partitionsCount := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		t.Logf("partitions_count: %d", partitionsCount)

		for j := uint32(0); j < partitionsCount; j++ {
			if len(data) < offset+30 { // partition response is 30 bytes
				t.Fatalf("Response missing partition data (need 30 bytes, have %d)", len(data)-offset)
			}

			// partition response: partition_id(4) + error_code(2) + base_offset(8) + log_append_time(8) + log_start_offset(8)
			partitionID := binary.BigEndian.Uint32(data[offset : offset+4])
			offset += 4
			errorCode := binary.BigEndian.Uint16(data[offset : offset+2])
			offset += 2
			baseOffset := binary.BigEndian.Uint64(data[offset : offset+8])
			offset += 8
			logAppendTime := binary.BigEndian.Uint64(data[offset : offset+8])
			offset += 8
			logStartOffset := binary.BigEndian.Uint64(data[offset : offset+8])
			offset += 8

			t.Logf("partition[%d]: id=%d, error=%d, base_offset=%d, log_append_time=%d, log_start_offset=%d",
				j, partitionID, errorCode, baseOffset, logAppendTime, logStartOffset)
		}
	}

	t.Logf("Total bytes consumed: %d/%d", offset, len(data))
	if offset != len(data) {
		t.Logf("WARNING: %d bytes remaining", len(data)-offset)
	}
}

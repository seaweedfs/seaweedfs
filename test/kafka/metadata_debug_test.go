package kafka

import (
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

// TestMetadataV1DebugCapture captures the exact bytes kafka-go sends and expects
func TestMetadataV1DebugCapture(t *testing.T) {
	// Start gateway server
	gatewayServer := gateway.NewServer(gateway.Options{
		Listen: ":0", // random port
	})

	go func() {
		if err := gatewayServer.Start(); err != nil {
			t.Errorf("Gateway server error: %v", err)
		}
	}()
	defer gatewayServer.Close()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Get the actual listening address
	host, port := gatewayServer.GetListenerAddr()
	brokerAddr := fmt.Sprintf("%s:%d", host, port)
	t.Logf("Gateway running on %s", brokerAddr)

	// Get handler and configure it
	handler := gatewayServer.GetHandler()
	handler.SetBrokerAddress(host, port)

	// Add test topic
	topicName := "debug-topic"
	handler.AddTopicForTesting(topicName, 1)

	// Create raw TCP connection to manually send Metadata v1 request
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Send ApiVersions request first
	t.Log("=== Sending ApiVersions request ===")
	apiVersionsRequest := []byte{
		// Request header: api_key(2) + api_version(2) + correlation_id(4) + client_id_len(2) + client_id
		0x00, 0x12, // api_key = 18 (ApiVersions)
		0x00, 0x00, // api_version = 0
		0x00, 0x00, 0x00, 0x01, // correlation_id = 1
		0x00, 0x09, // client_id length = 9
		'd', 'e', 'b', 'u', 'g', '-', 't', 'e', 's', 't', // client_id = "debug-test"
	}
	
	// Prepend message length
	messageLen := make([]byte, 4)
	binary.BigEndian.PutUint32(messageLen, uint32(len(apiVersionsRequest)))
	fullRequest := append(messageLen, apiVersionsRequest...)
	
	_, err = conn.Write(fullRequest)
	if err != nil {
		t.Fatalf("Failed to send ApiVersions: %v", err)
	}

	// Read ApiVersions response
	responseLen := make([]byte, 4)
	_, err = conn.Read(responseLen)
	if err != nil {
		t.Fatalf("Failed to read ApiVersions response length: %v", err)
	}
	
	respLen := binary.BigEndian.Uint32(responseLen)
	response := make([]byte, respLen)
	_, err = conn.Read(response)
	if err != nil {
		t.Fatalf("Failed to read ApiVersions response: %v", err)
	}
	
	t.Logf("ApiVersions response (%d bytes): %x", len(response), response)

	// Now send Metadata v1 request
	t.Log("=== Sending Metadata v1 request ===")
	metadataRequest := []byte{
		// Request header: api_key(2) + api_version(2) + correlation_id(4) + client_id_len(2) + client_id
		0x00, 0x03, // api_key = 3 (Metadata)
		0x00, 0x01, // api_version = 1
		0x00, 0x00, 0x00, 0x02, // correlation_id = 2
		0x00, 0x09, // client_id length = 9
		'd', 'e', 'b', 'u', 'g', '-', 't', 'e', 's', 't', // client_id = "debug-test"
		// Metadata request body: topics_count(4) + topic_name_len(2) + topic_name
		0x00, 0x00, 0x00, 0x01, // topics_count = 1
		0x00, 0x0B, // topic_name length = 11
		'd', 'e', 'b', 'u', 'g', '-', 't', 'o', 'p', 'i', 'c', // topic_name = "debug-topic"
	}
	
	// Prepend message length
	messageLen = make([]byte, 4)
	binary.BigEndian.PutUint32(messageLen, uint32(len(metadataRequest)))
	fullRequest = append(messageLen, metadataRequest...)
	
	t.Logf("Sending Metadata v1 request (%d bytes): %x", len(fullRequest), fullRequest)
	
	_, err = conn.Write(fullRequest)
	if err != nil {
		t.Fatalf("Failed to send Metadata: %v", err)
	}

	// Read Metadata response
	responseLen = make([]byte, 4)
	_, err = conn.Read(responseLen)
	if err != nil {
		t.Fatalf("Failed to read Metadata response length: %v", err)
	}
	
	respLen = binary.BigEndian.Uint32(responseLen)
	response = make([]byte, respLen)
	_, err = conn.Read(response)
	if err != nil {
		t.Fatalf("Failed to read Metadata response: %v", err)
	}
	
	t.Logf("Metadata v1 response (%d bytes): %x", len(response), response)
	
	// Parse the response manually to understand the format
	t.Log("=== Parsing Metadata v1 response ===")
	offset := 0
	
	// Correlation ID (4 bytes)
	correlationID := binary.BigEndian.Uint32(response[offset:offset+4])
	offset += 4
	t.Logf("Correlation ID: %d", correlationID)
	
	// Brokers array length (4 bytes)
	brokersCount := binary.BigEndian.Uint32(response[offset:offset+4])
	offset += 4
	t.Logf("Brokers count: %d", brokersCount)
	
	// Parse each broker
	for i := uint32(0); i < brokersCount; i++ {
		// node_id (4 bytes)
		nodeID := binary.BigEndian.Uint32(response[offset:offset+4])
		offset += 4
		t.Logf("Broker %d node_id: %d", i, nodeID)
		
		// host (STRING: 2 bytes length + bytes)
		hostLen := binary.BigEndian.Uint16(response[offset:offset+2])
		offset += 2
		hostBytes := response[offset:offset+int(hostLen)]
		offset += int(hostLen)
		t.Logf("Broker %d host: %s", i, string(hostBytes))
		
		// port (4 bytes)
		portNum := binary.BigEndian.Uint32(response[offset:offset+4])
		offset += 4
		t.Logf("Broker %d port: %d", i, portNum)
		
		// rack (STRING: 2 bytes length + bytes) - v1 addition
		rackLen := binary.BigEndian.Uint16(response[offset:offset+2])
		offset += 2
		if rackLen > 0 {
			rackBytes := response[offset:offset+int(rackLen)]
			offset += int(rackLen)
			t.Logf("Broker %d rack: %s", i, string(rackBytes))
		} else {
			t.Logf("Broker %d rack: (empty)", i)
		}
	}
	
	// Topics array length (4 bytes)
	if offset < len(response) {
		topicsCount := binary.BigEndian.Uint32(response[offset:offset+4])
		offset += 4
		t.Logf("Topics count: %d", topicsCount)
		
		// Parse each topic
		for i := uint32(0); i < topicsCount && offset < len(response); i++ {
			// error_code (2 bytes)
			errorCode := binary.BigEndian.Uint16(response[offset:offset+2])
			offset += 2
			t.Logf("Topic %d error_code: %d", i, errorCode)
			
			// name (STRING: 2 bytes length + bytes)
			nameLen := binary.BigEndian.Uint16(response[offset:offset+2])
			offset += 2
			nameBytes := response[offset:offset+int(nameLen)]
			offset += int(nameLen)
			t.Logf("Topic %d name: %s", i, string(nameBytes))
			
			// is_internal (1 byte) - v1 addition
			if offset < len(response) {
				isInternal := response[offset]
				offset += 1
				t.Logf("Topic %d is_internal: %d", i, isInternal)
			}
			
			// partitions array length (4 bytes)
			if offset+4 <= len(response) {
				partitionsCount := binary.BigEndian.Uint32(response[offset:offset+4])
				offset += 4
				t.Logf("Topic %d partitions count: %d", i, partitionsCount)
				
				// Skip partition details for brevity
				for j := uint32(0); j < partitionsCount && offset < len(response); j++ {
					// error_code (2) + partition_id (4) + leader (4) + replicas (4+n*4) + isr (4+n*4)
					if offset+2 <= len(response) {
						partErrorCode := binary.BigEndian.Uint16(response[offset:offset+2])
						offset += 2
						t.Logf("  Partition %d error_code: %d", j, partErrorCode)
					}
					if offset+4 <= len(response) {
						partitionID := binary.BigEndian.Uint32(response[offset:offset+4])
						offset += 4
						t.Logf("  Partition %d id: %d", j, partitionID)
					}
					if offset+4 <= len(response) {
						leader := binary.BigEndian.Uint32(response[offset:offset+4])
						offset += 4
						t.Logf("  Partition %d leader: %d", j, leader)
					}
					// Skip replicas and isr arrays for brevity - just advance offset
					if offset+4 <= len(response) {
						replicasCount := binary.BigEndian.Uint32(response[offset:offset+4])
						offset += 4 + int(replicasCount)*4
						t.Logf("  Partition %d replicas count: %d", j, replicasCount)
					}
					if offset+4 <= len(response) {
						isrCount := binary.BigEndian.Uint32(response[offset:offset+4])
						offset += 4 + int(isrCount)*4
						t.Logf("  Partition %d isr count: %d", j, isrCount)
					}
				}
			}
		}
	}
	
	t.Logf("Parsed %d bytes, remaining: %d", offset, len(response)-offset)
}

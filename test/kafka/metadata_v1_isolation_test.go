package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

// TestMetadataV1Isolation creates a minimal test to isolate the Metadata v1 parsing issue
func TestMetadataV1Isolation(t *testing.T) {
	// Start gateway
	gatewayServer := gateway.NewServer(gateway.Options{
		Listen: "127.0.0.1:0",
	})

	go gatewayServer.Start()
	defer gatewayServer.Close()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	host, port := gatewayServer.GetListenerAddr()
	addr := fmt.Sprintf("%s:%d", host, port)
	t.Logf("Gateway running on %s", addr)

	// Add test topic
	handler := gatewayServer.GetHandler()
	handler.AddTopicForTesting("isolation-topic", 1)
	t.Logf("Added topic: isolation-topic")

	// Test 1: Raw TCP connection to manually send/receive Metadata v1
	t.Logf("=== Test 1: Raw TCP Metadata v1 Request ===")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Send ApiVersions first
	apiVersionsReq := buildApiVersionsRequest()
	if err := sendRequest(conn, apiVersionsReq); err != nil {
		t.Fatalf("Failed to send ApiVersions: %v", err)
	}

	apiVersionsResp, err := readResponse(conn)
	if err != nil {
		t.Fatalf("Failed to read ApiVersions response: %v", err)
	}
	t.Logf("ApiVersions response: %d bytes", len(apiVersionsResp))

	// Send Metadata v1 request
	metadataReq := buildMetadataV1Request([]string{"isolation-topic"})
	if err := sendRequest(conn, metadataReq); err != nil {
		t.Fatalf("Failed to send Metadata v1: %v", err)
	}

	metadataResp, err := readResponse(conn)
	if err != nil {
		t.Fatalf("Failed to read Metadata v1 response: %v", err)
	}
	t.Logf("Metadata v1 response: %d bytes", len(metadataResp))
	t.Logf("Metadata v1 hex: %x", metadataResp)

	// Test 2: Parse our response manually to verify structure
	t.Logf("=== Test 2: Manual Parsing of Our Response ===")
	if err := parseAndValidateMetadataV1Response(metadataResp, t); err != nil {
		t.Errorf("Manual parsing failed: %v", err)
	}

	// Test 3: Try kafka-go connection with detailed error capture
	t.Logf("=== Test 3: kafka-go Connection with Error Capture ===")
	testKafkaGoConnection(addr, t)
}

func buildApiVersionsRequest() []byte {
	var buf bytes.Buffer
	
	// Request header
	binary.Write(&buf, binary.BigEndian, int32(22)) // message size (will be updated)
	binary.Write(&buf, binary.BigEndian, int16(18)) // ApiVersions API key
	binary.Write(&buf, binary.BigEndian, int16(0))  // version
	binary.Write(&buf, binary.BigEndian, int32(1))  // correlation ID
	binary.Write(&buf, binary.BigEndian, int16(9))  // client ID length
	buf.WriteString("debug-client")
	
	// Update message size
	data := buf.Bytes()
	binary.BigEndian.PutUint32(data[0:4], uint32(len(data)-4))
	return data
}

func buildMetadataV1Request(topics []string) []byte {
	var buf bytes.Buffer
	
	// Request header
	binary.Write(&buf, binary.BigEndian, int32(0)) // message size (will be updated)
	binary.Write(&buf, binary.BigEndian, int16(3)) // Metadata API key
	binary.Write(&buf, binary.BigEndian, int16(1)) // version 1
	binary.Write(&buf, binary.BigEndian, int32(2)) // correlation ID
	binary.Write(&buf, binary.BigEndian, int16(9)) // client ID length
	buf.WriteString("debug-client")
	
	// Request body - topics array
	binary.Write(&buf, binary.BigEndian, int32(len(topics)))
	for _, topic := range topics {
		binary.Write(&buf, binary.BigEndian, int16(len(topic)))
		buf.WriteString(topic)
	}
	
	// Update message size
	data := buf.Bytes()
	binary.BigEndian.PutUint32(data[0:4], uint32(len(data)-4))
	return data
}

func sendRequest(conn net.Conn, data []byte) error {
	_, err := conn.Write(data)
	return err
}

func readResponse(conn net.Conn) ([]byte, error) {
	// Read response size
	sizeBuf := make([]byte, 4)
	if _, err := conn.Read(sizeBuf); err != nil {
		return nil, fmt.Errorf("failed to read response size: %v", err)
	}
	
	size := binary.BigEndian.Uint32(sizeBuf)
	
	// Read response data
	data := make([]byte, size)
	if _, err := conn.Read(data); err != nil {
		return nil, fmt.Errorf("failed to read response data: %v", err)
	}
	
	return data, nil
}

func parseAndValidateMetadataV1Response(data []byte, t *testing.T) error {
	buf := bytes.NewReader(data)
	
	// Parse correlation ID
	var correlationID int32
	if err := binary.Read(buf, binary.BigEndian, &correlationID); err != nil {
		return fmt.Errorf("failed to read correlation ID: %v", err)
	}
	t.Logf("Correlation ID: %d", correlationID)
	
	// Parse brokers array
	var brokersCount int32
	if err := binary.Read(buf, binary.BigEndian, &brokersCount); err != nil {
		return fmt.Errorf("failed to read brokers count: %v", err)
	}
	t.Logf("Brokers count: %d", brokersCount)
	
	for i := 0; i < int(brokersCount); i++ {
		// NodeID
		var nodeID int32
		if err := binary.Read(buf, binary.BigEndian, &nodeID); err != nil {
			return fmt.Errorf("failed to read broker %d nodeID: %v", i, err)
		}
		
		// Host
		var hostLen int16
		if err := binary.Read(buf, binary.BigEndian, &hostLen); err != nil {
			return fmt.Errorf("failed to read broker %d host length: %v", i, err)
		}
		hostBytes := make([]byte, hostLen)
		if _, err := buf.Read(hostBytes); err != nil {
			return fmt.Errorf("failed to read broker %d host: %v", i, err)
		}
		
		// Port
		var port int32
		if err := binary.Read(buf, binary.BigEndian, &port); err != nil {
			return fmt.Errorf("failed to read broker %d port: %v", i, err)
		}
		
		// Rack
		var rackLen int16
		if err := binary.Read(buf, binary.BigEndian, &rackLen); err != nil {
			return fmt.Errorf("failed to read broker %d rack length: %v", i, err)
		}
		if rackLen > 0 {
			rackBytes := make([]byte, rackLen)
			if _, err := buf.Read(rackBytes); err != nil {
				return fmt.Errorf("failed to read broker %d rack: %v", i, err)
			}
		}
		
		t.Logf("Broker %d: NodeID=%d, Host=%s, Port=%d, Rack=empty", i, nodeID, string(hostBytes), port)
	}
	
	// Parse ControllerID
	var controllerID int32
	if err := binary.Read(buf, binary.BigEndian, &controllerID); err != nil {
		return fmt.Errorf("failed to read controller ID: %v", err)
	}
	t.Logf("Controller ID: %d", controllerID)
	
	// Parse topics array
	var topicsCount int32
	if err := binary.Read(buf, binary.BigEndian, &topicsCount); err != nil {
		return fmt.Errorf("failed to read topics count: %v", err)
	}
	t.Logf("Topics count: %d", topicsCount)
	
	for i := 0; i < int(topicsCount); i++ {
		// Error code
		var errorCode int16
		if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
			return fmt.Errorf("failed to read topic %d error code: %v", i, err)
		}
		
		// Name
		var nameLen int16
		if err := binary.Read(buf, binary.BigEndian, &nameLen); err != nil {
			return fmt.Errorf("failed to read topic %d name length: %v", i, err)
		}
		nameBytes := make([]byte, nameLen)
		if _, err := buf.Read(nameBytes); err != nil {
			return fmt.Errorf("failed to read topic %d name: %v", i, err)
		}
		
		// IsInternal
		var isInternal byte
		if err := binary.Read(buf, binary.BigEndian, &isInternal); err != nil {
			return fmt.Errorf("failed to read topic %d isInternal: %v", i, err)
		}
		
		// Partitions
		var partitionsCount int32
		if err := binary.Read(buf, binary.BigEndian, &partitionsCount); err != nil {
			return fmt.Errorf("failed to read topic %d partitions count: %v", i, err)
		}
		
		t.Logf("Topic %d: ErrorCode=%d, Name=%s, IsInternal=%d, Partitions=%d", 
			i, errorCode, string(nameBytes), isInternal, partitionsCount)
		
		// Parse each partition
		for j := 0; j < int(partitionsCount); j++ {
			var partErrorCode int16
			var partitionID int32
			var leaderID int32
			
			if err := binary.Read(buf, binary.BigEndian, &partErrorCode); err != nil {
				return fmt.Errorf("failed to read partition %d error code: %v", j, err)
			}
			if err := binary.Read(buf, binary.BigEndian, &partitionID); err != nil {
				return fmt.Errorf("failed to read partition %d ID: %v", j, err)
			}
			if err := binary.Read(buf, binary.BigEndian, &leaderID); err != nil {
				return fmt.Errorf("failed to read partition %d leader: %v", j, err)
			}
			
			// Replicas array
			var replicasCount int32
			if err := binary.Read(buf, binary.BigEndian, &replicasCount); err != nil {
				return fmt.Errorf("failed to read partition %d replicas count: %v", j, err)
			}
			replicas := make([]int32, replicasCount)
			for k := 0; k < int(replicasCount); k++ {
				if err := binary.Read(buf, binary.BigEndian, &replicas[k]); err != nil {
					return fmt.Errorf("failed to read partition %d replica %d: %v", j, k, err)
				}
			}
			
			// ISR array
			var isrCount int32
			if err := binary.Read(buf, binary.BigEndian, &isrCount); err != nil {
				return fmt.Errorf("failed to read partition %d ISR count: %v", j, err)
			}
			isr := make([]int32, isrCount)
			for k := 0; k < int(isrCount); k++ {
				if err := binary.Read(buf, binary.BigEndian, &isr[k]); err != nil {
					return fmt.Errorf("failed to read partition %d ISR %d: %v", j, k, err)
				}
			}
			
			t.Logf("  Partition %d: ErrorCode=%d, ID=%d, Leader=%d, Replicas=%v, ISR=%v", 
				j, partErrorCode, partitionID, leaderID, replicas, isr)
		}
	}
	
	remaining := buf.Len()
	if remaining > 0 {
		t.Logf("WARNING: %d bytes remaining in response", remaining)
	}
	
	return nil
}

func testKafkaGoConnection(addr string, t *testing.T) {
	// Create a kafka-go connection
	conn, err := kafka.Dial("tcp", addr)
	if err != nil {
		t.Errorf("kafka.Dial failed: %v", err)
		return
	}
	defer conn.Close()
	
	// Try ReadPartitions with detailed error handling
	t.Logf("Calling ReadPartitions...")
	partitions, err := conn.ReadPartitions("isolation-topic")
	if err != nil {
		t.Errorf("ReadPartitions failed: %v", err)
		
		// Try to get more details about the error
		if netErr, ok := err.(net.Error); ok {
			t.Errorf("Network error details: Timeout=%v, Temporary=%v", netErr.Timeout(), netErr.Temporary())
		}
		return
	}
	
	t.Logf("ReadPartitions succeeded! Found %d partitions", len(partitions))
	for i, p := range partitions {
		t.Logf("Partition %d: Topic=%s, ID=%d, Leader=%+v", i, p.Topic, p.ID, p.Leader)
	}
}

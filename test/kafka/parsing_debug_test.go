package kafka

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

// TestParsingDebug attempts to manually replicate kafka-go's parsing logic
func TestParsingDebug(t *testing.T) {
	// Start gateway
	gatewayServer := gateway.NewTestServer(gateway.Options{
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
	handler.AddTopicForTesting("parsing-topic", 1)

	// Get the actual response from our gateway
	response := captureMetadataResponse(addr, t)
	if response == nil {
		return
	}

	// Manually parse using kafka-go's logic
	t.Logf("=== Manual Parsing Simulation ===")
	simulateKafkaGoParsingV1(response, t)
}

func captureMetadataResponse(addr string, t *testing.T) []byte {
	// Create raw TCP connection and get response
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Errorf("Failed to dial: %v", err)
		return nil
	}
	defer conn.Close()

	// Send ApiVersions first
	apiVersionsReq := buildSimpleApiVersionsRequest()
	if _, err := conn.Write(apiVersionsReq); err != nil {
		t.Errorf("Failed to send ApiVersions: %v", err)
		return nil
	}

	// Read ApiVersions response
	if _, err := readSimpleResponse(conn); err != nil {
		t.Errorf("Failed to read ApiVersions response: %v", err)
		return nil
	}

	// Send Metadata v1 request
	metadataReq := buildSimpleMetadataV1Request([]string{"parsing-topic"})
	if _, err := conn.Write(metadataReq); err != nil {
		t.Errorf("Failed to send Metadata: %v", err)
		return nil
	}

	// Read Metadata response
	response, err := readSimpleResponse(conn)
	if err != nil {
		t.Errorf("Failed to read Metadata response: %v", err)
		return nil
	}

	t.Logf("Captured Metadata response (%d bytes): %x", len(response), response)
	return response
}

func buildSimpleApiVersionsRequest() []byte {
	clientID := "parser"
	payloadSize := 2 + 2 + 4 + 2 + len(clientID)

	req := make([]byte, 4)
	binary.BigEndian.PutUint32(req[0:4], uint32(payloadSize))
	req = append(req, 0, 18)                  // ApiVersions
	req = append(req, 0, 0)                   // version 0
	req = append(req, 0, 0, 0, 1)             // correlation ID
	req = append(req, 0, byte(len(clientID))) // client ID length
	req = append(req, []byte(clientID)...)
	return req
}

func buildSimpleMetadataV1Request(topics []string) []byte {
	clientID := "parser"
	payloadSize := 2 + 2 + 4 + 2 + len(clientID) + 4
	for _, topic := range topics {
		payloadSize += 2 + len(topic)
	}

	req := make([]byte, 4)
	binary.BigEndian.PutUint32(req[0:4], uint32(payloadSize))
	req = append(req, 0, 3)                   // Metadata
	req = append(req, 0, 1)                   // version 1
	req = append(req, 0, 0, 0, 2)             // correlation ID
	req = append(req, 0, byte(len(clientID))) // client ID length
	req = append(req, []byte(clientID)...)

	// Topics array
	topicsLen := uint32(len(topics))
	req = append(req, byte(topicsLen>>24), byte(topicsLen>>16), byte(topicsLen>>8), byte(topicsLen))
	for _, topic := range topics {
		topicLen := uint16(len(topic))
		req = append(req, byte(topicLen>>8), byte(topicLen))
		req = append(req, []byte(topic)...)
	}

	return req
}

func readSimpleResponse(conn net.Conn) ([]byte, error) {
	// Read response size
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(sizeBuf)

	// Read response data
	data := make([]byte, size)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, err
	}

	return data, nil
}

// simulateKafkaGoParsingV1 manually replicates kafka-go's parsing logic
func simulateKafkaGoParsingV1(data []byte, t *testing.T) {
	reader := bufio.NewReader(bytes.NewReader(data))
	totalSize := len(data)
	remainingSize := totalSize

	t.Logf("Starting parse of %d bytes", totalSize)

	// Simulate kafka-go's metadataResponseV1 struct parsing
	// type metadataResponseV1 struct {
	//     Brokers      []brokerMetadataV1
	//     ControllerID int32
	//     Topics       []topicMetadataV1
	// }

	// Parse correlation ID (this is handled before the struct parsing)
	correlationID, err := readInt32FromReader(reader, &remainingSize, t)
	if err != nil {
		t.Errorf("Failed to read correlation ID: %v", err)
		return
	}
	t.Logf("Correlation ID: %d, remaining: %d", correlationID, remainingSize)

	// Parse Brokers array
	brokersCount, err := readInt32FromReader(reader, &remainingSize, t)
	if err != nil {
		t.Errorf("Failed to read brokers count: %v", err)
		return
	}
	t.Logf("Brokers count: %d, remaining: %d", brokersCount, remainingSize)

	// Parse each broker (brokerMetadataV1)
	for i := 0; i < int(brokersCount); i++ {
		t.Logf("Parsing broker %d at remaining: %d", i, remainingSize)

		// NodeID (int32)
		nodeID, err := readInt32FromReader(reader, &remainingSize, t)
		if err != nil {
			t.Errorf("Failed to read broker %d nodeID: %v", i, err)
			return
		}

		// Host (string)
		host, err := readStringFromReader(reader, &remainingSize, t)
		if err != nil {
			t.Errorf("Failed to read broker %d host: %v", i, err)
			return
		}

		// Port (int32)
		port, err := readInt32FromReader(reader, &remainingSize, t)
		if err != nil {
			t.Errorf("Failed to read broker %d port: %v", i, err)
			return
		}

		// Rack (string) - v1 addition
		rack, err := readStringFromReader(reader, &remainingSize, t)
		if err != nil {
			t.Errorf("Failed to read broker %d rack: %v", i, err)
			return
		}

		t.Logf("Broker %d: NodeID=%d, Host=%s, Port=%d, Rack=%s, remaining: %d",
			i, nodeID, host, port, rack, remainingSize)
	}

	// Parse ControllerID (int32)
	controllerID, err := readInt32FromReader(reader, &remainingSize, t)
	if err != nil {
		t.Errorf("Failed to read controller ID: %v", err)
		return
	}
	t.Logf("Controller ID: %d, remaining: %d", controllerID, remainingSize)

	// Parse Topics array
	topicsCount, err := readInt32FromReader(reader, &remainingSize, t)
	if err != nil {
		t.Errorf("Failed to read topics count: %v", err)
		return
	}
	t.Logf("Topics count: %d, remaining: %d", topicsCount, remainingSize)

	// Parse each topic (topicMetadataV1)
	for i := 0; i < int(topicsCount); i++ {
		t.Logf("Parsing topic %d at remaining: %d", i, remainingSize)

		// TopicErrorCode (int16)
		errorCode, err := readInt16FromReader(reader, &remainingSize, t)
		if err != nil {
			t.Errorf("Failed to read topic %d error code: %v", i, err)
			return
		}

		// TopicName (string)
		name, err := readStringFromReader(reader, &remainingSize, t)
		if err != nil {
			t.Errorf("Failed to read topic %d name: %v", i, err)
			return
		}

		// Internal (bool) - v1 addition
		internal, err := readBoolFromReader(reader, &remainingSize, t)
		if err != nil {
			t.Errorf("Failed to read topic %d internal: %v", i, err)
			return
		}

		t.Logf("Topic %d: ErrorCode=%d, Name=%s, Internal=%v, remaining: %d",
			i, errorCode, name, internal, remainingSize)

		// Parse Partitions array
		partitionsCount, err := readInt32FromReader(reader, &remainingSize, t)
		if err != nil {
			t.Errorf("Failed to read topic %d partitions count: %v", i, err)
			return
		}
		t.Logf("Topic %d partitions count: %d, remaining: %d", i, partitionsCount, remainingSize)

		// Parse each partition (partitionMetadataV1)
		for j := 0; j < int(partitionsCount); j++ {
			t.Logf("Parsing partition %d at remaining: %d", j, remainingSize)

			// PartitionErrorCode (int16)
			partErrorCode, err := readInt16FromReader(reader, &remainingSize, t)
			if err != nil {
				t.Errorf("Failed to read partition %d error code: %v", j, err)
				return
			}

			// PartitionID (int32)
			partitionID, err := readInt32FromReader(reader, &remainingSize, t)
			if err != nil {
				t.Errorf("Failed to read partition %d ID: %v", j, err)
				return
			}

			// Leader (int32)
			leader, err := readInt32FromReader(reader, &remainingSize, t)
			if err != nil {
				t.Errorf("Failed to read partition %d leader: %v", j, err)
				return
			}

			// Replicas ([]int32)
			replicas, err := readInt32ArrayFromReader(reader, &remainingSize, t)
			if err != nil {
				t.Errorf("Failed to read partition %d replicas: %v", j, err)
				return
			}

			// Isr ([]int32)
			isr, err := readInt32ArrayFromReader(reader, &remainingSize, t)
			if err != nil {
				t.Errorf("Failed to read partition %d ISR: %v", j, err)
				return
			}

			t.Logf("Partition %d: ErrorCode=%d, ID=%d, Leader=%d, Replicas=%v, ISR=%v, remaining: %d",
				j, partErrorCode, partitionID, leader, replicas, isr, remainingSize)
		}
	}

	t.Logf("=== PARSING COMPLETE ===")
	t.Logf("Final remaining bytes: %d", remainingSize)

	if remainingSize == 0 {
		t.Logf("✅ SUCCESS: All bytes consumed correctly!")
	} else {
		t.Errorf("❌ FAILURE: %d bytes left unread - this is the expectZeroSize issue!", remainingSize)

		// Show the remaining bytes
		remaining := make([]byte, remainingSize)
		if n, err := reader.Read(remaining); err == nil {
			t.Logf("Remaining bytes: %x", remaining[:n])
		}
	}
}

// Helper functions to simulate kafka-go's reading logic
func readInt32FromReader(reader *bufio.Reader, remainingSize *int, t *testing.T) (int32, error) {
	if *remainingSize < 4 {
		return 0, fmt.Errorf("not enough bytes for int32: need 4, have %d", *remainingSize)
	}

	bytes, err := reader.Peek(4)
	if err != nil {
		return 0, err
	}

	value := int32(binary.BigEndian.Uint32(bytes))

	n, err := reader.Discard(4)
	if err != nil {
		return 0, err
	}

	*remainingSize -= n
	return value, nil
}

func readInt16FromReader(reader *bufio.Reader, remainingSize *int, t *testing.T) (int16, error) {
	if *remainingSize < 2 {
		return 0, fmt.Errorf("not enough bytes for int16: need 2, have %d", *remainingSize)
	}

	bytes, err := reader.Peek(2)
	if err != nil {
		return 0, err
	}

	value := int16(binary.BigEndian.Uint16(bytes))

	n, err := reader.Discard(2)
	if err != nil {
		return 0, err
	}

	*remainingSize -= n
	return value, nil
}

func readStringFromReader(reader *bufio.Reader, remainingSize *int, t *testing.T) (string, error) {
	// Read length first (int16)
	if *remainingSize < 2 {
		return "", fmt.Errorf("not enough bytes for string length: need 2, have %d", *remainingSize)
	}

	lengthBytes, err := reader.Peek(2)
	if err != nil {
		return "", err
	}

	length := int(binary.BigEndian.Uint16(lengthBytes))

	// Discard length bytes
	n, err := reader.Discard(2)
	if err != nil {
		return "", err
	}
	*remainingSize -= n

	// Read string data
	if *remainingSize < length {
		return "", fmt.Errorf("not enough bytes for string data: need %d, have %d", length, *remainingSize)
	}

	if length == 0 {
		return "", nil
	}

	stringBytes := make([]byte, length)
	n, err = reader.Read(stringBytes)
	if err != nil {
		return "", err
	}

	*remainingSize -= n
	return string(stringBytes), nil
}

func readBoolFromReader(reader *bufio.Reader, remainingSize *int, t *testing.T) (bool, error) {
	if *remainingSize < 1 {
		return false, fmt.Errorf("not enough bytes for bool: need 1, have %d", *remainingSize)
	}

	bytes, err := reader.Peek(1)
	if err != nil {
		return false, err
	}

	value := bytes[0] != 0

	n, err := reader.Discard(1)
	if err != nil {
		return false, err
	}

	*remainingSize -= n
	return value, nil
}

func readInt32ArrayFromReader(reader *bufio.Reader, remainingSize *int, t *testing.T) ([]int32, error) {
	// Read array length first (int32)
	length, err := readInt32FromReader(reader, remainingSize, t)
	if err != nil {
		return nil, fmt.Errorf("failed to read array length: %v", err)
	}

	if length < 0 {
		return nil, nil // Null array
	}

	result := make([]int32, length)
	for i := 0; i < int(length); i++ {
		value, err := readInt32FromReader(reader, remainingSize, t)
		if err != nil {
			return nil, fmt.Errorf("failed to read array element %d: %v", i, err)
		}
		result[i] = value
	}

	return result, nil
}

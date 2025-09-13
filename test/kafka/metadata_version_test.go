package kafka

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/protocol"
)

func TestMetadataVersionComparison(t *testing.T) {
	// Create handler
	handler := protocol.NewHandler()
	
	// Add test topic
	handler.AddTopicForTesting("test-topic", 1)
	
	// Set broker address
	handler.SetBrokerAddress("127.0.0.1", 9092)
	
	// Test v0 response
	v0Response, err := handler.HandleMetadataV0(12345, []byte{0, 0}) // empty client_id + empty topics
	if err != nil {
		t.Fatalf("v0 error: %v", err)
	}
	
	// Test v1 response  
	v1Response, err := handler.HandleMetadataV1(12345, []byte{0, 0}) // empty client_id + empty topics
	if err != nil {
		t.Fatalf("v1 error: %v", err)
	}
	
	fmt.Printf("Metadata v0 response (%d bytes): %x\n", len(v0Response), v0Response)
	fmt.Printf("Metadata v1 response (%d bytes): %x\n", len(v1Response), v1Response)
	
	// Compare lengths
	fmt.Printf("Length difference: v1 is %d bytes longer than v0\n", len(v1Response) - len(v0Response))
}

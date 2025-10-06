package gateway

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/protocol"
	filer_pb "github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	schema_pb "github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// mockSeaweedMQHandler is a minimal mock for unit testing without real SeaweedMQ
type mockSeaweedMQHandler struct{}

func (m *mockSeaweedMQHandler) TopicExists(topic string) bool                    { return false }
func (m *mockSeaweedMQHandler) ListTopics() []string                             { return []string{} }
func (m *mockSeaweedMQHandler) CreateTopic(topic string, partitions int32) error { return nil }
func (m *mockSeaweedMQHandler) CreateTopicWithSchemas(name string, partitions int32, keyRecordType *schema_pb.RecordType, valueRecordType *schema_pb.RecordType) error {
	return nil
}
func (m *mockSeaweedMQHandler) DeleteTopic(topic string) error { return nil }
func (m *mockSeaweedMQHandler) GetTopicInfo(topic string) (*integration.KafkaTopicInfo, bool) {
	return nil, false
}
func (m *mockSeaweedMQHandler) ProduceRecord(topicName string, partitionID int32, key, value []byte) (int64, error) {
	return 0, fmt.Errorf("mock handler: not implemented")
}
func (m *mockSeaweedMQHandler) ProduceRecordValue(topicName string, partitionID int32, key []byte, recordValueBytes []byte) (int64, error) {
	return 0, fmt.Errorf("mock handler: not implemented")
}
func (m *mockSeaweedMQHandler) GetStoredRecords(topic string, partition int32, fromOffset int64, maxRecords int) ([]integration.SMQRecord, error) {
	return nil, fmt.Errorf("mock handler: not implemented")
}
func (m *mockSeaweedMQHandler) GetEarliestOffset(topic string, partition int32) (int64, error) {
	return 0, fmt.Errorf("mock handler: not implemented")
}
func (m *mockSeaweedMQHandler) GetLatestOffset(topic string, partition int32) (int64, error) {
	return 0, fmt.Errorf("mock handler: not implemented")
}
func (m *mockSeaweedMQHandler) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fmt.Errorf("mock handler: not implemented")
}
func (m *mockSeaweedMQHandler) CreatePerConnectionBrokerClient() (*integration.BrokerClient, error) {
	// Return a minimal broker client that won't actually connect
	return nil, fmt.Errorf("mock handler: per-connection broker client not available in unit test mode")
}
func (m *mockSeaweedMQHandler) GetFilerClientAccessor() *filer_client.FilerClientAccessor {
	return nil
}
func (m *mockSeaweedMQHandler) GetBrokerAddresses() []string {
	return []string{"localhost:9092"} // Return a dummy broker address for unit tests
}
func (m *mockSeaweedMQHandler) Close() error                                     { return nil }
func (m *mockSeaweedMQHandler) SetProtocolHandler(h integration.ProtocolHandler) {}

// NewMinimalTestHandler creates a minimal handler for unit testing
// that won't actually process Kafka protocol requests
func NewMinimalTestHandler() *protocol.Handler {
	return protocol.NewTestHandlerWithMock(&mockSeaweedMQHandler{})
}

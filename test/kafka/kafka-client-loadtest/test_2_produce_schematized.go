package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/linkedin/goavro/v2"
)

// Test 2: Produce Schematized Messages
// This test verifies that we can produce Avro messages in Confluent Wire Format
// and that the topic gets created with proper schema information.

func main() {
	log.Println("=== Test 2: Produce Schematized Messages ===")

	topicName := "test-produce-topic"

	// Step 1: Register a schema in Schema Registry
	log.Println("Step 1: Registering schema in Schema Registry...")
	schemaID, schema, err := registerTestSchema(topicName)
	if err != nil {
		log.Fatalf("Failed to register schema: %v", err)
	}
	log.Printf("✓ Schema registered with ID: %d", schemaID)

	// Step 2: Create Avro codec
	log.Println("Step 2: Creating Avro codec...")
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Fatalf("Failed to create Avro codec: %v", err)
	}
	log.Printf("✓ Avro codec created")

	// Step 3: Create Kafka producer
	log.Println("Step 3: Creating Kafka producer...")
	producer, err := createKafkaProducer()
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()
	log.Printf("✓ Kafka producer created")

	// Step 4: Produce a schematized message
	log.Println("Step 4: Producing schematized message...")
	err = produceSchematizedMessage(producer, topicName, schemaID, codec)
	if err != nil {
		log.Fatalf("Failed to produce message: %v", err)
	}
	log.Printf("✓ Schematized message produced")

	// Step 5: Wait for topic creation and check topic.conf
	log.Println("Step 5: Checking topic.conf for schema information...")
	time.Sleep(3 * time.Second)

	topicConf, err := getTopicConf(topicName)
	if err != nil {
		log.Fatalf("Failed to get topic.conf: %v", err)
	}

	// Step 6: Verify schema information is present
	log.Println("Step 6: Verifying schema information...")
	if topicConf.MessageRecordType == nil {
		log.Printf("⚠️  WARNING: messageRecordType is null in topic.conf")
		log.Printf("This suggests schema management is not working properly")
	} else {
		log.Printf("✓ messageRecordType found in topic.conf")
	}

	// Pretty print the topic configuration
	confJSON, _ := json.MarshalIndent(topicConf, "", "  ")
	log.Printf("Complete topic.conf:\n%s", string(confJSON))

	log.Println("✅ Test 2 COMPLETED: Schematized message production test finished")
}

type TopicConf struct {
	BrokerPartitionAssignments []interface{} `json:"brokerPartitionAssignments"`
	Retention                  interface{}   `json:"retention"`
	MessageRecordType          interface{}   `json:"messageRecordType"`
	KeyColumns                 []string      `json:"keyColumns"`
}

func registerTestSchema(topicName string) (int, string, error) {
	schema := `{
		"type": "record",
		"name": "TestMessage",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "message", "type": "string"},
			{"name": "timestamp", "type": "long"}
		]
	}`

	payload := fmt.Sprintf(`{"schema": %q}`, schema)

	resp, err := http.Post(
		fmt.Sprintf("http://localhost:8081/subjects/%s-value/versions", topicName),
		"application/vnd.schemaregistry.v1+json",
		strings.NewReader(payload),
	)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return 0, "", fmt.Errorf("schema registration failed: %d - %s", resp.StatusCode, string(body))
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, "", err
	}

	return result.ID, schema, nil
}

func createKafkaProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3

	producer, err := sarama.NewSyncProducer([]string{"localhost:9093"}, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return producer, nil
}

func produceSchematizedMessage(producer sarama.SyncProducer, topicName string, schemaID int, codec *goavro.Codec) error {
	// Create test message data
	messageData := map[string]interface{}{
		"id":        123,
		"message":   "Hello, Avro World!",
		"timestamp": time.Now().UnixNano() / int64(time.Millisecond),
	}

	// Encode to Avro
	avroBytes, err := codec.BinaryFromNative(nil, messageData)
	if err != nil {
		return fmt.Errorf("failed to encode Avro: %w", err)
	}

	// Wrap in Confluent Wire Format
	wireFormatBytes := createConfluentWireFormat(schemaID, avroBytes)

	// Create Kafka message
	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("test-key"),
		Value: sarama.ByteEncoder(wireFormatBytes),
	}

	// Send message
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	log.Printf("✓ Message sent to partition %d at offset %d", partition, offset)
	log.Printf("  Message size: %d bytes (Avro: %d, Wire format: %d)",
		len(wireFormatBytes), len(avroBytes), len(wireFormatBytes))

	return nil
}

func createConfluentWireFormat(schemaID int, avroData []byte) []byte {
	// Confluent Wire Format: [magic_byte:1][schema_id:4][avro_data:...]
	wireFormat := make([]byte, 5+len(avroData))

	// Magic byte (0)
	wireFormat[0] = 0

	// Schema ID (big-endian)
	binary.BigEndian.PutUint32(wireFormat[1:5], uint32(schemaID))

	// Avro data
	copy(wireFormat[5:], avroData)

	return wireFormat
}

func getTopicConf(topicName string) (*TopicConf, error) {
	resp, err := http.Get(fmt.Sprintf("http://localhost:8888/topics/kafka/%s/topic.conf", topicName))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get topic.conf: %d - %s", resp.StatusCode, string(body))
	}

	var conf TopicConf
	if err := json.NewDecoder(resp.Body).Decode(&conf); err != nil {
		return nil, err
	}

	return &conf, nil
}


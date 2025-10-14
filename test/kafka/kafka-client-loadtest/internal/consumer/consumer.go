package consumer

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/linkedin/goavro/v2"
	"github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/config"
	"github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/metrics"
	pb "github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/schema/pb"
	"google.golang.org/protobuf/proto"
)

// Consumer represents a Kafka consumer for load testing
type Consumer struct {
	id               int
	config           *config.Config
	metricsCollector *metrics.Collector
	saramaConsumer   sarama.ConsumerGroup
	useConfluent     bool // Always false, Sarama only
	topics           []string
	consumerGroup    string
	avroCodec        *goavro.Codec

	// Schema format tracking per topic
	schemaFormats map[string]string // topic -> schema format mapping (AVRO, JSON, PROTOBUF)

	// Processing tracking
	messagesProcessed int64
	lastOffset        map[string]map[int32]int64
	offsetMutex       sync.RWMutex
}

// New creates a new consumer instance
func New(cfg *config.Config, collector *metrics.Collector, id int) (*Consumer, error) {
	// All consumers share the same group for load balancing across partitions
	consumerGroup := cfg.Consumers.GroupPrefix

	c := &Consumer{
		id:               id,
		config:           cfg,
		metricsCollector: collector,
		topics:           cfg.GetTopicNames(),
		consumerGroup:    consumerGroup,
		useConfluent:     false, // Use Sarama by default
		lastOffset:       make(map[string]map[int32]int64),
		schemaFormats:    make(map[string]string),
	}

	// Initialize schema formats for each topic (must match producer logic)
	// This mirrors the format distribution in cmd/loadtest/main.go registerSchemas()
	for i, topic := range c.topics {
		var schemaFormat string
		if cfg.Producers.SchemaFormat != "" {
			// Use explicit config if provided
			schemaFormat = cfg.Producers.SchemaFormat
		} else {
			// Distribute across formats (same as producer)
			switch i % 3 {
			case 0:
				schemaFormat = "AVRO"
			case 1:
				schemaFormat = "JSON"
			case 2:
				schemaFormat = "PROTOBUF"
			}
		}
		c.schemaFormats[topic] = schemaFormat
		log.Printf("Consumer %d: Topic %s will use schema format: %s", id, topic, schemaFormat)
	}

	// Initialize consumer based on configuration
	if c.useConfluent {
		if err := c.initConfluentConsumer(); err != nil {
			return nil, fmt.Errorf("failed to initialize Confluent consumer: %w", err)
		}
	} else {
		if err := c.initSaramaConsumer(); err != nil {
			return nil, fmt.Errorf("failed to initialize Sarama consumer: %w", err)
		}
	}

	// Initialize Avro codec if schemas are enabled
	if cfg.Schemas.Enabled {
		if err := c.initAvroCodec(); err != nil {
			return nil, fmt.Errorf("failed to initialize Avro codec: %w", err)
		}
	}

	log.Printf("Consumer %d initialized for group %s", id, consumerGroup)
	return c, nil
}

// initSaramaConsumer initializes the Sarama consumer group
func (c *Consumer) initSaramaConsumer() error {
	config := sarama.NewConfig()

	// Consumer configuration
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	if c.config.Consumers.AutoOffsetReset == "latest" {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	// Auto commit configuration
	config.Consumer.Offsets.AutoCommit.Enable = c.config.Consumers.EnableAutoCommit
	config.Consumer.Offsets.AutoCommit.Interval = time.Duration(c.config.Consumers.AutoCommitIntervalMs) * time.Millisecond

	// Session and heartbeat configuration
	config.Consumer.Group.Session.Timeout = time.Duration(c.config.Consumers.SessionTimeoutMs) * time.Millisecond
	config.Consumer.Group.Heartbeat.Interval = time.Duration(c.config.Consumers.HeartbeatIntervalMs) * time.Millisecond

	// Fetch configuration
	config.Consumer.Fetch.Min = int32(c.config.Consumers.FetchMinBytes)
	config.Consumer.Fetch.Default = 10 * 1024 * 1024 // 10MB per partition (increased from 1MB default)
	config.Consumer.Fetch.Max = int32(c.config.Consumers.FetchMaxBytes)
	config.Consumer.MaxWaitTime = time.Duration(c.config.Consumers.FetchMaxWaitMs) * time.Millisecond
	config.Consumer.MaxProcessingTime = time.Duration(c.config.Consumers.MaxPollIntervalMs) * time.Millisecond

	// Channel buffer sizes for concurrent partition consumption
	config.ChannelBufferSize = 256 // Increase from default 256 to allow more buffering

	// Enable concurrent partition fetching by increasing the number of broker connections
	// This allows Sarama to fetch from multiple partitions in parallel
	config.Net.MaxOpenRequests = 20 // Increase from default 5 to allow 20 concurrent requests

	// Version
	config.Version = sarama.V2_8_0_0

	// Create consumer group
	consumerGroup, err := sarama.NewConsumerGroup(c.config.Kafka.BootstrapServers, c.consumerGroup, config)
	if err != nil {
		return fmt.Errorf("failed to create Sarama consumer group: %w", err)
	}

	c.saramaConsumer = consumerGroup
	return nil
}

// initConfluentConsumer initializes the Confluent Kafka Go consumer
func (c *Consumer) initConfluentConsumer() error {
	// Confluent consumer disabled, using Sarama only
	return fmt.Errorf("confluent consumer not enabled")
}

// initAvroCodec initializes the Avro codec for schema-based messages
func (c *Consumer) initAvroCodec() error {
	// Use the LoadTestMessage schema (matches what producer uses)
	loadTestSchema := `{
		"type": "record",
		"name": "LoadTestMessage",
		"namespace": "com.seaweedfs.loadtest",
		"fields": [
			{"name": "id", "type": "string"},
			{"name": "timestamp", "type": "long"},
			{"name": "producer_id", "type": "int"},
			{"name": "counter", "type": "long"},
			{"name": "user_id", "type": "string"},
			{"name": "event_type", "type": "string"},
			{"name": "properties", "type": {"type": "map", "values": "string"}}
		]
	}`

	codec, err := goavro.NewCodec(loadTestSchema)
	if err != nil {
		return fmt.Errorf("failed to create Avro codec: %w", err)
	}

	c.avroCodec = codec
	return nil
}

// Run starts the consumer and consumes messages until the context is cancelled
func (c *Consumer) Run(ctx context.Context) {
	log.Printf("Consumer %d starting for group %s", c.id, c.consumerGroup)
	defer log.Printf("Consumer %d stopped", c.id)

	if c.useConfluent {
		c.runConfluentConsumer(ctx)
	} else {
		c.runSaramaConsumer(ctx)
	}
}

// runSaramaConsumer runs the Sarama consumer group
func (c *Consumer) runSaramaConsumer(ctx context.Context) {
	handler := &ConsumerGroupHandler{
		consumer: c,
	}

	var wg sync.WaitGroup

	// Start error handler
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case err, ok := <-c.saramaConsumer.Errors():
				if !ok {
					return
				}
				log.Printf("Consumer %d error: %v", c.id, err)
				c.metricsCollector.RecordConsumerError()
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start consumer group session
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := c.saramaConsumer.Consume(ctx, c.topics, handler); err != nil {
					log.Printf("Consumer %d: Error consuming: %v", c.id, err)
					c.metricsCollector.RecordConsumerError()

					// Wait briefly before retrying (reduced from 5s to 1s for faster recovery)
					select {
					case <-time.After(1 * time.Second):
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	// Start lag monitoring
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.monitorConsumerLag(ctx)
	}()

	// Wait for completion
	<-ctx.Done()
	log.Printf("Consumer %d: Context cancelled, shutting down", c.id)
	wg.Wait()
}

// runConfluentConsumer runs the Confluent consumer
func (c *Consumer) runConfluentConsumer(ctx context.Context) {
	// Confluent consumer disabled, using Sarama only
	log.Printf("Consumer %d: Confluent consumer not enabled", c.id)
}

// processMessage processes a consumed message
func (c *Consumer) processMessage(topicPtr *string, partition int32, offset int64, key, value []byte) error {
	topic := ""
	if topicPtr != nil {
		topic = *topicPtr
	}

	// Update offset tracking
	c.updateOffset(topic, partition, offset)

	// Decode message based on topic-specific schema format
	var decodedMessage interface{}
	var err error

	// Determine schema format for this topic (if schemas are enabled)
	var schemaFormat string
	if c.config.Schemas.Enabled {
		schemaFormat = c.schemaFormats[topic]
		if schemaFormat == "" {
			// Fallback to config if topic not in map
			schemaFormat = c.config.Producers.ValueType
		}
	} else {
		// No schemas, use global value type
		schemaFormat = c.config.Producers.ValueType
	}

	// Decode message based on format
	switch schemaFormat {
	case "avro", "AVRO":
		decodedMessage, err = c.decodeAvroMessage(value)
	case "json", "JSON", "JSON_SCHEMA":
		decodedMessage, err = c.decodeJSONSchemaMessage(value)
	case "protobuf", "PROTOBUF":
		decodedMessage, err = c.decodeProtobufMessage(value)
	case "binary":
		decodedMessage, err = c.decodeBinaryMessage(value)
	default:
		// Fallback to plain JSON
		decodedMessage, err = c.decodeJSONMessage(value)
	}

	if err != nil {
		return fmt.Errorf("failed to decode message: %w", err)
	}

	// Note: Removed artificial delay to allow maximum throughput
	// If you need to simulate processing time, add a configurable delay setting
	// time.Sleep(time.Millisecond) // Minimal processing delay

	// Record metrics
	c.metricsCollector.RecordConsumedMessage(len(value))
	c.messagesProcessed++

	// Log progress
	if c.id == 0 && c.messagesProcessed%1000 == 0 {
		log.Printf("Consumer %d: Processed %d messages (latest: %s[%d]@%d)",
			c.id, c.messagesProcessed, topic, partition, offset)
	}

	// Optional: Validate message content (for testing purposes)
	if c.config.Chaos.Enabled {
		if err := c.validateMessage(decodedMessage); err != nil {
			log.Printf("Consumer %d: Message validation failed: %v", c.id, err)
		}
	}

	return nil
}

// decodeJSONMessage decodes a JSON message
func (c *Consumer) decodeJSONMessage(value []byte) (interface{}, error) {
	var message map[string]interface{}
	if err := json.Unmarshal(value, &message); err != nil {
		// DEBUG: Log the raw bytes when JSON parsing fails
		log.Printf("Consumer %d: JSON decode failed. Length: %d, Raw bytes (hex): %x, Raw string: %q, Error: %v",
			c.id, len(value), value, string(value), err)
		return nil, err
	}
	return message, nil
}

// decodeAvroMessage decodes an Avro message (handles Confluent Wire Format)
func (c *Consumer) decodeAvroMessage(value []byte) (interface{}, error) {
	if c.avroCodec == nil {
		return nil, fmt.Errorf("Avro codec not initialized")
	}

	// Handle Confluent Wire Format when schemas are enabled
	var avroData []byte
	if c.config.Schemas.Enabled {
		if len(value) < 5 {
			return nil, fmt.Errorf("message too short for Confluent Wire Format: %d bytes", len(value))
		}

		// Check magic byte (should be 0)
		if value[0] != 0 {
			return nil, fmt.Errorf("invalid Confluent Wire Format magic byte: %d", value[0])
		}

		// Extract schema ID (bytes 1-4, big-endian)
		schemaID := binary.BigEndian.Uint32(value[1:5])
		_ = schemaID // TODO: Could validate schema ID matches expected schema

		// Extract Avro data (bytes 5+)
		avroData = value[5:]
	} else {
		// No wire format, use raw data
		avroData = value
	}

	native, _, err := c.avroCodec.NativeFromBinary(avroData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode Avro data: %w", err)
	}

	return native, nil
}

// decodeJSONSchemaMessage decodes a JSON Schema message (handles Confluent Wire Format)
func (c *Consumer) decodeJSONSchemaMessage(value []byte) (interface{}, error) {
	// Handle Confluent Wire Format when schemas are enabled
	var jsonData []byte
	if c.config.Schemas.Enabled {
		if len(value) < 5 {
			return nil, fmt.Errorf("message too short for Confluent Wire Format: %d bytes", len(value))
		}

		// Check magic byte (should be 0)
		if value[0] != 0 {
			return nil, fmt.Errorf("invalid Confluent Wire Format magic byte: %d", value[0])
		}

		// Extract schema ID (bytes 1-4, big-endian)
		schemaID := binary.BigEndian.Uint32(value[1:5])
		_ = schemaID // TODO: Could validate schema ID matches expected schema

		// Extract JSON data (bytes 5+)
		jsonData = value[5:]
	} else {
		// No wire format, use raw data
		jsonData = value
	}

	// Decode JSON
	var message map[string]interface{}
	if err := json.Unmarshal(jsonData, &message); err != nil {
		return nil, fmt.Errorf("failed to decode JSON data: %w", err)
	}

	return message, nil
}

// decodeProtobufMessage decodes a Protobuf message (handles Confluent Wire Format)
func (c *Consumer) decodeProtobufMessage(value []byte) (interface{}, error) {
	// Handle Confluent Wire Format when schemas are enabled
	var protoData []byte
	if c.config.Schemas.Enabled {
		if len(value) < 5 {
			return nil, fmt.Errorf("message too short for Confluent Wire Format: %d bytes", len(value))
		}

		// Check magic byte (should be 0)
		if value[0] != 0 {
			return nil, fmt.Errorf("invalid Confluent Wire Format magic byte: %d", value[0])
		}

		// Extract schema ID (bytes 1-4, big-endian)
		schemaID := binary.BigEndian.Uint32(value[1:5])
		_ = schemaID // TODO: Could validate schema ID matches expected schema

		// Extract Protobuf data (bytes 5+)
		protoData = value[5:]
	} else {
		// No wire format, use raw data
		protoData = value
	}

	// Unmarshal protobuf message
	var protoMsg pb.LoadTestMessage
	if err := proto.Unmarshal(protoData, &protoMsg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal Protobuf data: %w", err)
	}

	// Convert to map for consistency with other decoders
	return map[string]interface{}{
		"id":          protoMsg.Id,
		"timestamp":   protoMsg.Timestamp,
		"producer_id": protoMsg.ProducerId,
		"counter":     protoMsg.Counter,
		"user_id":     protoMsg.UserId,
		"event_type":  protoMsg.EventType,
		"properties":  protoMsg.Properties,
	}, nil
}

// decodeBinaryMessage decodes a binary message
func (c *Consumer) decodeBinaryMessage(value []byte) (interface{}, error) {
	if len(value) < 20 {
		return nil, fmt.Errorf("binary message too short")
	}

	// Extract fields from the binary format:
	// [producer_id:4][counter:8][timestamp:8][random_data:...]

	producerID := int(value[0])<<24 | int(value[1])<<16 | int(value[2])<<8 | int(value[3])

	var counter int64
	for i := 0; i < 8; i++ {
		counter |= int64(value[4+i]) << (56 - i*8)
	}

	var timestamp int64
	for i := 0; i < 8; i++ {
		timestamp |= int64(value[12+i]) << (56 - i*8)
	}

	return map[string]interface{}{
		"producer_id": producerID,
		"counter":     counter,
		"timestamp":   timestamp,
		"data_size":   len(value),
	}, nil
}

// validateMessage performs basic message validation
func (c *Consumer) validateMessage(message interface{}) error {
	// This is a placeholder for message validation logic
	// In a real load test, you might validate:
	// - Message structure
	// - Required fields
	// - Data consistency
	// - Schema compliance

	if message == nil {
		return fmt.Errorf("message is nil")
	}

	return nil
}

// updateOffset updates the last seen offset for lag calculation
func (c *Consumer) updateOffset(topic string, partition int32, offset int64) {
	c.offsetMutex.Lock()
	defer c.offsetMutex.Unlock()

	if c.lastOffset[topic] == nil {
		c.lastOffset[topic] = make(map[int32]int64)
	}
	c.lastOffset[topic][partition] = offset
}

// monitorConsumerLag monitors and reports consumer lag
func (c *Consumer) monitorConsumerLag(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.reportConsumerLag()
		}
	}
}

// reportConsumerLag calculates and reports consumer lag
func (c *Consumer) reportConsumerLag() {
	// This is a simplified lag calculation
	// In a real implementation, you would query the broker for high water marks

	c.offsetMutex.RLock()
	defer c.offsetMutex.RUnlock()

	for topic, partitions := range c.lastOffset {
		for partition, _ := range partitions {
			// For simplicity, assume lag is always 0 when we're consuming actively
			// In a real test, you would compare against the high water mark
			lag := int64(0)

			c.metricsCollector.UpdateConsumerLag(c.consumerGroup, topic, partition, lag)
		}
	}
}

// Close closes the consumer and cleans up resources
func (c *Consumer) Close() error {
	log.Printf("Consumer %d: Closing", c.id)

	if c.saramaConsumer != nil {
		return c.saramaConsumer.Close()
	}

	return nil
}

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler
type ConsumerGroupHandler struct {
	consumer *Consumer
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Printf("Consumer %d: Consumer group session setup", h.consumer.id)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Printf("Consumer %d: Consumer group session cleanup", h.consumer.id)
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages()
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	msgCount := 0
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			msgCount++

			// Process the message
			var key []byte
			if message.Key != nil {
				key = message.Key
			}

			if err := h.consumer.processMessage(&message.Topic, message.Partition, message.Offset, key, message.Value); err != nil {
				log.Printf("Consumer %d: Error processing message: %v", h.consumer.id, err)
				h.consumer.metricsCollector.RecordConsumerError()

				// Add a small delay for schema validation or other processing errors to avoid overloading
				// select {
				// case <-time.After(100 * time.Millisecond):
				// 	// Continue after brief delay
				// case <-session.Context().Done():
				// 	return nil
				// }
			} else {
				// Mark message as processed
				session.MarkMessage(message, "")
			}

		case <-session.Context().Done():
			log.Printf("Consumer %d: Session context cancelled for %s[%d]",
				h.consumer.id, claim.Topic(), claim.Partition())
			return nil
		}
	}
}

// Helper functions

func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}

	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}

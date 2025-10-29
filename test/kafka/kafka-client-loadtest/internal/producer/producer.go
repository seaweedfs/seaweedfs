package producer

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/linkedin/goavro/v2"
	"github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/config"
	"github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/metrics"
	"github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/schema"
	pb "github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/schema/pb"
	"github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/tracker"
	"google.golang.org/protobuf/proto"
)

// ErrCircuitBreakerOpen indicates that the circuit breaker is open due to consecutive failures
var ErrCircuitBreakerOpen = errors.New("circuit breaker is open")

// Producer represents a Kafka producer for load testing
type Producer struct {
	id               int
	config           *config.Config
	metricsCollector *metrics.Collector
	saramaProducer   sarama.SyncProducer
	useConfluent     bool
	topics           []string
	avroCodec        *goavro.Codec
	startTime        time.Time // Test run start time for generating unique keys

	// Schema management
	schemaIDs     map[string]int    // topic -> schema ID mapping
	schemaFormats map[string]string // topic -> schema format mapping (AVRO, JSON, etc.)

	// Rate limiting
	rateLimiter *time.Ticker

	// Message generation
	messageCounter int64
	random         *rand.Rand

	// Circuit breaker detection
	consecutiveFailures int

	// Record tracking
	tracker *tracker.Tracker
}

// Message represents a test message
type Message struct {
	ID         string                 `json:"id"`
	Timestamp  int64                  `json:"timestamp"`
	ProducerID int                    `json:"producer_id"`
	Counter    int64                  `json:"counter"`
	UserID     string                 `json:"user_id"`
	EventType  string                 `json:"event_type"`
	Properties map[string]interface{} `json:"properties"`
}

// New creates a new producer instance
func New(cfg *config.Config, collector *metrics.Collector, id int, recordTracker *tracker.Tracker) (*Producer, error) {
	p := &Producer{
		id:               id,
		config:           cfg,
		metricsCollector: collector,
		topics:           cfg.GetTopicNames(),
		random:           rand.New(rand.NewSource(time.Now().UnixNano() + int64(id))),
		useConfluent:     false, // Use Sarama by default, can be made configurable
		schemaIDs:        make(map[string]int),
		schemaFormats:    make(map[string]string),
		startTime:        time.Now(), // Record test start time for unique key generation
		tracker:          recordTracker,
	}

	// Initialize schema formats for each topic
	// Distribute across AVRO, JSON, and PROTOBUF formats
	for i, topic := range p.topics {
		var schemaFormat string
		if cfg.Producers.SchemaFormat != "" {
			// Use explicit config if provided
			schemaFormat = cfg.Producers.SchemaFormat
		} else {
			// Distribute across three formats: AVRO, JSON, PROTOBUF
			switch i % 3 {
			case 0:
				schemaFormat = "AVRO"
			case 1:
				schemaFormat = "JSON"
			case 2:
				schemaFormat = "PROTOBUF"
			}
		}
		p.schemaFormats[topic] = schemaFormat
		log.Printf("Producer %d: Topic %s will use schema format: %s", id, topic, schemaFormat)
	}

	// Set up rate limiter if specified
	if cfg.Producers.MessageRate > 0 {
		p.rateLimiter = time.NewTicker(time.Second / time.Duration(cfg.Producers.MessageRate))
	}

	// Initialize Sarama producer
	if err := p.initSaramaProducer(); err != nil {
		return nil, fmt.Errorf("failed to initialize Sarama producer: %w", err)
	}

	// Initialize Avro codec and register/fetch schemas if schemas are enabled
	if cfg.Schemas.Enabled {
		if err := p.initAvroCodec(); err != nil {
			return nil, fmt.Errorf("failed to initialize Avro codec: %w", err)
		}
		if err := p.ensureSchemasRegistered(); err != nil {
			return nil, fmt.Errorf("failed to ensure schemas are registered: %w", err)
		}
		if err := p.fetchSchemaIDs(); err != nil {
			return nil, fmt.Errorf("failed to fetch schema IDs: %w", err)
		}
	}

	log.Printf("Producer %d initialized successfully", id)
	return p, nil
}

// initSaramaProducer initializes the Sarama producer
func (p *Producer) initSaramaProducer() error {
	config := sarama.NewConfig()

	// Producer configuration
	config.Producer.RequiredAcks = sarama.WaitForAll
	if p.config.Producers.Acks == "0" {
		config.Producer.RequiredAcks = sarama.NoResponse
	} else if p.config.Producers.Acks == "1" {
		config.Producer.RequiredAcks = sarama.WaitForLocal
	}

	config.Producer.Retry.Max = p.config.Producers.Retries
	config.Producer.Retry.Backoff = time.Duration(p.config.Producers.RetryBackoffMs) * time.Millisecond
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// Compression
	switch p.config.Producers.CompressionType {
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		config.Producer.Compression = sarama.CompressionZSTD
	default:
		config.Producer.Compression = sarama.CompressionNone
	}

	// Batching
	config.Producer.Flush.Messages = p.config.Producers.BatchSize
	config.Producer.Flush.Frequency = time.Duration(p.config.Producers.LingerMs) * time.Millisecond

	// Timeouts
	config.Net.DialTimeout = 30 * time.Second
	config.Net.ReadTimeout = 30 * time.Second
	config.Net.WriteTimeout = 30 * time.Second

	// Version
	config.Version = sarama.V2_8_0_0

	// Create producer
	producer, err := sarama.NewSyncProducer(p.config.Kafka.BootstrapServers, config)
	if err != nil {
		return fmt.Errorf("failed to create Sarama producer: %w", err)
	}

	p.saramaProducer = producer
	return nil
}

// initAvroCodec initializes the Avro codec for schema-based messages
func (p *Producer) initAvroCodec() error {
	// Use the shared LoadTestMessage schema
	codec, err := goavro.NewCodec(schema.GetAvroSchema())
	if err != nil {
		return fmt.Errorf("failed to create Avro codec: %w", err)
	}

	p.avroCodec = codec
	return nil
}

// Run starts the producer and produces messages until the context is cancelled
func (p *Producer) Run(ctx context.Context) error {
	log.Printf("Producer %d starting", p.id)
	defer log.Printf("Producer %d stopped", p.id)

	// Create topics if they don't exist
	if err := p.createTopics(); err != nil {
		log.Printf("Producer %d: Failed to create topics: %v", p.id, err)
		p.metricsCollector.RecordProducerError()
		return err
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	// Main production loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := p.produceMessages(ctx); err != nil {
			errChan <- err
		}
	}()

	// Wait for completion or error
	select {
	case <-ctx.Done():
		log.Printf("Producer %d: Context cancelled, shutting down", p.id)
	case err := <-errChan:
		log.Printf("Producer %d: Stopping due to error: %v", p.id, err)
		return err
	}

	// Stop rate limiter
	if p.rateLimiter != nil {
		p.rateLimiter.Stop()
	}

	// Wait for goroutines to finish
	wg.Wait()
	return nil
}

// produceMessages is the main message production loop
func (p *Producer) produceMessages(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			// Rate limiting
			if p.rateLimiter != nil {
				select {
				case <-p.rateLimiter.C:
					// Proceed
				case <-ctx.Done():
					return nil
				}
			}

			if err := p.produceMessage(); err != nil {
				log.Printf("Producer %d: Failed to produce message: %v", p.id, err)
				p.metricsCollector.RecordProducerError()

				// Check for circuit breaker error
				if p.isCircuitBreakerError(err) {
					p.consecutiveFailures++
					log.Printf("Producer %d: Circuit breaker error detected (%d/%d consecutive failures)",
						p.id, p.consecutiveFailures, 3)

					// Progressive backoff delay to avoid overloading the gateway
					backoffDelay := time.Duration(p.consecutiveFailures) * 500 * time.Millisecond
					log.Printf("Producer %d: Backing off for %v to avoid overloading gateway", p.id, backoffDelay)

					select {
					case <-time.After(backoffDelay):
						// Continue after delay
					case <-ctx.Done():
						return nil
					}

					// If we've hit 3 consecutive circuit breaker errors, stop the producer
					if p.consecutiveFailures >= 3 {
						log.Printf("Producer %d: Circuit breaker is open - stopping producer after %d consecutive failures",
							p.id, p.consecutiveFailures)
						return fmt.Errorf("%w: stopping producer after %d consecutive failures", ErrCircuitBreakerOpen, p.consecutiveFailures)
					}
				} else {
					// Reset counter for non-circuit breaker errors
					p.consecutiveFailures = 0
				}
			} else {
				// Reset counter on successful message
				p.consecutiveFailures = 0
			}
		}
	}
}

// produceMessage produces a single message
func (p *Producer) produceMessage() error {
	startTime := time.Now()

	// Select random topic
	topic := p.topics[p.random.Intn(len(p.topics))]

	// Produce message using Sarama (message will be generated based on topic's schema format)
	return p.produceSaramaMessage(topic, startTime)
}

// produceSaramaMessage produces a message using Sarama
// The message is generated internally based on the topic's schema format
func (p *Producer) produceSaramaMessage(topic string, startTime time.Time) error {
	// Generate key
	key := p.generateMessageKey()

	// If schemas are enabled, wrap in Confluent Wire Format based on topic's schema format
	var messageValue []byte
	if p.config.Schemas.Enabled {
		schemaID, exists := p.schemaIDs[topic]
		if !exists {
			return fmt.Errorf("schema ID not found for topic %s", topic)
		}

		// Get the schema format for this topic
		schemaFormat := p.schemaFormats[topic]

		// CRITICAL FIX: Encode based on schema format, NOT config value_type
		// The encoding MUST match what the schema registry and gateway expect
		var encodedMessage []byte
		var err error
		switch schemaFormat {
		case "AVRO":
			// For Avro schema, encode as Avro binary
			encodedMessage, err = p.generateAvroMessage()
			if err != nil {
				return fmt.Errorf("failed to encode as Avro for topic %s: %w", topic, err)
			}
		case "JSON":
			// For JSON schema, encode as JSON
			encodedMessage, err = p.generateJSONMessage()
			if err != nil {
				return fmt.Errorf("failed to encode as JSON for topic %s: %w", topic, err)
			}
		case "PROTOBUF":
			// For PROTOBUF schema, encode as Protobuf binary
			encodedMessage, err = p.generateProtobufMessage()
			if err != nil {
				return fmt.Errorf("failed to encode as Protobuf for topic %s: %w", topic, err)
			}
		default:
			// Unknown format - fallback to JSON
			encodedMessage, err = p.generateJSONMessage()
			if err != nil {
				return fmt.Errorf("failed to encode as JSON (unknown format fallback) for topic %s: %w", topic, err)
			}
		}

		// Wrap in Confluent wire format (magic byte + schema ID + payload)
		messageValue = p.createConfluentWireFormat(schemaID, encodedMessage)
	} else {
		// No schemas - generate message based on config value_type
		var err error
		messageValue, err = p.generateMessage()
		if err != nil {
			return fmt.Errorf("failed to generate message: %w", err)
		}
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(messageValue),
	}

	// Add headers if configured
	if p.config.Producers.IncludeHeaders {
		msg.Headers = []sarama.RecordHeader{
			{Key: []byte("producer_id"), Value: []byte(fmt.Sprintf("%d", p.id))},
			{Key: []byte("timestamp"), Value: []byte(fmt.Sprintf("%d", startTime.UnixNano()))},
		}
	}

	// Produce message
	partition, offset, err := p.saramaProducer.SendMessage(msg)
	if err != nil {
		return err
	}

	// Track produced message
	if p.tracker != nil {
		p.tracker.TrackProduced(tracker.Record{
			Key:        key,
			Topic:      topic,
			Partition:  partition,
			Offset:     offset,
			Timestamp:  startTime.UnixNano(),
			ProducerID: p.id,
		})
	}

	// Record metrics
	latency := time.Since(startTime)
	p.metricsCollector.RecordProducedMessage(len(messageValue), latency)

	return nil
}

// generateMessage generates a test message
func (p *Producer) generateMessage() ([]byte, error) {
	p.messageCounter++

	switch p.config.Producers.ValueType {
	case "avro":
		return p.generateAvroMessage()
	case "json":
		return p.generateJSONMessage()
	case "binary":
		return p.generateBinaryMessage()
	default:
		return p.generateJSONMessage()
	}
}

// generateJSONMessage generates a JSON test message
func (p *Producer) generateJSONMessage() ([]byte, error) {
	msg := Message{
		ID:         fmt.Sprintf("msg-%d-%d", p.id, p.messageCounter),
		Timestamp:  time.Now().UnixNano(),
		ProducerID: p.id,
		Counter:    p.messageCounter,
		UserID:     fmt.Sprintf("user-%d", p.random.Intn(10000)),
		EventType:  p.randomEventType(),
		Properties: map[string]interface{}{
			"session_id":  fmt.Sprintf("sess-%d-%d", p.id, p.random.Intn(1000)),
			"page_views":  fmt.Sprintf("%d", p.random.Intn(100)),    // String for Avro map<string,string>
			"duration_ms": fmt.Sprintf("%d", p.random.Intn(300000)), // String for Avro map<string,string>
			"country":     p.randomCountry(),
			"device_type": p.randomDeviceType(),
			"app_version": fmt.Sprintf("v%d.%d.%d", p.random.Intn(10), p.random.Intn(10), p.random.Intn(100)),
		},
	}

	// Marshal to JSON (no padding - let natural message size be used)
	messageBytes, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	return messageBytes, nil
}

// generateProtobufMessage generates a Protobuf-encoded message
func (p *Producer) generateProtobufMessage() ([]byte, error) {
	// Create protobuf message
	protoMsg := &pb.LoadTestMessage{
		Id:         fmt.Sprintf("msg-%d-%d", p.id, p.messageCounter),
		Timestamp:  time.Now().UnixNano(),
		ProducerId: int32(p.id),
		Counter:    p.messageCounter,
		UserId:     fmt.Sprintf("user-%d", p.random.Intn(10000)),
		EventType:  p.randomEventType(),
		Properties: map[string]string{
			"session_id":  fmt.Sprintf("sess-%d-%d", p.id, p.random.Intn(1000)),
			"page_views":  fmt.Sprintf("%d", p.random.Intn(100)),
			"duration_ms": fmt.Sprintf("%d", p.random.Intn(300000)),
			"country":     p.randomCountry(),
			"device_type": p.randomDeviceType(),
			"app_version": fmt.Sprintf("v%d.%d.%d", p.random.Intn(10), p.random.Intn(10), p.random.Intn(100)),
		},
	}

	// Marshal to protobuf binary
	messageBytes, err := proto.Marshal(protoMsg)
	if err != nil {
		return nil, err
	}

	return messageBytes, nil
}

// generateAvroMessage generates an Avro-encoded message with Confluent Wire Format
// NOTE: Avro messages are NOT padded - they have their own binary format
func (p *Producer) generateAvroMessage() ([]byte, error) {
	if p.avroCodec == nil {
		return nil, fmt.Errorf("Avro codec not initialized")
	}

	// Create Avro-compatible record matching the LoadTestMessage schema
	record := map[string]interface{}{
		"id":          fmt.Sprintf("msg-%d-%d", p.id, p.messageCounter),
		"timestamp":   time.Now().UnixNano(),
		"producer_id": p.id,
		"counter":     p.messageCounter,
		"user_id":     fmt.Sprintf("user-%d", p.random.Intn(10000)),
		"event_type":  p.randomEventType(),
		"properties": map[string]interface{}{
			"session_id":  fmt.Sprintf("sess-%d-%d", p.id, p.random.Intn(1000)),
			"page_views":  fmt.Sprintf("%d", p.random.Intn(100)),
			"duration_ms": fmt.Sprintf("%d", p.random.Intn(300000)),
			"country":     p.randomCountry(),
			"device_type": p.randomDeviceType(),
			"app_version": fmt.Sprintf("v%d.%d.%d", p.random.Intn(10), p.random.Intn(10), p.random.Intn(100)),
		},
	}

	// Encode to Avro binary
	avroBytes, err := p.avroCodec.BinaryFromNative(nil, record)
	if err != nil {
		return nil, err
	}

	return avroBytes, nil
}

// generateBinaryMessage generates a binary test message (no padding)
func (p *Producer) generateBinaryMessage() ([]byte, error) {
	// Create a simple binary message format:
	// [producer_id:4][counter:8][timestamp:8]
	message := make([]byte, 20)

	// Producer ID (4 bytes)
	message[0] = byte(p.id >> 24)
	message[1] = byte(p.id >> 16)
	message[2] = byte(p.id >> 8)
	message[3] = byte(p.id)

	// Counter (8 bytes)
	for i := 0; i < 8; i++ {
		message[4+i] = byte(p.messageCounter >> (56 - i*8))
	}

	// Timestamp (8 bytes)
	timestamp := time.Now().UnixNano()
	for i := 0; i < 8; i++ {
		message[12+i] = byte(timestamp >> (56 - i*8))
	}

	return message, nil
}

// generateMessageKey generates a message key based on the configured distribution
// Keys are prefixed with a test run ID to track messages across test runs
func (p *Producer) generateMessageKey() string {
	// Use test start time as run ID (format: YYYYMMDD-HHMMSS)
	runID := p.startTime.Format("20060102-150405")

	switch p.config.Producers.KeyDistribution {
	case "sequential":
		return fmt.Sprintf("run-%s-key-%d", runID, p.messageCounter)
	case "uuid":
		return fmt.Sprintf("run-%s-uuid-%d-%d-%d", runID, p.id, time.Now().UnixNano(), p.random.Intn(1000000))
	default: // random
		return fmt.Sprintf("run-%s-key-%d", runID, p.random.Intn(10000))
	}
}

// createTopics creates the test topics if they don't exist
func (p *Producer) createTopics() error {
	// Use Sarama admin client to create topics
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin(p.config.Kafka.BootstrapServers, config)
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer admin.Close()

	// Create topic specifications
	topicSpecs := make(map[string]*sarama.TopicDetail)
	for _, topic := range p.topics {
		topicSpecs[topic] = &sarama.TopicDetail{
			NumPartitions:     int32(p.config.Topics.Partitions),
			ReplicationFactor: int16(p.config.Topics.ReplicationFactor),
			ConfigEntries: map[string]*string{
				"cleanup.policy": &p.config.Topics.CleanupPolicy,
				"retention.ms":   stringPtr(fmt.Sprintf("%d", p.config.Topics.RetentionMs)),
				"segment.ms":     stringPtr(fmt.Sprintf("%d", p.config.Topics.SegmentMs)),
			},
		}
	}

	// Create topics
	for _, topic := range p.topics {
		err = admin.CreateTopic(topic, topicSpecs[topic], false)
		if err != nil && err != sarama.ErrTopicAlreadyExists {
			log.Printf("Producer %d: Warning - failed to create topic %s: %v", p.id, topic, err)
		} else {
			log.Printf("Producer %d: Successfully created topic %s", p.id, topic)
		}
	}

	return nil
}

// Close closes the producer and cleans up resources
func (p *Producer) Close() error {
	log.Printf("Producer %d: Closing", p.id)

	if p.rateLimiter != nil {
		p.rateLimiter.Stop()
	}

	if p.saramaProducer != nil {
		return p.saramaProducer.Close()
	}

	return nil
}

// Helper functions

func stringPtr(s string) *string {
	return &s
}

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

func (p *Producer) randomEventType() string {
	events := []string{"login", "logout", "view", "click", "purchase", "signup", "search", "download"}
	return events[p.random.Intn(len(events))]
}

func (p *Producer) randomCountry() string {
	countries := []string{"US", "CA", "UK", "DE", "FR", "JP", "AU", "BR", "IN", "CN"}
	return countries[p.random.Intn(len(countries))]
}

func (p *Producer) randomDeviceType() string {
	devices := []string{"desktop", "mobile", "tablet", "tv", "watch"}
	return devices[p.random.Intn(len(devices))]
}

// fetchSchemaIDs fetches schema IDs from Schema Registry for all topics
func (p *Producer) fetchSchemaIDs() error {
	for _, topic := range p.topics {
		subject := topic + "-value"
		schemaID, err := p.getSchemaID(subject)
		if err != nil {
			return fmt.Errorf("failed to get schema ID for subject %s: %w", subject, err)
		}
		p.schemaIDs[topic] = schemaID
		log.Printf("Producer %d: Fetched schema ID %d for topic %s", p.id, schemaID, topic)
	}
	return nil
}

// getSchemaID fetches the latest schema ID for a subject from Schema Registry
func (p *Producer) getSchemaID(subject string) (int, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions/latest", p.config.SchemaRegistry.URL, subject)

	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("failed to get schema: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var schemaResp struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&schemaResp); err != nil {
		return 0, err
	}

	return schemaResp.ID, nil
}

// ensureSchemasRegistered ensures that schemas are registered for all topics
// It registers schemas if they don't exist, but doesn't fail if they already do
func (p *Producer) ensureSchemasRegistered() error {
	for _, topic := range p.topics {
		subject := topic + "-value"

		// First check if schema already exists
		schemaID, err := p.getSchemaID(subject)
		if err == nil {
			log.Printf("Producer %d: Schema already exists for topic %s (ID: %d), skipping registration", p.id, topic, schemaID)
			continue
		}

		// Schema doesn't exist, register it
		log.Printf("Producer %d: Registering schema for topic %s", p.id, topic)
		if err := p.registerTopicSchema(subject); err != nil {
			return fmt.Errorf("failed to register schema for topic %s: %w", topic, err)
		}
		log.Printf("Producer %d: Schema registered successfully for topic %s", p.id, topic)
	}
	return nil
}

// registerTopicSchema registers the schema for a specific topic based on configured format
func (p *Producer) registerTopicSchema(subject string) error {
	// Extract topic name from subject (remove -value or -key suffix)
	topicName := strings.TrimSuffix(strings.TrimSuffix(subject, "-value"), "-key")

	// Get schema format for this topic
	schemaFormat, ok := p.schemaFormats[topicName]
	if !ok {
		// Fallback to config or default
		schemaFormat = p.config.Producers.SchemaFormat
		if schemaFormat == "" {
			schemaFormat = "AVRO"
		}
	}

	var schemaStr string
	var schemaType string

	switch strings.ToUpper(schemaFormat) {
	case "AVRO":
		schemaStr = schema.GetAvroSchema()
		schemaType = "AVRO"
	case "JSON", "JSON_SCHEMA":
		schemaStr = schema.GetJSONSchema()
		schemaType = "JSON"
	case "PROTOBUF":
		schemaStr = schema.GetProtobufSchema()
		schemaType = "PROTOBUF"
	default:
		return fmt.Errorf("unsupported schema format: %s", schemaFormat)
	}

	url := fmt.Sprintf("%s/subjects/%s/versions", p.config.SchemaRegistry.URL, subject)

	payload := map[string]interface{}{
		"schema":     schemaStr,
		"schemaType": schemaType,
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal schema payload: %w", err)
	}

	resp, err := http.Post(url, "application/vnd.schemaregistry.v1+json", strings.NewReader(string(jsonPayload)))
	if err != nil {
		return fmt.Errorf("failed to register schema: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("schema registration failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	var registerResp struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&registerResp); err != nil {
		return fmt.Errorf("failed to decode registration response: %w", err)
	}

	log.Printf("Schema registered with ID: %d (format: %s)", registerResp.ID, schemaType)
	return nil
}

// createConfluentWireFormat creates a message in Confluent Wire Format
// This matches the implementation in weed/mq/kafka/schema/envelope.go CreateConfluentEnvelope
func (p *Producer) createConfluentWireFormat(schemaID int, avroData []byte) []byte {
	// Confluent Wire Format: [magic_byte(1)][schema_id(4)][payload(n)]
	// magic_byte = 0x00
	// schema_id = 4 bytes big-endian
	wireFormat := make([]byte, 5+len(avroData))
	wireFormat[0] = 0x00 // Magic byte
	binary.BigEndian.PutUint32(wireFormat[1:5], uint32(schemaID))
	copy(wireFormat[5:], avroData)
	return wireFormat
}

// isCircuitBreakerError checks if an error indicates that the circuit breaker is open
func (p *Producer) isCircuitBreakerError(err error) bool {
	return errors.Is(err, ErrCircuitBreakerOpen)
}

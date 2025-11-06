package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the complete load test configuration
type Config struct {
	TestMode string        `yaml:"test_mode"`
	Duration time.Duration `yaml:"duration"`

	Kafka          KafkaConfig          `yaml:"kafka"`
	SchemaRegistry SchemaRegistryConfig `yaml:"schema_registry"`
	Producers      ProducersConfig      `yaml:"producers"`
	Consumers      ConsumersConfig      `yaml:"consumers"`
	Topics         TopicsConfig         `yaml:"topics"`
	Schemas        SchemasConfig        `yaml:"schemas"`
	Metrics        MetricsConfig        `yaml:"metrics"`
	Scenarios      ScenariosConfig      `yaml:"scenarios"`
	Chaos          ChaosConfig          `yaml:"chaos"`
	Output         OutputConfig         `yaml:"output"`
	Logging        LoggingConfig        `yaml:"logging"`
}

type KafkaConfig struct {
	BootstrapServers []string `yaml:"bootstrap_servers"`
	SecurityProtocol string   `yaml:"security_protocol"`
	SASLMechanism    string   `yaml:"sasl_mechanism"`
	SASLUsername     string   `yaml:"sasl_username"`
	SASLPassword     string   `yaml:"sasl_password"`
}

type SchemaRegistryConfig struct {
	URL  string `yaml:"url"`
	Auth struct {
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"auth"`
}

type ProducersConfig struct {
	Count             int    `yaml:"count"`
	MessageRate       int    `yaml:"message_rate"`
	MessageSize       int    `yaml:"message_size"`
	BatchSize         int    `yaml:"batch_size"`
	LingerMs          int    `yaml:"linger_ms"`
	CompressionType   string `yaml:"compression_type"`
	Acks              string `yaml:"acks"`
	Retries           int    `yaml:"retries"`
	RetryBackoffMs    int    `yaml:"retry_backoff_ms"`
	RequestTimeoutMs  int    `yaml:"request_timeout_ms"`
	DeliveryTimeoutMs int    `yaml:"delivery_timeout_ms"`
	KeyDistribution   string `yaml:"key_distribution"`
	ValueType         string `yaml:"value_type"`    // json, avro, protobuf, binary
	SchemaFormat      string `yaml:"schema_format"` // AVRO, JSON, PROTOBUF (schema registry format)
	IncludeTimestamp  bool   `yaml:"include_timestamp"`
	IncludeHeaders    bool   `yaml:"include_headers"`
}

type ConsumersConfig struct {
	Count                int    `yaml:"count"`
	GroupPrefix          string `yaml:"group_prefix"`
	AutoOffsetReset      string `yaml:"auto_offset_reset"`
	EnableAutoCommit     bool   `yaml:"enable_auto_commit"`
	AutoCommitIntervalMs int    `yaml:"auto_commit_interval_ms"`
	SessionTimeoutMs     int    `yaml:"session_timeout_ms"`
	HeartbeatIntervalMs  int    `yaml:"heartbeat_interval_ms"`
	MaxPollRecords       int    `yaml:"max_poll_records"`
	MaxPollIntervalMs    int    `yaml:"max_poll_interval_ms"`
	FetchMinBytes        int    `yaml:"fetch_min_bytes"`
	FetchMaxBytes        int    `yaml:"fetch_max_bytes"`
	FetchMaxWaitMs       int    `yaml:"fetch_max_wait_ms"`
}

type TopicsConfig struct {
	Count             int    `yaml:"count"`
	Prefix            string `yaml:"prefix"`
	Partitions        int    `yaml:"partitions"`
	ReplicationFactor int    `yaml:"replication_factor"`
	CleanupPolicy     string `yaml:"cleanup_policy"`
	RetentionMs       int64  `yaml:"retention_ms"`
	SegmentMs         int64  `yaml:"segment_ms"`
}

type SchemaConfig struct {
	Type   string `yaml:"type"`
	Schema string `yaml:"schema"`
}

type SchemasConfig struct {
	Enabled           bool         `yaml:"enabled"`
	RegistryTimeoutMs int          `yaml:"registry_timeout_ms"`
	UserEvent         SchemaConfig `yaml:"user_event"`
	Transaction       SchemaConfig `yaml:"transaction"`
}

type MetricsConfig struct {
	Enabled            bool          `yaml:"enabled"`
	CollectionInterval time.Duration `yaml:"collection_interval"`
	PrometheusPort     int           `yaml:"prometheus_port"`
	TrackLatency       bool          `yaml:"track_latency"`
	TrackThroughput    bool          `yaml:"track_throughput"`
	TrackErrors        bool          `yaml:"track_errors"`
	TrackConsumerLag   bool          `yaml:"track_consumer_lag"`
	LatencyPercentiles []float64     `yaml:"latency_percentiles"`
}

type ScenarioConfig struct {
	ProducerRate   int           `yaml:"producer_rate"`
	RampUpTime     time.Duration `yaml:"ramp_up_time"`
	SteadyDuration time.Duration `yaml:"steady_duration"`
	RampDownTime   time.Duration `yaml:"ramp_down_time"`
	BaseRate       int           `yaml:"base_rate"`
	BurstRate      int           `yaml:"burst_rate"`
	BurstDuration  time.Duration `yaml:"burst_duration"`
	BurstInterval  time.Duration `yaml:"burst_interval"`
	StartRate      int           `yaml:"start_rate"`
	EndRate        int           `yaml:"end_rate"`
	RampDuration   time.Duration `yaml:"ramp_duration"`
	StepDuration   time.Duration `yaml:"step_duration"`
}

type ScenariosConfig struct {
	SteadyLoad ScenarioConfig `yaml:"steady_load"`
	BurstLoad  ScenarioConfig `yaml:"burst_load"`
	RampTest   ScenarioConfig `yaml:"ramp_test"`
}

type ChaosConfig struct {
	Enabled                     bool          `yaml:"enabled"`
	ProducerFailureRate         float64       `yaml:"producer_failure_rate"`
	ConsumerFailureRate         float64       `yaml:"consumer_failure_rate"`
	NetworkPartitionProbability float64       `yaml:"network_partition_probability"`
	BrokerRestartInterval       time.Duration `yaml:"broker_restart_interval"`
}

type OutputConfig struct {
	ResultsDir       string        `yaml:"results_dir"`
	ExportPrometheus bool          `yaml:"export_prometheus"`
	ExportCSV        bool          `yaml:"export_csv"`
	ExportJSON       bool          `yaml:"export_json"`
	RealTimeStats    bool          `yaml:"real_time_stats"`
	StatsInterval    time.Duration `yaml:"stats_interval"`
}

type LoggingConfig struct {
	Level           string `yaml:"level"`
	Format          string `yaml:"format"`
	EnableKafkaLogs bool   `yaml:"enable_kafka_logs"`
}

// Load reads and parses the configuration file
func Load(configFile string) (*Config, error) {
	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", configFile, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", configFile, err)
	}

	// Apply default values
	cfg.setDefaults()

	// Apply environment variable overrides
	cfg.applyEnvOverrides()

	return &cfg, nil
}

// ApplyOverrides applies command-line flag overrides
func (c *Config) ApplyOverrides(testMode string, duration time.Duration) {
	if testMode != "" {
		c.TestMode = testMode
	}
	if duration > 0 {
		c.Duration = duration
	}
}

// setDefaults sets default values for optional fields
func (c *Config) setDefaults() {
	if c.TestMode == "" {
		c.TestMode = "comprehensive"
	}

	if len(c.Kafka.BootstrapServers) == 0 {
		c.Kafka.BootstrapServers = []string{"kafka-gateway:9093"}
	}

	if c.SchemaRegistry.URL == "" {
		c.SchemaRegistry.URL = "http://schema-registry:8081"
	}

	// Schema support is always enabled since Kafka Gateway now enforces schema-first behavior
	c.Schemas.Enabled = true

	if c.Producers.Count == 0 {
		c.Producers.Count = 10
	}

	if c.Consumers.Count == 0 {
		c.Consumers.Count = 5
	}

	if c.Topics.Count == 0 {
		c.Topics.Count = 5
	}

	if c.Topics.Prefix == "" {
		c.Topics.Prefix = "loadtest-topic"
	}

	if c.Topics.Partitions == 0 {
		c.Topics.Partitions = 4 // Default to 4 partitions
	}

	if c.Topics.ReplicationFactor == 0 {
		c.Topics.ReplicationFactor = 1 // Default to 1 replica
	}

	if c.Consumers.GroupPrefix == "" {
		c.Consumers.GroupPrefix = "loadtest-group"
	}

	if c.Output.ResultsDir == "" {
		c.Output.ResultsDir = "/test-results"
	}

	if c.Metrics.CollectionInterval == 0 {
		c.Metrics.CollectionInterval = 10 * time.Second
	}

	if c.Output.StatsInterval == 0 {
		c.Output.StatsInterval = 30 * time.Second
	}
}

// applyEnvOverrides applies environment variable overrides
func (c *Config) applyEnvOverrides() {
	if servers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS"); servers != "" {
		c.Kafka.BootstrapServers = strings.Split(servers, ",")
	}

	if url := os.Getenv("SCHEMA_REGISTRY_URL"); url != "" {
		c.SchemaRegistry.URL = url
	}

	if mode := os.Getenv("TEST_MODE"); mode != "" {
		c.TestMode = mode
	}

	if duration := os.Getenv("TEST_DURATION"); duration != "" {
		if d, err := time.ParseDuration(duration); err == nil {
			c.Duration = d
		}
	}

	if count := os.Getenv("PRODUCER_COUNT"); count != "" {
		if i, err := strconv.Atoi(count); err == nil {
			c.Producers.Count = i
		}
	}

	if count := os.Getenv("CONSUMER_COUNT"); count != "" {
		if i, err := strconv.Atoi(count); err == nil {
			c.Consumers.Count = i
		}
	}

	if rate := os.Getenv("MESSAGE_RATE"); rate != "" {
		if i, err := strconv.Atoi(rate); err == nil {
			c.Producers.MessageRate = i
		}
	}

	if size := os.Getenv("MESSAGE_SIZE"); size != "" {
		if i, err := strconv.Atoi(size); err == nil {
			c.Producers.MessageSize = i
		}
	}

	if count := os.Getenv("TOPIC_COUNT"); count != "" {
		if i, err := strconv.Atoi(count); err == nil {
			c.Topics.Count = i
		}
	}

	if partitions := os.Getenv("PARTITIONS_PER_TOPIC"); partitions != "" {
		if i, err := strconv.Atoi(partitions); err == nil {
			c.Topics.Partitions = i
		}
	}

	if valueType := os.Getenv("VALUE_TYPE"); valueType != "" {
		c.Producers.ValueType = valueType
	}

	if schemaFormat := os.Getenv("SCHEMA_FORMAT"); schemaFormat != "" {
		c.Producers.SchemaFormat = schemaFormat
	}

	if enabled := os.Getenv("SCHEMAS_ENABLED"); enabled != "" {
		c.Schemas.Enabled = enabled == "true"
	}
}

// GetTopicNames returns the list of topic names to use for testing
func (c *Config) GetTopicNames() []string {
	topics := make([]string, c.Topics.Count)
	for i := 0; i < c.Topics.Count; i++ {
		topics[i] = fmt.Sprintf("%s-%d", c.Topics.Prefix, i)
	}
	return topics
}

// GetConsumerGroupNames returns the list of consumer group names
func (c *Config) GetConsumerGroupNames() []string {
	groups := make([]string, c.Consumers.Count)
	for i := 0; i < c.Consumers.Count; i++ {
		groups[i] = fmt.Sprintf("%s-%d", c.Consumers.GroupPrefix, i)
	}
	return groups
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.TestMode != "producer" && c.TestMode != "consumer" && c.TestMode != "comprehensive" {
		return fmt.Errorf("invalid test mode: %s", c.TestMode)
	}

	if len(c.Kafka.BootstrapServers) == 0 {
		return fmt.Errorf("kafka bootstrap servers not specified")
	}

	if c.Producers.Count <= 0 && (c.TestMode == "producer" || c.TestMode == "comprehensive") {
		return fmt.Errorf("producer count must be greater than 0 for producer or comprehensive tests")
	}

	if c.Consumers.Count <= 0 && (c.TestMode == "consumer" || c.TestMode == "comprehensive") {
		return fmt.Errorf("consumer count must be greater than 0 for consumer or comprehensive tests")
	}

	if c.Topics.Count <= 0 {
		return fmt.Errorf("topic count must be greater than 0")
	}

	if c.Topics.Partitions <= 0 {
		return fmt.Errorf("partitions per topic must be greater than 0")
	}

	return nil
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/config"
	"github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/consumer"
	"github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/metrics"
	"github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/producer"
	"github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/schema"
	"github.com/seaweedfs/seaweedfs/test/kafka/kafka-client-loadtest/internal/tracker"
)

var (
	configFile = flag.String("config", "/config/loadtest.yaml", "Path to configuration file")
	testMode   = flag.String("mode", "", "Test mode override (producer|consumer|comprehensive)")
	duration   = flag.Duration("duration", 0, "Test duration override")
	help       = flag.Bool("help", false, "Show help")
)

func main() {
	flag.Parse()

	if *help {
		printHelp()
		return
	}

	// Load configuration
	cfg, err := config.Load(*configFile)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Override configuration with environment variables and flags
	cfg.ApplyOverrides(*testMode, *duration)

	// Initialize metrics
	metricsCollector := metrics.NewCollector()

	// Start metrics HTTP server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/health", healthCheck)
		http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
			metricsCollector.WriteStats(w)
		})

		log.Printf("Starting metrics server on :8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	// Set up signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Starting Kafka Client Load Test")
	log.Printf("Mode: %s, Duration: %v", cfg.TestMode, cfg.Duration)
	log.Printf("Kafka Brokers: %v", cfg.Kafka.BootstrapServers)
	log.Printf("Schema Registry: %s", cfg.SchemaRegistry.URL)
	log.Printf("Schemas Enabled: %v", cfg.Schemas.Enabled)

	// Register schemas if enabled
	if cfg.Schemas.Enabled {
		log.Printf("Registering schemas with Schema Registry...")
		if err := registerSchemas(cfg); err != nil {
			log.Fatalf("Failed to register schemas: %v", err)
		}
		log.Printf("Schemas registered successfully")
	}

	var wg sync.WaitGroup

	// Start test based on mode
	var testErr error
	switch cfg.TestMode {
	case "producer":
		testErr = runProducerTest(ctx, cfg, metricsCollector, &wg)
	case "consumer":
		testErr = runConsumerTest(ctx, cfg, metricsCollector, &wg)
	case "comprehensive":
		testErr = runComprehensiveTest(ctx, cancel, cfg, metricsCollector, &wg)
	default:
		log.Fatalf("Unknown test mode: %s", cfg.TestMode)
	}

	// If test returned an error (e.g., circuit breaker), exit
	if testErr != nil {
		log.Printf("Test failed with error: %v", testErr)
		cancel() // Cancel context to stop any remaining goroutines
		return
	}

	// Wait for completion or signal
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-sigCh:
		log.Printf("Received shutdown signal, stopping tests...")
		cancel()

		// Wait for graceful shutdown with timeout
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		select {
		case <-done:
			log.Printf("All tests completed gracefully")
		case <-shutdownCtx.Done():
			log.Printf("Shutdown timeout, forcing exit")
		}
	case <-done:
		log.Printf("All tests completed")
	}

	// Print final statistics
	log.Printf("Final Test Statistics:")
	metricsCollector.PrintSummary()
}

func runProducerTest(ctx context.Context, cfg *config.Config, collector *metrics.Collector, wg *sync.WaitGroup) error {
	log.Printf("Starting producer-only test with %d producers", cfg.Producers.Count)

	// Create record tracker with current timestamp to filter old messages
	testStartTime := time.Now().UnixNano()
	recordTracker := tracker.NewTracker("/test-results/produced.jsonl", "/test-results/consumed.jsonl", testStartTime)

	errChan := make(chan error, cfg.Producers.Count)

	for i := 0; i < cfg.Producers.Count; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			prod, err := producer.New(cfg, collector, id, recordTracker)
			if err != nil {
				log.Printf("Failed to create producer %d: %v", id, err)
				errChan <- err
				return
			}
			defer prod.Close()

			if err := prod.Run(ctx); err != nil {
				log.Printf("Producer %d failed: %v", id, err)
				errChan <- err
				return
			}
		}(i)
	}

	// Wait for any producer error
	select {
	case err := <-errChan:
		log.Printf("Producer test failed: %v", err)
		return err
	default:
		return nil
	}
}

func runConsumerTest(ctx context.Context, cfg *config.Config, collector *metrics.Collector, wg *sync.WaitGroup) error {
	log.Printf("Starting consumer-only test with %d consumers", cfg.Consumers.Count)

	// Create record tracker with current timestamp to filter old messages
	testStartTime := time.Now().UnixNano()
	recordTracker := tracker.NewTracker("/test-results/produced.jsonl", "/test-results/consumed.jsonl", testStartTime)

	errChan := make(chan error, cfg.Consumers.Count)

	for i := 0; i < cfg.Consumers.Count; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			cons, err := consumer.New(cfg, collector, id, recordTracker)
			if err != nil {
				log.Printf("Failed to create consumer %d: %v", id, err)
				errChan <- err
				return
			}
			defer cons.Close()

			cons.Run(ctx)
		}(i)
	}

	// Consumers don't typically return errors in the same way, so just return nil
	return nil
}

func runComprehensiveTest(ctx context.Context, cancel context.CancelFunc, cfg *config.Config, collector *metrics.Collector, wg *sync.WaitGroup) error {
	log.Printf("Starting comprehensive test with %d producers and %d consumers",
		cfg.Producers.Count, cfg.Consumers.Count)

	// Create record tracker with current timestamp to filter old messages
	testStartTime := time.Now().UnixNano()
	log.Printf("Test run starting at %d - only tracking messages from this run", testStartTime)
	recordTracker := tracker.NewTracker("/test-results/produced.jsonl", "/test-results/consumed.jsonl", testStartTime)

	errChan := make(chan error, cfg.Producers.Count)

	// Create separate contexts for producers and consumers
	producerCtx, producerCancel := context.WithCancel(ctx)
	consumerCtx, consumerCancel := context.WithCancel(ctx)

	// Start producers
	for i := 0; i < cfg.Producers.Count; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			prod, err := producer.New(cfg, collector, id, recordTracker)
			if err != nil {
				log.Printf("Failed to create producer %d: %v", id, err)
				errChan <- err
				return
			}
			defer prod.Close()

			if err := prod.Run(producerCtx); err != nil {
				log.Printf("Producer %d failed: %v", id, err)
				errChan <- err
				return
			}
		}(i)
	}

	// Wait briefly for producers to start producing messages
	// Reduced from 5s to 2s to minimize message backlog
	time.Sleep(2 * time.Second)

	// Start consumers
	// NOTE: With unique ClientIDs, all consumers can start simultaneously without connection storms
	for i := 0; i < cfg.Consumers.Count; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			cons, err := consumer.New(cfg, collector, id, recordTracker)
			if err != nil {
				log.Printf("Failed to create consumer %d: %v", id, err)
				return
			}
			defer cons.Close()

			cons.Run(consumerCtx)
		}(i)
	}

	// Check for producer errors
	select {
	case err := <-errChan:
		log.Printf("Comprehensive test failed due to producer error: %v", err)
		producerCancel()
		consumerCancel()
		return err
	default:
		// No immediate error, continue
	}

	// If duration is set, stop producers first, then allow consumers extra time to drain
	if cfg.Duration > 0 {
		go func() {
			timer := time.NewTimer(cfg.Duration)
			defer timer.Stop()

			select {
			case <-timer.C:
				log.Printf("Test duration (%v) reached, stopping producers", cfg.Duration)
				producerCancel()

				// Allow consumers extra time to drain remaining messages
				// Calculate drain time based on test duration (minimum 60s, up to test duration)
				drainTime := 60 * time.Second
				if cfg.Duration > drainTime {
					drainTime = cfg.Duration // Match test duration for longer tests
				}
				log.Printf("Allowing %v for consumers to drain remaining messages...", drainTime)
				time.Sleep(drainTime)

				log.Printf("Stopping consumers after drain period")
				consumerCancel()
				cancel()
			case <-ctx.Done():
				// Context already cancelled
				producerCancel()
				consumerCancel()
			}
		}()
	} else {
		// No duration set, wait for cancellation and ensure cleanup
		go func() {
			<-ctx.Done()
			producerCancel()
			consumerCancel()
		}()
	}

	// Wait for all producer and consumer goroutines to complete
	log.Printf("Waiting for all producers and consumers to complete...")
	wg.Wait()
	log.Printf("All producers and consumers completed, starting verification...")

	// Save produced and consumed records
	log.Printf("Saving produced records...")
	if err := recordTracker.SaveProduced(); err != nil {
		log.Printf("Failed to save produced records: %v", err)
	}

	log.Printf("Saving consumed records...")
	if err := recordTracker.SaveConsumed(); err != nil {
		log.Printf("Failed to save consumed records: %v", err)
	}

	// Compare records
	log.Printf("Comparing produced vs consumed records...")
	result := recordTracker.Compare()
	result.PrintSummary()

	log.Printf("Verification complete!")
	return nil
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "OK")
}

func printHelp() {
	fmt.Printf(`Kafka Client Load Test for SeaweedFS

Usage: %s [options]

Options:
  -config string
        Path to configuration file (default "/config/loadtest.yaml")
  -mode string
        Test mode override (producer|consumer|comprehensive)
  -duration duration
        Test duration override
  -help
        Show this help message

Environment Variables:
  KAFKA_BOOTSTRAP_SERVERS  Comma-separated list of Kafka brokers
  SCHEMA_REGISTRY_URL      URL of the Schema Registry
  TEST_DURATION           Test duration (e.g., "5m", "300s")
  TEST_MODE               Test mode (producer|consumer|comprehensive)
  PRODUCER_COUNT          Number of producer instances
  CONSUMER_COUNT          Number of consumer instances
  MESSAGE_RATE            Messages per second per producer
  MESSAGE_SIZE            Message size in bytes
  TOPIC_COUNT             Number of topics to create
  PARTITIONS_PER_TOPIC    Number of partitions per topic
  VALUE_TYPE              Message value type (json/avro/binary)

Test Modes:
  producer       - Run only producers (generate load)
  consumer       - Run only consumers (consume existing messages)
  comprehensive  - Run both producers and consumers simultaneously

Example:
  %s -config ./config/loadtest.yaml -mode comprehensive -duration 10m

`, os.Args[0], os.Args[0])
}

// registerSchemas registers schemas with Schema Registry for all topics
func registerSchemas(cfg *config.Config) error {
	// Wait for Schema Registry to be ready
	if err := waitForSchemaRegistry(cfg.SchemaRegistry.URL); err != nil {
		return fmt.Errorf("schema registry not ready: %w", err)
	}

	// Register schemas for each topic with different formats for variety
	topics := cfg.GetTopicNames()

	// Determine schema formats - use different formats for different topics
	// This provides comprehensive testing of all schema format variations
	for i, topic := range topics {
		var schemaFormat string

		// Distribute topics across three schema formats for comprehensive testing
		// Format 0: AVRO (default, most common)
		// Format 1: JSON (modern, human-readable)
		// Format 2: PROTOBUF (efficient binary format)
		switch i % 3 {
		case 0:
			schemaFormat = "AVRO"
		case 1:
			schemaFormat = "JSON"
		case 2:
			schemaFormat = "PROTOBUF"
		}

		// Allow override from config if specified
		if cfg.Producers.SchemaFormat != "" {
			schemaFormat = cfg.Producers.SchemaFormat
		}

		if err := registerTopicSchema(cfg.SchemaRegistry.URL, topic, schemaFormat); err != nil {
			return fmt.Errorf("failed to register schema for topic %s (format: %s): %w", topic, schemaFormat, err)
		}
		log.Printf("Schema registered for topic %s with format: %s", topic, schemaFormat)
	}

	return nil
}

// waitForSchemaRegistry waits for Schema Registry to be ready
func waitForSchemaRegistry(url string) error {
	maxRetries := 30
	for i := 0; i < maxRetries; i++ {
		resp, err := http.Get(url + "/subjects")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("schema registry not ready after %d retries", maxRetries)
}

// registerTopicSchema registers a schema for a specific topic
func registerTopicSchema(registryURL, topicName, schemaFormat string) error {
	// Determine schema format, default to AVRO
	if schemaFormat == "" {
		schemaFormat = "AVRO"
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

	schemaReq := map[string]interface{}{
		"schema":     schemaStr,
		"schemaType": schemaType,
	}

	jsonData, err := json.Marshal(schemaReq)
	if err != nil {
		return err
	}

	// Register schema for topic value
	subject := topicName + "-value"
	url := fmt.Sprintf("%s/subjects/%s/versions", registryURL, subject)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(url, "application/vnd.schemaregistry.v1+json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("schema registration failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	log.Printf("Schema registered for topic %s (format: %s)", topicName, schemaType)
	return nil
}

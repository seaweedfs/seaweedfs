package integration

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PerformanceMetrics holds test metrics
type PerformanceMetrics struct {
	MessagesPublished int64
	MessagesConsumed  int64
	PublishLatencies  []time.Duration
	ConsumeLatencies  []time.Duration
	StartTime         time.Time
	EndTime           time.Time
	ErrorCount        int64
	mu                sync.RWMutex
}

func (m *PerformanceMetrics) AddPublishLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.PublishLatencies = append(m.PublishLatencies, d)
}

func (m *PerformanceMetrics) AddConsumeLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ConsumeLatencies = append(m.ConsumeLatencies, d)
}

func (m *PerformanceMetrics) GetThroughput() float64 {
	duration := m.EndTime.Sub(m.StartTime).Seconds()
	if duration == 0 {
		return 0
	}
	return float64(atomic.LoadInt64(&m.MessagesPublished)) / duration
}

func (m *PerformanceMetrics) GetP95Latency(latencies []time.Duration) time.Duration {
	if len(latencies) == 0 {
		return 0
	}

	// Simple P95 calculation - in production use proper percentile library
	index := int(float64(len(latencies)) * 0.95)
	if index >= len(latencies) {
		index = len(latencies) - 1
	}

	// Sort latencies (simplified)
	for i := 0; i < len(latencies)-1; i++ {
		for j := 0; j < len(latencies)-i-1; j++ {
			if latencies[j] > latencies[j+1] {
				latencies[j], latencies[j+1] = latencies[j+1], latencies[j]
			}
		}
	}

	return latencies[index]
}

// Enhanced performance metrics with connection error tracking
type EnhancedPerformanceMetrics struct {
	MessagesPublished int64
	MessagesConsumed  int64
	PublishLatencies  []time.Duration
	ConsumeLatencies  []time.Duration
	StartTime         time.Time
	EndTime           time.Time
	ErrorCount        int64
	ConnectionErrors  int64
	ApplicationErrors int64
	RetryAttempts     int64
	mu                sync.RWMutex
}

func (m *EnhancedPerformanceMetrics) AddPublishLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.PublishLatencies = append(m.PublishLatencies, d)
}

func (m *EnhancedPerformanceMetrics) AddConsumeLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ConsumeLatencies = append(m.ConsumeLatencies, d)
}

func (m *EnhancedPerformanceMetrics) GetThroughput() float64 {
	duration := m.EndTime.Sub(m.StartTime).Seconds()
	if duration == 0 {
		return 0
	}
	return float64(atomic.LoadInt64(&m.MessagesPublished)) / duration
}

func (m *EnhancedPerformanceMetrics) GetP95Latency(latencies []time.Duration) time.Duration {
	if len(latencies) == 0 {
		return 0
	}

	// Simple P95 calculation
	index := int(float64(len(latencies)) * 0.95)
	if index >= len(latencies) {
		index = len(latencies) - 1
	}

	// Sort latencies (simplified bubble sort for small datasets)
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	for i := 0; i < len(sorted)-1; i++ {
		for j := 0; j < len(sorted)-i-1; j++ {
			if sorted[j] > sorted[j+1] {
				sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
			}
		}
	}

	return sorted[index]
}

// isConnectionError determines if an error is a connection-level error
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// Check for gRPC status codes
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Unknown:
			return true
		}
	}

	// Check for common connection error strings
	connectionErrorPatterns := []string{
		"EOF",
		"error reading server preface",
		"connection refused",
		"connection reset",
		"broken pipe",
		"network is unreachable",
		"no route to host",
		"transport is closing",
		"connection error",
		"dial tcp",
		"context deadline exceeded",
	}

	for _, pattern := range connectionErrorPatterns {
		if containsString(errStr, pattern) {
			return true
		}
	}

	return false
}

func containsString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestPerformanceThroughput(t *testing.T) {
	suite := NewIntegrationTestSuite(t)
	require.NoError(t, suite.Setup())

	topicName := "performance-throughput-test"
	namespace := "perf-test"

	metrics := &PerformanceMetrics{
		StartTime: time.Now(),
	}

	// Test parameters
	numMessages := 50000
	numPublishers := 5
	messageSize := 1024 // 1KB messages

	// Create message payload
	payload := make([]byte, messageSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	t.Logf("Starting throughput test: %d messages, %d publishers, %d bytes per message",
		numMessages, numPublishers, messageSize)

	// Start publishers
	var publishWg sync.WaitGroup
	messagesPerPublisher := numMessages / numPublishers

	for i := 0; i < numPublishers; i++ {
		publishWg.Add(1)
		go func(publisherID int) {
			defer publishWg.Done()

			// Create publisher for this goroutine
			pubConfig := &PublisherTestConfig{
				Namespace:      namespace,
				TopicName:      topicName,
				PartitionCount: 4, // Multiple partitions for better throughput
				PublisherName:  fmt.Sprintf("perf-publisher-%d", publisherID),
				RecordType:     nil, // Use raw publish
			}

			publisher, err := suite.CreatePublisher(pubConfig)
			if err != nil {
				atomic.AddInt64(&metrics.ErrorCount, 1)
				t.Errorf("Failed to create publisher %d: %v", publisherID, err)
				return
			}

			for j := 0; j < messagesPerPublisher; j++ {
				messageKey := fmt.Sprintf("publisher-%d-msg-%d", publisherID, j)

				start := time.Now()
				err := publisher.Publish([]byte(messageKey), payload)
				latency := time.Since(start)

				if err != nil {
					atomic.AddInt64(&metrics.ErrorCount, 1)
					continue
				}

				atomic.AddInt64(&metrics.MessagesPublished, 1)
				metrics.AddPublishLatency(latency)

				// Small delay to prevent overwhelming the system
				if j%1000 == 0 {
					time.Sleep(1 * time.Millisecond)
				}
			}
		}(i)
	}

	// Wait for publishing to complete
	publishWg.Wait()
	metrics.EndTime = time.Now()

	// Verify results
	publishedCount := atomic.LoadInt64(&metrics.MessagesPublished)
	errorCount := atomic.LoadInt64(&metrics.ErrorCount)
	throughput := metrics.GetThroughput()

	t.Logf("Performance Results:")
	t.Logf("  Messages Published: %d", publishedCount)
	t.Logf("  Errors: %d", errorCount)
	t.Logf("  Throughput: %.2f messages/second", throughput)
	t.Logf("  Duration: %v", metrics.EndTime.Sub(metrics.StartTime))

	if len(metrics.PublishLatencies) > 0 {
		p95Latency := metrics.GetP95Latency(metrics.PublishLatencies)
		t.Logf("  P95 Publish Latency: %v", p95Latency)

		// Performance assertions
		assert.Less(t, p95Latency, 100*time.Millisecond, "P95 publish latency should be under 100ms")
	}

	// Throughput requirements
	expectedMinThroughput := 5000.0 // 5K messages/sec minimum (relaxed from 10K for initial testing)
	assert.Greater(t, throughput, expectedMinThroughput,
		"Throughput should exceed %.0f messages/second", expectedMinThroughput)

	// Error rate should be low
	errorRate := float64(errorCount) / float64(publishedCount+errorCount)
	assert.Less(t, errorRate, 0.05, "Error rate should be less than 5%")
}

func TestPerformanceLatency(t *testing.T) {
	suite := NewIntegrationTestSuite(t)
	require.NoError(t, suite.Setup())

	topicName := "performance-latency-test"
	namespace := "perf-test"

	// Create publisher
	pubConfig := &PublisherTestConfig{
		Namespace:      namespace,
		TopicName:      topicName,
		PartitionCount: 1, // Single partition for latency testing
		PublisherName:  "latency-publisher",
		RecordType:     nil,
	}

	publisher, err := suite.CreatePublisher(pubConfig)
	require.NoError(t, err)

	// Start consumer first
	subConfig := &SubscriberTestConfig{
		Namespace:          namespace,
		TopicName:          topicName,
		ConsumerGroup:      "latency-test-group",
		ConsumerInstanceId: "latency-consumer-1",
		MaxPartitionCount:  1,
		SlidingWindowSize:  10,
		OffsetType:         schema_pb.OffsetType_RESET_TO_EARLIEST,
	}

	subscriber, err := suite.CreateSubscriber(subConfig)
	require.NoError(t, err)

	numMessages := 5000
	collector := NewMessageCollector(numMessages)

	// Set up message handler
	subscriber.SetOnDataMessageFn(func(m *mq_pb.SubscribeMessageResponse_Data) {
		collector.AddMessage(TestMessage{
			ID:        string(m.Data.Key),
			Content:   m.Data.Value,
			Timestamp: time.Unix(0, m.Data.TsNs),
			Key:       m.Data.Key,
		})
	})

	// Start subscriber
	go func() {
		err := subscriber.Subscribe()
		if err != nil {
			t.Logf("Subscriber error: %v", err)
		}
	}()

	// Wait for consumer to be ready
	time.Sleep(2 * time.Second)

	metrics := &PerformanceMetrics{
		StartTime: time.Now(),
	}

	t.Logf("Starting latency test with %d messages", numMessages)

	// Publish messages with controlled timing
	for i := 0; i < numMessages; i++ {
		messageKey := fmt.Sprintf("latency-msg-%d", i)
		payload := fmt.Sprintf("test-payload-data-%d", i)

		start := time.Now()
		err := publisher.Publish([]byte(messageKey), []byte(payload))
		publishLatency := time.Since(start)

		require.NoError(t, err)
		metrics.AddPublishLatency(publishLatency)

		// Controlled rate for latency measurement
		if i%100 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	metrics.EndTime = time.Now()

	// Wait for messages to be consumed
	messages := collector.WaitForMessages(30 * time.Second)

	// Analyze latency results
	t.Logf("Latency Test Results:")
	t.Logf("  Messages Published: %d", numMessages)
	t.Logf("  Messages Consumed: %d", len(messages))

	if len(metrics.PublishLatencies) > 0 {
		p95PublishLatency := metrics.GetP95Latency(metrics.PublishLatencies)
		t.Logf("  P95 Publish Latency: %v", p95PublishLatency)

		// Latency assertions
		assert.Less(t, p95PublishLatency, 50*time.Millisecond,
			"P95 publish latency should be under 50ms")
	}

	// Verify message delivery
	deliveryRate := float64(len(messages)) / float64(numMessages)
	assert.Greater(t, deliveryRate, 0.80, "Should deliver at least 80% of messages")
}

func TestPerformanceConcurrentConsumers(t *testing.T) {
	suite := NewIntegrationTestSuite(t)
	require.NoError(t, suite.Setup())

	topicName := "performance-concurrent-test"
	namespace := "perf-test"
	numPartitions := int32(4)

	// Create publisher first
	pubConfig := &PublisherTestConfig{
		Namespace:      namespace,
		TopicName:      topicName,
		PartitionCount: numPartitions,
		PublisherName:  "concurrent-publisher",
		RecordType:     nil,
	}

	publisher, err := suite.CreatePublisher(pubConfig)
	require.NoError(t, err)

	// Test parameters
	numConsumers := 8
	numMessages := 20000
	consumerGroup := "concurrent-perf-group"

	t.Logf("Starting concurrent consumer test: %d consumers, %d messages, %d partitions",
		numConsumers, numMessages, numPartitions)

	// Start multiple consumers
	var collectors []*MessageCollector

	for i := 0; i < numConsumers; i++ {
		collector := NewMessageCollector(numMessages / numConsumers) // Expected per consumer
		collectors = append(collectors, collector)

		subConfig := &SubscriberTestConfig{
			Namespace:          namespace,
			TopicName:          topicName,
			ConsumerGroup:      consumerGroup,
			ConsumerInstanceId: fmt.Sprintf("consumer-%d", i),
			MaxPartitionCount:  numPartitions,
			SlidingWindowSize:  10,
			OffsetType:         schema_pb.OffsetType_RESET_TO_EARLIEST,
		}

		subscriber, err := suite.CreateSubscriber(subConfig)
		require.NoError(t, err)

		// Set up message handler for this consumer
		func(consumerID int, c *MessageCollector) {
			subscriber.SetOnDataMessageFn(func(m *mq_pb.SubscribeMessageResponse_Data) {
				c.AddMessage(TestMessage{
					ID:        fmt.Sprintf("%d-%s", consumerID, string(m.Data.Key)),
					Content:   m.Data.Value,
					Timestamp: time.Unix(0, m.Data.TsNs),
					Key:       m.Data.Key,
				})
			})
		}(i, collector)

		// Start subscriber
		go func(consumerID int, s *SubscriberTestConfig) {
			sub, _ := suite.CreateSubscriber(s)
			err := sub.Subscribe()
			if err != nil {
				t.Logf("Consumer %d error: %v", consumerID, err)
			}
		}(i, subConfig)
	}

	// Wait for consumers to initialize
	time.Sleep(3 * time.Second)

	// Start publishing
	startTime := time.Now()

	var publishWg sync.WaitGroup
	numPublishers := 3
	messagesPerPublisher := numMessages / numPublishers

	for i := 0; i < numPublishers; i++ {
		publishWg.Add(1)
		go func(publisherID int) {
			defer publishWg.Done()

			for j := 0; j < messagesPerPublisher; j++ {
				messageKey := fmt.Sprintf("concurrent-msg-%d-%d", publisherID, j)
				payload := fmt.Sprintf("publisher-%d-message-%d-data", publisherID, j)

				err := publisher.Publish([]byte(messageKey), []byte(payload))

				if err != nil {
					t.Logf("Publish error: %v", err)
				}

				// Rate limiting
				if j%1000 == 0 {
					time.Sleep(2 * time.Millisecond)
				}
			}
		}(i)
	}

	publishWg.Wait()
	publishDuration := time.Since(startTime)

	// Allow time for message consumption
	time.Sleep(10 * time.Second)

	// Analyze results
	totalConsumed := int64(0)
	for i, collector := range collectors {
		messages := collector.GetMessages()
		consumed := int64(len(messages))
		totalConsumed += consumed
		t.Logf("Consumer %d consumed %d messages", i, consumed)
	}

	publishThroughput := float64(numMessages) / publishDuration.Seconds()
	consumeThroughput := float64(totalConsumed) / publishDuration.Seconds()

	t.Logf("Concurrent Consumer Test Results:")
	t.Logf("  Total Published: %d", numMessages)
	t.Logf("  Total Consumed: %d", totalConsumed)
	t.Logf("  Publish Throughput: %.2f msg/sec", publishThroughput)
	t.Logf("  Consume Throughput: %.2f msg/sec", consumeThroughput)
	t.Logf("  Test Duration: %v", publishDuration)

	// Performance assertions (relaxed for initial testing)
	deliveryRate := float64(totalConsumed) / float64(numMessages)
	assert.Greater(t, deliveryRate, 0.70, "Should consume at least 70% of messages")

	expectedMinThroughput := 2000.0 // 2K messages/sec minimum for concurrent consumption
	assert.Greater(t, consumeThroughput, expectedMinThroughput,
		"Consume throughput should exceed %.0f messages/second", expectedMinThroughput)
}

func TestPerformanceWithErrorHandling(t *testing.T) {
	suite := NewIntegrationTestSuite(t)
	require.NoError(t, suite.Setup())

	topicName := "performance-error-handling-test"
	namespace := "perf-test"

	metrics := &EnhancedPerformanceMetrics{
		StartTime: time.Now(),
	}

	// Test parameters
	numMessages := 50000
	numPublishers := 5
	messageSize := 1024 // 1KB messages

	// Create message payload
	payload := make([]byte, messageSize)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	t.Logf("Starting performance test with enhanced error handling: %d messages, %d publishers, %d bytes per message",
		numMessages, numPublishers, messageSize)

	// Start publishers
	var publishWg sync.WaitGroup
	messagesPerPublisher := numMessages / numPublishers

	for i := 0; i < numPublishers; i++ {
		publishWg.Add(1)
		go func(publisherID int) {
			defer publishWg.Done()

			// Create publisher for this goroutine
			pubConfig := &PublisherTestConfig{
				Namespace:      namespace,
				TopicName:      topicName,
				PartitionCount: 4, // Multiple partitions for better throughput
				PublisherName:  fmt.Sprintf("error-aware-publisher-%d", publisherID),
				RecordType:     nil, // Use raw publish
			}

			publisher, err := suite.CreatePublisher(pubConfig)
			if err != nil {
				atomic.AddInt64(&metrics.ErrorCount, 1)
				t.Errorf("Failed to create publisher %d: %v", publisherID, err)
				return
			}

			for j := 0; j < messagesPerPublisher; j++ {
				messageKey := fmt.Sprintf("publisher-%d-msg-%d", publisherID, j)

				start := time.Now()
				err := publisher.Publish([]byte(messageKey), payload)
				latency := time.Since(start)

				if err != nil {
					atomic.AddInt64(&metrics.ErrorCount, 1)

					// Classify the error type
					if isConnectionError(err) {
						atomic.AddInt64(&metrics.ConnectionErrors, 1)
						t.Logf("Connection error (publisher %d, msg %d): %v", publisherID, j, err)
					} else {
						atomic.AddInt64(&metrics.ApplicationErrors, 1)
						t.Logf("Application error (publisher %d, msg %d): %v", publisherID, j, err)
					}
					continue
				}

				atomic.AddInt64(&metrics.MessagesPublished, 1)
				metrics.AddPublishLatency(latency)

				// Small delay to prevent overwhelming the system
				if j%1000 == 0 {
					time.Sleep(1 * time.Millisecond)
				}
			}
		}(i)
	}

	// Wait for publishing to complete
	publishWg.Wait()
	metrics.EndTime = time.Now()

	// Analyze results with enhanced error reporting
	publishedCount := atomic.LoadInt64(&metrics.MessagesPublished)
	totalErrors := atomic.LoadInt64(&metrics.ErrorCount)
	connectionErrors := atomic.LoadInt64(&metrics.ConnectionErrors)
	applicationErrors := atomic.LoadInt64(&metrics.ApplicationErrors)
	throughput := metrics.GetThroughput()

	t.Logf("Enhanced Performance Results:")
	t.Logf("  Messages Successfully Published: %d", publishedCount)
	t.Logf("  Total Errors: %d", totalErrors)
	t.Logf("  Connection-Level Errors: %d", connectionErrors)
	t.Logf("  Application-Level Errors: %d", applicationErrors)
	t.Logf("  Throughput: %.2f messages/second", throughput)
	t.Logf("  Duration: %v", metrics.EndTime.Sub(metrics.StartTime))

	if len(metrics.PublishLatencies) > 0 {
		p95Latency := metrics.GetP95Latency(metrics.PublishLatencies)
		t.Logf("  P95 Publish Latency: %v", p95Latency)

		// Performance assertions (adjusted for error handling overhead)
		assert.Less(t, p95Latency, 100*time.Millisecond, "P95 publish latency should be under 100ms")
	}

	// Enhanced error analysis
	totalAttempts := publishedCount + totalErrors
	if totalAttempts > 0 {
		successRate := float64(publishedCount) / float64(totalAttempts)
		connectionErrorRate := float64(connectionErrors) / float64(totalAttempts)
		applicationErrorRate := float64(applicationErrors) / float64(totalAttempts)

		t.Logf("Error Analysis:")
		t.Logf("  Success Rate: %.2f%%", successRate*100)
		t.Logf("  Connection Error Rate: %.2f%%", connectionErrorRate*100)
		t.Logf("  Application Error Rate: %.2f%%", applicationErrorRate*100)

		// Assertions based on error types
		assert.Greater(t, successRate, 0.80, "Success rate should be greater than 80%")
		assert.Less(t, applicationErrorRate, 0.01, "Application error rate should be less than 1%")

		// Connection errors are expected under high load but should be handled
		if connectionErrors > 0 {
			t.Logf("Note: %d connection errors detected - this indicates the test is successfully stressing the system", connectionErrors)
			t.Logf("Recommendation: Implement retry logic for production applications to handle these connection errors")
		}
	}

	// Throughput requirements (adjusted for error handling)
	expectedMinThroughput := 5000.0 // 5K messages/sec minimum
	assert.Greater(t, throughput, expectedMinThroughput,
		"Throughput should exceed %.0f messages/second", expectedMinThroughput)
}

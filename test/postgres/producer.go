package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
)

type UserEvent struct {
	ID        int64     `json:"id"`
	UserID    int64     `json:"user_id"`
	UserType  string    `json:"user_type"`
	Action    string    `json:"action"`
	Status    string    `json:"status"`
	Amount    float64   `json:"amount,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Metadata  string    `json:"metadata,omitempty"`
}

type SystemLog struct {
	ID        int64     `json:"id"`
	Level     string    `json:"level"`
	Service   string    `json:"service"`
	Message   string    `json:"message"`
	ErrorCode int       `json:"error_code,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

type MetricEntry struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Value     float64   `json:"value"`
	Tags      string    `json:"tags"`
	Timestamp time.Time `json:"timestamp"`
}

type ProductView struct {
	ID        int64     `json:"id"`
	ProductID int64     `json:"product_id"`
	UserID    int64     `json:"user_id"`
	Category  string    `json:"category"`
	Price     float64   `json:"price"`
	ViewCount int       `json:"view_count"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	// Get SeaweedFS configuration from environment
	masterAddr := getEnv("SEAWEEDFS_MASTER", "localhost:9333")
	filerAddr := getEnv("SEAWEEDFS_FILER", "localhost:8888")

	log.Printf("Creating MQ test data...")
	log.Printf("Master: %s", masterAddr)
	log.Printf("Filer: %s", filerAddr)

	// Wait for SeaweedFS to be ready
	log.Println("Waiting for SeaweedFS to be ready...")
	time.Sleep(10 * time.Second)

	// Create topics and populate with data
	topics := []struct {
		namespace string
		topic     string
		generator func() interface{}
		count     int
	}{
		{"analytics", "user_events", generateUserEvent, 1000},
		{"analytics", "system_logs", generateSystemLog, 500},
		{"analytics", "metrics", generateMetric, 800},
		{"ecommerce", "product_views", generateProductView, 1200},
		{"ecommerce", "user_events", generateUserEvent, 600},
		{"logs", "application_logs", generateSystemLog, 2000},
		{"logs", "error_logs", generateErrorLog, 300},
	}

	for _, topicConfig := range topics {
		log.Printf("Creating topic %s.%s with %d records...",
			topicConfig.namespace, topicConfig.topic, topicConfig.count)

		err := createTopicData(masterAddr, filerAddr,
			topicConfig.namespace, topicConfig.topic,
			topicConfig.generator, topicConfig.count)
		if err != nil {
			log.Printf("Error creating topic %s.%s: %v",
				topicConfig.namespace, topicConfig.topic, err)
		} else {
			log.Printf("✓ Successfully created %s.%s",
				topicConfig.namespace, topicConfig.topic)
		}

		// Small delay between topics
		time.Sleep(2 * time.Second)
	}

	log.Println("✓ MQ test data creation completed!")
	log.Println("\nCreated namespaces:")
	log.Println("  - analytics (user_events, system_logs, metrics)")
	log.Println("  - ecommerce (product_views, user_events)")
	log.Println("  - logs (application_logs, error_logs)")
	log.Println("\nYou can now test with PostgreSQL clients:")
	log.Println("  psql -h localhost -p 5432 -U seaweedfs -d analytics")
	log.Println("  postgres=> SHOW TABLES;")
	log.Println("  postgres=> SELECT COUNT(*) FROM user_events;")
}

func createTopicData(masterAddr, filerAddr, namespace, topicName string,
	generator func() interface{}, count int) error {

	// Create publisher configuration
	config := &pub_client.PublisherConfiguration{
		Topic:          topic.NewTopic(namespace, topicName),
		PartitionCount: 1,
		Brokers:        []string{strings.Replace(masterAddr, ":9333", ":17777", 1)}, // Use broker port
		PublisherName:  fmt.Sprintf("test-producer-%s-%s", namespace, topicName),
		RecordType:     nil, // Use simple byte publishing
	}

	// Create publisher
	publisher, err := pub_client.NewTopicPublisher(config)
	if err != nil {
		return fmt.Errorf("failed to create publisher: %v", err)
	}
	defer publisher.Shutdown()

	// Generate and publish data
	for i := 0; i < count; i++ {
		data := generator()

		// Convert to JSON
		jsonData, err := json.Marshal(data)
		if err != nil {
			log.Printf("Error marshaling data: %v", err)
			continue
		}

		// Publish message (RecordType is nil, so use regular Publish)
		err = publisher.Publish([]byte(fmt.Sprintf("key-%d", i)), jsonData)
		if err != nil {
			log.Printf("Error publishing message %d: %v", i+1, err)
			continue
		}

		// Small delay every 100 messages
		if (i+1)%100 == 0 {
			log.Printf("  Published %d/%d messages to %s.%s",
				i+1, count, namespace, topicName)
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Finish publishing
	err = publisher.FinishPublish()
	if err != nil {
		return fmt.Errorf("failed to finish publishing: %v", err)
	}

	return nil
}

func generateUserEvent() interface{} {
	userTypes := []string{"premium", "standard", "trial", "enterprise"}
	actions := []string{"login", "logout", "purchase", "view", "search", "click", "download"}
	statuses := []string{"active", "inactive", "pending", "completed", "failed"}

	return UserEvent{
		ID:        rand.Int63n(1000000) + 1,
		UserID:    rand.Int63n(10000) + 1,
		UserType:  userTypes[rand.Intn(len(userTypes))],
		Action:    actions[rand.Intn(len(actions))],
		Status:    statuses[rand.Intn(len(statuses))],
		Amount:    rand.Float64() * 1000,
		Timestamp: time.Now().Add(-time.Duration(rand.Intn(86400*30)) * time.Second),
		Metadata:  fmt.Sprintf("{\"session_id\":\"%d\"}", rand.Int63n(100000)),
	}
}

func generateSystemLog() interface{} {
	levels := []string{"debug", "info", "warning", "error", "critical"}
	services := []string{"auth-service", "payment-service", "user-service", "notification-service", "api-gateway"}
	messages := []string{
		"Request processed successfully",
		"User authentication completed",
		"Payment transaction initiated",
		"Database connection established",
		"Cache miss for key",
		"API rate limit exceeded",
		"Service health check passed",
	}

	return SystemLog{
		ID:        rand.Int63n(1000000) + 1,
		Level:     levels[rand.Intn(len(levels))],
		Service:   services[rand.Intn(len(services))],
		Message:   messages[rand.Intn(len(messages))],
		ErrorCode: rand.Intn(1000),
		Timestamp: time.Now().Add(-time.Duration(rand.Intn(86400*7)) * time.Second),
	}
}

func generateErrorLog() interface{} {
	levels := []string{"error", "critical", "fatal"}
	services := []string{"auth-service", "payment-service", "user-service", "notification-service", "api-gateway"}
	messages := []string{
		"Database connection failed",
		"Authentication token expired",
		"Payment processing error",
		"Service unavailable",
		"Memory limit exceeded",
		"Timeout waiting for response",
		"Invalid request parameters",
	}

	return SystemLog{
		ID:        rand.Int63n(1000000) + 1,
		Level:     levels[rand.Intn(len(levels))],
		Service:   services[rand.Intn(len(services))],
		Message:   messages[rand.Intn(len(messages))],
		ErrorCode: rand.Intn(100) + 400, // 400-499 error codes
		Timestamp: time.Now().Add(-time.Duration(rand.Intn(86400*7)) * time.Second),
	}
}

func generateMetric() interface{} {
	names := []string{"cpu_usage", "memory_usage", "disk_usage", "request_latency", "error_rate", "throughput"}
	tags := []string{
		"service=web,region=us-east",
		"service=api,region=us-west",
		"service=db,region=eu-central",
		"service=cache,region=asia-pacific",
	}

	return MetricEntry{
		ID:        rand.Int63n(1000000) + 1,
		Name:      names[rand.Intn(len(names))],
		Value:     rand.Float64() * 100,
		Tags:      tags[rand.Intn(len(tags))],
		Timestamp: time.Now().Add(-time.Duration(rand.Intn(86400*3)) * time.Second),
	}
}

func generateProductView() interface{} {
	categories := []string{"electronics", "books", "clothing", "home", "sports", "automotive"}

	return ProductView{
		ID:        rand.Int63n(1000000) + 1,
		ProductID: rand.Int63n(10000) + 1,
		UserID:    rand.Int63n(5000) + 1,
		Category:  categories[rand.Intn(len(categories))],
		Price:     rand.Float64() * 500,
		ViewCount: rand.Intn(100) + 1,
		Timestamp: time.Now().Add(-time.Duration(rand.Intn(86400*14)) * time.Second),
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

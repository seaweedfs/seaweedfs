package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type UserEvent struct {
	ID            int64     `json:"id"`
	UserID        int64     `json:"user_id"`
	UserType      string    `json:"user_type"`
	Action        string    `json:"action"`
	Status        string    `json:"status"`
	Amount        float64   `json:"amount,omitempty"`
	PreciseAmount string    `json:"precise_amount,omitempty"` // Will be converted to DECIMAL
	BirthDate     time.Time `json:"birth_date"`               // Will be converted to DATE
	Timestamp     time.Time `json:"timestamp"`
	Metadata      string    `json:"metadata,omitempty"`
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
			log.Printf("-Successfully created %s.%s",
				topicConfig.namespace, topicConfig.topic)
		}

		// Small delay between topics
		time.Sleep(2 * time.Second)
	}

	log.Println("-MQ test data creation completed!")
	log.Println("\nCreated namespaces:")
	log.Println("  - analytics (user_events, system_logs, metrics)")
	log.Println("  - ecommerce (product_views, user_events)")
	log.Println("  - logs (application_logs, error_logs)")
	log.Println("\nYou can now test with PostgreSQL clients:")
	log.Println("  psql -h localhost -p 5432 -U seaweedfs -d analytics")
	log.Println("  postgres=> SHOW TABLES;")
	log.Println("  postgres=> SELECT COUNT(*) FROM user_events;")
}

// createSchemaForTopic creates a proper RecordType schema based on topic name
func createSchemaForTopic(topicName string) *schema_pb.RecordType {
	switch topicName {
	case "user_events":
		return &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{Name: "id", FieldIndex: 0, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}}, IsRequired: true},
				{Name: "user_id", FieldIndex: 1, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}}, IsRequired: true},
				{Name: "user_type", FieldIndex: 2, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
				{Name: "action", FieldIndex: 3, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
				{Name: "status", FieldIndex: 4, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
				{Name: "amount", FieldIndex: 5, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_DOUBLE}}, IsRequired: false},
				{Name: "timestamp", FieldIndex: 6, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
				{Name: "metadata", FieldIndex: 7, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: false},
			},
		}
	case "system_logs":
		return &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{Name: "id", FieldIndex: 0, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}}, IsRequired: true},
				{Name: "level", FieldIndex: 1, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
				{Name: "service", FieldIndex: 2, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
				{Name: "message", FieldIndex: 3, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
				{Name: "error_code", FieldIndex: 4, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT32}}, IsRequired: false},
				{Name: "timestamp", FieldIndex: 5, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
			},
		}
	case "metrics":
		return &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{Name: "id", FieldIndex: 0, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}}, IsRequired: true},
				{Name: "name", FieldIndex: 1, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
				{Name: "value", FieldIndex: 2, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_DOUBLE}}, IsRequired: true},
				{Name: "tags", FieldIndex: 3, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
				{Name: "timestamp", FieldIndex: 4, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
			},
		}
	case "product_views":
		return &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{Name: "id", FieldIndex: 0, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}}, IsRequired: true},
				{Name: "product_id", FieldIndex: 1, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}}, IsRequired: true},
				{Name: "user_id", FieldIndex: 2, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}}, IsRequired: true},
				{Name: "category", FieldIndex: 3, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
				{Name: "price", FieldIndex: 4, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_DOUBLE}}, IsRequired: true},
				{Name: "view_count", FieldIndex: 5, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT32}}, IsRequired: true},
				{Name: "timestamp", FieldIndex: 6, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
			},
		}
	case "application_logs", "error_logs":
		return &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{Name: "id", FieldIndex: 0, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT64}}, IsRequired: true},
				{Name: "level", FieldIndex: 1, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
				{Name: "service", FieldIndex: 2, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
				{Name: "message", FieldIndex: 3, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
				{Name: "error_code", FieldIndex: 4, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_INT32}}, IsRequired: false},
				{Name: "timestamp", FieldIndex: 5, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_STRING}}, IsRequired: true},
			},
		}
	default:
		// Default generic schema
		return &schema_pb.RecordType{
			Fields: []*schema_pb.Field{
				{Name: "data", FieldIndex: 0, Type: &schema_pb.Type{Kind: &schema_pb.Type_ScalarType{ScalarType: schema_pb.ScalarType_BYTES}}, IsRequired: true},
			},
		}
	}
}

// convertToDecimal converts a string to decimal format for Parquet logical type
func convertToDecimal(value string) ([]byte, int32, int32) {
	// Parse the decimal string using big.Rat for precision
	rat := new(big.Rat)
	if _, success := rat.SetString(value); !success {
		return nil, 0, 0
	}

	// Convert to a fixed scale (e.g., 4 decimal places)
	scale := int32(4)
	precision := int32(18) // Total digits

	// Scale the rational number to integer representation
	multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	scaled := new(big.Int).Mul(rat.Num(), multiplier)
	scaled.Div(scaled, rat.Denom())

	return scaled.Bytes(), precision, scale
}

// convertToRecordValue converts Go structs to RecordValue format
func convertToRecordValue(data interface{}) (*schema_pb.RecordValue, error) {
	fields := make(map[string]*schema_pb.Value)

	switch v := data.(type) {
	case UserEvent:
		fields["id"] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: v.ID}}
		fields["user_id"] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: v.UserID}}
		fields["user_type"] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: v.UserType}}
		fields["action"] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: v.Action}}
		fields["status"] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: v.Status}}
		fields["amount"] = &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: v.Amount}}

		// Convert precise amount to DECIMAL logical type
		if v.PreciseAmount != "" {
			if decimal, precision, scale := convertToDecimal(v.PreciseAmount); decimal != nil {
				fields["precise_amount"] = &schema_pb.Value{Kind: &schema_pb.Value_DecimalValue{DecimalValue: &schema_pb.DecimalValue{
					Value:     decimal,
					Precision: precision,
					Scale:     scale,
				}}}
			}
		}

		// Convert birth date to DATE logical type
		fields["birth_date"] = &schema_pb.Value{Kind: &schema_pb.Value_DateValue{DateValue: &schema_pb.DateValue{
			DaysSinceEpoch: int32(v.BirthDate.Unix() / 86400), // Convert to days since epoch
		}}}

		fields["timestamp"] = &schema_pb.Value{Kind: &schema_pb.Value_TimestampValue{TimestampValue: &schema_pb.TimestampValue{
			TimestampMicros: v.Timestamp.UnixMicro(),
			IsUtc:           true,
		}}}
		fields["metadata"] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: v.Metadata}}

	case SystemLog:
		fields["id"] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: v.ID}}
		fields["level"] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: v.Level}}
		fields["service"] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: v.Service}}
		fields["message"] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: v.Message}}
		fields["error_code"] = &schema_pb.Value{Kind: &schema_pb.Value_Int32Value{Int32Value: int32(v.ErrorCode)}}
		fields["timestamp"] = &schema_pb.Value{Kind: &schema_pb.Value_TimestampValue{TimestampValue: &schema_pb.TimestampValue{
			TimestampMicros: v.Timestamp.UnixMicro(),
			IsUtc:           true,
		}}}

	case MetricEntry:
		fields["id"] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: v.ID}}
		fields["name"] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: v.Name}}
		fields["value"] = &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: v.Value}}
		fields["tags"] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: v.Tags}}
		fields["timestamp"] = &schema_pb.Value{Kind: &schema_pb.Value_TimestampValue{TimestampValue: &schema_pb.TimestampValue{
			TimestampMicros: v.Timestamp.UnixMicro(),
			IsUtc:           true,
		}}}

	case ProductView:
		fields["id"] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: v.ID}}
		fields["product_id"] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: v.ProductID}}
		fields["user_id"] = &schema_pb.Value{Kind: &schema_pb.Value_Int64Value{Int64Value: v.UserID}}
		fields["category"] = &schema_pb.Value{Kind: &schema_pb.Value_StringValue{StringValue: v.Category}}
		fields["price"] = &schema_pb.Value{Kind: &schema_pb.Value_DoubleValue{DoubleValue: v.Price}}
		fields["view_count"] = &schema_pb.Value{Kind: &schema_pb.Value_Int32Value{Int32Value: int32(v.ViewCount)}}
		fields["timestamp"] = &schema_pb.Value{Kind: &schema_pb.Value_TimestampValue{TimestampValue: &schema_pb.TimestampValue{
			TimestampMicros: v.Timestamp.UnixMicro(),
			IsUtc:           true,
		}}}

	default:
		// Fallback to JSON for unknown types
		jsonData, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal unknown type: %v", err)
		}
		fields["data"] = &schema_pb.Value{Kind: &schema_pb.Value_BytesValue{BytesValue: jsonData}}
	}

	return &schema_pb.RecordValue{Fields: fields}, nil
}

// No need for convertHTTPToGRPC - pb.ServerAddress.ToGrpcAddress() already handles this

// discoverFiler finds a filer from the master server
func discoverFiler(masterHTTPAddress string) (string, error) {
	httpAddr := pb.ServerAddress(masterHTTPAddress)
	masterGRPCAddress := httpAddr.ToGrpcAddress()

	conn, err := grpc.NewClient(masterGRPCAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("failed to connect to master at %s: %v", masterGRPCAddress, err)
	}
	defer conn.Close()

	client := master_pb.NewSeaweedClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ListClusterNodes(ctx, &master_pb.ListClusterNodesRequest{
		ClientType: cluster.FilerType,
	})
	if err != nil {
		return "", fmt.Errorf("failed to list filers from master: %v", err)
	}

	if len(resp.ClusterNodes) == 0 {
		return "", fmt.Errorf("no filers found in cluster")
	}

	// Use the first available filer and convert HTTP address to gRPC
	filerHTTPAddress := resp.ClusterNodes[0].Address
	httpAddr := pb.ServerAddress(filerHTTPAddress)
	return httpAddr.ToGrpcAddress(), nil
}

// discoverBroker finds the broker balancer using filer lock mechanism
func discoverBroker(masterHTTPAddress string) (string, error) {
	// First discover filer from master
	filerAddress, err := discoverFiler(masterHTTPAddress)
	if err != nil {
		return "", fmt.Errorf("failed to discover filer: %v", err)
	}

	conn, err := grpc.NewClient(filerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return "", fmt.Errorf("failed to connect to filer at %s: %v", filerAddress, err)
	}
	defer conn.Close()

	client := filer_pb.NewSeaweedFilerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.FindLockOwner(ctx, &filer_pb.FindLockOwnerRequest{
		Name: pub_balancer.LockBrokerBalancer,
	})
	if err != nil {
		return "", fmt.Errorf("failed to find broker balancer: %v", err)
	}

	return resp.Owner, nil
}

func createTopicData(masterAddr, filerAddr, namespace, topicName string,
	generator func() interface{}, count int) error {

	// Create schema based on topic type
	recordType := createSchemaForTopic(topicName)

	// Dynamically discover broker address instead of hardcoded port replacement
	brokerAddress, err := discoverBroker(masterAddr)
	if err != nil {
		// Fallback to hardcoded port replacement if discovery fails
		log.Printf("Warning: Failed to discover broker dynamically (%v), using hardcoded port replacement", err)
		brokerAddress = strings.Replace(masterAddr, ":9333", ":17777", 1)
	}

	// Create publisher configuration
	config := &pub_client.PublisherConfiguration{
		Topic:          topic.NewTopic(namespace, topicName),
		PartitionCount: 1,
		Brokers:        []string{brokerAddress}, // Use dynamically discovered broker address
		PublisherName:  fmt.Sprintf("test-producer-%s-%s", namespace, topicName),
		RecordType:     recordType, // Use structured schema
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

		// Convert struct to RecordValue
		recordValue, err := convertToRecordValue(data)
		if err != nil {
			log.Printf("Error converting data to RecordValue: %v", err)
			continue
		}

		// Publish structured record
		err = publisher.PublishRecord([]byte(fmt.Sprintf("key-%d", i)), recordValue)
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

	// Generate a birth date between 1970 and 2005 (18+ years old)
	birthYear := 1970 + rand.Intn(35)
	birthMonth := 1 + rand.Intn(12)
	birthDay := 1 + rand.Intn(28) // Keep it simple, avoid month-specific day issues
	birthDate := time.Date(birthYear, time.Month(birthMonth), birthDay, 0, 0, 0, 0, time.UTC)

	// Generate a precise amount as a string with 4 decimal places
	preciseAmount := fmt.Sprintf("%.4f", rand.Float64()*10000)

	return UserEvent{
		ID:            rand.Int63n(1000000) + 1,
		UserID:        rand.Int63n(10000) + 1,
		UserType:      userTypes[rand.Intn(len(userTypes))],
		Action:        actions[rand.Intn(len(actions))],
		Status:        statuses[rand.Intn(len(statuses))],
		Amount:        rand.Float64() * 1000,
		PreciseAmount: preciseAmount,
		BirthDate:     birthDate,
		Timestamp:     time.Now().Add(-time.Duration(rand.Intn(86400*30)) * time.Second),
		Metadata:      fmt.Sprintf("{\"session_id\":\"%d\"}", rand.Int63n(100000)),
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

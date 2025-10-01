#!/bin/bash
set -e

echo "Testing simple produce..."

# Use kafka-go library test
cat > /tmp/simple_produce.go << 'GOEOF'
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Create topic first
	conn, err := kafka.Dial("tcp", "kafka-gateway:9093")
	if err != nil {
		log.Fatal("Failed to dial:", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		log.Fatal("Failed to get controller:", err)
	}

	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		log.Fatal("Failed to dial controller:", err)
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             "simple-test-topic",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		log.Printf("Create topic (may already exist): %v", err)
	}

	// Wait for topic creation
	time.Sleep(2 * time.Second)

	// Producer
	w := &kafka.Writer{
		Addr:     kafka.TCP("kafka-gateway:9093"),
		Topic:    "simple-test-topic",
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("message-%d", i)),
		}
		
		err := w.WriteMessages(ctx, msg)
		if err != nil {
			log.Fatalf("Failed to write message %d: %v", i, err)
		}
		fmt.Printf("✅ Produced message %d\n", i)
	}

	fmt.Println("✅ All messages produced successfully!")
}
GOEOF

# Run the test
cd /tmp
go mod init simple_produce || true
go get github.com/segmentio/kafka-go@latest
docker run --rm --network kafka-client-loadtest \
  -v /tmp/simple_produce.go:/app/main.go \
  -v $GOPATH/pkg/mod:/go/pkg/mod \
  -w /app \
  golang:1.21-alpine \
  sh -c "apk add --no-cache git && go mod init test && go get github.com/segmentio/kafka-go@latest && go run main.go"

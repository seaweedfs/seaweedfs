#!/bin/bash
set -e

echo "=== Quick-Test Produce Verification ==="
echo ""

# Start services
echo "1. Starting services..."
docker compose up -d seaweedfs-master seaweedfs-volume seaweedfs-filer seaweedfs-mq-broker kafka-gateway
sleep 25

# Check service status
echo ""
echo "2. Checking service status..."
docker stats --no-stream | grep -E "(CONTAINER|master|volume|filer|broker|gateway)"

# Simple produce test using kafka-console-producer equivalent
echo ""
echo "3. Testing message production..."

# Create a simple Go producer
cat > /tmp/test_produce.go << 'GOEOF'
package main

import (
	"context"
	"fmt"
	"log"
	"time"
	
	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "test-topic"
	
	// Create topic
	conn, err := kafka.Dial("tcp", "localhost:9093")
	if err != nil {
		log.Fatal(err)
	}
	
	controller, _ := conn.Controller()
	conn.Close()
	
	controllerConn, _ := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	_ = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	controllerConn.Close()
	
	time.Sleep(2 * time.Second)
	
	// Produce messages
	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9093"),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	success := 0
	for i := 0; i < 10; i++ {
		err := w.WriteMessages(ctx, kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("Hello World %d", i)),
		})
		if err != nil {
			log.Printf("❌ Failed to produce message %d: %v", i, err)
		} else {
			success++
			fmt.Printf("✅ Produced message %d\n", i)
		}
	}
	
	fmt.Printf("\n=== RESULT: %d/10 messages produced successfully ===\n", success)
	if success == 10 {
		fmt.Println("✅ TEST PASSED")
	} else {
		fmt.Println("❌ TEST FAILED")
		log.Fatal("Not all messages produced")
	}
}
GOEOF

# Build and run
cd /tmp
go mod init test 2>/dev/null || true
go get github.com/segmentio/kafka-go@v0.4.47 2>/dev/null || true
go run test_produce.go

echo ""
echo "4. Checking broker logs for errors..."
docker logs loadtest-seaweedfs-mq-broker 2>&1 | tail -20 | grep -E "(ERROR|error|panic)" || echo "No errors found"

echo ""
echo "=== Verification Complete ==="

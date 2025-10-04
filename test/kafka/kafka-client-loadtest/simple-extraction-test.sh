#!/bin/bash

echo "=== Simple Record Extraction Test ==="
echo "Testing without Schema Registry..."
echo ""

# Stop Schema Registry to avoid noise
docker stop loadtest-schema-registry 2>/dev/null || true

# Create a simple Go test
cat > extraction_test.go << 'GOEOF'
package main

import (
    "context"
    "fmt"
    "log"
    "time"
    
    "github.com/segmentio/kafka-go"
)

func main() {
    topic := "extraction-validation"
    brokers := []string{"kafka-gateway:9092"}
    
    // Create topic
    conn, err := kafka.Dial("tcp", brokers[0])
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    
    controller, _ := conn.Controller()
    conn.Close()
    
    ctrlConn, err := kafka.Dial("tcp", controller.Host)
    if err != nil {
        log.Fatalf("Failed to connect to controller: %v", err)
    }
    defer ctrlConn.Close()
    
    err = ctrlConn.CreateTopics(kafka.TopicConfig{
        Topic:             topic,
        NumPartitions:     1,
        ReplicationFactor: 1,
    })
    if err != nil {
        fmt.Printf("Topic creation: %v\n", err)
    }
    
    time.Sleep(3 * time.Second)
    
    // Produce message
    writer := &kafka.Writer{
        Addr:     kafka.TCP(brokers[0]),
        Topic:    topic,
        Balancer: &kafka.LeastBytes{},
    }
    defer writer.Close()
    
    testData := `{"status":"FIXED","bug":"varint extraction","timestamp":"` + time.Now().Format(time.RFC3339) + `"}`
    
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    
    err = writer.WriteMessages(ctx, kafka.Message{
        Key:   []byte("extraction-test-key"),
        Value: []byte(testData),
    })
    
    if err != nil {
        log.Fatalf("Write failed: %v", err)
    }
    
    fmt.Printf("✅ Message produced: %s\n", testData)
    time.Sleep(2 * time.Second)
    
    // Consume message
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers: brokers,
        Topic:   topic,
        GroupID: "extraction-test-group",
    })
    defer reader.Close()
    
    readCtx, readCancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer readCancel()
    
    msg, err := reader.ReadMessage(readCtx)
    if err != nil {
        log.Fatalf("Read failed: %v", err)
    }
    
    fmt.Printf("✅ Message consumed successfully!\n")
    fmt.Printf("   Key: %s\n", string(msg.Key))
    fmt.Printf("   Value: %s\n", string(msg.Value))
    fmt.Printf("   Offset: %d\n", msg.Offset)
    
    if string(msg.Value) == testData {
        fmt.Printf("\n🎉 SUCCESS! Record extraction is WORKING!\n")
        fmt.Printf("   The varint bug fix is validated!\n")
    } else {
        fmt.Printf("\n❌ FAILURE: Data mismatch\n")
        fmt.Printf("   Expected: %s\n", testData)
        fmt.Printf("   Got: %s\n", string(msg.Value))
    }
}
GOEOF

# Run test in Docker
docker run --rm --network kafka-client-loadtest \
  -v $(pwd):/app -w /app \
  golang:1.23-alpine sh -c '
    apk add --no-cache git 2>/dev/null
    go mod init extraction-test 2>/dev/null || true
    go get github.com/segmentio/kafka-go
    go run extraction_test.go
  '

echo ""
echo "=== Test Complete ==="

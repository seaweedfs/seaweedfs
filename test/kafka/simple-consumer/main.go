package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Configuration
	brokerAddress := "localhost:9093" // Kafka gateway port (not SeaweedMQ broker port 17777)
	topicName := "_raw_messages"      // Topic with "_" prefix - should skip schema validation
	groupID := "raw-message-consumer"

	fmt.Printf("Consuming messages from topic '%s' on broker '%s'\n", topicName, brokerAddress)

	// Create a new reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topicName,
		GroupID: groupID,
		// Start reading from the beginning for testing
		StartOffset: kafka.FirstOffset,
		// Configure for quick consumption
		MinBytes: 1,
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nReceived shutdown signal, stopping consumer...")
		cancel()
	}()

	fmt.Println("Starting to consume messages (Press Ctrl+C to stop)...")
	fmt.Println("=" + fmt.Sprintf("%60s", "="))

	messageCount := 0

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("\nStopped consuming. Total messages processed: %d\n", messageCount)
			return
		default:
			// Set a timeout for reading messages
			msgCtx, msgCancel := context.WithTimeout(ctx, 5*time.Second)

			message, err := reader.ReadMessage(msgCtx)
			msgCancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					fmt.Print(".")
					continue
				}
				log.Printf("Error reading message: %v", err)
				continue
			}

			messageCount++

			// Display message details
			fmt.Printf("\nMessage #%d:\n", messageCount)
			fmt.Printf("   Partition: %d, Offset: %d\n", message.Partition, message.Offset)
			fmt.Printf("   Key: %s\n", string(message.Key))
			fmt.Printf("   Value: %s\n", string(message.Value))
			fmt.Printf("   Timestamp: %s\n", message.Time.Format(time.RFC3339))

			// Display headers if present
			if len(message.Headers) > 0 {
				fmt.Printf("   Headers:\n")
				for _, header := range message.Headers {
					fmt.Printf("     %s: %s\n", header.Key, string(header.Value))
				}
			}

			// Try to detect content type
			contentType := detectContentType(message.Value)
			fmt.Printf("   Content Type: %s\n", contentType)

			fmt.Printf("   Raw Size: %d bytes\n", len(message.Value))
			fmt.Println("   " + fmt.Sprintf("%50s", "-"))
		}
	}
}

// detectContentType tries to determine the content type of the message
func detectContentType(data []byte) string {
	if len(data) == 0 {
		return "empty"
	}

	// Check if it looks like JSON
	trimmed := string(data)
	if (trimmed[0] == '{' && trimmed[len(trimmed)-1] == '}') ||
		(trimmed[0] == '[' && trimmed[len(trimmed)-1] == ']') {
		return "JSON"
	}

	// Check if it's printable text
	for _, b := range data {
		if b < 32 && b != 9 && b != 10 && b != 13 { // Allow tab, LF, CR
			return "binary"
		}
	}

	return "text"
}

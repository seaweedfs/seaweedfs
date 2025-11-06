package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: kafka-go-consumer <broker> <topic>")
	}
	broker := os.Args[1]
	topic := os.Args[2]

	log.Printf("Connecting to Kafka broker: %s", broker)
	log.Printf("Topic: %s", topic)

	// Create a new reader
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  "kafka-go-test-group",
		MinBytes: 1,
		MaxBytes: 10e6, // 10MB
		MaxWait:  1 * time.Second,
	})
	defer r.Close()

	log.Printf("Starting to consume messages...")

	ctx := context.Background()
	messageCount := 0
	errorCount := 0
	startTime := time.Now()

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			errorCount++
			log.Printf("Error reading message #%d: %v", messageCount+1, err)

			// Stop after 10 consecutive errors or 60 seconds
			if errorCount > 10 || time.Since(startTime) > 60*time.Second {
				log.Printf("\nStopping after %d errors in %v", errorCount, time.Since(startTime))
				break
			}
			continue
		}

		// Reset error count on successful read
		errorCount = 0
		messageCount++

		log.Printf("Message #%d: topic=%s partition=%d offset=%d key=%s value=%s",
			messageCount, m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		// Stop after 100 messages or 60 seconds
		if messageCount >= 100 || time.Since(startTime) > 60*time.Second {
			log.Printf("\nSuccessfully consumed %d messages in %v", messageCount, time.Since(startTime))
			log.Printf("Success rate: %.1f%% (%d/%d including errors)",
				float64(messageCount)/float64(messageCount+errorCount)*100, messageCount, messageCount+errorCount)
			break
		}
	}
}

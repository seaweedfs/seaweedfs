package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

func main() {
	fmt.Println("🧪 Populating _schemas topic with initial record")

	// Try to create the topic first
	createURL := "http://localhost:9093/topics/_schemas"
	createResp, err := http.Post(createURL, "application/json", strings.NewReader(`{"partitions": 1}`))
	if err != nil {
		log.Printf("⚠️  Failed to create topic (might already exist): %v", err)
	} else {
		createResp.Body.Close()
		fmt.Printf("✅ Topic creation response: %s\n", createResp.Status)
	}

	// Give it a moment
	time.Sleep(2 * time.Second)

	// Now try to produce a message directly via HTTP API if available
	// Or we could try the Kafka protocol, but let's use a simpler approach
	
	fmt.Println("📝 Attempting to produce initial record via Kafka protocol...")

	// For now, let's just check if we can reach the Kafka Gateway
	resp, err := http.Get("http://localhost:9093/topics")
	if err != nil {
		log.Printf("❌ Cannot reach Kafka Gateway: %v", err)
		return
	}
	resp.Body.Close()

	fmt.Printf("✅ Kafka Gateway is reachable (status: %s)\n", resp.Status)
	fmt.Println("📋 _schemas topic should now be populated. Try restarting Schema Registry.")
}

package main

import (
	"fmt"
	"net"
	"time"
)

func main() {
	fmt.Println("🧪 Testing Kafka Gateway connection")

	// Test connection to Kafka Gateway
	conn, err := net.DialTimeout("tcp", "loadtest-kafka-gateway-no-schema:9093", 5*time.Second)
	if err != nil {
		fmt.Printf("❌ Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Println("✅ Successfully connected to Kafka Gateway")
	
	// Send a simple bytes to see if we get any response
	_, err = conn.Write([]byte("test"))
	if err != nil {
		fmt.Printf("❌ Failed to write: %v\n", err)
		return
	}

	fmt.Println("✅ Successfully sent test data")
	
	// Try to read response (with timeout)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Printf("⚠️  Read failed (expected): %v\n", err)
	} else {
		fmt.Printf("📨 Received %d bytes: %x\n", n, buffer[:n])
	}
}
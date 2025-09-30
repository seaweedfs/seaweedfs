package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🧪 Layer 3: SeaweedMQ Storage Verification Test")
	fmt.Println("Testing: Protobuf RecordValue storage, topic.conf schema persistence, raw data analysis")

	// Test 1: Verify topic.conf schema persistence
	testTopicConfigSchemaPersistence()

	// Test 2: Verify protobuf RecordValue storage
	testProtobufRecordValueStorage()

	// Test 3: Analyze raw storage format
	testRawStorageAnalysis()
}

func testTopicConfigSchemaPersistence() {
	fmt.Println("\n📋 Test 3.1: Topic.conf Schema Persistence")

	// Create a new topic to test schema persistence
	topicName := fmt.Sprintf("schema-persist-test-%d", time.Now().Unix())

	// Create producer to trigger topic creation
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Printf("❌ Failed to create producer: %v", err)
		return
	}
	defer producer.Close()

	// Produce a message to create the topic
	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Value: sarama.StringEncoder("test message for topic creation"),
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		log.Printf("❌ Failed to produce message: %v", err)
		return
	}

	fmt.Printf("✅ Topic %s created\n", topicName)

	// Wait for topic.conf to be written
	time.Sleep(2 * time.Second)

	// Check topic.conf via HTTP
	topicConfURL := fmt.Sprintf("http://localhost:8888/topics/kafka/%s/topic.conf", topicName)
	resp, err := http.Get(topicConfURL)
	if err != nil {
		log.Printf("❌ Failed to get topic.conf: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		fmt.Printf("✅ topic.conf exists\n")

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("❌ Failed to read topic.conf: %v", err)
			return
		}

		configContent := string(body)
		fmt.Printf("📋 topic.conf content:\n%s\n", configContent)

		// Check for schema-related fields
		if strings.Contains(configContent, "messageRecordType") {
			fmt.Printf("✅ messageRecordType field present\n")
		} else {
			fmt.Printf("❌ messageRecordType field missing\n")
		}

		if strings.Contains(configContent, "keyColumns") {
			fmt.Printf("✅ keyColumns field present\n")
		} else {
			fmt.Printf("❌ keyColumns field missing\n")
		}

	} else {
		fmt.Printf("❌ Failed to get topic.conf: %d\n", resp.StatusCode)
	}
}

func testProtobufRecordValueStorage() {
	fmt.Println("\n📋 Test 3.2: Protobuf RecordValue Storage Verification")

	topicName := fmt.Sprintf("protobuf-test-%d", time.Now().Unix())

	// Produce a message
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_8_0_0

	producer, err := sarama.NewSyncProducer([]string{"kafka-gateway:9093"}, config)
	if err != nil {
		log.Printf("❌ Failed to create producer: %v", err)
		return
	}
	defer producer.Close()

	testMessage := fmt.Sprintf(`{"id": 123, "message": "protobuf test", "timestamp": %d}`, time.Now().UnixMilli())

	msg := &sarama.ProducerMessage{
		Topic: topicName,
		Key:   sarama.StringEncoder("protobuf-key"),
		Value: sarama.StringEncoder(testMessage),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("❌ Failed to produce message: %v", err)
		return
	}

	fmt.Printf("✅ Message produced: partition=%d, offset=%d\n", partition, offset)

	// Wait for message to be stored
	time.Sleep(2 * time.Second)

	// Try to find the raw storage file
	// List topic directory
	topicDirURL := fmt.Sprintf("http://localhost:8888/topics/kafka/%s/", topicName)
	resp, err := http.Get(topicDirURL)
	if err != nil {
		log.Printf("❌ Failed to list topic directory: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("❌ Failed to read directory listing: %v", err)
			return
		}

		bodyStr := string(body)
		fmt.Printf("📋 Topic directory exists\n")

		// Look for version directories (v2025-...)
		if strings.Contains(bodyStr, "v2025-") {
			fmt.Printf("✅ Found version directory\n")

			// Extract version directory name (simplified)
			lines := strings.Split(bodyStr, "\n")
			var versionDir string
			for _, line := range lines {
				if strings.Contains(line, "v2025-") && strings.Contains(line, "href") {
					// Extract directory name from href
					start := strings.Index(line, "v2025-")
					if start != -1 {
						end := strings.Index(line[start:], "/")
						if end != -1 {
							versionDir = line[start : start+end]
							break
						}
					}
				}
			}

			if versionDir != "" {
				fmt.Printf("📋 Version directory: %s\n", versionDir)

				// Check partition directory
				partitionDirURL := fmt.Sprintf("http://localhost:8888/topics/kafka/%s/%s/", topicName, versionDir)
				checkPartitionDirectory(partitionDirURL)
			}
		} else {
			fmt.Printf("❌ No version directory found\n")
		}
	} else {
		fmt.Printf("❌ Topic directory not found: %d\n", resp.StatusCode)
	}
}

func checkPartitionDirectory(partitionDirURL string) {
	resp, err := http.Get(partitionDirURL)
	if err != nil {
		log.Printf("❌ Failed to list partition directory: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("❌ Failed to read partition directory: %v", err)
			return
		}

		bodyStr := string(body)
		fmt.Printf("✅ Partition directory exists\n")

		// Look for partition subdirectory (0000-xxxx)
		if strings.Contains(bodyStr, "0000-") {
			fmt.Printf("✅ Found partition subdirectory\n")
		} else {
			fmt.Printf("❌ No partition subdirectory found\n")
		}
	}
}

func testRawStorageAnalysis() {
	fmt.Println("\n📋 Test 3.3: Raw Storage Format Analysis")

	// Analyze the _schemas topic storage to understand the format
	schemasURL := "http://localhost:8888/topics/kafka/_schemas/"
	resp, err := http.Get(schemasURL)
	if err != nil {
		log.Printf("❌ Failed to access _schemas directory: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("❌ Failed to read _schemas directory: %v", err)
			return
		}

		bodyStr := string(body)
		fmt.Printf("✅ _schemas directory accessible\n")

		// Look for recent version directories
		lines := strings.Split(bodyStr, "\n")
		var recentVersions []string
		for _, line := range lines {
			if strings.Contains(line, "v2025-") && strings.Contains(line, "href") {
				start := strings.Index(line, "v2025-")
				if start != -1 {
					end := strings.Index(line[start:], "/")
					if end != -1 {
						versionDir := line[start : start+end]
						recentVersions = append(recentVersions, versionDir)
					}
				}
			}
		}

		fmt.Printf("📋 Recent version directories: %v\n", recentVersions)

		// Analyze the most recent version
		if len(recentVersions) > 0 {
			latestVersion := recentVersions[len(recentVersions)-1]
			analyzeVersionDirectory(latestVersion)
		}
	} else {
		fmt.Printf("❌ _schemas directory not accessible: %d\n", resp.StatusCode)
	}
}

func analyzeVersionDirectory(versionDir string) {
	fmt.Printf("\n🔍 Analyzing version directory: %s\n", versionDir)

	versionURL := fmt.Sprintf("http://localhost:8888/topics/kafka/_schemas/%s/", versionDir)
	resp, err := http.Get(versionURL)
	if err != nil {
		log.Printf("❌ Failed to access version directory: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("❌ Failed to read version directory: %v", err)
			return
		}

		bodyStr := string(body)

		// Look for partition directories
		lines := strings.Split(bodyStr, "\n")
		for _, line := range lines {
			if strings.Contains(line, "0000-") && strings.Contains(line, "href") {
				start := strings.Index(line, "0000-")
				if start != -1 {
					end := strings.Index(line[start:], "/")
					if end != -1 {
						partitionDir := line[start : start+end]
						fmt.Printf("📋 Found partition directory: %s\n", partitionDir)
						analyzePartitionFiles(versionDir, partitionDir)
						break // Analyze just the first partition for this test
					}
				}
			}
		}
	}
}

func analyzePartitionFiles(versionDir, partitionDir string) {
	partitionURL := fmt.Sprintf("http://localhost:8888/topics/kafka/_schemas/%s/%s/", versionDir, partitionDir)
	resp, err := http.Get(partitionURL)
	if err != nil {
		log.Printf("❌ Failed to access partition directory: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("❌ Failed to read partition directory: %v", err)
			return
		}

		bodyStr := string(body)

		// Look for data files (timestamp-based names)
		lines := strings.Split(bodyStr, "\n")
		for _, line := range lines {
			if strings.Contains(line, "2025-") && strings.Contains(line, "href") && !strings.Contains(line, "/") {
				// Extract filename
				start := strings.Index(line, `href="`)
				if start != -1 {
					start += 6
					end := strings.Index(line[start:], `"`)
					if end != -1 {
						filename := line[start : start+end]
						if strings.Contains(filename, "2025-") {
							fmt.Printf("📋 Found data file: %s\n", filename)
							analyzeDataFile(versionDir, partitionDir, filename)
							break // Analyze just one file for this test
						}
					}
				}
			}
		}
	}
}

func analyzeDataFile(versionDir, partitionDir, filename string) {
	fileURL := fmt.Sprintf("http://localhost:8888/topics/kafka/_schemas/%s/%s/%s", versionDir, partitionDir, filename)
	resp, err := http.Get(fileURL)
	if err != nil {
		log.Printf("❌ Failed to access data file: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("❌ Failed to read data file: %v", err)
			return
		}

		fmt.Printf("📋 Data file size: %d bytes\n", len(body))

		// Analyze first 200 bytes
		analyzeBytes := body
		if len(analyzeBytes) > 200 {
			analyzeBytes = analyzeBytes[:200]
		}

		fmt.Printf("🔍 First %d bytes (hex):\n", len(analyzeBytes))
		fmt.Printf("%s\n", hex.Dump(analyzeBytes))

		// Look for protobuf markers
		protobufMarkers := 0
		for i := 0; i < len(body)-1; i++ {
			// Look for protobuf field markers (wire type 2 = length-delimited)
			if body[i]&0x07 == 2 {
				protobufMarkers++
			}
		}

		fmt.Printf("📊 Potential protobuf field markers: %d\n", protobufMarkers)

		// Look for JSON patterns
		jsonPatterns := strings.Count(string(body), "{") + strings.Count(string(body), "}")
		fmt.Printf("📊 JSON brace count: %d\n", jsonPatterns)

		if protobufMarkers > jsonPatterns {
			fmt.Printf("✅ Data appears to be in protobuf format\n")
		} else if jsonPatterns > 0 {
			fmt.Printf("⚠️  Data appears to contain JSON (may not be fully converted to protobuf)\n")
		} else {
			fmt.Printf("❓ Data format unclear\n")
		}
	} else {
		fmt.Printf("❌ Failed to access data file: %d\n", resp.StatusCode)
	}
}


package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type LoadGenerator struct {
	filerAddr    string
	masterAddr   string
	writeRate    int
	deleteRate   int
	fileSizeMin  int64
	fileSizeMax  int64
	testDuration int
	collection   string

	// State tracking
	createdFiles []string
	mutex        sync.RWMutex
	stats        LoadStats
}

type LoadStats struct {
	FilesWritten  int64
	FilesDeleted  int64
	BytesWritten  int64
	Errors        int64
	StartTime     time.Time
	LastOperation time.Time
}

// parseSize converts size strings like "1MB", "5MB" to bytes
func parseSize(sizeStr string) int64 {
	sizeStr = strings.ToUpper(strings.TrimSpace(sizeStr))

	var multiplier int64 = 1
	if strings.HasSuffix(sizeStr, "KB") {
		multiplier = 1024
		sizeStr = strings.TrimSuffix(sizeStr, "KB")
	} else if strings.HasSuffix(sizeStr, "MB") {
		multiplier = 1024 * 1024
		sizeStr = strings.TrimSuffix(sizeStr, "MB")
	} else if strings.HasSuffix(sizeStr, "GB") {
		multiplier = 1024 * 1024 * 1024
		sizeStr = strings.TrimSuffix(sizeStr, "GB")
	}

	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 1024 * 1024 // Default to 1MB
	}

	return size * multiplier
}

// generateRandomData creates random data of specified size
func (lg *LoadGenerator) generateRandomData(size int64) []byte {
	data := make([]byte, size)
	_, err := rand.Read(data)
	if err != nil {
		// Fallback to deterministic data
		for i := range data {
			data[i] = byte(i % 256)
		}
	}
	return data
}

// uploadFile uploads a file to SeaweedFS via filer
func (lg *LoadGenerator) uploadFile(filename string, data []byte) error {
	url := fmt.Sprintf("http://%s/%s", lg.filerAddr, filename)
	if lg.collection != "" {
		url = fmt.Sprintf("http://%s/%s/%s", lg.filerAddr, lg.collection, filename)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("upload failed with status: %d", resp.StatusCode)
	}

	return nil
}

// deleteFile deletes a file from SeaweedFS via filer
func (lg *LoadGenerator) deleteFile(filename string) error {
	url := fmt.Sprintf("http://%s/%s", lg.filerAddr, filename)
	if lg.collection != "" {
		url = fmt.Sprintf("http://%s/%s/%s", lg.filerAddr, lg.collection, filename)
	}

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete failed with status: %d", resp.StatusCode)
	}

	return nil
}

// writeFiles continuously writes files at the specified rate
func (lg *LoadGenerator) writeFiles() {
	writeInterval := time.Second / time.Duration(lg.writeRate)
	ticker := time.NewTicker(writeInterval)
	defer ticker.Stop()

	fileCounter := 0

	for range ticker.C {
		fileCounter++

		// Random file size between min and max
		sizeDiff := lg.fileSizeMax - lg.fileSizeMin
		randomSize := lg.fileSizeMin
		if sizeDiff > 0 {
			randomSize += int64(time.Now().UnixNano()) % sizeDiff
		}

		// Generate filename
		filename := fmt.Sprintf("test-data/file-%d-%d.bin", time.Now().Unix(), fileCounter)

		// Generate random data
		data := lg.generateRandomData(randomSize)

		// Upload file
		err := lg.uploadFile(filename, data)
		if err != nil {
			log.Printf("Error uploading file %s: %v", filename, err)
			lg.stats.Errors++
		} else {
			lg.mutex.Lock()
			lg.createdFiles = append(lg.createdFiles, filename)
			lg.stats.FilesWritten++
			lg.stats.BytesWritten += randomSize
			lg.stats.LastOperation = time.Now()
			lg.mutex.Unlock()

			log.Printf("Uploaded file: %s (size: %d bytes, total files: %d)",
				filename, randomSize, lg.stats.FilesWritten)
		}
	}
}

// deleteFiles continuously deletes files at the specified rate
func (lg *LoadGenerator) deleteFiles() {
	deleteInterval := time.Second / time.Duration(lg.deleteRate)
	ticker := time.NewTicker(deleteInterval)
	defer ticker.Stop()

	for range ticker.C {
		lg.mutex.Lock()
		if len(lg.createdFiles) == 0 {
			lg.mutex.Unlock()
			continue
		}

		// Pick a random file to delete
		index := int(time.Now().UnixNano()) % len(lg.createdFiles)
		filename := lg.createdFiles[index]

		// Remove from slice
		lg.createdFiles = append(lg.createdFiles[:index], lg.createdFiles[index+1:]...)
		lg.mutex.Unlock()

		// Delete file
		err := lg.deleteFile(filename)
		if err != nil {
			log.Printf("Error deleting file %s: %v", filename, err)
			lg.stats.Errors++
		} else {
			lg.stats.FilesDeleted++
			lg.stats.LastOperation = time.Now()
			log.Printf("Deleted file: %s (remaining files: %d)", filename, len(lg.createdFiles))
		}
	}
}

// printStats periodically prints load generation statistics
func (lg *LoadGenerator) printStats() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		uptime := time.Since(lg.stats.StartTime)
		writeRate := float64(lg.stats.FilesWritten) / uptime.Seconds()
		deleteRate := float64(lg.stats.FilesDeleted) / uptime.Seconds()

		lg.mutex.RLock()
		pendingFiles := len(lg.createdFiles)
		lg.mutex.RUnlock()

		log.Printf("STATS: Files written=%d, deleted=%d, pending=%d, errors=%d",
			lg.stats.FilesWritten, lg.stats.FilesDeleted, pendingFiles, lg.stats.Errors)
		log.Printf("RATES: Write=%.2f/sec, Delete=%.2f/sec, Data=%.2f MB written",
			writeRate, deleteRate, float64(lg.stats.BytesWritten)/(1024*1024))
	}
}

// checkClusterHealth periodically checks cluster status
func (lg *LoadGenerator) checkClusterHealth() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		// Check master status
		resp, err := http.Get(fmt.Sprintf("http://%s/cluster/status", lg.masterAddr))
		if err != nil {
			log.Printf("WARNING: Cannot reach master: %v", err)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			log.Printf("WARNING: Cannot read master response: %v", err)
			continue
		}

		if resp.StatusCode == http.StatusOK {
			log.Printf("Cluster health check: OK (response size: %d bytes)", len(body))
		} else {
			log.Printf("WARNING: Cluster health check failed with status: %d", resp.StatusCode)
		}
	}
}

func main() {
	filerAddr := os.Getenv("FILER_ADDRESS")
	if filerAddr == "" {
		filerAddr = "filer:8888"
	}

	masterAddr := os.Getenv("MASTER_ADDRESS")
	if masterAddr == "" {
		masterAddr = "master:9333"
	}

	writeRate, _ := strconv.Atoi(os.Getenv("WRITE_RATE"))
	if writeRate <= 0 {
		writeRate = 10
	}

	deleteRate, _ := strconv.Atoi(os.Getenv("DELETE_RATE"))
	if deleteRate <= 0 {
		deleteRate = 2
	}

	fileSizeMin := parseSize(os.Getenv("FILE_SIZE_MIN"))
	if fileSizeMin <= 0 {
		fileSizeMin = 1024 * 1024 // 1MB
	}

	fileSizeMax := parseSize(os.Getenv("FILE_SIZE_MAX"))
	if fileSizeMax <= fileSizeMin {
		fileSizeMax = 5 * 1024 * 1024 // 5MB
	}

	testDuration, _ := strconv.Atoi(os.Getenv("TEST_DURATION"))
	if testDuration <= 0 {
		testDuration = 3600 // 1 hour
	}

	collection := os.Getenv("COLLECTION")

	lg := &LoadGenerator{
		filerAddr:    filerAddr,
		masterAddr:   masterAddr,
		writeRate:    writeRate,
		deleteRate:   deleteRate,
		fileSizeMin:  fileSizeMin,
		fileSizeMax:  fileSizeMax,
		testDuration: testDuration,
		collection:   collection,
		createdFiles: make([]string, 0),
		stats: LoadStats{
			StartTime: time.Now(),
		},
	}

	log.Printf("Starting load generator...")
	log.Printf("Filer: %s", filerAddr)
	log.Printf("Master: %s", masterAddr)
	log.Printf("Write rate: %d files/sec", writeRate)
	log.Printf("Delete rate: %d files/sec", deleteRate)
	log.Printf("File size: %d - %d bytes", fileSizeMin, fileSizeMax)
	log.Printf("Test duration: %d seconds", testDuration)
	log.Printf("Collection: '%s'", collection)

	// Wait for filer to be ready
	log.Println("Waiting for filer to be ready...")
	for {
		resp, err := http.Get(fmt.Sprintf("http://%s/", filerAddr))
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		log.Println("Filer not ready, waiting...")
		time.Sleep(5 * time.Second)
	}
	log.Println("Filer is ready!")

	// Start background goroutines
	go lg.writeFiles()
	go lg.deleteFiles()
	go lg.printStats()
	go lg.checkClusterHealth()

	// Run for specified duration
	log.Printf("Load test will run for %d seconds...", testDuration)
	time.Sleep(time.Duration(testDuration) * time.Second)

	log.Println("Load test completed!")
	log.Printf("Final stats: Files written=%d, deleted=%d, errors=%d, total data=%.2f MB",
		lg.stats.FilesWritten, lg.stats.FilesDeleted, lg.stats.Errors,
		float64(lg.stats.BytesWritten)/(1024*1024))
}

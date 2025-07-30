package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

var (
	master      = flag.String("master", "master:9333", "SeaweedFS master server address")
	fileCount   = flag.Int("files", 20, "Number of files to create")
	deleteRatio = flag.Float64("delete", 0.4, "Ratio of files to delete (0.0-1.0)")
	fileSizeKB  = flag.Int("size", 100, "Size of each file in KB")
)

type AssignResult struct {
	Fid       string `json:"fid"`
	Url       string `json:"url"`
	PublicUrl string `json:"publicUrl"`
	Count     int    `json:"count"`
	Error     string `json:"error"`
}

func main() {
	flag.Parse()

	fmt.Println("🧪 Creating fake data for vacuum task testing...")
	fmt.Printf("Master: %s\n", *master)
	fmt.Printf("Files to create: %d\n", *fileCount)
	fmt.Printf("Delete ratio: %.1f%%\n", *deleteRatio*100)
	fmt.Printf("File size: %d KB\n", *fileSizeKB)
	fmt.Println()

	if *fileCount == 0 {
		// Just check volume status
		fmt.Println("📊 Checking volume status...")
		checkVolumeStatus()
		return
	}

	// Step 1: Create test files
	fmt.Println("📁 Step 1: Creating test files...")
	fids := createTestFiles()

	// Step 2: Delete some files to create garbage
	fmt.Println("🗑️  Step 2: Deleting files to create garbage...")
	deleteFiles(fids)

	// Step 3: Check volume status
	fmt.Println("📊 Step 3: Checking volume status...")
	checkVolumeStatus()

	// Step 4: Configure vacuum for testing
	fmt.Println("⚙️  Step 4: Instructions for testing...")
	printTestingInstructions()
}

func createTestFiles() []string {
	var fids []string

	for i := 0; i < *fileCount; i++ {
		// Generate random file content
		fileData := make([]byte, *fileSizeKB*1024)
		rand.Read(fileData)

		// Get file ID assignment
		assign, err := assignFileId()
		if err != nil {
			log.Printf("Failed to assign file ID for file %d: %v", i, err)
			continue
		}

		// Upload file
		err = uploadFile(assign, fileData, fmt.Sprintf("test_file_%d.dat", i))
		if err != nil {
			log.Printf("Failed to upload file %d: %v", i, err)
			continue
		}

		fids = append(fids, assign.Fid)

		if (i+1)%5 == 0 {
			fmt.Printf("  Created %d/%d files...\n", i+1, *fileCount)
		}
	}

	fmt.Printf("✅ Created %d files successfully\n\n", len(fids))
	return fids
}

func deleteFiles(fids []string) {
	deleteCount := int(float64(len(fids)) * *deleteRatio)

	for i := 0; i < deleteCount; i++ {
		err := deleteFile(fids[i])
		if err != nil {
			log.Printf("Failed to delete file %s: %v", fids[i], err)
			continue
		}

		if (i+1)%5 == 0 {
			fmt.Printf("  Deleted %d/%d files...\n", i+1, deleteCount)
		}
	}

	fmt.Printf("✅ Deleted %d files (%.1f%% of total)\n\n", deleteCount, *deleteRatio*100)
}

func assignFileId() (*AssignResult, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/dir/assign", *master))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result AssignResult
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	if result.Error != "" {
		return nil, fmt.Errorf("assignment error: %s", result.Error)
	}

	return &result, nil
}

func uploadFile(assign *AssignResult, data []byte, filename string) error {
	url := fmt.Sprintf("http://%s/%s", assign.Url, assign.Fid)

	body := &bytes.Buffer{}
	body.Write(data)

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	if filename != "" {
		req.Header.Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func deleteFile(fid string) error {
	url := fmt.Sprintf("http://%s/%s", *master, fid)

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

	return nil
}

func checkVolumeStatus() {
	// Get volume list from master
	resp, err := http.Get(fmt.Sprintf("http://%s/vol/status", *master))
	if err != nil {
		log.Printf("Failed to get volume status: %v", err)
		return
	}
	defer resp.Body.Close()

	var volumes map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&volumes)
	if err != nil {
		log.Printf("Failed to decode volume status: %v", err)
		return
	}

	fmt.Println("📊 Volume Status Summary:")

	if vols, ok := volumes["Volumes"].([]interface{}); ok {
		for _, vol := range vols {
			if v, ok := vol.(map[string]interface{}); ok {
				id := int(v["Id"].(float64))
				size := uint64(v["Size"].(float64))
				fileCount := int(v["FileCount"].(float64))
				deleteCount := int(v["DeleteCount"].(float64))
				deletedBytes := uint64(v["DeletedByteCount"].(float64))

				garbageRatio := 0.0
				if size > 0 {
					garbageRatio = float64(deletedBytes) / float64(size) * 100
				}

				fmt.Printf("  Volume %d:\n", id)
				fmt.Printf("    Size: %s\n", formatBytes(size))
				fmt.Printf("    Files: %d (active), %d (deleted)\n", fileCount, deleteCount)
				fmt.Printf("    Garbage: %s (%.1f%%)\n", formatBytes(deletedBytes), garbageRatio)

				if garbageRatio > 30 {
					fmt.Printf("    🎯 This volume should trigger vacuum (>30%% garbage)\n")
				}
				fmt.Println()
			}
		}
	}
}

func formatBytes(bytes uint64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	} else if bytes < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
	} else if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%.1f MB", float64(bytes)/(1024*1024))
	} else {
		return fmt.Sprintf("%.1f GB", float64(bytes)/(1024*1024*1024))
	}
}

func printTestingInstructions() {
	fmt.Println("🧪 Testing Instructions:")
	fmt.Println()
	fmt.Println("1. Configure Vacuum for Testing:")
	fmt.Println("   Visit: http://localhost:23646/maintenance/config/vacuum")
	fmt.Println("   Set:")
	fmt.Printf("   - Garbage Percentage Threshold: 20 (20%% - lower than default 30)\n")
	fmt.Printf("   - Scan Interval: [30] [Seconds] (faster than default)\n")
	fmt.Printf("   - Min Volume Age: [0] [Minutes] (no age requirement)\n")
	fmt.Printf("   - Max Concurrent: 2\n")
	fmt.Printf("   - Min Interval: 1m (faster repeat)\n")
	fmt.Println()

	fmt.Println("2. Monitor Vacuum Tasks:")
	fmt.Println("   Visit: http://localhost:23646/maintenance")
	fmt.Println("   Watch for vacuum tasks to appear in the queue")
	fmt.Println()

	fmt.Println("3. Manual Vacuum (Optional):")
	fmt.Println("   curl -X POST 'http://localhost:9333/vol/vacuum?garbageThreshold=0.20'")
	fmt.Println("   (Note: Master API still uses 0.0-1.0 decimal format)")
	fmt.Println()

	fmt.Println("4. Check Logs:")
	fmt.Println("   Look for messages like:")
	fmt.Println("   - 'Vacuum detector found X volumes needing vacuum'")
	fmt.Println("   - 'Applied vacuum configuration'")
	fmt.Println("   - 'Worker executing task: vacuum'")
	fmt.Println()

	fmt.Println("5. Verify Results:")
	fmt.Println("   Re-run this script with -files=0 to check volume status")
	fmt.Println("   Garbage ratios should decrease after vacuum operations")
	fmt.Println()

	fmt.Printf("🚀 Quick test command:\n")
	fmt.Printf("   go run create_vacuum_test_data.go -files=0\n")
	fmt.Println()
}

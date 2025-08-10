package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"
)

var (
	master      = flag.String("master", "master:9333", "SeaweedFS master server address")
	filer       = flag.String("filer", "filer:8888", "SeaweedFS filer server address")
	phase       = flag.String("phase", "", "Phase to execute: generate, delete, status (for EC vacuum testing)")
	fileCount   = flag.Int("files", 20, "Number of files to create")
	deleteRatio = flag.Float64("delete", 0.4, "Ratio of files to delete (0.0-1.0)")
	fileSizeKB  = flag.Int("size", 100, "Size of each file in KB")
)

// No longer needed - using filer-based operations

func main() {
	flag.Parse()

	// Handle EC vacuum testing phases
	if *phase != "" {
		handleECVacuumPhase()
		return
	}

	fmt.Println("🧪 Creating fake data for vacuum task testing...")
	fmt.Printf("Master: %s\n", *master)
	fmt.Printf("Filer: %s\n", *filer)
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
	filePaths := createTestFiles()

	// Step 2: Delete some files to create garbage
	fmt.Println("🗑️  Step 2: Deleting files to create garbage...")
	deleteFiles(filePaths)

	// Step 3: Check volume status
	fmt.Println("📊 Step 3: Checking volume status...")
	checkVolumeStatus()

	// Step 4: Configure vacuum for testing
	fmt.Println("⚙️  Step 4: Instructions for testing...")
	printTestingInstructions()
}

func createTestFiles() []string {
	var filePaths []string

	for i := 0; i < *fileCount; i++ {
		// Generate random file content
		fileData := make([]byte, *fileSizeKB*1024)
		rand.Read(fileData)

		// Create file path
		filePath := fmt.Sprintf("/vacuum_test/test_file_%d_%d.dat", time.Now().Unix(), i)

		// Upload file to filer
		err := uploadFileToFiler(filePath, fileData)
		if err != nil {
			log.Printf("Failed to upload file %d to filer: %v", i, err)
			continue
		}

		filePaths = append(filePaths, filePath)

		if (i+1)%5 == 0 {
			fmt.Printf("  Created %d/%d files...\n", i+1, *fileCount)
		}
	}

	fmt.Printf("✅ Created %d files successfully\n\n", len(filePaths))
	return filePaths
}

func deleteFiles(filePaths []string) {
	deleteCount := int(float64(len(filePaths)) * *deleteRatio)

	for i := 0; i < deleteCount; i++ {
		err := deleteFileFromFiler(filePaths[i])
		if err != nil {
			log.Printf("Failed to delete file %s: %v", filePaths[i], err)
			continue
		}

		if (i+1)%5 == 0 {
			fmt.Printf("  Deleted %d/%d files...\n", i+1, deleteCount)
		}
	}

	fmt.Printf("✅ Deleted %d files (%.1f%% of total)\n\n", deleteCount, *deleteRatio*100)
}

// Filer-based functions for file operations

func uploadFileToFiler(filePath string, data []byte) error {
	url := fmt.Sprintf("http://%s%s", *filer, filePath)

	req, err := http.NewRequest("PUT", url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(data))

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload to filer: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func deleteFileFromFiler(filePath string) error {
	url := fmt.Sprintf("http://%s%s", *filer, filePath)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create delete request: %v", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete from filer: %v", err)
	}
	defer resp.Body.Close()

	// Accept both 204 (No Content) and 404 (Not Found) as success
	// 404 means file was already deleted
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete failed with status %d: %s", resp.StatusCode, string(body))
	}

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

	fmt.Printf("🚀 Quick test commands:\n")
	fmt.Printf("   go run create_vacuum_test_data.go -files=0          # Check volume status\n")
	fmt.Printf("   go run create_vacuum_test_data.go -phase=status     # Check EC volumes\n")
	fmt.Println()
	fmt.Println("💡 All operations now use the filer for realistic file management")
}

// EC Vacuum Testing Functions

func handleECVacuumPhase() {
	fmt.Printf("🧪 EC Vacuum Test Data Script - Phase: %s\n", *phase)
	fmt.Printf("Master: %s\n", *master)
	fmt.Printf("Filer: %s\n", *filer)
	fmt.Println()

	switch *phase {
	case "generate":
		generateECTestData()
	case "delete":
		deleteFromECVolumes()
	case "status":
		checkECVolumeStatus()
	default:
		fmt.Printf("❌ Unknown phase: %s\n", *phase)
		fmt.Println("Valid phases: generate, delete, status")
	}
}

func generateECTestData() {
	fmt.Println("📁 Generating large files to trigger EC encoding...")
	fmt.Printf("Files to create: %d\n", *fileCount)
	fmt.Printf("File size: %d KB\n", *fileSizeKB)
	fmt.Printf("Filer: %s\n", *filer)
	fmt.Println()

	var filePaths []string

	for i := 0; i < *fileCount; i++ {
		// Generate random file content
		fileData := make([]byte, *fileSizeKB*1024)
		rand.Read(fileData)

		// Create file path
		filePath := fmt.Sprintf("/ec_test/large_file_%d_%d.dat", time.Now().Unix(), i)

		// Upload file to filer
		err := uploadFileToFiler(filePath, fileData)
		if err != nil {
			log.Printf("Failed to upload file %d to filer: %v", i, err)
			continue
		}

		filePaths = append(filePaths, filePath)

		if (i+1)%5 == 0 {
			fmt.Printf("  Created %d/%d files... (latest: %s)\n", i+1, *fileCount, filePath)
		}
	}

	fmt.Printf("✅ Created %d files successfully\n", len(filePaths))

	// Store file paths for later deletion (using mounted working directory)
	err := storeFilePathsToFile(filePaths, "ec_test_files.json")
	if err != nil {
		fmt.Printf("⚠️  Warning: Failed to store file paths for deletion: %v\n", err)
		fmt.Println("💡 You can still test EC vacuum manually through the admin UI")
	} else {
		fmt.Printf("📝 Stored %d file paths for deletion phase\n", len(filePaths))
	}

	fmt.Println()
	fmt.Println("📊 Current volume status:")
	checkVolumeStatus()

	fmt.Println()
	fmt.Println("⏳ Wait 2-3 minutes for EC encoding to complete...")
	fmt.Println("💡 EC encoding happens when volumes exceed 50MB")
	fmt.Println("💡 Run 'make ec-vacuum-status' to check EC volume creation")
	fmt.Println("💡 Then run 'make ec-vacuum-delete' to create garbage")
}

func deleteFromECVolumes() {
	fmt.Printf("🗑️ Creating deletions on EC volumes (ratio: %.1f%%)\n", *deleteRatio*100)
	fmt.Printf("Filer: %s\n", *filer)
	fmt.Println()

	// Load stored file paths from previous generation (using mounted working directory)
	filePaths, err := loadFilePathsFromFile("ec_test_files.json")
	if err != nil {
		fmt.Printf("❌ Failed to load stored file paths: %v\n", err)
		fmt.Println("💡 Run 'make ec-vacuum-generate' first to create files")
		return
	}

	if len(filePaths) == 0 {
		fmt.Println("❌ No stored file paths found. Run generate phase first.")
		return
	}

	fmt.Printf("Found %d stored file paths from previous generation\n", len(filePaths))

	deleteCount := int(float64(len(filePaths)) * *deleteRatio)
	fmt.Printf("Will delete %d files to create garbage\n", deleteCount)
	fmt.Println()

	deletedCount := 0
	for i := 0; i < deleteCount && i < len(filePaths); i++ {
		err := deleteFileFromFiler(filePaths[i])
		if err != nil {
			log.Printf("Failed to delete file %s: %v", filePaths[i], err)
		} else {
			deletedCount++
		}

		if (i+1)%5 == 0 {
			fmt.Printf("  Deleted %d/%d files...\n", i+1, deleteCount)
		}
	}

	fmt.Printf("✅ Successfully deleted %d files (%.1f%% of total)\n", deletedCount, *deleteRatio*100)
	fmt.Println()
	fmt.Println("📊 Updated status:")
	time.Sleep(5 * time.Second) // Wait for deletion to be processed
	checkECVolumeStatus()
}

func checkECVolumeStatus() {
	fmt.Println("📊 EC Volume Status and Garbage Analysis")
	fmt.Println("========================================")

	volumes := getVolumeStatusForDeletion()
	if len(volumes) == 0 {
		fmt.Println("❌ No volumes found")
		return
	}

	fmt.Println()
	fmt.Println("📈 Volume Analysis (potential EC candidates and EC volumes):")

	regularECCandidates := 0
	ecVolumes := 0
	highGarbageCount := 0

	for _, vol := range volumes {
		garbageRatio := 0.0
		if vol.Size > 0 {
			garbageRatio = float64(vol.DeletedByteCount) / float64(vol.Size) * 100
		}

		status := "📁"
		volumeType := "Regular"

		if vol.ReadOnly && vol.Size > 40*1024*1024 {
			status = "🔧"
			volumeType = "EC Volume"
			ecVolumes++
			if garbageRatio > 30 {
				status = "🧹"
				highGarbageCount++
			}
		} else if vol.Size > 40*1024*1024 {
			status = "📈"
			volumeType = "EC Candidate"
			regularECCandidates++
		}

		fmt.Printf("  %s Volume %d (%s): %s, Files: %d/%d, Garbage: %.1f%%",
			status, vol.Id, volumeType, formatBytes(vol.Size), vol.FileCount, vol.DeleteCount, garbageRatio)

		if volumeType == "EC Volume" && garbageRatio > 30 {
			fmt.Printf(" (Should trigger EC vacuum!)")
		}
		fmt.Printf("\n")
	}

	fmt.Println()
	fmt.Println("🎯 EC Vacuum Testing Summary:")
	fmt.Printf("  • Total volumes: %d\n", len(volumes))
	fmt.Printf("  • EC volumes (read-only >40MB): %d\n", ecVolumes)
	fmt.Printf("  • EC candidates (>40MB): %d\n", regularECCandidates)
	fmt.Printf("  • EC volumes with >30%% garbage: %d\n", highGarbageCount)

	if highGarbageCount > 0 {
		fmt.Println()
		fmt.Println("✅ EC volumes with high garbage found!")
		fmt.Println("💡 Configure EC vacuum at: http://localhost:23646/maintenance/config/ec_vacuum")
		fmt.Println("💡 Monitor tasks at: http://localhost:23646/maintenance")
	} else if ecVolumes > 0 {
		fmt.Println()
		fmt.Println("ℹ️  EC volumes exist but garbage ratio is low")
		fmt.Println("💡 Run 'make ec-vacuum-delete' to create more garbage")
	} else if regularECCandidates > 0 {
		fmt.Println()
		fmt.Println("ℹ️  Large volumes found, waiting for EC encoding...")
		fmt.Println("💡 Wait a few more minutes for EC encoding to complete")
	} else {
		fmt.Println()
		fmt.Println("ℹ️  No large volumes found")
		fmt.Println("💡 Run 'make ec-vacuum-generate' to create large files for EC encoding")
	}
}

type VolumeInfo struct {
	Id               int    `json:"Id"`
	Size             uint64 `json:"Size"`
	FileCount        int    `json:"FileCount"`
	DeleteCount      int    `json:"DeleteCount"`
	DeletedByteCount uint64 `json:"DeletedByteCount"`
	ReadOnly         bool   `json:"ReadOnly"`
	Collection       string `json:"Collection"`
}

type VolumeStatus struct {
	Version string       `json:"Version"`
	Volumes VolumeLayout `json:"Volumes"`
}

type VolumeLayout struct {
	DataCenters map[string]map[string]map[string][]VolumeInfo `json:"DataCenters"`
	Free        int                                           `json:"Free"`
	Max         int                                           `json:"Max"`
}

func getVolumeStatusForDeletion() []VolumeInfo {
	resp, err := http.Get(fmt.Sprintf("http://%s/vol/status", *master))
	if err != nil {
		log.Printf("Failed to get volume status: %v", err)
		return nil
	}
	defer resp.Body.Close()

	var volumeStatus VolumeStatus
	err = json.NewDecoder(resp.Body).Decode(&volumeStatus)
	if err != nil {
		log.Printf("Failed to decode volume status: %v", err)
		return nil
	}

	// Extract all volumes from the nested structure
	var allVolumes []VolumeInfo
	for dcName, dataCenter := range volumeStatus.Volumes.DataCenters {
		log.Printf("Processing data center: %s", dcName)
		for rackName, rack := range dataCenter {
			log.Printf("Processing rack: %s", rackName)
			for serverName, volumes := range rack {
				log.Printf("Found %d volumes on server %s", len(volumes), serverName)
				allVolumes = append(allVolumes, volumes...)
			}
		}
	}

	return allVolumes
}

type StoredFilePaths struct {
	FilePaths []string  `json:"file_paths"`
	Timestamp time.Time `json:"timestamp"`
	FileCount int       `json:"file_count"`
	FileSize  int       `json:"file_size_kb"`
}

func storeFilePathsToFile(filePaths []string, filename string) error {
	data := StoredFilePaths{
		FilePaths: filePaths,
		Timestamp: time.Now(),
		FileCount: len(filePaths),
		FileSize:  *fileSizeKB,
	}

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal file paths: %v", err)
	}

	err = ioutil.WriteFile(filename, jsonData, 0644)
	if err != nil {
		return fmt.Errorf("failed to write file paths to file: %v", err)
	}

	return nil
}

func loadFilePathsFromFile(filename string) ([]string, error) {
	// Check if file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil, fmt.Errorf("file paths storage file does not exist: %s", filename)
	}

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file paths file: %v", err)
	}

	var storedData StoredFilePaths
	err = json.Unmarshal(data, &storedData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal file paths: %v", err)
	}

	// Check if data is recent (within last 24 hours)
	if time.Since(storedData.Timestamp) > 24*time.Hour {
		return nil, fmt.Errorf("stored file paths are too old (%v), please regenerate",
			time.Since(storedData.Timestamp))
	}

	fmt.Printf("Loaded %d file paths from %v (File size: %dKB each)\n",
		len(storedData.FilePaths), storedData.Timestamp.Format("15:04:05"), storedData.FileSize)

	return storedData.FilePaths, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

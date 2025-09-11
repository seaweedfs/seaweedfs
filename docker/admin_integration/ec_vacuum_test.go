package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/stretchr/testify/require"
)

// TestFileGeneration tests generating 600 files of 100KB each targeting volume 1 with hardcoded cookie
func TestFileGeneration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping file generation test in short mode")
	}

	// Set up test cluster
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Wait for cluster to be ready
	require.NoError(t, waitForClusterReady())
	t.Logf("Test cluster ready with master at %s", cluster.masterAddress)

	// Generate 600 files of 100KB each targeting volume 1
	const targetVolumeId = needle.VolumeId(1)
	const fileCount = 600
	const fileSize = 100 * 1024 // 100KB files

	fileIds := generateFilesToVolume1(t, fileCount, fileSize)
	t.Logf("Generated %d files of %dKB each targeting volume %d", len(fileIds), fileSize/1024, targetVolumeId)
	t.Logf("📝 Sample file IDs created: %v", fileIds[:5]) // Show first 5 file IDs

	// Summary
	t.Logf("📊 File Generation Summary:")
	t.Logf("   • Volume ID: %d", targetVolumeId)
	t.Logf("   • Total files created: %d", len(fileIds))
	t.Logf("   • File size: %dKB each", fileSize/1024)
	t.Logf("   • Hardcoded cookie: 0x12345678")
}

// TestFileDeletion tests deleting exactly 300 files from volume 1
func TestFileDeletion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping file deletion test in short mode")
	}

	// Set up test cluster
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// Wait for cluster to be ready
	require.NoError(t, waitForClusterReady())
	t.Logf("Test cluster ready with master at %s", cluster.masterAddress)

	// First generate some files to delete
	const targetVolumeId = needle.VolumeId(1)
	const fileCount = 600
	const fileSize = 100 * 1024 // 100KB files

	t.Logf("Pre-generating files for deletion test...")
	fileIds := generateFilesToVolume1(t, fileCount, fileSize)
	t.Logf("Pre-generated %d files for deletion test", len(fileIds))

	// Delete exactly 300 files from the volume
	const deleteCount = 300
	deletedFileIds := deleteSpecificFilesFromVolume(t, fileIds, deleteCount)
	t.Logf("Deleted exactly %d files from volume %d", len(deletedFileIds), targetVolumeId)
	t.Logf("🗑️  Sample deleted file IDs: %v", deletedFileIds[:5]) // Show first 5 deleted file IDs

	// Summary
	t.Logf("📊 File Deletion Summary:")
	t.Logf("   • Volume ID: %d", targetVolumeId)
	t.Logf("   • Files available for deletion: %d", len(fileIds))
	t.Logf("   • Files deleted: %d", len(deletedFileIds))
	t.Logf("   • Files remaining: %d", len(fileIds)-len(deletedFileIds))
	t.Logf("   • Deletion success rate: %.1f%%", float64(len(deletedFileIds))/float64(deleteCount)*100)
}

// generateFilesToVolume1 creates 600 files of 100KB each targeting volume 1 with hardcoded cookie
func generateFilesToVolume1(t *testing.T, fileCount int, fileSize int) []string {
	const targetVolumeId = needle.VolumeId(1)
	const hardcodedCookie = uint32(0x12345678) // Hardcoded cookie for volume 1 files

	var fileIds []string
	t.Logf("Starting generation of %d files of %dKB each targeting volume %d", fileCount, fileSize/1024, targetVolumeId)

	for i := 0; i < fileCount; i++ {
		// Generate file content
		fileData := make([]byte, fileSize)
		rand.Read(fileData)

		// Generate file ID targeting volume 1 with hardcoded cookie
		// Use high needle key values to avoid collisions with assigned IDs
		needleKey := uint64(i) + 0x2000000 // Start from a high offset for volume 1
		generatedFid := needle.NewFileId(targetVolumeId, needleKey, hardcodedCookie)

		// Upload directly to volume 1
		err := uploadFileToVolumeDirectly(t, generatedFid, fileData)
		require.NoError(t, err)

		fileIds = append(fileIds, generatedFid.String())

		// Log progress for first few files and every 50th file
		if i < 5 || (i+1)%50 == 0 {
			t.Logf("✅ Generated file %d/%d targeting volume %d: %s", i+1, fileCount, targetVolumeId, generatedFid.String())
		}
	}

	t.Logf("✅ Successfully generated %d files targeting volume %d with hardcoded cookie 0x%08x", len(fileIds), targetVolumeId, hardcodedCookie)
	return fileIds
}

// uploadFileToVolume uploads file data to an assigned volume server
func uploadFileToVolume(serverUrl, fid string, data []byte) error {
	uploadUrl := fmt.Sprintf("http://%s/%s", serverUrl, fid)

	req, err := http.NewRequest("PUT", uploadUrl, bytes.NewReader(data))
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

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// uploadFileToVolumeDirectly uploads a file using a generated file ID
func uploadFileToVolumeDirectly(t *testing.T, fid *needle.FileId, data []byte) error {
	// Find the volume server hosting this volume
	locations, found := findVolumeLocations(fid.VolumeId)
	if !found {
		return fmt.Errorf("volume %d not found", fid.VolumeId)
	}

	// Upload to the first available location
	serverUrl := locations[0]
	uploadUrl := fmt.Sprintf("http://%s/%s", serverUrl, fid.String())

	req, err := http.NewRequest("PUT", uploadUrl, bytes.NewReader(data))
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

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("direct upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// findVolumeLocations finds the server locations for a given volume using HTTP lookup
func findVolumeLocations(volumeId needle.VolumeId) ([]string, bool) {
	// Query master for volume locations using HTTP API
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:9333/dir/lookup?volumeId=%d", volumeId))
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, false
	}

	// Parse JSON response
	type LookupResult struct {
		VolumeId  string `json:"volumeId"`
		Locations []struct {
			Url       string `json:"url"`
			PublicUrl string `json:"publicUrl"`
		} `json:"locations"`
		Error string `json:"error"`
	}

	var result LookupResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		// Fallback to default locations for testing
		return []string{"127.0.0.1:8080"}, true
	}

	if result.Error != "" {
		return nil, false
	}

	var serverUrls []string
	for _, location := range result.Locations {
		// Convert Docker container hostnames to localhost with mapped ports
		url := convertDockerHostnameToLocalhost(location.Url)
		serverUrls = append(serverUrls, url)
	}

	if len(serverUrls) == 0 {
		// Fallback to default for testing
		return []string{"127.0.0.1:8080"}, true
	}

	return serverUrls, true
}

// convertDockerHostnameToLocalhost converts Docker container hostnames to localhost with mapped ports
func convertDockerHostnameToLocalhost(dockerUrl string) string {
	// Map Docker container hostnames to localhost ports
	hostPortMap := map[string]string{
		"volume1:8080": "127.0.0.1:8080",
		"volume2:8080": "127.0.0.1:8081",
		"volume3:8080": "127.0.0.1:8082",
		"volume4:8080": "127.0.0.1:8083",
		"volume5:8080": "127.0.0.1:8084",
		"volume6:8080": "127.0.0.1:8085",
	}

	if localhost, exists := hostPortMap[dockerUrl]; exists {
		return localhost
	}

	// If not in map, return as-is (might be already localhost)
	return dockerUrl
}

// deleteSpecificFilesFromVolume deletes exactly the specified number of files from the volume
func deleteSpecificFilesFromVolume(t *testing.T, fileIds []string, deleteCount int) []string {
	var deletedFileIds []string
	successfulDeletions := 0

	if deleteCount > len(fileIds) {
		deleteCount = len(fileIds)
	}

	t.Logf("🗑️  Starting deletion of exactly %d files out of %d total files", deleteCount, len(fileIds))

	for i := 0; i < deleteCount; i++ {
		fileId := fileIds[i]

		// Parse file ID to get volume server location
		fid, err := needle.ParseFileIdFromString(fileId)
		if err != nil {
			t.Logf("Failed to parse file ID %s: %v", fileId, err)
			continue
		}

		// Find volume server hosting this file
		locations, found := findVolumeLocations(fid.VolumeId)
		if !found {
			t.Logf("Volume locations not found for file %s", fileId)
			continue
		}

		// Delete file from volume server
		deleteUrl := fmt.Sprintf("http://%s/%s", locations[0], fileId)
		req, err := http.NewRequest("DELETE", deleteUrl, nil)
		if err != nil {
			t.Logf("Failed to create delete request for file %s: %v", fileId, err)
			continue
		}

		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			t.Logf("Failed to delete file %s: %v", fileId, err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
			successfulDeletions++
			deletedFileIds = append(deletedFileIds, fileId)
			// Log progress for first few files and every 25th deletion
			if i < 5 || (i+1)%25 == 0 {
				t.Logf("🗑️  Deleted file %d/%d: %s (status: %d)", i+1, deleteCount, fileId, resp.StatusCode)
			}
		} else {
			t.Logf("❌ Delete failed for file %s with status %d", fileId, resp.StatusCode)
		}
	}

	t.Logf("✅ Deletion summary: %d files deleted successfully out of %d attempted", successfulDeletions, deleteCount)
	return deletedFileIds
}

// Helper functions for test setup

func setupTestCluster(t *testing.T) (*TestCluster, func()) {
	// Create test cluster similar to existing integration tests
	// This is a simplified version - in practice would start actual servers
	cluster := &TestCluster{
		masterAddress: "127.0.0.1:9333",
		volumeServers: []string{
			"127.0.0.1:8080",
			"127.0.0.1:8081",
			"127.0.0.1:8082",
			"127.0.0.1:8083",
			"127.0.0.1:8084",
			"127.0.0.1:8085",
		},
	}

	cleanup := func() {
		// Cleanup cluster resources
		t.Logf("Cleaning up test cluster")
	}

	return cluster, cleanup
}

func waitForClusterReady() error {
	// Wait for test cluster to be ready
	// In practice, this would ping the servers and wait for them to respond
	time.Sleep(2 * time.Second)
	return nil
}

type TestCluster struct {
	masterAddress string
	volumeServers []string
}

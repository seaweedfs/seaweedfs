package mount

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

// RDMAMountClient provides RDMA acceleration for SeaweedFS mount operations
type RDMAMountClient struct {
	sidecarAddr   string
	httpClient    *http.Client
	maxConcurrent int
	timeout       time.Duration
	semaphore     chan struct{}

	// Volume lookup
	lookupFileIdFn wdclient.LookupFileIdFunctionType

	// Statistics
	totalRequests   atomic.Int64
	successfulReads atomic.Int64
	failedReads     atomic.Int64
	totalBytesRead  atomic.Int64
	totalLatencyNs  atomic.Int64
}

// RDMAReadRequest represents a request to read data via RDMA
type RDMAReadRequest struct {
	VolumeID uint32 `json:"volume_id"`
	NeedleID uint64 `json:"needle_id"`
	Cookie   uint32 `json:"cookie"`
	Offset   uint64 `json:"offset"`
	Size     uint64 `json:"size"`
}

// RDMAReadResponse represents the response from an RDMA read operation
type RDMAReadResponse struct {
	Success   bool   `json:"success"`
	IsRDMA    bool   `json:"is_rdma"`
	Source    string `json:"source"`
	Duration  string `json:"duration"`
	DataSize  int    `json:"data_size"`
	SessionID string `json:"session_id,omitempty"`
	ErrorMsg  string `json:"error,omitempty"`

	// Zero-copy optimization fields
	UseTempFile bool   `json:"use_temp_file"`
	TempFile    string `json:"temp_file"`
}

// RDMAHealthResponse represents the health status of the RDMA sidecar
type RDMAHealthResponse struct {
	Status string `json:"status"`
	RDMA   struct {
		Enabled   bool `json:"enabled"`
		Connected bool `json:"connected"`
	} `json:"rdma"`
	Timestamp string `json:"timestamp"`
}

// NewRDMAMountClient creates a new RDMA client for mount operations
func NewRDMAMountClient(sidecarAddr string, lookupFileIdFn wdclient.LookupFileIdFunctionType, maxConcurrent int, timeoutMs int) (*RDMAMountClient, error) {
	client := &RDMAMountClient{
		sidecarAddr:   sidecarAddr,
		maxConcurrent: maxConcurrent,
		timeout:       time.Duration(timeoutMs) * time.Millisecond,
		httpClient: &http.Client{
			Timeout: time.Duration(timeoutMs) * time.Millisecond,
		},
		semaphore:      make(chan struct{}, maxConcurrent),
		lookupFileIdFn: lookupFileIdFn,
	}

	// Test connectivity and RDMA availability
	if err := client.healthCheck(); err != nil {
		return nil, fmt.Errorf("RDMA sidecar health check failed: %w", err)
	}

	glog.Infof("RDMA mount client initialized: sidecar=%s, maxConcurrent=%d, timeout=%v",
		sidecarAddr, maxConcurrent, client.timeout)

	return client, nil
}

// lookupVolumeLocationByFileID finds the best volume server for a given file ID
func (c *RDMAMountClient) lookupVolumeLocationByFileID(ctx context.Context, fileID string) (string, error) {
	glog.V(4).Infof("Looking up volume location for file ID %s", fileID)

	targetUrls, err := c.lookupFileIdFn(ctx, fileID)
	if err != nil {
		return "", fmt.Errorf("failed to lookup volume for file %s: %w", fileID, err)
	}

	if len(targetUrls) == 0 {
		return "", fmt.Errorf("no locations found for file %s", fileID)
	}

	// Choose the first URL and extract the server address
	targetUrl := targetUrls[0]
	// Extract server address from URL like "http://server:port/fileId"
	parts := strings.Split(targetUrl, "/")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid target URL format: %s", targetUrl)
	}
	bestAddress := fmt.Sprintf("http://%s", parts[2])

	glog.V(4).Infof("File %s located at %s", fileID, bestAddress)
	return bestAddress, nil
}

// lookupVolumeLocation finds the best volume server for a given volume ID (legacy method)
func (c *RDMAMountClient) lookupVolumeLocation(ctx context.Context, volumeID uint32, needleID uint64, cookie uint32) (string, error) {
	// Create a file ID for lookup (format: volumeId,needleId,cookie)
	fileID := fmt.Sprintf("%d,%x,%d", volumeID, needleID, cookie)
	return c.lookupVolumeLocationByFileID(ctx, fileID)
}

// healthCheck verifies that the RDMA sidecar is available and functioning
func (c *RDMAMountClient) healthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("http://%s/health", c.sidecarAddr), nil)
	if err != nil {
		return fmt.Errorf("failed to create health check request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("health check request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed with status: %s", resp.Status)
	}

	// Parse health response
	var health RDMAHealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return fmt.Errorf("failed to parse health response: %w", err)
	}

	if health.Status != "healthy" {
		return fmt.Errorf("sidecar reports unhealthy status: %s", health.Status)
	}

	if !health.RDMA.Enabled {
		return fmt.Errorf("RDMA is not enabled on sidecar")
	}

	if !health.RDMA.Connected {
		glog.Warningf("RDMA sidecar is healthy but not connected to RDMA engine")
	}

	return nil
}

// ReadNeedle reads data from a specific needle using RDMA acceleration
func (c *RDMAMountClient) ReadNeedle(ctx context.Context, fileID string, offset, size uint64) ([]byte, bool, error) {
	// Acquire semaphore for concurrency control
	select {
	case c.semaphore <- struct{}{}:
		defer func() { <-c.semaphore }()
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}

	c.totalRequests.Add(1)
	startTime := time.Now()

	// Lookup volume location using file ID directly
	volumeServer, err := c.lookupVolumeLocationByFileID(ctx, fileID)
	if err != nil {
		c.failedReads.Add(1)
		return nil, false, fmt.Errorf("failed to lookup volume for file %s: %w", fileID, err)
	}

	// Prepare request URL with file_id parameter (simpler than individual components)
	reqURL := fmt.Sprintf("http://%s/read?file_id=%s&offset=%d&size=%d&volume_server=%s",
		c.sidecarAddr, fileID, offset, size, volumeServer)

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		c.failedReads.Add(1)
		return nil, false, fmt.Errorf("failed to create RDMA request: %w", err)
	}

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.failedReads.Add(1)
		return nil, false, fmt.Errorf("RDMA request failed: %w", err)
	}
	defer resp.Body.Close()

	duration := time.Since(startTime)
	c.totalLatencyNs.Add(duration.Nanoseconds())

	if resp.StatusCode != http.StatusOK {
		c.failedReads.Add(1)
		body, _ := io.ReadAll(resp.Body)
		return nil, false, fmt.Errorf("RDMA read failed with status %s: %s", resp.Status, string(body))
	}

	// Check if response indicates RDMA was used
	contentType := resp.Header.Get("Content-Type")
	isRDMA := strings.Contains(resp.Header.Get("X-Source"), "rdma") ||
		resp.Header.Get("X-RDMA-Used") == "true"

	// Check for zero-copy temp file optimization
	tempFilePath := resp.Header.Get("X-Temp-File")
	useTempFile := resp.Header.Get("X-Use-Temp-File") == "true"

	var data []byte

	if useTempFile && tempFilePath != "" {
		// Zero-copy path: read from temp file (page cache)
		glog.V(4).Infof("🔥 Using zero-copy temp file: %s", tempFilePath)

		// Allocate buffer for temp file read
		var bufferSize uint64 = 1024 * 1024 // Default 1MB
		if size > 0 {
			bufferSize = size
		}
		buffer := make([]byte, bufferSize)

		n, err := c.readFromTempFile(tempFilePath, buffer)
		if err != nil {
			glog.V(2).Infof("Zero-copy failed, falling back to HTTP body: %v", err)
			// Fall back to reading HTTP body
			data, err = io.ReadAll(resp.Body)
		} else {
			data = buffer[:n]
			glog.V(4).Infof("🔥 Zero-copy successful: %d bytes from page cache", n)
		}

		// Important: Cleanup temp file after reading (consumer responsibility)
		// This prevents accumulation of temp files in /tmp/rdma-cache
		go c.cleanupTempFile(tempFilePath)
	} else {
		// Regular path: read from HTTP response body
		data, err = io.ReadAll(resp.Body)
	}

	if err != nil {
		c.failedReads.Add(1)
		return nil, false, fmt.Errorf("failed to read RDMA response: %w", err)
	}

	c.successfulReads.Add(1)
	c.totalBytesRead.Add(int64(len(data)))

	// Log successful operation
	glog.V(4).Infof("RDMA read completed: fileID=%s, size=%d, duration=%v, rdma=%v, contentType=%s",
		fileID, size, duration, isRDMA, contentType)

	return data, isRDMA, nil
}

// cleanupTempFile requests cleanup of a temp file from the sidecar
func (c *RDMAMountClient) cleanupTempFile(tempFilePath string) {
	if tempFilePath == "" {
		return
	}

	// Give the page cache a brief moment to be utilized before cleanup
	// This preserves the zero-copy performance window
	time.Sleep(100 * time.Millisecond)

	// Call sidecar cleanup endpoint
	cleanupURL := fmt.Sprintf("http://%s/cleanup?temp_file=%s", c.sidecarAddr, url.QueryEscape(tempFilePath))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "DELETE", cleanupURL, nil)
	if err != nil {
		glog.V(2).Infof("Failed to create cleanup request for %s: %v", tempFilePath, err)
		return
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		glog.V(2).Infof("Failed to cleanup temp file %s: %v", tempFilePath, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		glog.V(4).Infof("🧹 Temp file cleaned up: %s", tempFilePath)
	} else {
		glog.V(2).Infof("Cleanup failed for %s: status %s", tempFilePath, resp.Status)
	}
}

// GetStats returns current RDMA client statistics
func (c *RDMAMountClient) GetStats() map[string]interface{} {
	totalRequests := c.totalRequests.Load()
	successfulReads := c.successfulReads.Load()
	failedReads := c.failedReads.Load()
	totalBytesRead := c.totalBytesRead.Load()
	totalLatencyNs := c.totalLatencyNs.Load()

	successRate := float64(0)
	avgLatencyNs := int64(0)

	if totalRequests > 0 {
		successRate = float64(successfulReads) / float64(totalRequests) * 100
		avgLatencyNs = totalLatencyNs / totalRequests
	}

	return map[string]interface{}{
		"sidecar_addr":     c.sidecarAddr,
		"max_concurrent":   c.maxConcurrent,
		"timeout_ms":       int(c.timeout / time.Millisecond),
		"total_requests":   totalRequests,
		"successful_reads": successfulReads,
		"failed_reads":     failedReads,
		"success_rate_pct": fmt.Sprintf("%.1f", successRate),
		"total_bytes_read": totalBytesRead,
		"avg_latency_ns":   avgLatencyNs,
		"avg_latency_ms":   fmt.Sprintf("%.3f", float64(avgLatencyNs)/1000000),
	}
}

// Close shuts down the RDMA client and releases resources
func (c *RDMAMountClient) Close() error {
	// No need to close semaphore channel; closing it may cause panics if goroutines are still using it.
	// The semaphore will be garbage collected when the client is no longer referenced.

	// Log final statistics
	stats := c.GetStats()
	glog.Infof("RDMA mount client closing: %+v", stats)

	return nil
}

// IsHealthy checks if the RDMA sidecar is currently healthy
func (c *RDMAMountClient) IsHealthy() bool {
	err := c.healthCheck()
	return err == nil
}

// readFromTempFile performs zero-copy read from temp file using page cache
func (c *RDMAMountClient) readFromTempFile(tempFilePath string, buffer []byte) (int, error) {
	if tempFilePath == "" {
		return 0, fmt.Errorf("empty temp file path")
	}

	// Open temp file for reading
	file, err := os.Open(tempFilePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open temp file %s: %w", tempFilePath, err)
	}
	defer file.Close()

	// Read from temp file (this should be served from page cache)
	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return n, fmt.Errorf("failed to read from temp file: %w", err)
	}

	glog.V(4).Infof("🔥 Zero-copy read: %d bytes from temp file %s", n, tempFilePath)

	return n, nil
}

package mount

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
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
	totalRequests   int64
	successfulReads int64
	failedReads     int64
	totalBytesRead  int64
	totalLatencyNs  int64
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

// lookupVolumeLocation finds the best volume server for a given volume ID
func (c *RDMAMountClient) lookupVolumeLocation(ctx context.Context, volumeID uint32, needleID uint64, cookie uint32) (string, error) {
	// Create a file ID for lookup (format: volumeId,needleId,cookie)
	fileId := fmt.Sprintf("%d,%x,%d", volumeID, needleID, cookie)

	glog.V(4).Infof("Looking up volume %d using fileId %s", volumeID, fileId)

	targetUrls, err := c.lookupFileIdFn(ctx, fileId)
	if err != nil {
		return "", fmt.Errorf("failed to lookup volume %d: %w", volumeID, err)
	}

	if len(targetUrls) == 0 {
		return "", fmt.Errorf("no locations found for volume %d", volumeID)
	}

	// Choose the first URL and extract the server address
	targetUrl := targetUrls[0]
	// Extract server address from URL like "http://server:port/fileId"
	parts := strings.Split(targetUrl, "/")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid target URL format: %s", targetUrl)
	}
	bestAddress := fmt.Sprintf("http://%s", parts[2])

	glog.V(4).Infof("Volume %d located at %s", volumeID, bestAddress)
	return bestAddress, nil
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
func (c *RDMAMountClient) ReadNeedle(ctx context.Context, volumeID uint32, needleID uint64, cookie uint32, offset, size uint64) ([]byte, bool, error) {
	// Acquire semaphore for concurrency control
	select {
	case c.semaphore <- struct{}{}:
		defer func() { <-c.semaphore }()
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}

	atomic.AddInt64(&c.totalRequests, 1)
	startTime := time.Now()

	// Lookup volume location
	volumeServer, err := c.lookupVolumeLocation(ctx, volumeID, needleID, cookie)
	if err != nil {
		atomic.AddInt64(&c.failedReads, 1)
		return nil, false, fmt.Errorf("failed to lookup volume %d: %w", volumeID, err)
	}

	// Prepare request URL with volume_server parameter
	reqURL := fmt.Sprintf("http://%s/read?volume=%d&needle=%d&cookie=%d&offset=%d&size=%d&volume_server=%s",
		c.sidecarAddr, volumeID, needleID, cookie, offset, size, volumeServer)

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		atomic.AddInt64(&c.failedReads, 1)
		return nil, false, fmt.Errorf("failed to create RDMA request: %w", err)
	}

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		atomic.AddInt64(&c.failedReads, 1)
		return nil, false, fmt.Errorf("RDMA request failed: %w", err)
	}
	defer resp.Body.Close()

	duration := time.Since(startTime)
	atomic.AddInt64(&c.totalLatencyNs, duration.Nanoseconds())

	if resp.StatusCode != http.StatusOK {
		atomic.AddInt64(&c.failedReads, 1)
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
	} else {
		// Regular path: read from HTTP response body
		data, err = io.ReadAll(resp.Body)
	}

	if err != nil {
		atomic.AddInt64(&c.failedReads, 1)
		return nil, false, fmt.Errorf("failed to read RDMA response: %w", err)
	}

	atomic.AddInt64(&c.successfulReads, 1)
	atomic.AddInt64(&c.totalBytesRead, int64(len(data)))

	// Log successful operation
	glog.V(4).Infof("RDMA read completed: volume=%d, needle=%d, size=%d, duration=%v, rdma=%v, contentType=%s",
		volumeID, needleID, size, duration, isRDMA, contentType)

	return data, isRDMA, nil
}

// GetStats returns current RDMA client statistics
func (c *RDMAMountClient) GetStats() map[string]interface{} {
	totalRequests := atomic.LoadInt64(&c.totalRequests)
	successfulReads := atomic.LoadInt64(&c.successfulReads)
	failedReads := atomic.LoadInt64(&c.failedReads)
	totalBytesRead := atomic.LoadInt64(&c.totalBytesRead)
	totalLatencyNs := atomic.LoadInt64(&c.totalLatencyNs)

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

// ParseFileId extracts volume ID, needle ID, and cookie from a SeaweedFS file ID
// Uses existing SeaweedFS parsing logic to ensure compatibility
func ParseFileId(fileId string) (volumeID uint32, needleID uint64, cookie uint32, err error) {
	// Use existing SeaweedFS file ID parsing
	fid, err := needle.ParseFileIdFromString(fileId)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to parse file ID %s: %w", fileId, err)
	}

	volumeID = uint32(fid.VolumeId)
	needleID = uint64(fid.Key)
	cookie = uint32(fid.Cookie)

	return volumeID, needleID, cookie, nil
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

	// Clean up temp file after reading
	go func() {
		if removeErr := os.Remove(tempFilePath); removeErr != nil {
			glog.V(2).Infof("Failed to cleanup temp file %s: %v", tempFilePath, removeErr)
		} else {
			glog.V(4).Infof("🧹 Cleaned up temp file %s", tempFilePath)
		}
	}()

	return n, nil
}

package mount

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// RDMAMountClient provides RDMA acceleration for SeaweedFS mount operations
type RDMAMountClient struct {
	sidecarAddr   string
	httpClient    *http.Client
	maxConcurrent int
	timeout       time.Duration
	semaphore     chan struct{}
	
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
	Success     bool   `json:"success"`
	IsRDMA      bool   `json:"is_rdma"`
	Source      string `json:"source"`
	Duration    string `json:"duration"`
	DataSize    int    `json:"data_size"`
	SessionID   string `json:"session_id,omitempty"`
	ErrorMsg    string `json:"error,omitempty"`
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
func NewRDMAMountClient(sidecarAddr string, maxConcurrent int, timeoutMs int) (*RDMAMountClient, error) {
	client := &RDMAMountClient{
		sidecarAddr:   sidecarAddr,
		maxConcurrent: maxConcurrent,
		timeout:       time.Duration(timeoutMs) * time.Millisecond,
		httpClient: &http.Client{
			Timeout: time.Duration(timeoutMs) * time.Millisecond,
		},
		semaphore: make(chan struct{}, maxConcurrent),
	}

	// Test connectivity and RDMA availability
	if err := client.healthCheck(); err != nil {
		return nil, fmt.Errorf("RDMA sidecar health check failed: %w", err)
	}

	glog.Infof("RDMA mount client initialized: sidecar=%s, maxConcurrent=%d, timeout=%v",
		sidecarAddr, maxConcurrent, client.timeout)

	return client, nil
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

	// Prepare request URL
	reqURL := fmt.Sprintf("http://%s/read?volume=%d&needle=%d&cookie=%d&offset=%d&size=%d",
		c.sidecarAddr, volumeID, needleID, cookie, offset, size)

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

	// Read response data
	data, err := io.ReadAll(resp.Body)
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

// ReadNeedleWithFallback attempts RDMA read with automatic HTTP fallback
func (c *RDMAMountClient) ReadNeedleWithFallback(ctx context.Context, volumeID uint32, needleID uint64, cookie uint32, offset, size uint64, httpFallback func() ([]byte, error)) ([]byte, bool, error) {
	// Try RDMA first
	data, isRDMA, err := c.ReadNeedle(ctx, volumeID, needleID, cookie, offset, size)
	if err == nil {
		return data, isRDMA, nil
	}

	// Log RDMA failure
	glog.V(2).Infof("RDMA read failed for volume=%d, needle=%d: %v, falling back to HTTP", volumeID, needleID, err)

	// Fall back to HTTP if provided
	if httpFallback != nil {
		data, err := httpFallback()
		if err != nil {
			return nil, false, fmt.Errorf("both RDMA and HTTP fallback failed: RDMA=%v, HTTP=%v", err, err)
		}
		return data, false, nil
	}

	return nil, false, err
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
		"sidecar_addr":      c.sidecarAddr,
		"max_concurrent":    c.maxConcurrent,
		"timeout_ms":        int(c.timeout / time.Millisecond),
		"total_requests":    totalRequests,
		"successful_reads":  successfulReads,
		"failed_reads":      failedReads,
		"success_rate_pct":  fmt.Sprintf("%.1f", successRate),
		"total_bytes_read":  totalBytesRead,
		"avg_latency_ns":    avgLatencyNs,
		"avg_latency_ms":    fmt.Sprintf("%.3f", float64(avgLatencyNs)/1000000),
	}
}

// Close shuts down the RDMA client and releases resources
func (c *RDMAMountClient) Close() error {
	// Close semaphore channel
	close(c.semaphore)
	
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
func ParseFileId(fileId string) (volumeID uint32, needleID uint64, cookie uint32, err error) {
	// Parse file ID format: "volumeId,needleIdCookie"
	// Example: "3,01637037d6"
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid file ID format: %s", fileId)
	}

	// Parse volume ID
	vol, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid volume ID: %s", parts[0])
	}
	volumeID = uint32(vol)

	// Parse needle ID and cookie (combined hex string)
	needleCookieHex := parts[1]
	if len(needleCookieHex) < 8 {
		return 0, 0, 0, fmt.Errorf("invalid needle ID format: %s", needleCookieHex)
	}

	// Last 8 hex characters are the cookie
	cookieHex := needleCookieHex[len(needleCookieHex)-8:]
	needleHex := needleCookieHex[:len(needleCookieHex)-8]

	// Parse needle ID
	needle, err := strconv.ParseUint(needleHex, 16, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid needle ID: %s", needleHex)
	}
	needleID = needle

	// Parse cookie
	cook, err := strconv.ParseUint(cookieHex, 16, 32)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid cookie: %s", cookieHex)
	}
	cookie = uint32(cook)

	return volumeID, needleID, cookie, nil
}

// Package seaweedfs provides SeaweedFS-specific RDMA integration
package seaweedfs

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"seaweedfs-rdma-sidecar/pkg/rdma"

	"github.com/sirupsen/logrus"
)

// SeaweedFSRDMAClient provides SeaweedFS-specific RDMA operations
type SeaweedFSRDMAClient struct {
	rdmaClient      *rdma.Client
	logger          *logrus.Logger
	volumeServerURL string
	enabled         bool
}

// Config holds configuration for the SeaweedFS RDMA client
type Config struct {
	RDMASocketPath  string
	VolumeServerURL string
	Enabled         bool
	DefaultTimeout  time.Duration
	Logger          *logrus.Logger
}

// NeedleReadRequest represents a SeaweedFS needle read request
type NeedleReadRequest struct {
	VolumeID      uint32
	NeedleID      uint64
	Cookie        uint32
	Offset        uint64
	Size          uint64
	VolumeServer  string // Override volume server URL for this request
}

// NeedleReadResponse represents the result of a needle read
type NeedleReadResponse struct {
	Data      []byte
	IsRDMA    bool
	Latency   time.Duration
	Source    string // "rdma" or "http"
	SessionID string
}

// NewSeaweedFSRDMAClient creates a new SeaweedFS RDMA client
func NewSeaweedFSRDMAClient(config *Config) (*SeaweedFSRDMAClient, error) {
	if config.Logger == nil {
		config.Logger = logrus.New()
		config.Logger.SetLevel(logrus.InfoLevel)
	}

	var rdmaClient *rdma.Client
	if config.Enabled && config.RDMASocketPath != "" {
		rdmaConfig := &rdma.Config{
			EngineSocketPath: config.RDMASocketPath,
			DefaultTimeout:   config.DefaultTimeout,
			Logger:           config.Logger,
		}
		rdmaClient = rdma.NewClient(rdmaConfig)
	}

	return &SeaweedFSRDMAClient{
		rdmaClient:      rdmaClient,
		logger:          config.Logger,
		volumeServerURL: config.VolumeServerURL,
		enabled:         config.Enabled,
	}, nil
}

// Start initializes the RDMA client connection
func (c *SeaweedFSRDMAClient) Start(ctx context.Context) error {
	if !c.enabled || c.rdmaClient == nil {
		c.logger.Info("🔄 RDMA disabled, using HTTP fallback only")
		return nil
	}

	c.logger.Info("🚀 Starting SeaweedFS RDMA client...")

	if err := c.rdmaClient.Connect(ctx); err != nil {
		c.logger.WithError(err).Error("❌ Failed to connect to RDMA engine")
		return fmt.Errorf("failed to connect to RDMA engine: %w", err)
	}

	c.logger.Info("✅ SeaweedFS RDMA client started successfully")
	return nil
}

// Stop shuts down the RDMA client
func (c *SeaweedFSRDMAClient) Stop() {
	if c.rdmaClient != nil {
		c.rdmaClient.Disconnect()
		c.logger.Info("🔌 SeaweedFS RDMA client stopped")
	}
}

// IsEnabled returns true if RDMA is enabled and available
func (c *SeaweedFSRDMAClient) IsEnabled() bool {
	return c.enabled && c.rdmaClient != nil && c.rdmaClient.IsConnected()
}

// ReadNeedle reads a needle using RDMA fast path or HTTP fallback
func (c *SeaweedFSRDMAClient) ReadNeedle(ctx context.Context, req *NeedleReadRequest) (*NeedleReadResponse, error) {
	start := time.Now()

	// Try RDMA fast path first
	if c.IsEnabled() {
		c.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeID,
			"needle_id": req.NeedleID,
			"offset":    req.Offset,
			"size":      req.Size,
		}).Debug("🚀 Attempting RDMA fast path")

		rdmaReq := &rdma.ReadRequest{
			VolumeID: req.VolumeID,
			NeedleID: req.NeedleID,
			Cookie:   req.Cookie,
			Offset:   req.Offset,
			Size:     req.Size,
		}

		resp, err := c.rdmaClient.Read(ctx, rdmaReq)
		if err != nil {
			c.logger.WithError(err).Warn("⚠️  RDMA read failed, falling back to HTTP")
		} else {
			c.logger.WithFields(logrus.Fields{
				"volume_id":     req.VolumeID,
				"needle_id":     req.NeedleID,
				"bytes_read":    resp.BytesRead,
				"transfer_rate": resp.TransferRate,
				"latency":       time.Since(start),
			}).Info("🚀 RDMA fast path successful")

			return &NeedleReadResponse{
				Data:      resp.Data,
				IsRDMA:    true,
				Latency:   time.Since(start),
				Source:    "rdma",
				SessionID: resp.SessionID,
			}, nil
		}
	}

	// Fallback to HTTP
	c.logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeID,
		"needle_id": req.NeedleID,
		"reason":    "rdma_unavailable",
	}).Debug("🌐 Using HTTP fallback")

	data, err := c.httpFallback(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("both RDMA and HTTP failed: %w", err)
	}

	return &NeedleReadResponse{
		Data:    data,
		IsRDMA:  false,
		Latency: time.Since(start),
		Source:  "http",
	}, nil
}

// ReadNeedleRange reads a specific range from a needle
func (c *SeaweedFSRDMAClient) ReadNeedleRange(ctx context.Context, volumeID uint32, needleID uint64, cookie uint32, offset, size uint64) (*NeedleReadResponse, error) {
	req := &NeedleReadRequest{
		VolumeID: volumeID,
		NeedleID: needleID,
		Cookie:   cookie,
		Offset:   offset,
		Size:     size,
	}
	return c.ReadNeedle(ctx, req)
}

// httpFallback performs HTTP fallback read from SeaweedFS volume server
func (c *SeaweedFSRDMAClient) httpFallback(ctx context.Context, req *NeedleReadRequest) ([]byte, error) {
	// Use volume server from request, fallback to configured URL
	volumeServerURL := req.VolumeServer
	if volumeServerURL == "" {
		volumeServerURL = c.volumeServerURL
	}
	
	if volumeServerURL == "" {
		return nil, fmt.Errorf("no volume server URL provided in request or configured")
	}

	// Build URL for volume server read
	url := fmt.Sprintf("%s/%d,%s,%d", volumeServerURL, req.VolumeID,
		fmt.Sprintf("%x", req.NeedleID), req.Cookie)

	if req.Offset > 0 || req.Size > 0 {
		url += fmt.Sprintf("?offset=%d&size=%d", req.Offset, req.Size)
	}

	c.logger.WithField("url", url).Debug("📥 HTTP fallback request")

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status: %d", resp.StatusCode)
	}

	// Read response data
	data := make([]byte, 0, 1024*1024) // Pre-allocate 1MB
	buffer := make([]byte, 32*1024)    // 32KB buffer
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		n, err := resp.Body.Read(buffer)
		if n > 0 {
			data = append(data, buffer[:n]...)
		}
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, fmt.Errorf("failed to read HTTP response: %w", err)
		}
	}

	c.logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeID,
		"needle_id": req.NeedleID,
		"data_size": len(data),
	}).Debug("📥 HTTP fallback successful")

	return data, nil
}

// HealthCheck verifies that the RDMA client is healthy
func (c *SeaweedFSRDMAClient) HealthCheck(ctx context.Context) error {
	if !c.enabled {
		return fmt.Errorf("RDMA is disabled")
	}

	if c.rdmaClient == nil {
		return fmt.Errorf("RDMA client not initialized")
	}

	if !c.rdmaClient.IsConnected() {
		return fmt.Errorf("RDMA client not connected")
	}

	// Try a ping to the RDMA engine
	_, err := c.rdmaClient.Ping(ctx)
	return err
}

// GetStats returns statistics about the RDMA client
func (c *SeaweedFSRDMAClient) GetStats() map[string]interface{} {
	stats := map[string]interface{}{
		"enabled":           c.enabled,
		"volume_server_url": c.volumeServerURL,
		"rdma_socket_path":  "",
	}

	if c.rdmaClient != nil {
		stats["connected"] = c.rdmaClient.IsConnected()
		// Note: Capabilities method may not be available, skip for now
	} else {
		stats["connected"] = false
		stats["error"] = "RDMA client not initialized"
	}

	return stats
}

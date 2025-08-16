// Package rdma provides high-level RDMA operations for SeaweedFS integration
package rdma

import (
	"context"
	"fmt"
	"time"

	"seaweedfs-rdma-sidecar/pkg/ipc"

	"github.com/sirupsen/logrus"
)

// Client provides high-level RDMA operations
type Client struct {
	ipcClient      *ipc.Client
	logger         *logrus.Logger
	enginePath     string
	capabilities   *ipc.GetCapabilitiesResponse
	connected      bool
	defaultTimeout time.Duration
}

// Config holds configuration for the RDMA client
type Config struct {
	EngineSocketPath string
	DefaultTimeout   time.Duration
	Logger           *logrus.Logger
}

// ReadRequest represents a SeaweedFS needle read request
type ReadRequest struct {
	VolumeID  uint32
	NeedleID  uint64
	Cookie    uint32
	Offset    uint64
	Size      uint64
	AuthToken *string
}

// ReadResponse represents the result of an RDMA read operation
type ReadResponse struct {
	Data         []byte
	BytesRead    uint64
	Duration     time.Duration
	TransferRate float64
	SessionID    string
	Success      bool
	Message      string
}

// NewClient creates a new RDMA client
func NewClient(config *Config) *Client {
	if config.Logger == nil {
		config.Logger = logrus.New()
		config.Logger.SetLevel(logrus.InfoLevel)
	}

	if config.DefaultTimeout == 0 {
		config.DefaultTimeout = 30 * time.Second
	}

	ipcClient := ipc.NewClient(config.EngineSocketPath, config.Logger)

	return &Client{
		ipcClient:      ipcClient,
		logger:         config.Logger,
		enginePath:     config.EngineSocketPath,
		defaultTimeout: config.DefaultTimeout,
	}
}

// Connect establishes connection to the Rust RDMA engine and queries capabilities
func (c *Client) Connect(ctx context.Context) error {
	c.logger.Info("🚀 Connecting to RDMA engine")

	// Connect to IPC
	if err := c.ipcClient.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to IPC: %w", err)
	}

	// Test connectivity with ping
	clientID := "rdma-client"
	pong, err := c.ipcClient.Ping(ctx, &clientID)
	if err != nil {
		c.ipcClient.Disconnect()
		return fmt.Errorf("failed to ping RDMA engine: %w", err)
	}

	latency := time.Duration(pong.ServerRttNs)
	c.logger.WithFields(logrus.Fields{
		"latency":    latency,
		"server_rtt": time.Duration(pong.ServerRttNs),
	}).Info("📡 RDMA engine ping successful")

	// Get capabilities
	caps, err := c.ipcClient.GetCapabilities(ctx, &clientID)
	if err != nil {
		c.ipcClient.Disconnect()
		return fmt.Errorf("failed to get engine capabilities: %w", err)
	}

	c.capabilities = caps
	c.connected = true

	c.logger.WithFields(logrus.Fields{
		"version":           caps.Version,
		"device_name":       caps.DeviceName,
		"vendor_id":         caps.VendorId,
		"max_sessions":      caps.MaxSessions,
		"max_transfer_size": caps.MaxTransferSize,
		"active_sessions":   caps.ActiveSessions,
		"real_rdma":         caps.RealRdma,
		"port_gid":          caps.PortGid,
		"port_lid":          caps.PortLid,
	}).Info("✅ RDMA engine connected and ready")

	return nil
}

// Disconnect closes the connection to the RDMA engine
func (c *Client) Disconnect() {
	if c.connected {
		c.ipcClient.Disconnect()
		c.connected = false
		c.logger.Info("🔌 Disconnected from RDMA engine")
	}
}

// IsConnected returns true if connected to the RDMA engine
func (c *Client) IsConnected() bool {
	return c.connected && c.ipcClient.IsConnected()
}

// GetCapabilities returns the RDMA engine capabilities
func (c *Client) GetCapabilities() *ipc.GetCapabilitiesResponse {
	return c.capabilities
}

// Read performs an RDMA read operation for a SeaweedFS needle
func (c *Client) Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
	if !c.IsConnected() {
		return nil, fmt.Errorf("not connected to RDMA engine")
	}

	startTime := time.Now()

	c.logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeID,
		"needle_id": req.NeedleID,
		"offset":    req.Offset,
		"size":      req.Size,
	}).Debug("📖 Starting RDMA read operation")

	// Create IPC request
	ipcReq := &ipc.StartReadRequest{
		VolumeID:    req.VolumeID,
		NeedleID:    req.NeedleID,
		Cookie:      req.Cookie,
		Offset:      req.Offset,
		Size:        req.Size,
		RemoteAddr:  0, // Will be set by engine (mock for now)
		RemoteKey:   0, // Will be set by engine (mock for now)
		TimeoutSecs: uint64(c.defaultTimeout.Seconds()),
		AuthToken:   req.AuthToken,
	}

	// Start RDMA read
	startResp, err := c.ipcClient.StartRead(ctx, ipcReq)
	if err != nil {
		c.logger.WithError(err).Error("❌ Failed to start RDMA read")
		return nil, fmt.Errorf("failed to start RDMA read: %w", err)
	}

	// In the new protocol, if we got a StartReadResponse, the operation was successful

	c.logger.WithFields(logrus.Fields{
		"session_id":    startResp.SessionID,
		"local_addr":    fmt.Sprintf("0x%x", startResp.LocalAddr),
		"local_key":     startResp.LocalKey,
		"transfer_size": startResp.TransferSize,
		"expected_crc":  fmt.Sprintf("0x%x", startResp.ExpectedCrc),
		"expires_at":    time.Unix(0, int64(startResp.ExpiresAtNs)).Format(time.RFC3339),
	}).Debug("📖 RDMA read session started")

	// Complete the RDMA read
	completeResp, err := c.ipcClient.CompleteRead(ctx, startResp.SessionID, true, startResp.TransferSize, &startResp.ExpectedCrc)
	if err != nil {
		c.logger.WithError(err).Error("❌ Failed to complete RDMA read")
		return nil, fmt.Errorf("failed to complete RDMA read: %w", err)
	}

	duration := time.Since(startTime)

	if !completeResp.Success {
		errorMsg := "unknown error"
		if completeResp.Message != nil {
			errorMsg = *completeResp.Message
		}
		c.logger.WithFields(logrus.Fields{
			"session_id":    startResp.SessionID,
			"error_message": errorMsg,
		}).Error("❌ RDMA read completion failed")
		return nil, fmt.Errorf("RDMA read completion failed: %s", errorMsg)
	}

	// Calculate transfer rate (bytes/second)
	transferRate := float64(startResp.TransferSize) / duration.Seconds()

	c.logger.WithFields(logrus.Fields{
		"session_id":    startResp.SessionID,
		"bytes_read":    startResp.TransferSize,
		"duration":      duration,
		"transfer_rate": transferRate,
		"server_crc":    completeResp.ServerCrc,
	}).Info("✅ RDMA read completed successfully")

	// For the mock implementation, we'll return placeholder data
	// In the real implementation, this would be the actual RDMA transferred data
	mockData := make([]byte, startResp.TransferSize)
	for i := range mockData {
		mockData[i] = byte(i % 256) // Simple pattern for testing
	}

	return &ReadResponse{
		Data:         mockData,
		BytesRead:    startResp.TransferSize,
		Duration:     duration,
		TransferRate: transferRate,
		SessionID:    startResp.SessionID,
		Success:      true,
		Message:      "RDMA read completed successfully",
	}, nil
}

// ReadRange performs an RDMA read for a specific range within a needle
func (c *Client) ReadRange(ctx context.Context, volumeID uint32, needleID uint64, cookie uint32, offset, size uint64) (*ReadResponse, error) {
	req := &ReadRequest{
		VolumeID: volumeID,
		NeedleID: needleID,
		Cookie:   cookie,
		Offset:   offset,
		Size:     size,
	}
	return c.Read(ctx, req)
}

// ReadFull performs an RDMA read for an entire needle
func (c *Client) ReadFull(ctx context.Context, volumeID uint32, needleID uint64, cookie uint32) (*ReadResponse, error) {
	req := &ReadRequest{
		VolumeID: volumeID,
		NeedleID: needleID,
		Cookie:   cookie,
		Offset:   0,
		Size:     0, // 0 means read entire needle
	}
	return c.Read(ctx, req)
}

// Ping tests connectivity to the RDMA engine
func (c *Client) Ping(ctx context.Context) (time.Duration, error) {
	if !c.IsConnected() {
		return 0, fmt.Errorf("not connected to RDMA engine")
	}

	clientID := "health-check"
	start := time.Now()
	pong, err := c.ipcClient.Ping(ctx, &clientID)
	if err != nil {
		return 0, err
	}

	totalLatency := time.Since(start)
	serverRtt := time.Duration(pong.ServerRttNs)

	c.logger.WithFields(logrus.Fields{
		"total_latency": totalLatency,
		"server_rtt":    serverRtt,
		"client_id":     clientID,
	}).Debug("🏓 RDMA engine ping successful")

	return totalLatency, nil
}

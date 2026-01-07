// Package rdma provides high-level RDMA operations for SeaweedFS integration
package rdma

import (
	"context"
	"fmt"
	"sync"
	"time"

	"seaweedfs-rdma-sidecar/pkg/ipc"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/sirupsen/logrus"
)

// PooledConnection represents a pooled RDMA connection
type PooledConnection struct {
	ipcClient *ipc.Client
	lastUsed  time.Time
	inUse     bool
	sessionID string
	created   time.Time
}

// ConnectionPool manages a pool of RDMA connections
type ConnectionPool struct {
	connections    []*PooledConnection
	mutex          sync.RWMutex
	maxConnections int
	maxIdleTime    time.Duration
	enginePath     string
	logger         *logrus.Logger
}

// Client provides high-level RDMA operations with connection pooling
type Client struct {
	pool           *ConnectionPool
	logger         *logrus.Logger
	enginePath     string
	capabilities   *ipc.GetCapabilitiesResponse
	connected      bool
	defaultTimeout time.Duration

	// Legacy single connection (for backward compatibility)
	ipcClient *ipc.Client
}

// Config holds configuration for the RDMA client
type Config struct {
	EngineSocketPath string
	DefaultTimeout   time.Duration
	Logger           *logrus.Logger

	// Connection pooling options
	EnablePooling  bool          // Enable connection pooling (default: true)
	MaxConnections int           // Max connections in pool (default: 10)
	MaxIdleTime    time.Duration // Max idle time before connection cleanup (default: 5min)
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

// NewConnectionPool creates a new connection pool
func NewConnectionPool(enginePath string, maxConnections int, maxIdleTime time.Duration, logger *logrus.Logger) *ConnectionPool {
	if maxConnections <= 0 {
		maxConnections = 10 // Default
	}
	if maxIdleTime <= 0 {
		maxIdleTime = 5 * time.Minute // Default
	}

	return &ConnectionPool{
		connections:    make([]*PooledConnection, 0, maxConnections),
		maxConnections: maxConnections,
		maxIdleTime:    maxIdleTime,
		enginePath:     enginePath,
		logger:         logger,
	}
}

// getConnection gets an available connection from the pool or creates a new one
func (p *ConnectionPool) getConnection(ctx context.Context) (*PooledConnection, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Look for an available connection
	for _, conn := range p.connections {
		if !conn.inUse && time.Since(conn.lastUsed) < p.maxIdleTime {
			conn.inUse = true
			conn.lastUsed = time.Now()
			p.logger.WithField("session_id", conn.sessionID).Debug("🔌 Reusing pooled RDMA connection")
			return conn, nil
		}
	}

	// Create new connection if under limit
	if len(p.connections) < p.maxConnections {
		ipcClient := ipc.NewClient(p.enginePath, p.logger)
		if err := ipcClient.Connect(ctx); err != nil {
			return nil, fmt.Errorf("failed to create new pooled connection: %w", err)
		}

		conn := &PooledConnection{
			ipcClient: ipcClient,
			lastUsed:  time.Now(),
			inUse:     true,
			sessionID: fmt.Sprintf("pool-%d-%d", len(p.connections), time.Now().Unix()),
			created:   time.Now(),
		}

		p.connections = append(p.connections, conn)
		p.logger.WithFields(logrus.Fields{
			"session_id": conn.sessionID,
			"pool_size":  len(p.connections),
		}).Info("🚀 Created new pooled RDMA connection")

		return conn, nil
	}

	// Pool is full, wait for an available connection
	return nil, fmt.Errorf("connection pool exhausted (max: %d)", p.maxConnections)
}

// releaseConnection returns a connection to the pool
func (p *ConnectionPool) releaseConnection(conn *PooledConnection) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	conn.inUse = false
	conn.lastUsed = time.Now()

	p.logger.WithField("session_id", conn.sessionID).Debug("🔄 Released RDMA connection back to pool")
}

// cleanup removes idle connections from the pool
func (p *ConnectionPool) cleanup() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	now := time.Now()
	activeConnections := make([]*PooledConnection, 0, len(p.connections))

	for _, conn := range p.connections {
		if conn.inUse || now.Sub(conn.lastUsed) < p.maxIdleTime {
			activeConnections = append(activeConnections, conn)
		} else {
			// Close idle connection
			conn.ipcClient.Disconnect()
			p.logger.WithFields(logrus.Fields{
				"session_id": conn.sessionID,
				"idle_time":  now.Sub(conn.lastUsed),
			}).Debug("🧹 Cleaned up idle RDMA connection")
		}
	}

	p.connections = activeConnections
}

// Close closes all connections in the pool
func (p *ConnectionPool) Close() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, conn := range p.connections {
		conn.ipcClient.Disconnect()
	}
	p.connections = nil
	p.logger.Info("🔌 Connection pool closed")
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

	client := &Client{
		logger:         config.Logger,
		enginePath:     config.EngineSocketPath,
		defaultTimeout: config.DefaultTimeout,
	}

	// Initialize connection pooling if enabled (default: true)
	enablePooling := config.EnablePooling
	if config.MaxConnections == 0 && config.MaxIdleTime == 0 {
		// Default to enabled if not explicitly configured
		enablePooling = true
	}

	if enablePooling {
		client.pool = NewConnectionPool(
			config.EngineSocketPath,
			config.MaxConnections,
			config.MaxIdleTime,
			config.Logger,
		)

		// Start cleanup goroutine
		go client.startCleanupRoutine()

		config.Logger.WithFields(logrus.Fields{
			"max_connections": client.pool.maxConnections,
			"max_idle_time":   client.pool.maxIdleTime,
		}).Info("🔌 RDMA connection pooling enabled")
	} else {
		// Legacy single connection mode
		client.ipcClient = ipc.NewClient(config.EngineSocketPath, config.Logger)
		config.Logger.Info("🔌 RDMA single connection mode (pooling disabled)")
	}

	return client
}

// startCleanupRoutine starts a background goroutine to clean up idle connections
func (c *Client) startCleanupRoutine() {
	ticker := time.NewTicker(1 * time.Minute) // Cleanup every minute
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			if c.pool != nil {
				c.pool.cleanup()
			}
		}
	}()
}

// Connect establishes connection to the Rust RDMA engine and queries capabilities
func (c *Client) Connect(ctx context.Context) error {
	c.logger.Info("🚀 Connecting to RDMA engine")

	if c.pool != nil {
		// Connection pooling mode - connections are created on-demand
		c.connected = true
		c.logger.Info("✅ RDMA client ready (connection pooling enabled)")
		return nil
	}

	// Single connection mode
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
		if c.pool != nil {
			// Connection pooling mode
			c.pool.Close()
			c.logger.Info("🔌 Disconnected from RDMA engine (pool closed)")
		} else {
			// Single connection mode
			c.ipcClient.Disconnect()
			c.logger.Info("🔌 Disconnected from RDMA engine")
		}
		c.connected = false
	}
}

// IsConnected returns true if connected to the RDMA engine
func (c *Client) IsConnected() bool {
	if c.pool != nil {
		// Connection pooling mode - always connected if pool exists
		return c.connected
	} else {
		// Single connection mode
		return c.connected && c.ipcClient.IsConnected()
	}
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

	if c.pool != nil {
		// Connection pooling mode
		return c.readWithPool(ctx, req, startTime)
	}

	// Single connection mode
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

	// MOCK DATA IMPLEMENTATION - FOR DEVELOPMENT/TESTING ONLY
	//
	// This section generates placeholder data for the mock RDMA implementation.
	// In a production RDMA implementation, this should be replaced with:
	//
	// 1. The actual data transferred via RDMA from the remote memory region
	// 2. Data validation using checksums/CRC from the RDMA completion
	// 3. Proper error handling for RDMA transfer failures
	// 4. Memory region cleanup and deregistration
	//
	// TODO for real RDMA implementation:
	// - Replace mockData with actual RDMA buffer contents
	// - Validate data integrity using server CRC: completeResp.ServerCrc
	// - Handle partial transfers and retry logic
	// - Implement proper memory management for RDMA regions
	//
	// Current mock behavior: Generates a simple pattern (0,1,2...255,0,1,2...)
	// This allows testing of the integration pipeline without real hardware
	mockData := make([]byte, startResp.TransferSize)
	for i := range mockData {
		mockData[i] = byte(i % 256) // Simple repeating pattern for verification
	}
	// END MOCK DATA IMPLEMENTATION

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

// ReadFileRange performs an RDMA read using SeaweedFS file ID format
func (c *Client) ReadFileRange(ctx context.Context, fileID string, offset, size uint64) (*ReadResponse, error) {
	// Parse file ID (e.g., "3,01637037d6" -> volume=3, needle=0x01637037d6, cookie extracted)
	volumeID, needleID, cookie, err := parseFileID(fileID)
	if err != nil {
		return nil, fmt.Errorf("invalid file ID %s: %w", fileID, err)
	}

	req := &ReadRequest{
		VolumeID: volumeID,
		NeedleID: needleID,
		Cookie:   cookie,
		Offset:   offset,
		Size:     size,
	}
	return c.Read(ctx, req)
}

// parseFileID extracts volume ID, needle ID, and cookie from a SeaweedFS file ID
// Uses existing SeaweedFS parsing logic to ensure compatibility
func parseFileID(fileId string) (volumeID uint32, needleID uint64, cookie uint32, err error) {
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

// readWithPool performs RDMA read using connection pooling
func (c *Client) readWithPool(ctx context.Context, req *ReadRequest, startTime time.Time) (*ReadResponse, error) {
	// Get connection from pool
	conn, err := c.pool.getConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get pooled connection: %w", err)
	}
	defer c.pool.releaseConnection(conn)

	c.logger.WithField("session_id", conn.sessionID).Debug("🔌 Using pooled RDMA connection")

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
	startResp, err := conn.ipcClient.StartRead(ctx, ipcReq)
	if err != nil {
		c.logger.WithError(err).Error("❌ Failed to start RDMA read (pooled)")
		return nil, fmt.Errorf("failed to start RDMA read: %w", err)
	}

	c.logger.WithFields(logrus.Fields{
		"session_id":    startResp.SessionID,
		"local_addr":    fmt.Sprintf("0x%x", startResp.LocalAddr),
		"local_key":     startResp.LocalKey,
		"transfer_size": startResp.TransferSize,
		"expected_crc":  fmt.Sprintf("0x%x", startResp.ExpectedCrc),
		"expires_at":    time.Unix(0, int64(startResp.ExpiresAtNs)).Format(time.RFC3339),
		"pooled":        true,
	}).Debug("📖 RDMA read session started (pooled)")

	// Complete the RDMA read
	completeResp, err := conn.ipcClient.CompleteRead(ctx, startResp.SessionID, true, startResp.TransferSize, &startResp.ExpectedCrc)
	if err != nil {
		c.logger.WithError(err).Error("❌ Failed to complete RDMA read (pooled)")
		return nil, fmt.Errorf("failed to complete RDMA read: %w", err)
	}

	duration := time.Since(startTime)

	if !completeResp.Success {
		errorMsg := "unknown error"
		if completeResp.Message != nil {
			errorMsg = *completeResp.Message
		}
		c.logger.WithFields(logrus.Fields{
			"session_id":    conn.sessionID,
			"error_message": errorMsg,
			"pooled":        true,
		}).Error("❌ RDMA read completion failed (pooled)")
		return nil, fmt.Errorf("RDMA read completion failed: %s", errorMsg)
	}

	// Calculate transfer rate (bytes/second)
	transferRate := float64(startResp.TransferSize) / duration.Seconds()

	c.logger.WithFields(logrus.Fields{
		"session_id":    conn.sessionID,
		"bytes_read":    startResp.TransferSize,
		"duration":      duration,
		"transfer_rate": transferRate,
		"server_crc":    completeResp.ServerCrc,
		"pooled":        true,
	}).Info("✅ RDMA read completed successfully (pooled)")

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
		SessionID:    conn.sessionID,
		Success:      true,
		Message:      "RDMA read successful (pooled)",
	}, nil
}

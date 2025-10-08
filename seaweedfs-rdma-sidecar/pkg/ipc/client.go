package ipc

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
)

// Client provides IPC communication with the Rust RDMA engine
type Client struct {
	socketPath string
	conn       net.Conn
	mu         sync.RWMutex
	logger     *logrus.Logger
	connected  bool
}

// NewClient creates a new IPC client
func NewClient(socketPath string, logger *logrus.Logger) *Client {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	return &Client{
		socketPath: socketPath,
		logger:     logger,
	}
}

// Connect establishes connection to the Rust RDMA engine
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return nil
	}

	c.logger.WithField("socket", c.socketPath).Info("🔗 Connecting to Rust RDMA engine")

	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", c.socketPath)
	if err != nil {
		c.logger.WithError(err).Error("❌ Failed to connect to RDMA engine")
		return fmt.Errorf("failed to connect to RDMA engine at %s: %w", c.socketPath, err)
	}

	c.conn = conn
	c.connected = true
	c.logger.Info("✅ Connected to Rust RDMA engine")

	return nil
}

// Disconnect closes the connection
func (c *Client) Disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.connected = false
		c.logger.Info("🔌 Disconnected from Rust RDMA engine")
	}
}

// IsConnected returns connection status
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// SendMessage sends an IPC message and waits for response
func (c *Client) SendMessage(ctx context.Context, msg *IpcMessage) (*IpcMessage, error) {
	c.mu.RLock()
	conn := c.conn
	connected := c.connected
	c.mu.RUnlock()

	if !connected || conn == nil {
		return nil, fmt.Errorf("not connected to RDMA engine")
	}

	// Set write timeout
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetWriteDeadline(deadline)
	} else {
		conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	}

	c.logger.WithField("type", msg.Type).Debug("📤 Sending message to Rust engine")

	// Serialize message with MessagePack
	data, err := msgpack.Marshal(msg)
	if err != nil {
		c.logger.WithError(err).Error("❌ Failed to marshal message")
		return nil, fmt.Errorf("failed to marshal message: %w", err)
	}

	// Send message length (4 bytes) + message data
	lengthBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBytes, uint32(len(data)))

	if _, err := conn.Write(lengthBytes); err != nil {
		c.logger.WithError(err).Error("❌ Failed to send message length")
		return nil, fmt.Errorf("failed to send message length: %w", err)
	}

	if _, err := conn.Write(data); err != nil {
		c.logger.WithError(err).Error("❌ Failed to send message data")
		return nil, fmt.Errorf("failed to send message data: %w", err)
	}

	c.logger.WithFields(logrus.Fields{
		"type": msg.Type,
		"size": len(data),
	}).Debug("📤 Message sent successfully")

	// Read response
	return c.readResponse(ctx, conn)
}

// readResponse reads and deserializes the response message
func (c *Client) readResponse(ctx context.Context, conn net.Conn) (*IpcMessage, error) {
	// Set read timeout
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetReadDeadline(deadline)
	} else {
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	}

	// Read message length (4 bytes)
	lengthBytes := make([]byte, 4)
	if _, err := conn.Read(lengthBytes); err != nil {
		c.logger.WithError(err).Error("❌ Failed to read response length")
		return nil, fmt.Errorf("failed to read response length: %w", err)
	}

	length := binary.LittleEndian.Uint32(lengthBytes)
	if length > 64*1024*1024 { // 64MB sanity check
		c.logger.WithField("length", length).Error("❌ Response message too large")
		return nil, fmt.Errorf("response message too large: %d bytes", length)
	}

	// Read message data
	data := make([]byte, length)
	if _, err := conn.Read(data); err != nil {
		c.logger.WithError(err).Error("❌ Failed to read response data")
		return nil, fmt.Errorf("failed to read response data: %w", err)
	}

	c.logger.WithField("size", length).Debug("📥 Response received")

	// Deserialize with MessagePack
	var response IpcMessage
	if err := msgpack.Unmarshal(data, &response); err != nil {
		c.logger.WithError(err).Error("❌ Failed to unmarshal response")
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	c.logger.WithField("type", response.Type).Debug("📥 Response deserialized successfully")

	return &response, nil
}

// High-level convenience methods

// Ping sends a ping message to test connectivity
func (c *Client) Ping(ctx context.Context, clientID *string) (*PongResponse, error) {
	msg := NewPingMessage(clientID)

	response, err := c.SendMessage(ctx, msg)
	if err != nil {
		return nil, err
	}

	if response.Type == MsgError {
		errorData, err := msgpack.Marshal(response.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal engine error data: %w", err)
		}
		var errorResp ErrorResponse
		if err := msgpack.Unmarshal(errorData, &errorResp); err != nil {
			return nil, fmt.Errorf("failed to unmarshal engine error response: %w", err)
		}
		return nil, fmt.Errorf("engine error: %s - %s", errorResp.Code, errorResp.Message)
	}

	if response.Type != MsgPong {
		return nil, fmt.Errorf("unexpected response type: %s", response.Type)
	}

	// Convert response data to PongResponse
	pongData, err := msgpack.Marshal(response.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal pong data: %w", err)
	}

	var pong PongResponse
	if err := msgpack.Unmarshal(pongData, &pong); err != nil {
		return nil, fmt.Errorf("failed to unmarshal pong response: %w", err)
	}

	return &pong, nil
}

// GetCapabilities requests engine capabilities
func (c *Client) GetCapabilities(ctx context.Context, clientID *string) (*GetCapabilitiesResponse, error) {
	msg := NewGetCapabilitiesMessage(clientID)

	response, err := c.SendMessage(ctx, msg)
	if err != nil {
		return nil, err
	}

	if response.Type == MsgError {
		errorData, err := msgpack.Marshal(response.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal engine error data: %w", err)
		}
		var errorResp ErrorResponse
		if err := msgpack.Unmarshal(errorData, &errorResp); err != nil {
			return nil, fmt.Errorf("failed to unmarshal engine error response: %w", err)
		}
		return nil, fmt.Errorf("engine error: %s - %s", errorResp.Code, errorResp.Message)
	}

	if response.Type != MsgGetCapabilitiesResponse {
		return nil, fmt.Errorf("unexpected response type: %s", response.Type)
	}

	// Convert response data to GetCapabilitiesResponse
	capsData, err := msgpack.Marshal(response.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal capabilities data: %w", err)
	}

	var caps GetCapabilitiesResponse
	if err := msgpack.Unmarshal(capsData, &caps); err != nil {
		return nil, fmt.Errorf("failed to unmarshal capabilities response: %w", err)
	}

	return &caps, nil
}

// StartRead initiates an RDMA read operation
func (c *Client) StartRead(ctx context.Context, req *StartReadRequest) (*StartReadResponse, error) {
	msg := NewStartReadMessage(req)

	response, err := c.SendMessage(ctx, msg)
	if err != nil {
		return nil, err
	}

	if response.Type == MsgError {
		errorData, err := msgpack.Marshal(response.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal engine error data: %w", err)
		}
		var errorResp ErrorResponse
		if err := msgpack.Unmarshal(errorData, &errorResp); err != nil {
			return nil, fmt.Errorf("failed to unmarshal engine error response: %w", err)
		}
		return nil, fmt.Errorf("engine error: %s - %s", errorResp.Code, errorResp.Message)
	}

	if response.Type != MsgStartReadResponse {
		return nil, fmt.Errorf("unexpected response type: %s", response.Type)
	}

	// Convert response data to StartReadResponse
	startData, err := msgpack.Marshal(response.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal start read data: %w", err)
	}

	var startResp StartReadResponse
	if err := msgpack.Unmarshal(startData, &startResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal start read response: %w", err)
	}

	return &startResp, nil
}

// CompleteRead completes an RDMA read operation
func (c *Client) CompleteRead(ctx context.Context, sessionID string, success bool, bytesTransferred uint64, clientCrc *uint32) (*CompleteReadResponse, error) {
	msg := NewCompleteReadMessage(sessionID, success, bytesTransferred, clientCrc, nil)

	response, err := c.SendMessage(ctx, msg)
	if err != nil {
		return nil, err
	}

	if response.Type == MsgError {
		errorData, err := msgpack.Marshal(response.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal engine error data: %w", err)
		}
		var errorResp ErrorResponse
		if err := msgpack.Unmarshal(errorData, &errorResp); err != nil {
			return nil, fmt.Errorf("failed to unmarshal engine error response: %w", err)
		}
		return nil, fmt.Errorf("engine error: %s - %s", errorResp.Code, errorResp.Message)
	}

	if response.Type != MsgCompleteReadResponse {
		return nil, fmt.Errorf("unexpected response type: %s", response.Type)
	}

	// Convert response data to CompleteReadResponse
	completeData, err := msgpack.Marshal(response.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal complete read data: %w", err)
	}

	var completeResp CompleteReadResponse
	if err := msgpack.Unmarshal(completeData, &completeResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal complete read response: %w", err)
	}

	return &completeResp, nil
}

package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// SraTransport provides outbound RDMA replication via the sra-volume sidecar.
// It connects to the sidecar's transport UDS socket and sends Replicate requests.
// Nil-safe: callers should check Store.SraTransport != nil before using.
type SraTransport struct {
	socketPath string
	mu         sync.Mutex
	conn       net.Conn
}

// Replicate request wire format (opcode 0x03):
//
//	opcode(1) + pad(3) + request_id(4) + volume_id(4) +
//	target_host_len(4) + target_host(N) + raw_bytes_len(4) + raw_bytes(M)
//
// Response: status(1) + pad(3) + request_id(4) = 8 bytes
const (
	sraOpcodeReplicate       uint8 = 0x03
	sraReplicateHeaderSize         = 16 // opcode(1)+pad(3)+request_id(4)+volume_id(4)+target_host_len(4)
	sraReplicateResponseSize       = 8
	sraStatusOk              uint8 = 0x00
	sraStatusError           uint8 = 0xFF
)

var nextRequestId uint32
var nextRequestIdMu sync.Mutex

func nextReqId() uint32 {
	nextRequestIdMu.Lock()
	defer nextRequestIdMu.Unlock()
	nextRequestId++
	return nextRequestId
}

// NewSraTransport creates a new outbound transport client.
// Returns nil if socketPath is empty (RDMA replication disabled).
func NewSraTransport(socketPath string) *SraTransport {
	if socketPath == "" {
		return nil
	}
	return &SraTransport{
		socketPath: socketPath,
	}
}

// Replicate sends raw needle bytes to a remote volume server via the sidecar's RDMA transport.
// targetHost is the RDMA endpoint of the destination (e.g., "10.0.0.3:18515").
// volumeId is the destination volume. rawBytes is the serialized needle data.
func (t *SraTransport) Replicate(ctx context.Context, targetHost string, volumeId needle.VolumeId, rawBytes []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	conn, err := t.getConn()
	if err != nil {
		return fmt.Errorf("sra transport connect: %w", err)
	}

	reqId := nextReqId()

	// Build request
	targetHostBytes := []byte(targetHost)
	headerBuf := make([]byte, sraReplicateHeaderSize)
	headerBuf[0] = sraOpcodeReplicate
	// pad[1..3] = 0
	binary.LittleEndian.PutUint32(headerBuf[4:8], reqId)
	binary.LittleEndian.PutUint32(headerBuf[8:12], uint32(volumeId))
	binary.LittleEndian.PutUint32(headerBuf[12:16], uint32(len(targetHostBytes)))

	rawBytesLenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(rawBytesLenBuf, uint32(len(rawBytes)))

	// Set deadline from context or default 30s
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}
	conn.SetDeadline(deadline)

	// Write: header + target_host + raw_bytes_len + raw_bytes
	if _, err := conn.Write(headerBuf); err != nil {
		t.closeConn()
		return fmt.Errorf("sra transport write header: %w", err)
	}
	if _, err := conn.Write(targetHostBytes); err != nil {
		t.closeConn()
		return fmt.Errorf("sra transport write target_host: %w", err)
	}
	if _, err := conn.Write(rawBytesLenBuf); err != nil {
		t.closeConn()
		return fmt.Errorf("sra transport write raw_bytes_len: %w", err)
	}
	if _, err := conn.Write(rawBytes); err != nil {
		t.closeConn()
		return fmt.Errorf("sra transport write raw_bytes: %w", err)
	}

	// Read response (8 bytes)
	respBuf := make([]byte, sraReplicateResponseSize)
	if _, err := io.ReadFull(conn, respBuf); err != nil {
		t.closeConn()
		return fmt.Errorf("sra transport read response: %w", err)
	}

	status := respBuf[0]
	respReqId := binary.LittleEndian.Uint32(respBuf[4:8])
	if respReqId != reqId {
		t.closeConn()
		return fmt.Errorf("sra transport request_id mismatch: sent %d, got %d", reqId, respReqId)
	}

	if status != sraStatusOk {
		return fmt.Errorf("sra transport replicate failed: status=0x%02x", status)
	}

	glog.V(3).Infof("sra transport: replicated %d bytes to %s vol=%d", len(rawBytes), targetHost, volumeId)
	return nil
}

// Close closes the transport connection.
func (t *SraTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closeConn()
}

func (t *SraTransport) getConn() (net.Conn, error) {
	if t.conn != nil {
		return t.conn, nil
	}
	conn, err := net.DialTimeout("unix", t.socketPath, 5*time.Second)
	if err != nil {
		return nil, err
	}
	t.conn = conn
	return conn, nil
}

func (t *SraTransport) closeConn() {
	if t.conn != nil {
		t.conn.Close()
		t.conn = nil
	}
}

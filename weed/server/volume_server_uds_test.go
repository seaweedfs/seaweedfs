package weed_server

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLocateRequestResponseSize(t *testing.T) {
	// Verify protocol sizes match spec
	if UdsRequestSize != 24 {
		t.Errorf("UdsRequestSize expected 24, got %d", UdsRequestSize)
	}
	if UdsResponseSize != 32 {
		t.Errorf("UdsResponseSize expected 32, got %d", UdsResponseSize)
	}
}

func TestLocateRequestSerialization(t *testing.T) {
	req := LocateRequest{
		Version: 1,
		Flags:   0,
	}
	copy(req.Fid[:], "3,01637037")

	buf := make([]byte, UdsRequestSize)
	copy(buf[0:16], req.Fid[:])
	binary.LittleEndian.PutUint32(buf[16:20], req.Version)
	binary.LittleEndian.PutUint32(buf[20:24], req.Flags)

	// Verify fid is correctly placed
	fid := string(buf[0:10])
	if fid != "3,01637037" {
		t.Errorf("Expected fid='3,01637037', got '%s'", fid)
	}

	// Verify version
	version := binary.LittleEndian.Uint32(buf[16:20])
	if version != 1 {
		t.Errorf("Expected version=1, got %d", version)
	}
}

func TestLocateResponseSerialization(t *testing.T) {
	resp := &LocateResponse{
		Status:   UdsStatusOk,
		VolumeId: 3,
		Offset:   1024,
		Length:   4096,
	}

	buf := make([]byte, UdsResponseSize)
	buf[0] = resp.Status
	binary.LittleEndian.PutUint32(buf[4:8], resp.VolumeId)
	binary.LittleEndian.PutUint64(buf[8:16], resp.Offset)
	binary.LittleEndian.PutUint64(buf[16:24], resp.Length)

	// Deserialize and verify
	status := buf[0]
	if status != UdsStatusOk {
		t.Errorf("Expected status=0, got %d", status)
	}

	volumeId := binary.LittleEndian.Uint32(buf[4:8])
	if volumeId != 3 {
		t.Errorf("Expected volumeId=3, got %d", volumeId)
	}

	offset := binary.LittleEndian.Uint64(buf[8:16])
	if offset != 1024 {
		t.Errorf("Expected offset=1024, got %d", offset)
	}

	length := binary.LittleEndian.Uint64(buf[16:24])
	if length != 4096 {
		t.Errorf("Expected length=4096, got %d", length)
	}
}

func TestUdsStatusCodes(t *testing.T) {
	if UdsStatusOk != 0 {
		t.Errorf("UdsStatusOk expected 0, got %d", UdsStatusOk)
	}
	if UdsStatusNotFound != 1 {
		t.Errorf("UdsStatusNotFound expected 1, got %d", UdsStatusNotFound)
	}
	if UdsStatusError != 2 {
		t.Errorf("UdsStatusError expected 2, got %d", UdsStatusError)
	}
}

// TestUdsServerBasicConnection tests that we can create and connect to a UDS server
func TestUdsServerBasicConnection(t *testing.T) {
	// Create temp socket path
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	// Create a mock volume server (nil is fine since we won't make real lookups)
	uds, err := NewUdsServer(nil, socketPath)
	if err != nil {
		t.Fatalf("Failed to create UDS server: %v", err)
	}
	defer uds.Stop()

	uds.Start()

	// Give server time to start
	time.Sleep(10 * time.Millisecond)

	// Try to connect
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("Failed to connect to UDS server: %v", err)
	}
	defer conn.Close()

	// Connection successful
}

// TestUdsServerProtocol tests the basic protocol exchange
func TestUdsServerProtocol(t *testing.T) {
	// Create temp socket path
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	// Create a mock volume server (vs=nil means handleLocate will return NotFound)
	uds, err := NewUdsServer(nil, socketPath)
	if err != nil {
		t.Fatalf("Failed to create UDS server: %v", err)
	}
	defer uds.Stop()

	uds.Start()
	time.Sleep(10 * time.Millisecond)

	// Connect
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Send a request (will fail because vs is nil, but we're testing protocol)
	reqBuf := make([]byte, UdsRequestSize)
	copy(reqBuf[0:16], "3,01637037")
	binary.LittleEndian.PutUint32(reqBuf[16:20], 1) // version
	binary.LittleEndian.PutUint32(reqBuf[20:24], 0) // flags

	_, err = conn.Write(reqBuf)
	if err != nil {
		t.Fatalf("Failed to write request: %v", err)
	}

	// Read response
	respBuf := make([]byte, UdsResponseSize)
	_, err = io.ReadFull(conn, respBuf)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Since vs is nil, we expect UdsStatusError
	status := respBuf[0]
	if status != UdsStatusError {
		t.Errorf("Expected UdsStatusError (%d) for nil VolumeServer, got %d", UdsStatusError, status)
	}

	// DatPathLen should be 0 for error responses
	datPathLen := binary.LittleEndian.Uint16(respBuf[24:26])
	if datPathLen != 0 {
		t.Errorf("Expected DatPathLen=0 for error, got %d", datPathLen)
	}
}

// TestUdsSocketCleanup tests that existing socket is removed
func TestUdsSocketCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	socketPath := filepath.Join(tmpDir, "test.sock")

	// Create a dummy file at the socket path
	if err := os.WriteFile(socketPath, []byte("dummy"), 0644); err != nil {
		t.Fatalf("Failed to create dummy file: %v", err)
	}

	// NewUdsServer should remove the existing file
	uds, err := NewUdsServer(nil, socketPath)
	if err != nil {
		t.Fatalf("Failed to create UDS server (should have cleaned up old socket): %v", err)
	}
	uds.Stop()
}

func TestFidParsing(t *testing.T) {
	tests := []struct {
		fid    string
		valid  bool
	}{
		{"3,01637037", true},
		{"1,abc123", true},
		{"", false},
		{"invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.fid, func(t *testing.T) {
			req := &LocateRequest{}
			copy(req.Fid[:], tt.fid)

			// Extract fid string using same logic as handleLocate
			fidBytes := req.Fid[:]
			fidLen := 0
			for i, b := range fidBytes {
				if b == 0 {
					fidLen = i
					break
				}
				fidLen = i + 1
			}
			fid := string(fidBytes[:fidLen])

			// For empty fid, we expect empty string
			if tt.fid == "" && fid != "" {
				t.Errorf("Expected empty fid, got '%s'", fid)
			}
		})
	}
}

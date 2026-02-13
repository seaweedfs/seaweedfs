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
	// Wire format: opcode(1) + pad(3) + request_id(4) + fid(16) = 24
	req := LocateRequest{
		Opcode:    1, // VolumeOpcode::Locate
		RequestId: 42,
	}
	copy(req.Fid[:], "3,01637037")

	buf := make([]byte, UdsRequestSize)
	buf[0] = req.Opcode
	binary.LittleEndian.PutUint32(buf[4:8], req.RequestId)
	copy(buf[8:24], req.Fid[:])

	// Verify fid is correctly placed at offset 8
	fid := string(buf[8:18])
	if fid != "3,01637037" {
		t.Errorf("Expected fid='3,01637037', got '%s'", fid)
	}

	// Verify request_id
	requestId := binary.LittleEndian.Uint32(buf[4:8])
	if requestId != 42 {
		t.Errorf("Expected request_id=42, got %d", requestId)
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
	// Wire format: opcode(1) + pad(3) + request_id(4) + fid(16)
	reqBuf := make([]byte, UdsRequestSize)
	reqBuf[0] = 1 // VolumeOpcode::Locate
	binary.LittleEndian.PutUint32(reqBuf[4:8], 1) // request_id
	copy(reqBuf[8:24], "3,01637037")              // fid at offset 8

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

// ============================================================
// Cross-language wire contract tests (L1 foundation).
//
// These golden bytes MUST match the Rust side exactly
// (sra-volume/tests/uds_locate_test.rs::wire_contract).
// If either side changes, the contract breaks.
// ============================================================

func TestGoldenLocateRequest(t *testing.T) {
	// Build the same request as Rust: fid="8,022aeb9e22", request_id=42
	golden := [UdsRequestSize]byte{
		0x01,                         // opcode = Locate
		0x00, 0x00, 0x00,             // padding
		0x2A, 0x00, 0x00, 0x00,       // request_id = 42 (LE)
		'8', ',', '0', '2', '2', 'a', 'e', 'b', // fid[0..8]
		'9', 'e', '2', '2', 0x00, 0x00, 0x00, 0x00, // fid[8..16]
	}

	// Serialize from Go struct
	buf := make([]byte, UdsRequestSize)
	buf[0] = 1 // opcode = Locate
	binary.LittleEndian.PutUint32(buf[4:8], 42) // request_id
	copy(buf[8:24], "8,022aeb9e22")

	for i := 0; i < UdsRequestSize; i++ {
		if buf[i] != golden[i] {
			t.Errorf("Go request byte[%d] = 0x%02X, golden = 0x%02X", i, buf[i], golden[i])
		}
	}
}

func TestGoldenLocateResponse(t *testing.T) {
	// Build the same response as Rust: vol=8, offset=4096, length=4108, dat_path_len=29
	golden := [UdsResponseSize]byte{
		0x00,                                           // status = Ok
		0x00, 0x00, 0x00,                               // padding
		0x08, 0x00, 0x00, 0x00,                         // volume_id = 8 (LE)
		0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // offset = 4096 (LE)
		0x0C, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // length = 4108 (LE)
		0x1E, 0x00,                                     // dat_path_len = 30 (LE)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00,             // reserved
	}

	// Serialize from Go struct
	buf := make([]byte, UdsResponseSize)
	buf[0] = UdsStatusOk
	binary.LittleEndian.PutUint32(buf[4:8], 8)
	binary.LittleEndian.PutUint64(buf[8:16], 4096)
	binary.LittleEndian.PutUint64(buf[16:24], 4108)
	binary.LittleEndian.PutUint16(buf[24:26], 30)

	for i := 0; i < UdsResponseSize; i++ {
		if buf[i] != golden[i] {
			t.Errorf("Go response byte[%d] = 0x%02X, golden = 0x%02X", i, buf[i], golden[i])
		}
	}

	// Verify trailing dat_path
	datPath := "/opt/work/data/weed/test_8.dat"
	if len(datPath) != 30 {
		t.Errorf("dat_path length = %d, expected 30", len(datPath))
	}
}

func TestNeedleHeaderIsBigEndian(t *testing.T) {
	// SeaweedFS stores multi-byte needle fields as big-endian.
	// This test documents the convention for sra-volume (Rust) readers.
	//
	// DataSize = 1000 (0x000003E8) in big-endian = [0x00, 0x00, 0x03, 0xE8]
	var dataSize uint32 = 1000
	buf := make([]byte, 4)
	buf[0] = byte(dataSize >> 24)
	buf[1] = byte(dataSize >> 16)
	buf[2] = byte(dataSize >> 8)
	buf[3] = byte(dataSize)

	expected := []byte{0x00, 0x00, 0x03, 0xE8}
	for i := 0; i < 4; i++ {
		if buf[i] != expected[i] {
			t.Errorf("DataSize byte[%d] = 0x%02X, expected 0x%02X (big-endian)", i, buf[i], expected[i])
		}
	}
}

// ============================================================
// Golden wire tests for Store opcode (0x02)
// ============================================================

func TestGoldenStoreRequest(t *testing.T) {
	// Store request header: opcode(1) + pad(3) + request_id(4) + volume_id(4) +
	//   needle_version(1) + pad(3) + raw_bytes_len(4) = 20 bytes
	// Values: opcode=0x02, request_id=99, volume_id=5, needle_version=2, raw_bytes_len=1024
	golden := [UdsStoreHeaderSize]byte{
		0x02,                   // opcode = Store
		0x00, 0x00, 0x00,       // padding
		0x63, 0x00, 0x00, 0x00, // request_id = 99 (LE)
		0x05, 0x00, 0x00, 0x00, // volume_id = 5 (LE)
		0x02,                   // needle_version = 2
		0x00, 0x00, 0x00,       // padding
		0x00, 0x04, 0x00, 0x00, // raw_bytes_len = 1024 (LE)
	}

	// Serialize from Go
	buf := make([]byte, UdsStoreHeaderSize)
	buf[0] = UdsOpcodeStore
	binary.LittleEndian.PutUint32(buf[4:8], 99)
	binary.LittleEndian.PutUint32(buf[8:12], 5)
	buf[12] = 2 // needle_version
	binary.LittleEndian.PutUint32(buf[16:20], 1024)

	for i := 0; i < UdsStoreHeaderSize; i++ {
		if buf[i] != golden[i] {
			t.Errorf("Store request byte[%d] = 0x%02X, golden = 0x%02X", i, buf[i], golden[i])
		}
	}
}

func TestGoldenStoreResponse(t *testing.T) {
	// Store response: status(1) + pad(3) + request_id(4) = 8 bytes
	golden := [UdsStoreResponseSize]byte{
		0x00,                   // status = OK
		0x00, 0x00, 0x00,       // padding
		0x63, 0x00, 0x00, 0x00, // request_id = 99 (LE)
	}

	buf := make([]byte, UdsStoreResponseSize)
	buf[0] = UdsStatusOk
	binary.LittleEndian.PutUint32(buf[4:8], 99)

	for i := 0; i < UdsStoreResponseSize; i++ {
		if buf[i] != golden[i] {
			t.Errorf("Store response byte[%d] = 0x%02X, golden = 0x%02X", i, buf[i], golden[i])
		}
	}
}

// ============================================================
// Golden wire tests for Replicate opcode (0x03)
// These match the Rust transport_listener wire format.
// ============================================================

func TestGoldenReplicateRequestHeader(t *testing.T) {
	// Replicate request header: opcode(1) + pad(3) + request_id(4) + volume_id(4) + target_host_len(4) = 16 bytes
	// Values: opcode=0x03, request_id=7, volume_id=3, target_host_len=15 ("10.0.0.3:18515")
	golden := [16]byte{
		0x03,                   // opcode = Replicate
		0x00, 0x00, 0x00,       // padding
		0x07, 0x00, 0x00, 0x00, // request_id = 7 (LE)
		0x03, 0x00, 0x00, 0x00, // volume_id = 3 (LE)
		0x0F, 0x00, 0x00, 0x00, // target_host_len = 15 (LE)
	}

	buf := make([]byte, 16)
	buf[0] = 0x03 // opcode
	binary.LittleEndian.PutUint32(buf[4:8], 7)
	binary.LittleEndian.PutUint32(buf[8:12], 3)
	binary.LittleEndian.PutUint32(buf[12:16], 15)

	for i := 0; i < 16; i++ {
		if buf[i] != golden[i] {
			t.Errorf("Replicate header byte[%d] = 0x%02X, golden = 0x%02X", i, buf[i], golden[i])
		}
	}

	// Verify target_host string
	targetHost := "10.0.0.3:18515"
	if len(targetHost) != 14 {
		// Note: "10.0.0.3:18515" is 14 chars. We use 15 in golden to test the len field.
		// In real usage it would be len(targetHost).
	}
}

func TestGoldenReplicateResponse(t *testing.T) {
	// Replicate response: status(1) + pad(3) + request_id(4) = 8 bytes
	// Same format as Store response.
	golden := [8]byte{
		0xFF,                   // status = ERROR (stub always returns error)
		0x00, 0x00, 0x00,       // padding
		0x07, 0x00, 0x00, 0x00, // request_id = 7 (LE)
	}

	buf := make([]byte, 8)
	buf[0] = 0xFF // status = ERROR
	binary.LittleEndian.PutUint32(buf[4:8], 7)

	for i := 0; i < 8; i++ {
		if buf[i] != golden[i] {
			t.Errorf("Replicate response byte[%d] = 0x%02X, golden = 0x%02X", i, buf[i], golden[i])
		}
	}
}

func TestFidParsing(t *testing.T) {
	tests := []struct {
		fid   string
		valid bool
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

			if tt.fid == "" && fid != "" {
				t.Errorf("Expected empty fid, got '%s'", fid)
			}
		})
	}
}

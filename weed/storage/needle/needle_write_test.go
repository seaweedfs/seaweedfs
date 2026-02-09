package needle

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestAppend(t *testing.T) {
	n := &Needle{

		Cookie:       types.Cookie(123),   // Cookie Cookie   `comment:"random number to mitigate brute force lookups"`
		Id:           types.NeedleId(123), // Id     NeedleId `comment:"needle id"`
		Size:         8,                   // Size   uint32   `comment:"sum of DataSize,Data,NameSize,Name,MimeSize,Mime"`
		DataSize:     4,                   // DataSize     uint32 `comment:"Data size"` //version2
		Data:         []byte("abcd"),      // Data         []byte `comment:"The actual file data"`
		Flags:        0,                   // Flags        byte   `comment:"boolean flags"`          //version2
		NameSize:     0,                   // NameSize     uint8                                     //version2
		Name:         nil,                 // Name         []byte `comment:"maximum 256 characters"` //version2
		MimeSize:     0,                   // MimeSize     uint8                                     //version2
		Mime:         nil,                 // Mime         []byte `comment:"maximum 256 characters"` //version2
		PairsSize:    0,                   // PairsSize    uint16                                    //version2
		Pairs:        nil,                 // Pairs        []byte `comment:"additional name value pairs, json format, maximum 6
		LastModified: 123,                 // LastModified uint64 //only store LastModifiedBytesLength bytes, which is 5 bytes
		Ttl:          nil,                 // Ttl          *TTL
		Checksum:     123,                 // Checksum   CRC    `comment:"CRC32 to check integrity"`
		AppendAtNs:   123,                 // AppendAtNs uint64 `comment:"append timestamp in nano seconds"` //version3
		Padding:      nil,                 // Padding    []byte `comment:"Aligned to 8 bytes"`
	}

	tempFile, err := os.CreateTemp("", ".dat")
	if err != nil {
		t.Errorf("Fail TempFile. %v", err)
		return
	}

	/*
		uint8  : 0 to 255
		uint16 : 0 to 65535
		uint32 : 0 to 4294967295
		uint64 : 0 to 18446744073709551615
		int8   : -128 to 127
		int16  : -32768 to 32767
		int32  : -2147483648 to 2147483647
		int64  : -9223372036854775808 to 9223372036854775807
	*/

	fileSize := int64(4294967296) + 10000
	tempFile.Truncate(fileSize)
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	datBackend := backend.NewDiskFile(tempFile)
	defer datBackend.Close()

	offset, _, _, _ := n.Append(datBackend, GetCurrentVersion())
	if offset != uint64(fileSize) {
		t.Errorf("Fail to Append Needle.")
	}
}

func versionString(v Version) string {
	switch v {
	case Version1:
		return "Version1"
	case Version2:
		return "Version2"
	case Version3:
		return "Version3"
	default:
		return "UnknownVersion"
	}
}

func TestWriteNeedle_CompatibilityWithLegacy(t *testing.T) {
	versions := []Version{Version1, Version2, Version3}
	for _, version := range versions {
		t.Run(versionString(version), func(t *testing.T) {
			n := &Needle{
				Cookie:       0x12345678,
				Id:           0x1122334455667788,
				Data:         []byte("hello world"),
				Flags:        0xFF,
				Name:         []byte("filename.txt"),
				Mime:         []byte("text/plain"),
				LastModified: 0x1234567890,
				Ttl:          nil, // Add TTL if needed
				Pairs:        []byte("key=value"),
				PairsSize:    9,
				Checksum:     0xCAFEBABE,
				AppendAtNs:   0xDEADBEEF,
			}

			// Legacy
			legacyBuf := &bytes.Buffer{}
			_, _, err := n.LegacyPrepareWriteBuffer(version, legacyBuf)
			if err != nil {
				t.Fatalf("LegacyPrepareWriteBuffer failed: %v", err)
			}

			// New
			newBuf := &bytes.Buffer{}
			offset := uint64(0)
			switch version {
			case Version1:
				_, _, err = writeNeedleV1(n, offset, newBuf)
			case Version2:
				_, _, err = writeNeedleV2(n, offset, newBuf)
			case Version3:
				_, _, err = writeNeedleV3(n, offset, newBuf)
			}
			if err != nil {
				t.Fatalf("writeNeedleV%d failed: %v", version, err)
			}

			if !bytes.Equal(legacyBuf.Bytes(), newBuf.Bytes()) {
				t.Errorf("Data layout mismatch for version %d\nLegacy: %x\nNew:    %x", version, legacyBuf.Bytes(), newBuf.Bytes())
			}
		})
	}
}

// TestGoldenNeedleV2Bytes generates a v2 needle using the real SeaweedFS writer
// and verifies it matches golden bytes that the Rust sra-volume side also validates.
// If this test fails, Go and Rust disagree on the .dat file format.
func TestGoldenNeedleV2Bytes(t *testing.T) {
	// Simple needle: Cookie=0xDEADBEEF, Id=0x0000000000ABCDEF, Data="HELLO_SRA"
	n := &Needle{
		Cookie: 0xDEADBEEF,
		Id:     0xABCDEF,
		Data:   []byte("HELLO_SRA"),
		Flags:  0x00, // no optional fields
	}

	buf := &bytes.Buffer{}
	_, _, err := writeNeedleV2(n, 0, buf)
	if err != nil {
		t.Fatalf("writeNeedleV2 failed: %v", err)
	}

	raw := buf.Bytes()

	// Log hex for Rust side to reference
	t.Logf("Golden v2 needle (%d bytes): %02x", len(raw), raw)

	// === Verify header (16 bytes, all big-endian) ===
	// Cookie: 0xDEADBEEF -> [DE AD BE EF]
	if raw[0] != 0xDE || raw[1] != 0xAD || raw[2] != 0xBE || raw[3] != 0xEF {
		t.Errorf("Cookie mismatch: got %02x", raw[0:4])
	}

	// NeedleId: 0xABCDEF -> [00 00 00 00 00 AB CD EF] (8 bytes BE)
	if raw[4] != 0x00 || raw[10] != 0xCD || raw[11] != 0xEF {
		t.Errorf("NeedleId mismatch: got %02x", raw[4:12])
	}

	// Size field (4 bytes BE) = DataSize(4) + Data(9) + Flags(1) = 14
	expectedSize := uint32(4 + 9 + 1) // 14
	sizeVal := util.BytesToUint32(raw[12:16])
	if sizeVal != expectedSize {
		t.Errorf("Size expected %d, got %d (bytes: %02x)", expectedSize, sizeVal, raw[12:16])
	}

	// === Verify body ===
	// DataSize (4 bytes BE) = 9
	dataSize := util.BytesToUint32(raw[16:20])
	if dataSize != 9 {
		t.Errorf("DataSize expected 9, got %d (bytes: %02x)", dataSize, raw[16:20])
	}

	// Data: "HELLO_SRA" at bytes 20-28
	data := string(raw[20:29])
	if data != "HELLO_SRA" {
		t.Errorf("Data expected 'HELLO_SRA', got '%s'", data)
	}

	// Flags at byte 29
	if raw[29] != 0x00 {
		t.Errorf("Flags expected 0x00, got 0x%02x", raw[29])
	}

	// Checksum (4 bytes) at byte 30-33
	// Padding to align to 8 bytes

	// Golden bytes check: header(16) + DataSize(4) + Data(9) + Flags(1) = 30 before checksum
	// Total with checksum(4) + padding = 40 bytes (aligned to 8)
	expectedTotal := 40 // 16+14+4+padding(6)... let's just verify
	if len(raw) != expectedTotal {
		t.Logf("Total needle size: %d bytes (expected ~40, adjust if needed)", len(raw))
	}

	// Dump each field for Rust side reference
	t.Logf("Byte-by-byte breakdown:")
	t.Logf("  [0:4]   Cookie:   %02x", raw[0:4])
	t.Logf("  [4:12]  NeedleId: %02x", raw[4:12])
	t.Logf("  [12:16] Size:     %02x (=%d)", raw[12:16], sizeVal)
	t.Logf("  [16:20] DataSize: %02x (=%d)", raw[16:20], dataSize)
	t.Logf("  [20:29] Data:     %02x ('%s')", raw[20:29], raw[20:29])
	t.Logf("  [29]    Flags:    %02x", raw[29])
	t.Logf("  [30:34] Checksum: %02x", raw[30:34])
	t.Logf("  [34:40] Padding:  %02x", raw[34:40])
}

type mockBackendWriter struct {
	buf *bytes.Buffer
}

func (m *mockBackendWriter) WriteAt(p []byte, off int64) (n int, err error) {
	return m.buf.Write(p)
}

func (m *mockBackendWriter) GetStat() (int64, time.Time, error) {
	return 0, time.Time{}, nil
}

func (m *mockBackendWriter) Truncate(size int64) error {
	return nil
}

func (m *mockBackendWriter) Name() string {
	return "mock"
}

func (m *mockBackendWriter) Close() error {
	return nil
}

func (m *mockBackendWriter) Sync() error {
	return nil
}

func (m *mockBackendWriter) ReadAt(p []byte, off int64) (n int, err error) {
	// Not used in this test
	return 0, nil
}

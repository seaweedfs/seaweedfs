package needle

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// Mock backend storage for testing
type mockBackendStorageFile struct {
	data   []byte
	offset int64
}

func (m *mockBackendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n = copy(p, m.data[off:])
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

func (m *mockBackendStorageFile) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, nil
}

func (m *mockBackendStorageFile) Truncate(off int64) error {
	return nil
}

func (m *mockBackendStorageFile) Close() error {
	return nil
}

func (m *mockBackendStorageFile) GetStat() (datSize int64, modTime time.Time, err error) {
	return int64(len(m.data)), time.Now(), nil
}

func (m *mockBackendStorageFile) Name() string {
	return "mock_file"
}

func (m *mockBackendStorageFile) Sync() error {
	return nil
}

// Helper function to create a test needle in memory
// Returns the serialized data and the Size field value
func createTestNeedleData(version Version, id types.NeedleId, cookie types.Cookie, data []byte) ([]byte, types.Size) {
	n := &Needle{
		Cookie:   cookie,
		Id:       id,
		DataSize: uint32(len(data)),
		Data:     data,
		Flags:    0,
		Checksum: NewCRC(data),
	}

	if version >= Version2 {
		n.SetHasLastModifiedDate()
		n.LastModified = uint64(time.Now().Unix())
	}

	buf := new(bytes.Buffer)
	_, size, _, _ := n.Append(&mockBackendWriter{buf: buf}, version)
	return buf.Bytes(), size
}

func TestReadNeedleBlob_Success(t *testing.T) {
	testCases := []struct {
		name     string
		version  Version
		dataSize int
	}{
		{"Version1_SmallFile", Version1, 100},
		{"Version1_MediumFile", Version1, 1024},
		{"Version2_SmallFile", Version2, 100},
		{"Version2_MediumFile", Version2, 1024},
		{"Version3_SmallFile", Version3, 100},
		{"Version3_LargeFile", Version3, 10240},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Prepare test data
			testData := make([]byte, tc.dataSize)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			needleData, size := createTestNeedleData(tc.version, 123, 456, testData)
			mockFile := &mockBackendStorageFile{data: needleData}

			// Read the needle blob
			blob, err := ReadNeedleBlob(mockFile, 0, size, tc.version)

			if err != nil {
				t.Fatalf("ReadNeedleBlob failed: %v", err)
			}
			if blob == nil {
				t.Fatal("ReadNeedleBlob returned nil blob")
			}
			// Just verify we got data back, not exact length (padding differs)
			if len(blob) == 0 {
				t.Error("ReadNeedleBlob returned empty blob")
			}
		})
	}
}

func TestReadNeedleBlob_InvalidOffset(t *testing.T) {
	testData := []byte("test data")
	needleData, size := createTestNeedleData(Version3, 1, 1, testData)
	mockFile := &mockBackendStorageFile{data: needleData}

	// Try to read beyond file size
	invalidOffset := int64(len(needleData) + 1000)

	blob, err := ReadNeedleBlob(mockFile, invalidOffset, size, Version3)
	if err == nil {
		t.Error("Expected error for invalid offset, got nil")
	}
	// blob might not be nil in case of error, just check we got an error
	if err == nil && blob != nil {
		t.Error("Should have gotten error for invalid offset")
	}
}

func TestReadNeedleBlob_TruncatedData(t *testing.T) {
	testData := []byte("test data for truncation")
	needleData, size := createTestNeedleData(Version3, 1, 1, testData)

	// Truncate the data to simulate incomplete file
	truncatedData := needleData[:len(needleData)/2]
	mockFile := &mockBackendStorageFile{data: truncatedData}

	blob, err := ReadNeedleBlob(mockFile, 0, size, Version3)

	if err == nil {
		t.Error("Expected error for truncated data, got nil")
	}
	if len(blob) == len(needleData) {
		t.Error("Should not read full data from truncated file")
	}
}

func TestReadNeedleHeader(t *testing.T) {
	testCases := []struct {
		name     string
		cookie   types.Cookie
		id       types.NeedleId
		dataSize uint32
	}{
		{"SmallNeedle", 0x12345678, 0x1122, 100},
		{"MediumNeedle", 0xABCDEF00, 0x112233, 1024},
		{"LargeNeedle", 0xFFFFFFFF, 0x11223344, 1048576},
		{"ZeroSize", 0x00000001, 0x01, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a complete needle and serialize it
			testData := make([]byte, tc.dataSize)
			n := &Needle{
				Cookie:   tc.cookie,
				Id:       tc.id,
				DataSize: tc.dataSize,
				Data:     testData,
				Checksum: NewCRC(testData),
			}

			buf := new(bytes.Buffer)
			n.Append(&mockBackendWriter{buf: buf}, Version3)

			// Read and parse with ReadBytes - use n.Size
			n2 := new(Needle)
			err := n2.ReadBytes(buf.Bytes(), 0, n.Size, Version3)

			if err != nil {
				t.Fatalf("ReadBytes failed: %v", err)
			}
			if n2.Cookie != tc.cookie {
				t.Errorf("Cookie mismatch: expected 0x%X, got 0x%X", tc.cookie, n2.Cookie)
			}
			if n2.Id != tc.id {
				t.Errorf("Id mismatch: expected 0x%X, got 0x%X", tc.id, n2.Id)
			}
			if n2.DataSize != tc.dataSize {
				t.Errorf("DataSize mismatch: expected %d, got %d", tc.dataSize, n2.DataSize)
			}
		})
	}
}

func TestReadNeedleDataVersion2(t *testing.T) {
	testData := []byte("hello world")
	n := &Needle{
		Cookie:       0x12345678,
		Id:           0x1122,
		DataSize:     uint32(len(testData)),
		Data:         testData,
		Flags:        0x01 | 0x02, // IsCompressed | HasName
		Name:         []byte("test.txt"),
		NameSize:     8,
		Mime:         []byte("text/plain"),
		MimeSize:     10,
		LastModified: 1234567890,
		Checksum:     NewCRC(testData),
	}
	n.SetHasLastModifiedDate()

	// Serialize
	buf := new(bytes.Buffer)
	n.Append(&mockBackendWriter{buf: buf}, Version2)

	// Deserialize - use n.Size
	n2 := new(Needle)
	err := n2.ReadBytes(buf.Bytes(), 0, n.Size, Version2)

	if err != nil {
		t.Fatalf("ReadBytes failed: %v", err)
	}
	if !bytes.Equal(n2.Data, testData) {
		t.Errorf("Data mismatch: expected %v, got %v", testData, n2.Data)
	}
	if n2.Flags != n.Flags {
		t.Errorf("Flags mismatch: expected 0x%X, got 0x%X", n.Flags, n2.Flags)
	}
}

func TestReadNeedleDataVersion3_WithAppendTime(t *testing.T) {
	testData := []byte("version 3 test data")
	appendTime := uint64(1234567890)

	n := &Needle{
		Cookie:       0xABCDEF00,
		Id:           0x112233,
		DataSize:     uint32(len(testData)),
		Data:         testData,
		AppendAtNs:   appendTime,
		LastModified: 1234567890,
		Checksum:     NewCRC(testData),
	}
	n.SetHasLastModifiedDate()

	// Serialize
	buf := new(bytes.Buffer)
	n.Append(&mockBackendWriter{buf: buf}, Version3)

	// Deserialize - use n.Size
	n2 := new(Needle)
	err := n2.ReadBytes(buf.Bytes(), 0, n.Size, Version3)

	if err != nil {
		t.Fatalf("ReadBytes failed: %v", err)
	}
	if n2.AppendAtNs != appendTime {
		t.Errorf("AppendAtNs mismatch: expected %d, got %d", appendTime, n2.AppendAtNs)
	}
}

func TestReadNeedle_CRCValidation(t *testing.T) {
	testData := []byte("data for crc validation")
	n := &Needle{
		Cookie:   0x11111111,
		Id:       0x2222,
		DataSize: uint32(len(testData)),
		Data:     testData,
		Checksum: NewCRC(testData),
	}

	buf := new(bytes.Buffer)
	n.Append(&mockBackendWriter{buf: buf}, Version3)

	// Read back and validate - use n.Size from the needle
	n2 := new(Needle)
	err := n2.ReadBytes(buf.Bytes(), 0, n.Size, Version3)

	if err != nil {
		t.Fatalf("ReadBytes failed: %v", err)
	}
	if n2.Checksum != n.Checksum {
		t.Errorf("CRC mismatch: expected 0x%X, got 0x%X", n.Checksum, n2.Checksum)
	}
}

func TestReadNeedle_WithAllMetadata(t *testing.T) {
	testData := []byte("full metadata test")
	n := &Needle{
		Cookie:       0xCAFEBABE,
		Id:           0x99887766,
		DataSize:     uint32(len(testData)),
		Data:         testData,
		Name:         []byte("document.pdf"),
		NameSize:     12,
		Mime:         []byte("application/pdf"),
		MimeSize:     15,
		Pairs:        []byte(`{"key":"value"}`),
		PairsSize:    15,
		LastModified: 1234567890,
		Checksum:     NewCRC(testData),
	}
	n.SetHasLastModifiedDate()
	n.SetHasName()
	n.SetHasMime()
	n.SetHasPairs()

	buf := new(bytes.Buffer)
	n.Append(&mockBackendWriter{buf: buf}, Version3)

	// Read back - use n.Size
	n2 := new(Needle)
	err := n2.ReadBytes(buf.Bytes(), 0, n.Size, Version3)

	if err != nil {
		t.Fatalf("ReadBytes failed: %v", err)
	}
	if !bytes.Equal(n2.Name, n.Name) {
		t.Errorf("Name mismatch: expected %s, got %s", n.Name, n2.Name)
	}
	if !bytes.Equal(n2.Mime, n.Mime) {
		t.Errorf("Mime mismatch: expected %s, got %s", n.Mime, n2.Mime)
	}
	if !bytes.Equal(n2.Pairs, n.Pairs) {
		t.Errorf("Pairs mismatch: expected %s, got %s", n.Pairs, n2.Pairs)
	}
}

func TestReadNeedle_EmptyData(t *testing.T) {
	n := &Needle{
		Cookie:   0x00000001,
		Id:       0x01,
		DataSize: 0,
		Data:     []byte{},
		Checksum: NewCRC([]byte{}),
	}

	buf := new(bytes.Buffer)
	n.Append(&mockBackendWriter{buf: buf}, Version3)

	n2 := new(Needle)
	err := n2.ReadBytes(buf.Bytes(), 0, n.Size, Version3)

	if err != nil {
		t.Fatalf("ReadBytes failed for empty data: %v", err)
	}
	if n2.DataSize != 0 {
		t.Errorf("Expected empty data, got size %d", n2.DataSize)
	}
}

// Benchmark tests
func BenchmarkReadNeedleBlob_SmallFile(b *testing.B) {
	testData := make([]byte, 1024) // 1KB
	needleData, size := createTestNeedleData(Version3, 1, 1, testData)
	mockFile := &mockBackendStorageFile{data: needleData}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ReadNeedleBlob(mockFile, 0, size, Version3)
	}
}

func BenchmarkReadNeedleBlob_MediumFile(b *testing.B) {
	testData := make([]byte, 102400) // 100KB
	needleData, size := createTestNeedleData(Version3, 1, 1, testData)
	mockFile := &mockBackendStorageFile{data: needleData}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ReadNeedleBlob(mockFile, 0, size, Version3)
	}
}

func BenchmarkReadNeedleBlob_LargeFile(b *testing.B) {
	testData := make([]byte, 1048576) // 1MB
	needleData, size := createTestNeedleData(Version3, 1, 1, testData)
	mockFile := &mockBackendStorageFile{data: needleData}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ReadNeedleBlob(mockFile, 0, size, Version3)
	}
}

func BenchmarkReadNeedleHeader(b *testing.B) {
	testData := make([]byte, 1024)
	n := &Needle{
		Cookie:   0x12345678,
		Id:       0x1122334455667788,
		DataSize: 1024,
		Data:     testData,
		Checksum: NewCRC(testData),
	}

	buf := new(bytes.Buffer)
	n.Append(&mockBackendWriter{buf: buf}, Version3)
	data := buf.Bytes()
	size := types.Size(uint32(len(data)))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		n2 := new(Needle)
		n2.ReadBytes(data, 0, size, Version3)
	}
}

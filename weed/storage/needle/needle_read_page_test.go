package needle

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// TestReadNeedleData verifies reading needle data at specific offset
func TestReadNeedleData(t *testing.T) {
	testCases := []struct {
		name         string
		dataSize     int
		needleOffset int64
		bufferSize   int
		expectedRead int
		expectError  bool
	}{
		{
			name:         "Read from start",
			dataSize:     1000,
			needleOffset: 0,
			bufferSize:   100,
			expectedRead: 100,
			expectError:  false,
		},
		{
			name:         "Read from middle",
			dataSize:     1000,
			needleOffset: 500,
			bufferSize:   100,
			expectedRead: 100,
			expectError:  false,
		},
		{
			name:         "Read to end",
			dataSize:     1000,
			needleOffset: 900,
			bufferSize:   200,
			expectedRead: 100,
			expectError:  false,
		},
		{
			name:         "Read beyond end",
			dataSize:     1000,
			needleOffset: 1000,
			bufferSize:   100,
			expectedRead: 0,
			expectError:  true,
		},
		{
			name:         "Read all data",
			dataSize:     500,
			needleOffset: 0,
			bufferSize:   500,
			expectedRead: 500,
			expectError:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create test data
			testData := make([]byte, tc.dataSize)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			// Create needle with data written to mock file
			n := &Needle{
				Id:       NeedleId(123),
				Cookie:   Cookie(456),
				DataSize: uint32(tc.dataSize),
				Data:     testData,
			}

			// Write needle to buffer
			buf := new(bytes.Buffer)
			n.Append(&mockBackendWriter{buf: buf}, Version2)

			// Create mock file
			mockFile := &mockBackendStorageFile{data: buf.Bytes()}

			// Read data at offset
			readBuffer := make([]byte, tc.bufferSize)
			count, err := n.ReadNeedleData(mockFile, 0, readBuffer, tc.needleOffset)

			if tc.expectError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				if err != io.EOF {
					t.Errorf("Expected EOF, got: %v", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("ReadNeedleData failed: %v", err)
			}

			if count != tc.expectedRead {
				t.Errorf("Read count mismatch: expected %d, got %d", tc.expectedRead, count)
			}

			// Verify data correctness
			for i := 0; i < count; i++ {
				expected := testData[tc.needleOffset+int64(i)]
				if readBuffer[i] != expected {
					t.Errorf("Data mismatch at offset %d: expected %d, got %d",
						i, expected, readBuffer[i])
					break
				}
			}
		})
	}
}

// TestReadNeedleMeta verifies reading needle metadata without data
func TestReadNeedleMeta(t *testing.T) {
	testCases := []struct {
		name    string
		version Version
		flags   byte
		hasPairs bool
	}{
		{
			name:    "Version2 - no metadata",
			version: Version2,
			flags:   0,
			hasPairs: false,
		},
		{
			name:    "Version2 - with LastModified",
			version: Version2,
			flags:   FlagHasLastModifiedDate,
			hasPairs: false,
		},
		{
			name:    "Version2 - with Pairs",
			version: Version2,
			flags:   FlagHasPairs,
			hasPairs: true,
		},
		{
			name:    "Version3 - with timestamp",
			version: Version3,
			flags:   FlagHasLastModifiedDate,
			hasPairs: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create test needle
			testData := make([]byte, 512)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			n := &Needle{
				Id:       NeedleId(789),
				Cookie:   Cookie(321),
				DataSize: uint32(len(testData)),
				Data:     testData,
				Flags:    tc.flags,
			}

			if tc.flags&FlagHasLastModifiedDate != 0 {
				n.SetHasLastModifiedDate()
				n.LastModified = 1234567890
			}

			if tc.hasPairs {
				n.Pairs = []byte(`{"key":"value"}`)
				n.PairsSize = uint16(len(n.Pairs))
				n.SetHasPairs()
			}

			n.Checksum = NewCRC(testData)

			// Write needle to buffer
			buf := new(bytes.Buffer)
			_, returnedSize, _, err := n.Append(&mockBackendWriter{buf: buf}, tc.version)
			if err != nil {
				t.Fatalf("Append failed: %v", err)
			}

			// Create mock file
			mockFile := &mockBackendStorageFile{data: buf.Bytes()}

			// Read metadata only (use n.Size not returnedSize for ReadNeedleMeta)
			n2 := &Needle{}
			err = n2.ReadNeedleMeta(mockFile, 0, n.Size, tc.version)
			if err != nil {
				t.Fatalf("ReadNeedleMeta failed: %v (returnedSize=%d, n.Size=%d)", err, returnedSize, n.Size)
			}

			// Verify metadata
			if n2.Id != n.Id {
				t.Errorf("Id mismatch: expected %d, got %d", n.Id, n2.Id)
			}
			if n2.Cookie != n.Cookie {
				t.Errorf("Cookie mismatch: expected %d, got %d", n.Cookie, n2.Cookie)
			}
			if n2.DataSize != n.DataSize {
				t.Errorf("DataSize mismatch: expected %d, got %d", n.DataSize, n2.DataSize)
			}

			// Data should not be loaded
			if len(n2.Data) != 0 {
				t.Error("Data should not be loaded in ReadNeedleMeta")
			}

			// Checksum should be set
			if n2.Checksum == 0 {
				t.Error("Checksum not set")
			}
		})
	}
}

// TestReadNeedleMeta_SizeMismatch verifies size validation
func TestReadNeedleMeta_SizeMismatch(t *testing.T) {
	// Create test needle
	testData := []byte("test data")
	n := &Needle{
		Id:       NeedleId(111),
		Cookie:   Cookie(222),
		DataSize: uint32(len(testData)),
		Data:     testData,
		Checksum: NewCRC(testData),
	}

	buf := new(bytes.Buffer)
	_, size, _, err := n.Append(&mockBackendWriter{buf: buf}, Version2)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	mockFile := &mockBackendStorageFile{data: buf.Bytes()}

	// Try to read with wrong size
	n2 := &Needle{}
	wrongSize := size + 1000
	err = n2.ReadNeedleMeta(mockFile, 0, wrongSize, Version2)

	if err == nil {
		t.Error("Expected size mismatch error")
	}
}

// TestReadNeedleMeta_InvalidOffset verifies offset handling
func TestReadNeedleMeta_InvalidOffset(t *testing.T) {
	testData := []byte("some data")
	n := &Needle{
		Id:       NeedleId(333),
		Cookie:   Cookie(444),
		DataSize: uint32(len(testData)),
		Data:     testData,
		Checksum: NewCRC(testData),
	}

	buf := new(bytes.Buffer)
	_, size, _, err := n.Append(&mockBackendWriter{buf: buf}, Version2)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	mockFile := &mockBackendStorageFile{data: buf.Bytes()}

	// Try to read at invalid offset
	n2 := &Needle{}
	invalidOffset := int64(len(buf.Bytes()) + 1000)
	err = n2.ReadNeedleMeta(mockFile, invalidOffset, size, Version2)

	if err == nil {
		t.Error("Expected error for invalid offset")
	}
}

// TestReadNeedleData_SmallReads verifies reading in small chunks
func TestReadNeedleData_SmallReads(t *testing.T) {
	dataSize := 1000
	testData := make([]byte, dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	n := &Needle{
		Id:       NeedleId(555),
		Cookie:   Cookie(666),
		DataSize: uint32(dataSize),
		Data:     testData,
	}

	buf := new(bytes.Buffer)
	n.Append(&mockBackendWriter{buf: buf}, Version2)
	mockFile := &mockBackendStorageFile{data: buf.Bytes()}

	// Read in small chunks
	chunkSize := 100
	readBuffer := make([]byte, chunkSize)
	reconstructed := make([]byte, 0, dataSize)

	for offset := int64(0); offset < int64(dataSize); offset += int64(chunkSize) {
		count, err := n.ReadNeedleData(mockFile, 0, readBuffer, offset)
		if err != nil && err != io.EOF {
			t.Fatalf("ReadNeedleData failed at offset %d: %v", offset, err)
		}
		reconstructed = append(reconstructed, readBuffer[:count]...)
	}

	if !bytes.Equal(reconstructed, testData) {
		t.Error("Reconstructed data doesn't match original")
	}
}

// TestReadNeedleData_ZeroDataSize verifies handling of empty needle
func TestReadNeedleData_ZeroDataSize(t *testing.T) {
	n := &Needle{
		Id:       NeedleId(777),
		Cookie:   Cookie(888),
		DataSize: 0,
		Data:     []byte{},
	}

	buf := new(bytes.Buffer)
	n.Append(&mockBackendWriter{buf: buf}, Version2)
	mockFile := &mockBackendStorageFile{data: buf.Bytes()}

	readBuffer := make([]byte, 100)
	count, err := n.ReadNeedleData(mockFile, 0, readBuffer, 0)

	if err != io.EOF {
		t.Errorf("Expected EOF for empty data, got: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 bytes read, got %d", count)
	}
}

// TestMin verifies min helper function
func TestMin(t *testing.T) {
	testCases := []struct {
		x, y, expected int64
	}{
		{0, 0, 0},
		{1, 2, 1},
		{2, 1, 1},
		{-1, 1, -1},
		{100, 100, 100},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("min(%d,%d)", tc.x, tc.y), func(t *testing.T) {
			result := min(tc.x, tc.y)
			if result != tc.expected {
				t.Errorf("Expected %d, got %d", tc.expected, result)
			}
		})
	}
}

// BenchmarkReadNeedleData benchmarks needle data reading
func BenchmarkReadNeedleData(b *testing.B) {
	dataSize := 10240
	testData := make([]byte, dataSize)
	n := &Needle{
		Id:       NeedleId(999),
		Cookie:   Cookie(111),
		DataSize: uint32(dataSize),
		Data:     testData,
	}

	buf := new(bytes.Buffer)
	n.Append(&mockBackendWriter{buf: buf}, Version3)
	mockFile := &mockBackendStorageFile{data: buf.Bytes()}

	readBuffer := make([]byte, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(i % (dataSize - 1024))
		n.ReadNeedleData(mockFile, 0, readBuffer, offset)
	}
}

// BenchmarkReadNeedleMeta benchmarks metadata reading
func BenchmarkReadNeedleMeta(b *testing.B) {
	testData := make([]byte, 1024)
	n := &Needle{
		Id:       NeedleId(123),
		Cookie:   Cookie(456),
		DataSize: uint32(len(testData)),
		Data:     testData,
		Checksum: NewCRC(testData),
	}

	buf := new(bytes.Buffer)
	_, size, _, _ := n.Append(&mockBackendWriter{buf: buf}, Version3)
	mockFile := &mockBackendStorageFile{data: buf.Bytes()}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n2 := &Needle{}
		n2.ReadNeedleMeta(mockFile, 0, size, Version3)
	}
}


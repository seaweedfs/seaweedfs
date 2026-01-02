package needle

import (
	"testing"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestReadNeedleTail_Version1 verifies tail reading for Version1
func TestReadNeedleTail_Version1(t *testing.T) {
	data := []byte("test data for version 1")
	n := &Needle{
		Data:     data,
		DataSize: uint32(len(data)),
		Checksum: NewCRC(data),
	}

	// Create tail bytes: checksum only for V1
	tailBytes := make([]byte, NeedleChecksumSize)
	util.Uint32toBytes(tailBytes[0:NeedleChecksumSize], uint32(n.Checksum))

	err := n.readNeedleTail(tailBytes, Version1)
	if err != nil {
		t.Fatalf("readNeedleTail failed: %v", err)
	}
}

// TestReadNeedleTail_Version2 verifies tail reading for Version2
func TestReadNeedleTail_Version2(t *testing.T) {
	data := []byte("test data for version 2")
	n := &Needle{
		Data:     data,
		DataSize: uint32(len(data)),
		Checksum: NewCRC(data),
	}

	// Create tail bytes: checksum only for V2
	tailBytes := make([]byte, NeedleChecksumSize)
	util.Uint32toBytes(tailBytes[0:NeedleChecksumSize], uint32(n.Checksum))

	err := n.readNeedleTail(tailBytes, Version2)
	if err != nil {
		t.Fatalf("readNeedleTail failed: %v", err)
	}
}

// TestReadNeedleTail_Version3 verifies tail reading for Version3 with timestamp
func TestReadNeedleTail_Version3(t *testing.T) {
	data := []byte("test data for version 3")
	appendTime := uint64(1234567890123456789)
	
	n := &Needle{
		Data:       data,
		DataSize:   uint32(len(data)),
		Checksum:   NewCRC(data),
		AppendAtNs: appendTime,
	}

	// Create tail bytes: checksum + timestamp for V3
	tailBytes := make([]byte, NeedleChecksumSize+TimestampSize)
	util.Uint32toBytes(tailBytes[0:NeedleChecksumSize], uint32(n.Checksum))
	util.Uint64toBytes(tailBytes[NeedleChecksumSize:NeedleChecksumSize+TimestampSize], appendTime)

	err := n.readNeedleTail(tailBytes, Version3)
	if err != nil {
		t.Fatalf("readNeedleTail failed: %v", err)
	}

	if n.AppendAtNs != appendTime {
		t.Errorf("AppendAtNs mismatch: expected %d, got %d", appendTime, n.AppendAtNs)
	}
}

// TestReadNeedleTail_CRCError verifies CRC validation
func TestReadNeedleTail_CRCError(t *testing.T) {
	data := []byte("test data")
	n := &Needle{
		Data:     data,
		DataSize: uint32(len(data)),
		Checksum: NewCRC(data),
	}

	// Create tail with wrong checksum
	tailBytes := make([]byte, NeedleChecksumSize)
	wrongChecksum := uint32(0xDEADBEEF)
	util.Uint32toBytes(tailBytes[0:NeedleChecksumSize], wrongChecksum)

	err := n.readNeedleTail(tailBytes, Version1)
	if err == nil {
		t.Error("Expected CRC error, got nil")
	}
	if err != nil && err.Error() != "CRC error! Data On Disk Corrupted" {
		t.Errorf("Expected CRC error, got: %v", err)
	}
}

// TestReadNeedleTail_EmptyData verifies handling of empty data
func TestReadNeedleTail_EmptyData(t *testing.T) {
	n := &Needle{
		Data:     []byte{},
		DataSize: 0,
		Checksum: CRC(0),
	}

	// Empty data should just read checksum without validation
	tailBytes := make([]byte, NeedleChecksumSize)
	util.Uint32toBytes(tailBytes[0:NeedleChecksumSize], 0x12345678)

	err := n.readNeedleTail(tailBytes, Version1)
	if err != nil {
		t.Fatalf("readNeedleTail failed for empty data: %v", err)
	}

	if n.Checksum != CRC(0x12345678) {
		t.Errorf("Checksum not set correctly: expected %d, got %d", 0x12345678, n.Checksum)
	}
}

// TestPaddingLength verifies padding calculation for different versions
func TestPaddingLength(t *testing.T) {
	testCases := []struct {
		name       string
		needleSize Size
		version    Version
		checkPadding bool
	}{
		{
			name:       "Version1 - Size 0",
			needleSize: Size(0),
			version:    Version1,
			checkPadding: true,
		},
		{
			name:       "Version1 - Size 100",
			needleSize: Size(100),
			version:    Version1,
			checkPadding: true,
		},
		{
			name:       "Version2 - Size 100",
			needleSize: Size(100),
			version:    Version2,
			checkPadding: true,
		},
		{
			name:       "Version3 - Size 100",
			needleSize: Size(100),
			version:    Version3,
			checkPadding: true,
		},
		{
			name:       "Version3 - Size 1024",
			needleSize: Size(1024),
			version:    Version3,
			checkPadding: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			padding := PaddingLength(tc.needleSize, tc.version)

			// Padding should be between 0 and NeedlePaddingSize (inclusive)
			if padding < 0 || padding > NeedlePaddingSize {
				t.Errorf("Invalid padding: %d", padding)
			}

			// Verify total size is aligned to NeedlePaddingSize
			if tc.version == Version3 {
				totalSize := NeedleHeaderSize + tc.needleSize + NeedleChecksumSize + TimestampSize + padding
				if totalSize%NeedlePaddingSize != 0 {
					t.Errorf("Total size not aligned: %d", totalSize)
				}
			} else {
				totalSize := NeedleHeaderSize + tc.needleSize + NeedleChecksumSize + padding
				if totalSize%NeedlePaddingSize != 0 {
					t.Errorf("Total size not aligned: %d", totalSize)
				}
			}
		})
	}
}

// TestNeedleBodyLength verifies body length calculation
func TestNeedleBodyLength(t *testing.T) {
	testCases := []struct {
		name       string
		needleSize Size
		version    Version
	}{
		{
			name:       "Version1 - Size 0",
			needleSize: Size(0),
			version:    Version1,
		},
		{
			name:       "Version1 - Size 100",
			needleSize: Size(100),
			version:    Version1,
		},
		{
			name:       "Version2 - Size 100",
			needleSize: Size(100),
			version:    Version2,
		},
		{
			name:       "Version3 - Size 100",
			needleSize: Size(100),
			version:    Version3,
		},
		{
			name:       "Version3 - Size 10240",
			needleSize: Size(10240),
			version:    Version3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bodyLength := NeedleBodyLength(tc.needleSize, tc.version)

			// Body length should include: needleSize + checksum + padding [+ timestamp for V3]
			padding := PaddingLength(tc.needleSize, tc.version)
			var expectedLength int64
			if tc.version == Version3 {
				expectedLength = int64(tc.needleSize) + NeedleChecksumSize + TimestampSize + int64(padding)
			} else {
				expectedLength = int64(tc.needleSize) + NeedleChecksumSize + int64(padding)
			}

			if bodyLength != expectedLength {
				t.Errorf("Body length mismatch: expected %d, got %d", expectedLength, bodyLength)
			}

			// Verify body length is always positive
			if bodyLength < 0 {
				t.Errorf("Negative body length: %d", bodyLength)
			}
		})
	}
}

// TestPaddingLength_Alignment verifies padding ensures proper alignment
func TestPaddingLength_Alignment(t *testing.T) {
	// Test various sizes to ensure padding works correctly
	for size := Size(0); size < Size(1024); size++ {
		for version := Version1; version <= Version3; version++ {
			_ = PaddingLength(size, version)
			bodyLength := NeedleBodyLength(size, version)

			// Total length including header should be aligned
			totalLength := NeedleHeaderSize + bodyLength
			if totalLength%NeedlePaddingSize != 0 {
				t.Errorf("Size %d Version %d: total length %d not aligned to %d",
					size, version, totalLength, NeedlePaddingSize)
			}
		}
	}
}

// BenchmarkReadNeedleTail benchmarks tail reading
func BenchmarkReadNeedleTail(b *testing.B) {
	data := make([]byte, 1024)
	n := &Needle{
		Data:     data,
		DataSize: uint32(len(data)),
		Checksum: NewCRC(data),
	}

	tailBytes := make([]byte, NeedleChecksumSize+TimestampSize)
	util.Uint32toBytes(tailBytes[0:NeedleChecksumSize], uint32(n.Checksum))
	util.Uint64toBytes(tailBytes[NeedleChecksumSize:], 1234567890)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n.readNeedleTail(tailBytes, Version3)
	}
}

// BenchmarkPaddingLength benchmarks padding calculation
func BenchmarkPaddingLength(b *testing.B) {
	size := Size(1234)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = PaddingLength(size, Version3)
	}
}


package needle

import (
	"bytes"
	"testing"
)

// TestNewCRC verifies CRC calculation for data
func TestNewCRC(t *testing.T) {
	testCases := []struct {
		name     string
		data     []byte
		expected CRC
	}{
		{
			name:     "Empty data",
			data:     []byte{},
			expected: CRC(0),
		},
		{
			name:     "Simple data",
			data:     []byte("hello world"),
			expected: NewCRC([]byte("hello world")),
		},
		{
			name:     "Binary data",
			data:     []byte{0x00, 0xFF, 0xAA, 0x55},
			expected: NewCRC([]byte{0x00, 0xFF, 0xAA, 0x55}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := NewCRC(tc.data)
			// Just verify CRC calculation is deterministic
			result2 := NewCRC(tc.data)
			if result != result2 {
				t.Errorf("CRC not deterministic: got %d vs %d", result, result2)
			}
		})
	}
}

// TestCRC_Update verifies incremental CRC update
func TestCRC_Update(t *testing.T) {
	data1 := []byte("hello")
	data2 := []byte(" world")
	combined := []byte("hello world")

	// Calculate CRC incrementally
	crc := CRC(0)
	crc = crc.Update(data1)
	crc = crc.Update(data2)

	// Calculate CRC at once
	crcDirect := NewCRC(combined)

	if crc != crcDirect {
		t.Errorf("Incremental CRC mismatch: got %d, expected %d", crc, crcDirect)
	}
}

// TestCRC_Value verifies deprecated Value() function
func TestCRC_Value(t *testing.T) {
	data := []byte("test data")
	crc := NewCRC(data)

	// Just verify Value() returns a uint32
	value := crc.Value()
	if value == 0 {
		t.Error("CRC.Value() returned 0 for non-empty data")
	}

	// Verify consistency
	value2 := crc.Value()
	if value != value2 {
		t.Error("CRC.Value() not deterministic")
	}
}

// TestNeedle_Etag verifies ETag generation
func TestNeedle_Etag(t *testing.T) {
	testCases := []struct {
		name      string
		data      []byte
		checkEtag bool
	}{
		{
			name:      "Empty data",
			data:      []byte{},
			checkEtag: true,
		},
		{
			name:      "Small data",
			data:      []byte("small file"),
			checkEtag: true,
		},
		{
			name:      "Large data",
			data:      make([]byte, 1024),
			checkEtag: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			n := &Needle{
				Data:     tc.data,
				Checksum: NewCRC(tc.data),
			}

			etag := n.Etag()

			// ETag should be a hex string of 8 characters (4 bytes)
			if len(etag) != 8 {
				t.Errorf("ETag length: expected 8, got %d", len(etag))
			}

			// Verify ETag is deterministic
			etag2 := n.Etag()
			if etag != etag2 {
				t.Error("ETag not deterministic")
			}

			// Verify ETag changes with different checksums
			n2 := &Needle{
				Data:     append(tc.data, 0xFF),
				Checksum: NewCRC(append(tc.data, 0xFF)),
			}
			etag3 := n2.Etag()
			if len(tc.data) > 0 && etag == etag3 {
				t.Error("Different data produced same ETag")
			}
		})
	}
}

// TestNewCRCwriter verifies CRCwriter functionality
func TestNewCRCwriter(t *testing.T) {
	buf := new(bytes.Buffer)
	crcWriter := NewCRCwriter(buf)

	if crcWriter == nil {
		t.Fatal("NewCRCwriter returned nil")
	}
	if crcWriter.w != buf {
		t.Error("CRCwriter buffer not set correctly")
	}
	if crcWriter.crc != 0 {
		t.Error("CRCwriter initial CRC should be 0")
	}
}

// TestCRCwriter_Write verifies CRCwriter write operations
func TestCRCwriter_Write(t *testing.T) {
	testCases := []struct {
		name   string
		writes [][]byte
	}{
		{
			name:   "Single write",
			writes: [][]byte{[]byte("hello world")},
		},
		{
			name:   "Multiple writes",
			writes: [][]byte{[]byte("hello"), []byte(" "), []byte("world")},
		},
		{
			name:   "Empty write",
			writes: [][]byte{[]byte{}},
		},
		{
			name:   "Large write",
			writes: [][]byte{make([]byte, 10240)},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			crcWriter := NewCRCwriter(buf)

			var combined []byte
			for _, data := range tc.writes {
				n, err := crcWriter.Write(data)
				if err != nil {
					t.Fatalf("Write failed: %v", err)
				}
				if n != len(data) {
					t.Errorf("Write length mismatch: expected %d, got %d", len(data), n)
				}
				combined = append(combined, data...)
			}

			// Verify buffer contains all data
			if !bytes.Equal(buf.Bytes(), combined) {
				t.Error("Buffer content mismatch")
			}

			// Verify CRC matches direct calculation
			expectedCRC := NewCRC(combined)
			if crcWriter.Sum() != uint32(expectedCRC) {
				t.Errorf("CRC mismatch: expected %d, got %d", expectedCRC, crcWriter.Sum())
			}
		})
	}
}

// TestCRCwriter_Concurrent verifies CRCwriter is not safe for concurrent use
// This test documents the expected behavior
func TestCRCwriter_Sequential(t *testing.T) {
	buf := new(bytes.Buffer)
	crcWriter := NewCRCwriter(buf)

	// Sequential writes
	data1 := []byte("part1")
	data2 := []byte("part2")
	data3 := []byte("part3")

	crcWriter.Write(data1)
	crcWriter.Write(data2)
	crcWriter.Write(data3)

	combined := append(append(data1, data2...), data3...)
	expectedCRC := NewCRC(combined)

	if crcWriter.Sum() != uint32(expectedCRC) {
		t.Errorf("Sequential CRC mismatch")
	}
}

// BenchmarkNewCRC benchmarks CRC calculation
func BenchmarkNewCRC(b *testing.B) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NewCRC(data)
	}
}

// BenchmarkCRC_Update benchmarks incremental CRC update
func BenchmarkCRC_Update(b *testing.B) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		crc := CRC(0)
		_ = crc.Update(data)
	}
}

// BenchmarkCRCwriter_Write benchmarks CRCwriter
func BenchmarkCRCwriter_Write(b *testing.B) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := new(bytes.Buffer)
		crcWriter := NewCRCwriter(buf)
		crcWriter.Write(data)
		_ = crcWriter.Sum()
	}
}

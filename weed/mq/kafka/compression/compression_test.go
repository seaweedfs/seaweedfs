package compression

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCompressionCodec_String tests the string representation of compression codecs
func TestCompressionCodec_String(t *testing.T) {
	tests := []struct {
		codec    CompressionCodec
		expected string
	}{
		{None, "none"},
		{Gzip, "gzip"},
		{Snappy, "snappy"},
		{Lz4, "lz4"},
		{Zstd, "zstd"},
		{CompressionCodec(99), "unknown(99)"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			assert.Equal(t, test.expected, test.codec.String())
		})
	}
}

// TestCompressionCodec_IsValid tests codec validation
func TestCompressionCodec_IsValid(t *testing.T) {
	tests := []struct {
		codec CompressionCodec
		valid bool
	}{
		{None, true},
		{Gzip, true},
		{Snappy, true},
		{Lz4, true},
		{Zstd, true},
		{CompressionCodec(-1), false},
		{CompressionCodec(5), false},
		{CompressionCodec(99), false},
	}

	for _, test := range tests {
		t.Run(test.codec.String(), func(t *testing.T) {
			assert.Equal(t, test.valid, test.codec.IsValid())
		})
	}
}

// TestExtractCompressionCodec tests extracting compression codec from attributes
func TestExtractCompressionCodec(t *testing.T) {
	tests := []struct {
		name       string
		attributes int16
		expected   CompressionCodec
	}{
		{"None", 0x0000, None},
		{"Gzip", 0x0001, Gzip},
		{"Snappy", 0x0002, Snappy},
		{"Lz4", 0x0003, Lz4},
		{"Zstd", 0x0004, Zstd},
		{"Gzip with transactional", 0x0011, Gzip}, // Bit 4 set (transactional)
		{"Snappy with control", 0x0022, Snappy},   // Bit 5 set (control)
		{"Lz4 with both flags", 0x0033, Lz4},      // Both flags set
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			codec := ExtractCompressionCodec(test.attributes)
			assert.Equal(t, test.expected, codec)
		})
	}
}

// TestSetCompressionCodec tests setting compression codec in attributes
func TestSetCompressionCodec(t *testing.T) {
	tests := []struct {
		name       string
		attributes int16
		codec      CompressionCodec
		expected   int16
	}{
		{"Set None", 0x0000, None, 0x0000},
		{"Set Gzip", 0x0000, Gzip, 0x0001},
		{"Set Snappy", 0x0000, Snappy, 0x0002},
		{"Set Lz4", 0x0000, Lz4, 0x0003},
		{"Set Zstd", 0x0000, Zstd, 0x0004},
		{"Replace Gzip with Snappy", 0x0001, Snappy, 0x0002},
		{"Set Gzip preserving transactional", 0x0010, Gzip, 0x0011},
		{"Set Lz4 preserving control", 0x0020, Lz4, 0x0023},
		{"Set Zstd preserving both flags", 0x0030, Zstd, 0x0034},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := SetCompressionCodec(test.attributes, test.codec)
			assert.Equal(t, test.expected, result)
		})
	}
}

// TestCompress_None tests compression with None codec
func TestCompress_None(t *testing.T) {
	data := []byte("Hello, World!")

	compressed, err := Compress(None, data)
	require.NoError(t, err)
	assert.Equal(t, data, compressed, "None codec should return original data")
}

// TestCompress_Gzip tests gzip compression
func TestCompress_Gzip(t *testing.T) {
	data := []byte("Hello, World! This is a test message for gzip compression.")

	compressed, err := Compress(Gzip, data)
	require.NoError(t, err)
	assert.NotEqual(t, data, compressed, "Gzip should compress data")
	assert.True(t, len(compressed) > 0, "Compressed data should not be empty")
}

// TestCompress_Snappy tests snappy compression
func TestCompress_Snappy(t *testing.T) {
	data := []byte("Hello, World! This is a test message for snappy compression.")

	compressed, err := Compress(Snappy, data)
	require.NoError(t, err)
	assert.NotEqual(t, data, compressed, "Snappy should compress data")
	assert.True(t, len(compressed) > 0, "Compressed data should not be empty")
}

// TestCompress_Lz4 tests lz4 compression
func TestCompress_Lz4(t *testing.T) {
	data := []byte("Hello, World! This is a test message for lz4 compression.")

	compressed, err := Compress(Lz4, data)
	require.NoError(t, err)
	assert.NotEqual(t, data, compressed, "Lz4 should compress data")
	assert.True(t, len(compressed) > 0, "Compressed data should not be empty")
}

// TestCompress_Zstd tests zstd compression
func TestCompress_Zstd(t *testing.T) {
	data := []byte("Hello, World! This is a test message for zstd compression.")

	compressed, err := Compress(Zstd, data)
	require.NoError(t, err)
	assert.NotEqual(t, data, compressed, "Zstd should compress data")
	assert.True(t, len(compressed) > 0, "Compressed data should not be empty")
}

// TestCompress_InvalidCodec tests compression with invalid codec
func TestCompress_InvalidCodec(t *testing.T) {
	data := []byte("Hello, World!")

	_, err := Compress(CompressionCodec(99), data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported compression codec")
}

// TestDecompress_None tests decompression with None codec
func TestDecompress_None(t *testing.T) {
	data := []byte("Hello, World!")

	decompressed, err := Decompress(None, data)
	require.NoError(t, err)
	assert.Equal(t, data, decompressed, "None codec should return original data")
}

// TestRoundTrip tests compression and decompression round trip for all codecs
func TestRoundTrip(t *testing.T) {
	testData := [][]byte{
		[]byte("Hello, World!"),
		[]byte(""),
		[]byte("A"),
		[]byte(string(bytes.Repeat([]byte("Test data for compression round trip. "), 100))),
		[]byte("Special characters: àáâãäåæçèéêëìíîïðñòóôõö÷øùúûüýþÿ"),
		bytes.Repeat([]byte{0x00, 0x01, 0x02, 0xFF}, 256), // Binary data
	}

	codecs := []CompressionCodec{None, Gzip, Snappy, Lz4, Zstd}

	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			for i, data := range testData {
				t.Run(fmt.Sprintf("data_%d", i), func(t *testing.T) {
					// Compress
					compressed, err := Compress(codec, data)
					require.NoError(t, err, "Compression should succeed")

					// Decompress
					decompressed, err := Decompress(codec, compressed)
					require.NoError(t, err, "Decompression should succeed")

					// Verify round trip
					assert.Equal(t, data, decompressed, "Round trip should preserve data")
				})
			}
		})
	}
}

// TestDecompress_InvalidCodec tests decompression with invalid codec
func TestDecompress_InvalidCodec(t *testing.T) {
	data := []byte("Hello, World!")

	_, err := Decompress(CompressionCodec(99), data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported compression codec")
}

// TestDecompress_CorruptedData tests decompression with corrupted data
func TestDecompress_CorruptedData(t *testing.T) {
	corruptedData := []byte("This is not compressed data")

	codecs := []CompressionCodec{Gzip, Snappy, Lz4, Zstd}

	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			_, err := Decompress(codec, corruptedData)
			assert.Error(t, err, "Decompression of corrupted data should fail")
		})
	}
}

// TestCompressRecordBatch tests record batch compression
func TestCompressRecordBatch(t *testing.T) {
	recordsData := []byte("Record batch data for compression testing")

	t.Run("None codec", func(t *testing.T) {
		compressed, attributes, err := CompressRecordBatch(None, recordsData)
		require.NoError(t, err)
		assert.Equal(t, recordsData, compressed)
		assert.Equal(t, int16(0), attributes)
	})

	t.Run("Gzip codec", func(t *testing.T) {
		compressed, attributes, err := CompressRecordBatch(Gzip, recordsData)
		require.NoError(t, err)
		assert.NotEqual(t, recordsData, compressed)
		assert.Equal(t, int16(1), attributes)
	})

	t.Run("Snappy codec", func(t *testing.T) {
		compressed, attributes, err := CompressRecordBatch(Snappy, recordsData)
		require.NoError(t, err)
		assert.NotEqual(t, recordsData, compressed)
		assert.Equal(t, int16(2), attributes)
	})
}

// TestDecompressRecordBatch tests record batch decompression
func TestDecompressRecordBatch(t *testing.T) {
	recordsData := []byte("Record batch data for decompression testing")

	t.Run("None codec", func(t *testing.T) {
		attributes := int16(0) // No compression
		decompressed, err := DecompressRecordBatch(attributes, recordsData)
		require.NoError(t, err)
		assert.Equal(t, recordsData, decompressed)
	})

	t.Run("Round trip with Gzip", func(t *testing.T) {
		// Compress
		compressed, attributes, err := CompressRecordBatch(Gzip, recordsData)
		require.NoError(t, err)

		// Decompress
		decompressed, err := DecompressRecordBatch(attributes, compressed)
		require.NoError(t, err)
		assert.Equal(t, recordsData, decompressed)
	})

	t.Run("Round trip with Snappy", func(t *testing.T) {
		// Compress
		compressed, attributes, err := CompressRecordBatch(Snappy, recordsData)
		require.NoError(t, err)

		// Decompress
		decompressed, err := DecompressRecordBatch(attributes, compressed)
		require.NoError(t, err)
		assert.Equal(t, recordsData, decompressed)
	})
}

// TestCompressionEfficiency tests compression efficiency for different codecs
func TestCompressionEfficiency(t *testing.T) {
	// Create highly compressible data
	data := bytes.Repeat([]byte("This is a repeated string for compression testing. "), 100)

	codecs := []CompressionCodec{Gzip, Snappy, Lz4, Zstd}

	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			compressed, err := Compress(codec, data)
			require.NoError(t, err)

			compressionRatio := float64(len(compressed)) / float64(len(data))
			t.Logf("Codec: %s, Original: %d bytes, Compressed: %d bytes, Ratio: %.2f",
				codec.String(), len(data), len(compressed), compressionRatio)

			// All codecs should achieve some compression on this highly repetitive data
			assert.Less(t, len(compressed), len(data), "Compression should reduce data size")
		})
	}
}

// BenchmarkCompression benchmarks compression performance for different codecs
func BenchmarkCompression(b *testing.B) {
	data := bytes.Repeat([]byte("Benchmark data for compression testing. "), 1000)
	codecs := []CompressionCodec{None, Gzip, Snappy, Lz4, Zstd}

	for _, codec := range codecs {
		b.Run(fmt.Sprintf("Compress_%s", codec.String()), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := Compress(codec, data)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDecompression benchmarks decompression performance for different codecs
func BenchmarkDecompression(b *testing.B) {
	data := bytes.Repeat([]byte("Benchmark data for decompression testing. "), 1000)
	codecs := []CompressionCodec{None, Gzip, Snappy, Lz4, Zstd}

	for _, codec := range codecs {
		// Pre-compress the data
		compressed, err := Compress(codec, data)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(fmt.Sprintf("Decompress_%s", codec.String()), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := Decompress(codec, compressed)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

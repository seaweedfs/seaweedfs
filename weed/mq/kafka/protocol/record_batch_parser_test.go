package protocol

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/compression"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRecordBatchParser_ParseRecordBatch tests basic record batch parsing
func TestRecordBatchParser_ParseRecordBatch(t *testing.T) {
	parser := NewRecordBatchParser()

	// Create a minimal valid record batch
	recordData := []byte("test record data")
	batch, err := CreateRecordBatch(100, recordData, compression.None)
	require.NoError(t, err)

	// Parse the batch
	parsed, err := parser.ParseRecordBatch(batch)
	require.NoError(t, err)

	// Verify parsed fields
	assert.Equal(t, int64(100), parsed.BaseOffset)
	assert.Equal(t, int8(2), parsed.Magic)
	assert.Equal(t, int32(1), parsed.RecordCount)
	assert.Equal(t, compression.None, parsed.GetCompressionCodec())
	assert.False(t, parsed.IsCompressed())
}

// TestRecordBatchParser_ParseRecordBatch_TooSmall tests parsing with insufficient data
func TestRecordBatchParser_ParseRecordBatch_TooSmall(t *testing.T) {
	parser := NewRecordBatchParser()

	// Test with data that's too small
	smallData := make([]byte, 30) // Less than 61 bytes minimum
	_, err := parser.ParseRecordBatch(smallData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "record batch too small")
}

// TestRecordBatchParser_ParseRecordBatch_InvalidMagic tests parsing with invalid magic byte
func TestRecordBatchParser_ParseRecordBatch_InvalidMagic(t *testing.T) {
	parser := NewRecordBatchParser()

	// Create a batch with invalid magic byte
	recordData := []byte("test record data")
	batch, err := CreateRecordBatch(100, recordData, compression.None)
	require.NoError(t, err)

	// Corrupt the magic byte (at offset 16)
	batch[16] = 1 // Invalid magic byte

	// Parse should fail
	_, err = parser.ParseRecordBatch(batch)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported record batch magic byte")
}

// TestRecordBatchParser_Compression tests compression support
func TestRecordBatchParser_Compression(t *testing.T) {
	parser := NewRecordBatchParser()
	recordData := []byte("This is a test record that should compress well when repeated. " +
		"This is a test record that should compress well when repeated. " +
		"This is a test record that should compress well when repeated.")

	codecs := []compression.CompressionCodec{
		compression.None,
		compression.Gzip,
		compression.Snappy,
		compression.Lz4,
		compression.Zstd,
	}

	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			// Create compressed batch
			batch, err := CreateRecordBatch(200, recordData, codec)
			require.NoError(t, err)

			// Parse the batch
			parsed, err := parser.ParseRecordBatch(batch)
			require.NoError(t, err)

			// Verify compression codec
			assert.Equal(t, codec, parsed.GetCompressionCodec())
			assert.Equal(t, codec != compression.None, parsed.IsCompressed())

			// Decompress and verify data
			decompressed, err := parsed.DecompressRecords()
			require.NoError(t, err)
			assert.Equal(t, recordData, decompressed)
		})
	}
}

// TestRecordBatchParser_CRCValidation tests CRC32 validation
func TestRecordBatchParser_CRCValidation(t *testing.T) {
	parser := NewRecordBatchParser()
	recordData := []byte("test record for CRC validation")

	// Create a valid batch
	batch, err := CreateRecordBatch(300, recordData, compression.None)
	require.NoError(t, err)

	t.Run("Valid CRC", func(t *testing.T) {
		// Parse with CRC validation should succeed
		parsed, err := parser.ParseRecordBatchWithValidation(batch, true)
		require.NoError(t, err)
		assert.Equal(t, int64(300), parsed.BaseOffset)
	})

	t.Run("Invalid CRC", func(t *testing.T) {
		// Corrupt the CRC field
		corruptedBatch := make([]byte, len(batch))
		copy(corruptedBatch, batch)
		corruptedBatch[17] = 0xFF // Corrupt CRC

		// Parse with CRC validation should fail
		_, err := parser.ParseRecordBatchWithValidation(corruptedBatch, true)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "CRC validation failed")
	})

	t.Run("Skip CRC validation", func(t *testing.T) {
		// Corrupt the CRC field
		corruptedBatch := make([]byte, len(batch))
		copy(corruptedBatch, batch)
		corruptedBatch[17] = 0xFF // Corrupt CRC

		// Parse without CRC validation should succeed
		parsed, err := parser.ParseRecordBatchWithValidation(corruptedBatch, false)
		require.NoError(t, err)
		assert.Equal(t, int64(300), parsed.BaseOffset)
	})
}

// TestRecordBatchParser_ExtractRecords tests record extraction
func TestRecordBatchParser_ExtractRecords(t *testing.T) {
	parser := NewRecordBatchParser()
	recordData := []byte("test record data for extraction")

	// Create a batch
	batch, err := CreateRecordBatch(400, recordData, compression.Gzip)
	require.NoError(t, err)

	// Parse the batch
	parsed, err := parser.ParseRecordBatch(batch)
	require.NoError(t, err)

	// Extract records
	records, err := parsed.ExtractRecords()
	require.NoError(t, err)

	// Verify extracted records (simplified implementation returns 1 record)
	assert.Len(t, records, 1)
	assert.Equal(t, int64(400), records[0].Offset)
	assert.Equal(t, recordData, records[0].Value)
}

// TestCompressRecordBatch tests the compression helper function
func TestCompressRecordBatch(t *testing.T) {
	recordData := []byte("test data for compression")

	t.Run("No compression", func(t *testing.T) {
		compressed, attributes, err := CompressRecordBatch(compression.None, recordData)
		require.NoError(t, err)
		assert.Equal(t, recordData, compressed)
		assert.Equal(t, int16(0), attributes)
	})

	t.Run("Gzip compression", func(t *testing.T) {
		compressed, attributes, err := CompressRecordBatch(compression.Gzip, recordData)
		require.NoError(t, err)
		assert.NotEqual(t, recordData, compressed)
		assert.Equal(t, int16(1), attributes)

		// Verify we can decompress
		decompressed, err := compression.Decompress(compression.Gzip, compressed)
		require.NoError(t, err)
		assert.Equal(t, recordData, decompressed)
	})
}

// TestCreateRecordBatch tests record batch creation
func TestCreateRecordBatch(t *testing.T) {
	recordData := []byte("test record data")
	baseOffset := int64(500)

	t.Run("Uncompressed batch", func(t *testing.T) {
		batch, err := CreateRecordBatch(baseOffset, recordData, compression.None)
		require.NoError(t, err)
		assert.True(t, len(batch) >= 61) // Minimum header size

		// Parse and verify
		parser := NewRecordBatchParser()
		parsed, err := parser.ParseRecordBatch(batch)
		require.NoError(t, err)
		assert.Equal(t, baseOffset, parsed.BaseOffset)
		assert.Equal(t, compression.None, parsed.GetCompressionCodec())
	})

	t.Run("Compressed batch", func(t *testing.T) {
		batch, err := CreateRecordBatch(baseOffset, recordData, compression.Snappy)
		require.NoError(t, err)
		assert.True(t, len(batch) >= 61) // Minimum header size

		// Parse and verify
		parser := NewRecordBatchParser()
		parsed, err := parser.ParseRecordBatch(batch)
		require.NoError(t, err)
		assert.Equal(t, baseOffset, parsed.BaseOffset)
		assert.Equal(t, compression.Snappy, parsed.GetCompressionCodec())
		assert.True(t, parsed.IsCompressed())

		// Verify decompression works
		decompressed, err := parsed.DecompressRecords()
		require.NoError(t, err)
		assert.Equal(t, recordData, decompressed)
	})
}

// TestRecordBatchParser_InvalidRecordCount tests handling of invalid record counts
func TestRecordBatchParser_InvalidRecordCount(t *testing.T) {
	parser := NewRecordBatchParser()

	// Create a valid batch first
	recordData := []byte("test record data")
	batch, err := CreateRecordBatch(100, recordData, compression.None)
	require.NoError(t, err)

	// Corrupt the record count field (at offset 57-60)
	// Set to a very large number
	batch[57] = 0xFF
	batch[58] = 0xFF
	batch[59] = 0xFF
	batch[60] = 0xFF

	// Parse should fail
	_, err = parser.ParseRecordBatch(batch)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid record count")
}

// BenchmarkRecordBatchParser tests parsing performance
func BenchmarkRecordBatchParser(b *testing.B) {
	parser := NewRecordBatchParser()
	recordData := make([]byte, 1024) // 1KB record
	for i := range recordData {
		recordData[i] = byte(i % 256)
	}

	codecs := []compression.CompressionCodec{
		compression.None,
		compression.Gzip,
		compression.Snappy,
		compression.Lz4,
		compression.Zstd,
	}

	for _, codec := range codecs {
		batch, err := CreateRecordBatch(0, recordData, codec)
		if err != nil {
			b.Fatal(err)
		}

		b.Run("Parse_"+codec.String(), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := parser.ParseRecordBatch(batch)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run("Decompress_"+codec.String(), func(b *testing.B) {
			parsed, err := parser.ParseRecordBatch(batch)
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := parsed.DecompressRecords()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

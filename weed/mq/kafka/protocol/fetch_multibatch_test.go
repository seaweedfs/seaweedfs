package protocol

import (
	"encoding/binary"
	"testing"
)

func TestHandler_getMultipleRecordBatches(t *testing.T) {
	// Create mock record batches
	batch1 := createMockRecordBatch(100, 1) // offset 100, 1 record
	batch2 := createMockRecordBatch(101, 2) // offset 101, 2 records
	batch3 := createMockRecordBatch(102, 1) // offset 102, 1 record

	// Test the concatenation logic directly by creating a custom handler
	handler := &Handler{}

	// Store batches in a test map
	testBatches := map[int64][]byte{
		100: batch1,
		101: batch2,
		102: batch3,
	}

	// Create a test version of getMultipleRecordBatches that uses our test data
	getMultipleRecordBatchesTest := func(topicName string, partitionID int32, startOffset, highWaterMark int64) []byte {
		var combinedBatches []byte
		var batchCount int
		const maxBatchSize = 1024 * 1024 // 1MB limit for combined batches

		// Try to get all available record batches from startOffset to highWaterMark-1
		for offset := startOffset; offset < highWaterMark && len(combinedBatches) < maxBatchSize; offset++ {
			if batch, exists := testBatches[offset]; exists {
				// Validate batch format before concatenation
				if !handler.isValidRecordBatch(batch) {
					continue
				}

				// Check if adding this batch would exceed size limit
				if len(combinedBatches)+len(batch) > maxBatchSize {
					break
				}

				// Concatenate the batch directly
				combinedBatches = append(combinedBatches, batch...)
				batchCount++
			} else {
				break
			}
		}

		return combinedBatches
	}

	tests := []struct {
		name            string
		startOffset     int64
		highWaterMark   int64
		expectedBatches int
		expectedSize    int
	}{
		{
			name:            "single batch",
			startOffset:     100,
			highWaterMark:   101,
			expectedBatches: 1,
			expectedSize:    len(batch1),
		},
		{
			name:            "multiple batches",
			startOffset:     100,
			highWaterMark:   103,
			expectedBatches: 3,
			expectedSize:    len(batch1) + len(batch2) + len(batch3),
		},
		{
			name:            "no batches available",
			startOffset:     200,
			highWaterMark:   201,
			expectedBatches: 0,
			expectedSize:    0,
		},
		{
			name:            "partial range",
			startOffset:     101,
			highWaterMark:   103,
			expectedBatches: 2,
			expectedSize:    len(batch2) + len(batch3),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getMultipleRecordBatchesTest("test-topic", 0, tt.startOffset, tt.highWaterMark)

			if len(result) != tt.expectedSize {
				t.Errorf("Expected combined size %d, got %d", tt.expectedSize, len(result))
			}

			// Verify that the result contains valid concatenated batches
			if tt.expectedBatches > 0 && len(result) > 0 {
				// For partial range test, the first batch is batch2, not batch1
				var expectedFirstBatch []byte
				if tt.startOffset == 101 {
					expectedFirstBatch = batch2
				} else {
					expectedFirstBatch = batch1
				}

				// Check that we can parse the first batch in the result
				if len(result) >= len(expectedFirstBatch) {
					if !handler.isValidRecordBatch(result[:len(expectedFirstBatch)]) {
						t.Error("First batch in concatenated result is not valid")
					}
				}
			}
		})
	}
}

// isValidRecordBatch validates a record batch for testing purposes
func (h *Handler) isValidRecordBatch(data []byte) bool {
	if len(data) < 61 { // Minimum record batch header size
		return false
	}

	// Check magic byte (at offset 16)
	magic := int8(data[16])
	if magic != 2 {
		return false
	}

	return true
}

func TestHandler_isValidRecordBatch(t *testing.T) {
	handler := &Handler{}

	tests := []struct {
		name     string
		batch    []byte
		expected bool
	}{
		{
			name:     "valid batch",
			batch:    createMockRecordBatch(0, 1),
			expected: true,
		},
		{
			name:     "too short",
			batch:    []byte{1, 2, 3},
			expected: false,
		},
		{
			name:     "invalid magic byte",
			batch:    createInvalidMagicBatch(),
			expected: false,
		},
		{
			name:     "empty batch",
			batch:    []byte{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.isValidRecordBatch(tt.batch)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// createMockRecordBatch creates a minimal valid record batch for testing
func createMockRecordBatch(baseOffset int64, recordCount int32) []byte {
	// Create a minimal record batch with correct structure
	batch := make([]byte, 0, 100)

	// Base offset (8 bytes)
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(baseOffset))
	batch = append(batch, baseOffsetBytes...)

	// Batch length placeholder (4 bytes) - will be filled later
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Partition leader epoch (4 bytes)
	batch = append(batch, 0, 0, 0, 0)

	// Magic byte (1 byte) - version 2
	batch = append(batch, 2)

	// CRC32 (4 bytes) - placeholder
	batch = append(batch, 0, 0, 0, 0)

	// Attributes (2 bytes) - no compression
	batch = append(batch, 0, 0)

	// Last offset delta (4 bytes)
	lastOffsetDelta := recordCount - 1
	batch = append(batch, byte(lastOffsetDelta>>24), byte(lastOffsetDelta>>16), byte(lastOffsetDelta>>8), byte(lastOffsetDelta))

	// First timestamp (8 bytes)
	batch = append(batch, 0, 0, 0, 0, 0, 0, 0, 0)

	// Max timestamp (8 bytes)
	batch = append(batch, 0, 0, 0, 0, 0, 0, 0, 0)

	// Producer ID (8 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	// Producer Epoch (2 bytes) - -1
	batch = append(batch, 0xFF, 0xFF)

	// Base Sequence (4 bytes) - -1
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Record count (4 bytes)
	batch = append(batch, byte(recordCount>>24), byte(recordCount>>16), byte(recordCount>>8), byte(recordCount))

	// Add some dummy record data
	for i := int32(0); i < recordCount; i++ {
		// Length (varint)
		batch = append(batch, 10) // 10 bytes record
		// Attributes (varint)
		batch = append(batch, 0)
		// Timestamp delta (varint)
		batch = append(batch, 0)
		// Offset delta (varint)
		batch = append(batch, byte(i))
		// Key length (varint) - null
		batch = append(batch, 1) // -1 encoded as varint
		// Value length (varint)
		batch = append(batch, 8) // 4 bytes
		// Value
		batch = append(batch, []byte("test")...)
		// Headers count (varint)
		batch = append(batch, 0)
	}

	// Fill in the batch length (excluding base offset and batch length field itself)
	batchLength := len(batch) - 12
	binary.BigEndian.PutUint32(batch[batchLengthPos:batchLengthPos+4], uint32(batchLength))

	return batch
}

// createInvalidMagicBatch creates a batch with invalid magic byte for testing
func createInvalidMagicBatch() []byte {
	batch := createMockRecordBatch(0, 1)
	// Change magic byte to invalid value
	batch[16] = 1 // Should be 2
	return batch
}

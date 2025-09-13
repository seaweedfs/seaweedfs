package protocol

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/compression"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
)

func TestMultiBatchFetcher_FetchMultipleBatches(t *testing.T) {
	handler := NewTestHandler()
	handler.AddTopicForTesting("multibatch-topic", 1)
	
	// Add some test messages
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		handler.seaweedMQHandler.ProduceRecord("multibatch-topic", 0, key, value)
	}

	fetcher := NewMultiBatchFetcher(handler)

	tests := []struct {
		name           string
		startOffset    int64
		highWaterMark  int64
		maxBytes       int32
		expectBatches  int
		expectMinSize  int32
		expectMaxSize  int32
	}{
		{
			name:          "Small maxBytes - few batches",
			startOffset:   0,
			highWaterMark: 100,
			maxBytes:      1000,
			expectBatches: 3, // Algorithm creates ~10 records per batch
			expectMinSize: 600,
			expectMaxSize: 1000,
		},
		{
			name:          "Medium maxBytes - many batches",
			startOffset:   0,
			highWaterMark: 100,
			maxBytes:      5000,
			expectBatches: 10, // Will fetch all 100 records in 10 batches
			expectMinSize: 2000,
			expectMaxSize: 5000,
		},
		{
			name:          "Large maxBytes - all records",
			startOffset:   0,
			highWaterMark: 100,
			maxBytes:      50000,
			expectBatches: 10, // Will fetch all 100 records in 10 batches
			expectMinSize: 2000,
			expectMaxSize: 50000,
		},
		{
			name:          "Limited records",
			startOffset:   90,
			highWaterMark: 95,
			maxBytes:      50000,
			expectBatches: 1,
			expectMinSize: 100,
			expectMaxSize: 2000,
		},
		{
			name:          "No records available",
			startOffset:   100,
			highWaterMark: 100,
			maxBytes:      1000,
			expectBatches: 0,
			expectMinSize: 0,
			expectMaxSize: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fetcher.FetchMultipleBatches("multibatch-topic", 0, tt.startOffset, tt.highWaterMark, tt.maxBytes)
			if err != nil {
				t.Fatalf("FetchMultipleBatches() error = %v", err)
			}

			// Check batch count
			if result.BatchCount != tt.expectBatches {
				t.Errorf("BatchCount = %d, want %d", result.BatchCount, tt.expectBatches)
			}

			// Check size constraints
			if result.TotalSize < tt.expectMinSize {
				t.Errorf("TotalSize = %d, want >= %d", result.TotalSize, tt.expectMinSize)
			}
			if result.TotalSize > tt.expectMaxSize {
				t.Errorf("TotalSize = %d, want <= %d", result.TotalSize, tt.expectMaxSize)
			}

			// Check that response doesn't exceed maxBytes
			if result.TotalSize > tt.maxBytes && tt.expectBatches > 0 {
				t.Errorf("TotalSize %d exceeds maxBytes %d", result.TotalSize, tt.maxBytes)
			}

			// Check next offset progression
			if tt.expectBatches > 0 && result.NextOffset <= tt.startOffset {
				t.Errorf("NextOffset %d should be > startOffset %d", result.NextOffset, tt.startOffset)
			}

			// Validate record batch structure if we have data
			if len(result.RecordBatches) > 0 {
				if err := validateMultiBatchStructure(result.RecordBatches, result.BatchCount); err != nil {
					t.Errorf("Invalid multi-batch structure: %v", err)
				}
			}
		})
	}
}

func TestMultiBatchFetcher_ConstructSingleRecordBatch(t *testing.T) {
	handler := NewTestHandler()
	fetcher := NewMultiBatchFetcher(handler)

	// Test with mock SMQ records
	mockRecords := createMockSMQRecords(5)
	
	// Convert to interface slice
	var smqRecords []offset.SMQRecord
	for i := range mockRecords {
		smqRecords = append(smqRecords, &mockRecords[i])
	}
	
	batch := fetcher.constructSingleRecordBatch(10, smqRecords)
	
	if len(batch) == 0 {
		t.Fatal("Expected non-empty batch")
	}

	// Check batch structure
	if err := validateRecordBatchStructure(batch); err != nil {
		t.Errorf("Invalid batch structure: %v", err)
	}

	// Check base offset
	baseOffset := int64(binary.BigEndian.Uint64(batch[0:8]))
	if baseOffset != 10 {
		t.Errorf("Base offset = %d, want 10", baseOffset)
	}

	// Check magic byte
	if batch[16] != 2 {
		t.Errorf("Magic byte = %d, want 2", batch[16])
	}
}

func TestMultiBatchFetcher_EmptyBatch(t *testing.T) {
	handler := NewTestHandler()
	fetcher := NewMultiBatchFetcher(handler)

	emptyBatch := fetcher.constructEmptyRecordBatch(42)
	
	if len(emptyBatch) == 0 {
		t.Fatal("Expected non-empty batch even for empty records")
	}

	// Check base offset
	baseOffset := int64(binary.BigEndian.Uint64(emptyBatch[0:8]))
	if baseOffset != 42 {
		t.Errorf("Base offset = %d, want 42", baseOffset)
	}

	// Check record count (should be 0)
	recordCountPos := len(emptyBatch) - 4
	recordCount := binary.BigEndian.Uint32(emptyBatch[recordCountPos : recordCountPos+4])
	if recordCount != 0 {
		t.Errorf("Record count = %d, want 0", recordCount)
	}
}

func TestMultiBatchFetcher_CreateCompressedBatch(t *testing.T) {
	handler := NewTestHandler()
	fetcher := NewMultiBatchFetcher(handler)

	mockRecords := createMockSMQRecords(10)
	
	// Convert to interface slice
	var smqRecords []offset.SMQRecord
	for i := range mockRecords {
		smqRecords = append(smqRecords, &mockRecords[i])
	}

	tests := []struct {
		name  string
		codec compression.CompressionCodec
	}{
		{"No compression", compression.None},
		{"GZIP compression", compression.Gzip},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := fetcher.CreateCompressedBatch(0, smqRecords, tt.codec)
			if err != nil {
				t.Fatalf("CreateCompressedBatch() error = %v", err)
			}

			if result.Codec != tt.codec {
				t.Errorf("Codec = %v, want %v", result.Codec, tt.codec)
			}

			if len(result.CompressedData) == 0 {
				t.Error("Expected non-empty compressed data")
			}

			if result.CompressedSize != int32(len(result.CompressedData)) {
				t.Errorf("CompressedSize = %d, want %d", result.CompressedSize, len(result.CompressedData))
			}

			// For GZIP compression, compressed size should typically be smaller than original
			// (though not guaranteed for very small data)
			if tt.codec == compression.Gzip && result.OriginalSize > 1000 {
				if result.CompressedSize >= result.OriginalSize {
					t.Logf("NOTE: Compressed size (%d) not smaller than original (%d) - may be expected for small data", 
						result.CompressedSize, result.OriginalSize)
				}
			}
		})
	}
}

func TestMultiBatchFetcher_SizeRespectingMaxBytes(t *testing.T) {
	handler := NewTestHandler()
	handler.AddTopicForTesting("size-test-topic", 1)
	
	// Add many large messages
	for i := 0; i < 50; i++ {
		key := make([]byte, 100)   // 100-byte keys
		value := make([]byte, 500) // 500-byte values
		for j := range key {
			key[j] = byte(i % 256)
		}
		for j := range value {
			value[j] = byte((i + j) % 256)
		}
		handler.seaweedMQHandler.ProduceRecord("size-test-topic", 0, key, value)
	}

	fetcher := NewMultiBatchFetcher(handler)

	// Test with strict size limit
	result, err := fetcher.FetchMultipleBatches("size-test-topic", 0, 0, 50, 2000)
	if err != nil {
		t.Fatalf("FetchMultipleBatches() error = %v", err)
	}

	// Should not exceed maxBytes (unless it's a single large batch - Kafka behavior)
	if result.TotalSize > 2000 && result.BatchCount > 1 {
		t.Errorf("TotalSize %d exceeds maxBytes 2000 with %d batches", result.TotalSize, result.BatchCount)
	}
	
	// If we exceed maxBytes, it should be because we have at least one batch
	// (Kafka always returns some data, even if it exceeds maxBytes for the first batch)
	if result.TotalSize > 2000 && result.BatchCount == 0 {
		t.Errorf("TotalSize %d exceeds maxBytes 2000 but no batches returned", result.TotalSize)
	}

	// Should have fetched at least one batch
	if result.BatchCount == 0 {
		t.Error("Expected at least one batch")
	}

	// Should make progress
	if result.NextOffset == 0 {
		t.Error("Expected NextOffset > 0")
	}
}

func TestMultiBatchFetcher_ConcatenationFormat(t *testing.T) {
	handler := NewTestHandler()
	handler.AddTopicForTesting("concat-topic", 1)
	
	// Add enough messages to force multiple batches (30 records > 10 per batch)
	for i := 0; i < 30; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		handler.seaweedMQHandler.ProduceRecord("concat-topic", 0, key, value)
	}

	fetcher := NewMultiBatchFetcher(handler)

	// Fetch multiple batches with smaller maxBytes to force multiple batches
	result, err := fetcher.FetchMultipleBatches("concat-topic", 0, 0, 30, 800)
	if err != nil {
		t.Fatalf("FetchMultipleBatches() error = %v", err)
	}

	if result.BatchCount < 2 {
		t.Skip("Test requires at least 2 batches, got", result.BatchCount)
	}

	// Verify that the concatenated batches can be parsed sequentially
	if err := validateMultiBatchStructure(result.RecordBatches, result.BatchCount); err != nil {
		t.Errorf("Invalid multi-batch concatenation structure: %v", err)
	}
}

// Helper functions

func createMockSMQRecords(count int) []BasicSMQRecord {
	records := make([]BasicSMQRecord, count)
	for i := 0; i < count; i++ {
		records[i] = BasicSMQRecord{
			MessageRecord: &MessageRecord{
				Key:       []byte(fmt.Sprintf("key-%d", i)),
				Value:     []byte(fmt.Sprintf("value-%d-data", i)),
				Timestamp: 1640995200000 + int64(i*1000), // 1 second apart
			},
			offset: int64(i),
		}
	}
	return records
}

func validateRecordBatchStructure(batch []byte) error {
	if len(batch) < 61 {
		return fmt.Errorf("batch too short: %d bytes", len(batch))
	}

	// Check magic byte (position 16)
	if batch[16] != 2 {
		return fmt.Errorf("invalid magic byte: %d", batch[16])
	}

	// Check batch length consistency
	batchLength := binary.BigEndian.Uint32(batch[8:12])
	expectedTotalSize := 12 + int(batchLength)
	if len(batch) != expectedTotalSize {
		return fmt.Errorf("batch length mismatch: header says %d, actual %d", expectedTotalSize, len(batch))
	}

	return nil
}

func validateMultiBatchStructure(concatenatedBatches []byte, expectedBatchCount int) error {
	if len(concatenatedBatches) == 0 {
		if expectedBatchCount == 0 {
			return nil
		}
		return fmt.Errorf("empty concatenated batches but expected %d batches", expectedBatchCount)
	}

	actualBatchCount := 0
	offset := 0

	for offset < len(concatenatedBatches) {
		// Each batch should start with a valid base offset (8 bytes)
		if offset+8 > len(concatenatedBatches) {
			return fmt.Errorf("not enough data for base offset at position %d", offset)
		}

		// Get batch length (next 4 bytes)
		if offset+12 > len(concatenatedBatches) {
			return fmt.Errorf("not enough data for batch length at position %d", offset)
		}

		batchLength := int(binary.BigEndian.Uint32(concatenatedBatches[offset+8 : offset+12]))
		totalBatchSize := 12 + batchLength // base offset (8) + length field (4) + batch content

		if offset+totalBatchSize > len(concatenatedBatches) {
			return fmt.Errorf("batch extends beyond available data: need %d, have %d", offset+totalBatchSize, len(concatenatedBatches))
		}

		// Validate this individual batch
		individualBatch := concatenatedBatches[offset : offset+totalBatchSize]
		if err := validateRecordBatchStructure(individualBatch); err != nil {
			return fmt.Errorf("invalid batch %d structure: %v", actualBatchCount, err)
		}

		offset += totalBatchSize
		actualBatchCount++
	}

	if actualBatchCount != expectedBatchCount {
		return fmt.Errorf("parsed %d batches, expected %d", actualBatchCount, expectedBatchCount)
	}

	return nil
}

func BenchmarkMultiBatchFetcher_FetchMultipleBatches(b *testing.B) {
	handler := NewTestHandler()
	handler.AddTopicForTesting("benchmark-topic", 1)
	
	// Pre-populate with many messages
	for i := 0; i < 1000; i++ {
		key := []byte("benchmark-key-" + string(rune(i)))
		value := make([]byte, 200) // 200-byte values
		for j := range value {
			value[j] = byte((i + j) % 256)
		}
		handler.seaweedMQHandler.ProduceRecord("benchmark-topic", 0, key, value)
	}

	fetcher := NewMultiBatchFetcher(handler)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		startOffset := int64(i % 900) // Vary starting position
		_, err := fetcher.FetchMultipleBatches("benchmark-topic", 0, startOffset, 1000, 10000)
		if err != nil {
			b.Fatalf("FetchMultipleBatches() error = %v", err)
		}
	}
}

func BenchmarkMultiBatchFetcher_ConstructSingleRecordBatch(b *testing.B) {
	handler := NewTestHandler()
	fetcher := NewMultiBatchFetcher(handler)
	mockRecords := createMockSMQRecords(50)
	
	// Convert to interface slice
	var smqRecords []offset.SMQRecord
	for i := range mockRecords {
		smqRecords = append(smqRecords, &mockRecords[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = fetcher.constructSingleRecordBatch(int64(i), smqRecords)
	}
}

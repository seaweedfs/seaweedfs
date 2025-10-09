package protocol

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/compression"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
)

// MultiBatchFetcher handles fetching multiple record batches with size limits
type MultiBatchFetcher struct {
	handler *Handler
}

// NewMultiBatchFetcher creates a new multi-batch fetcher
func NewMultiBatchFetcher(handler *Handler) *MultiBatchFetcher {
	return &MultiBatchFetcher{handler: handler}
}

// FetchResult represents the result of a multi-batch fetch operation
type FetchResult struct {
	RecordBatches []byte // Concatenated record batches
	NextOffset    int64  // Next offset to fetch from
	TotalSize     int32  // Total size of all batches
	BatchCount    int    // Number of batches included
}

// FetchMultipleBatches fetches multiple record batches up to maxBytes limit
func (f *MultiBatchFetcher) FetchMultipleBatches(topicName string, partitionID int32, startOffset, highWaterMark int64, maxBytes int32) (*FetchResult, error) {
	Debug("[DEBUG_MULTIBATCH] FetchMultipleBatches: topic=%s partition=%d startOffset=%d highWaterMark=%d maxBytes=%d",
		topicName, partitionID, startOffset, highWaterMark, maxBytes)

	if startOffset >= highWaterMark {
		Debug("[DEBUG_MULTIBATCH] startOffset >= highWaterMark, returning empty result")
		return &FetchResult{
			RecordBatches: []byte{},
			NextOffset:    startOffset,
			TotalSize:     0,
			BatchCount:    0,
		}, nil
	}

	// Minimum size for basic response headers and one empty batch
	minResponseSize := int32(200)
	if maxBytes < minResponseSize {
		maxBytes = minResponseSize
	}

	var combinedBatches []byte
	currentOffset := startOffset
	totalSize := int32(0)
	batchCount := 0

	// Parameters for batch fetching - start smaller to respect maxBytes better
	recordsPerBatch := int32(10) // Start with smaller batch size
	maxBatchesPerFetch := 10     // Limit number of batches to avoid infinite loops

	Debug("[DEBUG_MULTIBATCH] Starting batch fetch loop: batchCount=%d currentOffset=%d", batchCount, currentOffset)

	for batchCount < maxBatchesPerFetch && currentOffset < highWaterMark {
		Debug("[DEBUG_MULTIBATCH] Loop iteration: batchCount=%d currentOffset=%d highWaterMark=%d", batchCount, currentOffset, highWaterMark)

		// Calculate remaining space
		remainingBytes := maxBytes - totalSize
		if remainingBytes < 100 { // Need at least 100 bytes for a minimal batch
			Debug("[DEBUG_MULTIBATCH] Not enough remaining bytes: %d", remainingBytes)
			break
		}

		// Adapt records per batch based on remaining space
		if remainingBytes < 1000 {
			recordsPerBatch = 10 // Smaller batches when space is limited
		}

		// Calculate how many records to fetch for this batch
		recordsAvailable := highWaterMark - currentOffset
		if recordsAvailable <= 0 {
			Debug("[DEBUG_MULTIBATCH] No records available (currentOffset=%d >= highWaterMark=%d), breaking",
				currentOffset, highWaterMark)
			break
		}

		recordsToFetch := recordsPerBatch
		if int64(recordsToFetch) > recordsAvailable {
			recordsToFetch = int32(recordsAvailable)
		}

		Debug("[DEBUG_MULTIBATCH] About to call GetStoredRecords: topic=%s partition=%d offset=%d count=%d (available=%d)",
			topicName, partitionID, currentOffset, recordsToFetch, recordsAvailable)

		// Check if handler is nil
		if f.handler == nil {
			Debug("[DEBUG_MULTIBATCH] ERROR: f.handler is nil")
			break
		}
		if f.handler.seaweedMQHandler == nil {
			Debug("[DEBUG_MULTIBATCH] ERROR: f.handler.seaweedMQHandler is nil")
			break
		}

		// Fetch records for this batch
		getRecordsStartTime := time.Now()
		smqRecords, err := f.handler.seaweedMQHandler.GetStoredRecords(topicName, partitionID, currentOffset, int(recordsToFetch))
		getRecordsDuration := time.Since(getRecordsStartTime)
		Debug("[DEBUG_MULTIBATCH] GetStoredRecords returned: records=%d err=%v duration=%v", len(smqRecords), err, getRecordsDuration)

		if err != nil || len(smqRecords) == 0 {
			Debug("[DEBUG_MULTIBATCH] Breaking loop: err=%v recordCount=%d", err, len(smqRecords))
			break
		}

		// Note: we construct the batch and check actual size after construction

		// Construct record batch
		batch := f.constructSingleRecordBatch(topicName, currentOffset, smqRecords)
		batchSize := int32(len(batch))

		if strings.HasPrefix(topicName, "_schemas") {
			// Log first record details for debugging deserialization
			if len(smqRecords) > 0 {
				for i, rec := range smqRecords {
					if i < 3 { // Log first 3 records
						_ = i
						_ = rec
					}
				}
			}
		}

		// Double-check actual size doesn't exceed maxBytes
		if totalSize+batchSize > maxBytes && batchCount > 0 {
			break
		}

		// Add this batch to combined result
		combinedBatches = append(combinedBatches, batch...)
		totalSize += batchSize
		currentOffset += int64(len(smqRecords))
		batchCount++

		// If this is a small batch, we might be at the end
		if len(smqRecords) < int(recordsPerBatch) {
			break
		}
	}

	result := &FetchResult{
		RecordBatches: combinedBatches,
		NextOffset:    currentOffset,
		TotalSize:     totalSize,
		BatchCount:    batchCount,
	}

	// Log for _schemas topic
	if strings.HasPrefix(topicName, "_schemas") {
		glog.Infof("SR MULTIBATCH RESULT: topic=%s partition=%d startOffset=%d nextOffset=%d batchCount=%d totalSize=%d",
			topicName, partitionID, startOffset, currentOffset, batchCount, totalSize)
	}

	return result, nil
}

// constructSingleRecordBatch creates a single record batch from SMQ records
func (f *MultiBatchFetcher) constructSingleRecordBatch(topicName string, baseOffset int64, smqRecords []integration.SMQRecord) []byte {
	if len(smqRecords) == 0 {
		return f.constructEmptyRecordBatch(baseOffset)
	}

	// Create record batch using the SMQ records
	batch := make([]byte, 0, 512)

	// Record batch header
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(baseOffset))
	batch = append(batch, baseOffsetBytes...) // base offset (8 bytes)

	// Calculate batch length (will be filled after we know the size)
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0) // batch length placeholder (4 bytes)

	// Partition leader epoch (4 bytes) - use -1 for no epoch
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Magic byte (1 byte) - v2 format
	batch = append(batch, 2)

	// CRC placeholder (4 bytes) - will be calculated later
	crcPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Attributes (2 bytes) - no compression, etc.
	batch = append(batch, 0, 0)

	// Last offset delta (4 bytes)
	lastOffsetDelta := int32(len(smqRecords) - 1)
	lastOffsetDeltaBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lastOffsetDeltaBytes, uint32(lastOffsetDelta))
	batch = append(batch, lastOffsetDeltaBytes...)

	// Base timestamp (8 bytes) - convert from nanoseconds to milliseconds for Kafka compatibility
	baseTimestamp := smqRecords[0].GetTimestamp() / 1000000 // Convert nanoseconds to milliseconds
	baseTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseTimestampBytes, uint64(baseTimestamp))
	batch = append(batch, baseTimestampBytes...)

	// Max timestamp (8 bytes) - convert from nanoseconds to milliseconds for Kafka compatibility
	maxTimestamp := baseTimestamp
	if len(smqRecords) > 1 {
		maxTimestamp = smqRecords[len(smqRecords)-1].GetTimestamp() / 1000000 // Convert nanoseconds to milliseconds
	}
	maxTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(maxTimestampBytes, uint64(maxTimestamp))
	batch = append(batch, maxTimestampBytes...)

	// Producer ID (8 bytes) - use -1 for no producer ID
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	// Producer epoch (2 bytes) - use -1 for no producer epoch
	batch = append(batch, 0xFF, 0xFF)

	// Base sequence (4 bytes) - use -1 for no base sequence
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Records count (4 bytes)
	recordCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordCountBytes, uint32(len(smqRecords)))
	batch = append(batch, recordCountBytes...)

	// Add individual records from SMQ records
	for i, smqRecord := range smqRecords {
		// Build individual record
		recordBytes := make([]byte, 0, 128)

		// Record attributes (1 byte)
		recordBytes = append(recordBytes, 0)

		// Timestamp delta (varint) - calculate from base timestamp (both in milliseconds)
		recordTimestampMs := smqRecord.GetTimestamp() / 1000000 // Convert nanoseconds to milliseconds
		timestampDelta := recordTimestampMs - baseTimestamp     // Both in milliseconds now
		recordBytes = append(recordBytes, encodeVarint(timestampDelta)...)

		// Offset delta (varint)
		offsetDelta := int64(i)
		recordBytes = append(recordBytes, encodeVarint(offsetDelta)...)

		// Key length and key (varint + data) - decode RecordValue to get original Kafka message
		key := f.handler.decodeRecordValueToKafkaMessage(topicName, smqRecord.GetKey())
		if key == nil {
			recordBytes = append(recordBytes, encodeVarint(-1)...) // null key
		} else {
			recordBytes = append(recordBytes, encodeVarint(int64(len(key)))...)
			recordBytes = append(recordBytes, key...)
		}

		// Value length and value (varint + data) - decode RecordValue to get original Kafka message
		value := f.handler.decodeRecordValueToKafkaMessage(topicName, smqRecord.GetValue())

		if value == nil {
			recordBytes = append(recordBytes, encodeVarint(-1)...) // null value
		} else {
			recordBytes = append(recordBytes, encodeVarint(int64(len(value)))...)
			recordBytes = append(recordBytes, value...)
		}

		// Headers count (varint) - 0 headers
		recordBytes = append(recordBytes, encodeVarint(0)...)

		// Prepend record length (varint)
		recordLength := int64(len(recordBytes))
		batch = append(batch, encodeVarint(recordLength)...)
		batch = append(batch, recordBytes...)
	}

	// Fill in the batch length
	batchLength := uint32(len(batch) - batchLengthPos - 4)
	binary.BigEndian.PutUint32(batch[batchLengthPos:batchLengthPos+4], batchLength)

	// Log reconstructed batch size and detailed field breakdown
	fmt.Printf("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Printf("📏 RECONSTRUCTED BATCH: topic=%s baseOffset=%d size=%d bytes, recordCount=%d\n",
		topicName, baseOffset, len(batch), len(smqRecords))

	if len(batch) >= 61 {
		fmt.Printf("  Header Structure:\n")
		fmt.Printf("    Base Offset (0-7):     %x\n", batch[0:8])
		fmt.Printf("    Batch Length (8-11):   %x\n", batch[8:12])
		fmt.Printf("    Leader Epoch (12-15):  %x\n", batch[12:16])
		fmt.Printf("    Magic (16):            %x\n", batch[16:17])
		fmt.Printf("    CRC (17-20):           %x (WILL BE CALCULATED)\n", batch[17:21])
		fmt.Printf("    Attributes (21-22):    %x\n", batch[21:23])
		fmt.Printf("    Last Offset Delta (23-26): %x\n", batch[23:27])
		fmt.Printf("    Base Timestamp (27-34): %x\n", batch[27:35])
		fmt.Printf("    Max Timestamp (35-42):  %x\n", batch[35:43])
		fmt.Printf("    Producer ID (43-50):    %x\n", batch[43:51])
		fmt.Printf("    Producer Epoch (51-52): %x\n", batch[51:53])
		fmt.Printf("    Base Sequence (53-56):  %x\n", batch[53:57])
		fmt.Printf("    Record Count (57-60):   %x\n", batch[57:61])
		if len(batch) > 61 {
			fmt.Printf("    Records Section (61+):  %x... (%d bytes)\n",
				batch[61:min(81, len(batch))], len(batch)-61)
		}
	}

	// Calculate CRC32 for the batch
	// Per Kafka spec: CRC covers ONLY from attributes offset (byte 21) onwards
	// See: DefaultRecordBatch.java computeChecksum() - Crc32C.compute(buffer, ATTRIBUTES_OFFSET, ...)
	crcData := batch[crcPos+4:] // Skip CRC field itself, include rest
	crc := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))

	// === COMPREHENSIVE CRC DEBUG ===
	batchLengthValue := binary.BigEndian.Uint32(batch[8:12])
	expectedTotalSize := 12 + int(batchLengthValue)
	actualTotalSize := len(batch)

	fmt.Printf("\n  === CRC CALCULATION DEBUG ===\n")
	fmt.Printf("    Batch length field (bytes 8-11): %d\n", batchLengthValue)
	fmt.Printf("    Expected total batch size: %d bytes (12 + %d)\n", expectedTotalSize, batchLengthValue)
	fmt.Printf("    Actual batch size: %d bytes\n", actualTotalSize)
	fmt.Printf("    CRC position: byte %d\n", crcPos)
	fmt.Printf("    CRC data range: bytes %d to %d (%d bytes)\n", crcPos+4, actualTotalSize-1, len(crcData))

	if expectedTotalSize != actualTotalSize {
		fmt.Printf("    SIZE MISMATCH: %d bytes difference!\n", actualTotalSize-expectedTotalSize)
	}

	if crcPos != 17 {
		fmt.Printf("    CRC POSITION WRONG: expected 17, got %d!\n", crcPos)
	}

	fmt.Printf("    CRC data (first 100 bytes of %d):\n", len(crcData))
	dumpSize := 100
	if len(crcData) < dumpSize {
		dumpSize = len(crcData)
	}
	for i := 0; i < dumpSize; i += 20 {
		end := i + 20
		if end > dumpSize {
			end = dumpSize
		}
		fmt.Printf("      [%3d-%3d]: %x\n", i, end-1, crcData[i:end])
	}

	manualCRC := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	fmt.Printf("    Calculated CRC: 0x%08x\n", crc)
	fmt.Printf("    Manual verify:  0x%08x", manualCRC)
	if crc == manualCRC {
		fmt.Printf(" OK\n")
	} else {
		fmt.Printf(" MISMATCH!\n")
	}

	if actualTotalSize <= 200 {
		fmt.Printf("    Complete batch hex dump (%d bytes):\n", actualTotalSize)
		for i := 0; i < actualTotalSize; i += 16 {
			end := i + 16
			if end > actualTotalSize {
				end = actualTotalSize
			}
			fmt.Printf("      %04d: %x\n", i, batch[i:end])
		}
	}
	fmt.Printf("  === END CRC DEBUG ===\n\n")
	binary.BigEndian.PutUint32(batch[crcPos:crcPos+4], crc)

	fmt.Printf("    Final CRC (17-20):     %x (calculated over %d bytes)\n", batch[17:21], len(crcData))

	// VERIFICATION: Read back what we just wrote
	writtenCRC := binary.BigEndian.Uint32(batch[17:21])
	fmt.Printf("    VERIFICATION: CRC we calculated=0x%x, CRC written to batch=0x%x", crc, writtenCRC)
	if crc == writtenCRC {
		fmt.Printf(" OK\n")
	} else {
		fmt.Printf(" MISMATCH!\n")
	}

	// DEBUG: Hash the entire batch to check if reconstructions are identical
	batchHash := crc32.ChecksumIEEE(batch)
	fmt.Printf("    BATCH IDENTITY: hash=0x%08x size=%d topic=%s baseOffset=%d recordCount=%d\n",
		batchHash, len(batch), topicName, baseOffset, len(smqRecords))

	// DEBUG: Show first few record keys/values to verify consistency
	if len(smqRecords) > 0 && strings.Contains(topicName, "loadtest") {
		fmt.Printf("    RECORD SAMPLES:\n")
		for i := 0; i < min(3, len(smqRecords)); i++ {
			keyPreview := smqRecords[i].GetKey()
			if len(keyPreview) > 20 {
				keyPreview = keyPreview[:20]
			}
			valuePreview := smqRecords[i].GetValue()
			if len(valuePreview) > 40 {
				valuePreview = valuePreview[:40]
			}
			fmt.Printf("      [%d] keyLen=%d valueLen=%d keyHex=%x valueHex=%x\n",
				i, len(smqRecords[i].GetKey()), len(smqRecords[i].GetValue()),
				keyPreview, valuePreview)
		}
	}

	fmt.Printf("    Batch for topic=%s baseOffset=%d recordCount=%d\n", topicName, baseOffset, len(smqRecords))
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

	return batch
}

// constructEmptyRecordBatch creates an empty record batch
func (f *MultiBatchFetcher) constructEmptyRecordBatch(baseOffset int64) []byte {
	// Create minimal empty record batch
	batch := make([]byte, 0, 61)

	// Base offset (8 bytes)
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(baseOffset))
	batch = append(batch, baseOffsetBytes...)

	// Batch length (4 bytes) - will be filled at the end
	lengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Partition leader epoch (4 bytes) - -1
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Magic byte (1 byte) - version 2
	batch = append(batch, 2)

	// CRC32 (4 bytes) - placeholder
	crcPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Attributes (2 bytes) - no compression, no transactional
	batch = append(batch, 0, 0)

	// Last offset delta (4 bytes) - -1 for empty batch
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Base timestamp (8 bytes)
	timestamp := uint64(1640995200000) // Fixed timestamp for empty batches
	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, timestamp)
	batch = append(batch, timestampBytes...)

	// Max timestamp (8 bytes) - same as base for empty batch
	batch = append(batch, timestampBytes...)

	// Producer ID (8 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	// Producer Epoch (2 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF)

	// Base Sequence (4 bytes) - -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Record count (4 bytes) - 0 for empty batch
	batch = append(batch, 0, 0, 0, 0)

	// Fill in the batch length
	batchLength := len(batch) - 12 // Exclude base offset and length field itself
	binary.BigEndian.PutUint32(batch[lengthPos:lengthPos+4], uint32(batchLength))

	// Calculate CRC32 for the batch
	// Per Kafka spec: CRC covers ONLY from attributes offset (byte 21) onwards
	// See: DefaultRecordBatch.java computeChecksum() - Crc32C.compute(buffer, ATTRIBUTES_OFFSET, ...)
	crcData := batch[crcPos+4:] // Skip CRC field itself, include rest
	crc := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[crcPos:crcPos+4], crc)

	return batch
}

// CompressedBatchResult represents a compressed record batch result
type CompressedBatchResult struct {
	CompressedData []byte
	OriginalSize   int32
	CompressedSize int32
	Codec          compression.CompressionCodec
}

// CreateCompressedBatch creates a compressed record batch (basic support)
func (f *MultiBatchFetcher) CreateCompressedBatch(baseOffset int64, smqRecords []integration.SMQRecord, codec compression.CompressionCodec) (*CompressedBatchResult, error) {
	if codec == compression.None {
		// No compression requested
		batch := f.constructSingleRecordBatch("", baseOffset, smqRecords)
		return &CompressedBatchResult{
			CompressedData: batch,
			OriginalSize:   int32(len(batch)),
			CompressedSize: int32(len(batch)),
			Codec:          compression.None,
		}, nil
	}

	// For Phase 5, implement basic GZIP compression support
	originalBatch := f.constructSingleRecordBatch("", baseOffset, smqRecords)
	originalSize := int32(len(originalBatch))

	compressedData, err := f.compressData(originalBatch, codec)
	if err != nil {
		// Fall back to uncompressed if compression fails
		return &CompressedBatchResult{
			CompressedData: originalBatch,
			OriginalSize:   originalSize,
			CompressedSize: originalSize,
			Codec:          compression.None,
		}, nil
	}

	// Create compressed record batch with proper headers
	compressedBatch := f.constructCompressedRecordBatch(baseOffset, compressedData, codec, originalSize)

	return &CompressedBatchResult{
		CompressedData: compressedBatch,
		OriginalSize:   originalSize,
		CompressedSize: int32(len(compressedBatch)),
		Codec:          codec,
	}, nil
}

// constructCompressedRecordBatch creates a record batch with compressed records
func (f *MultiBatchFetcher) constructCompressedRecordBatch(baseOffset int64, compressedRecords []byte, codec compression.CompressionCodec, originalSize int32) []byte {
	// Validate size to prevent overflow
	const maxBatchSize = 1 << 30 // 1 GB limit
	if len(compressedRecords) > maxBatchSize-100 {
		glog.Errorf("Compressed records too large: %d bytes", len(compressedRecords))
		return nil
	}
	batch := make([]byte, 0, len(compressedRecords)+100)

	// Record batch header is similar to regular batch
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(baseOffset))
	batch = append(batch, baseOffsetBytes...)

	// Batch length (4 bytes) - will be filled later
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Partition leader epoch (4 bytes)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Magic byte (1 byte) - v2 format
	batch = append(batch, 2)

	// CRC placeholder (4 bytes)
	crcPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Attributes (2 bytes) - set compression bits
	var compressionBits uint16
	switch codec {
	case compression.Gzip:
		compressionBits = 1
	case compression.Snappy:
		compressionBits = 2
	case compression.Lz4:
		compressionBits = 3
	case compression.Zstd:
		compressionBits = 4
	default:
		compressionBits = 0 // no compression
	}
	batch = append(batch, byte(compressionBits>>8), byte(compressionBits))

	// Last offset delta (4 bytes) - for compressed batches, this represents the logical record count
	batch = append(batch, 0, 0, 0, 0) // Will be set based on logical records

	// Timestamps (16 bytes) - use current time for compressed batches
	timestamp := uint64(1640995200000)
	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, timestamp)
	batch = append(batch, timestampBytes...) // first timestamp
	batch = append(batch, timestampBytes...) // max timestamp

	// Producer fields (14 bytes total)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF) // producer ID
	batch = append(batch, 0xFF, 0xFF)                                     // producer epoch
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)                         // base sequence

	// Record count (4 bytes) - for compressed batches, this is the number of logical records
	batch = append(batch, 0, 0, 0, 1) // Placeholder: treat as 1 logical record

	// Compressed records data
	batch = append(batch, compressedRecords...)

	// Fill in the batch length
	batchLength := uint32(len(batch) - batchLengthPos - 4)
	binary.BigEndian.PutUint32(batch[batchLengthPos:batchLengthPos+4], batchLength)

	// Calculate CRC32 for the batch
	// Per Kafka spec: CRC covers ONLY from attributes offset (byte 21) onwards
	// See: DefaultRecordBatch.java computeChecksum() - Crc32C.compute(buffer, ATTRIBUTES_OFFSET, ...)
	crcData := batch[crcPos+4:] // Skip CRC field itself, include rest
	crc := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[crcPos:crcPos+4], crc)

	return batch
}

// estimateBatchSize estimates the size of a record batch before constructing it
func (f *MultiBatchFetcher) estimateBatchSize(smqRecords []integration.SMQRecord) int32 {
	if len(smqRecords) == 0 {
		return 61 // empty batch header size
	}

	// Record batch header: 61 bytes (base_offset + batch_length + leader_epoch + magic + crc + attributes +
	// last_offset_delta + first_ts + max_ts + producer_id + producer_epoch + base_seq + record_count)
	headerSize := int32(61)

	baseTs := smqRecords[0].GetTimestamp()
	recordsSize := int32(0)
	for i, rec := range smqRecords {
		// attributes(1)
		rb := int32(1)

		// timestamp_delta(varint)
		tsDelta := rec.GetTimestamp() - baseTs
		rb += int32(len(encodeVarint(tsDelta)))

		// offset_delta(varint)
		rb += int32(len(encodeVarint(int64(i))))

		// key length varint + data or -1
		if k := rec.GetKey(); k != nil {
			rb += int32(len(encodeVarint(int64(len(k))))) + int32(len(k))
		} else {
			rb += int32(len(encodeVarint(-1)))
		}

		// value length varint + data or -1
		if v := rec.GetValue(); v != nil {
			rb += int32(len(encodeVarint(int64(len(v))))) + int32(len(v))
		} else {
			rb += int32(len(encodeVarint(-1)))
		}

		// headers count (varint = 0)
		rb += int32(len(encodeVarint(0)))

		// prepend record length varint
		recordsSize += int32(len(encodeVarint(int64(rb)))) + rb
	}

	return headerSize + recordsSize
}

// sizeOfVarint returns the number of bytes encodeVarint would use for value
func sizeOfVarint(value int64) int32 {
	// ZigZag encode to match encodeVarint
	u := uint64(uint64(value<<1) ^ uint64(value>>63))
	size := int32(1)
	for u >= 0x80 {
		u >>= 7
		size++
	}
	return size
}

// compressData compresses data using the specified codec (basic implementation)
func (f *MultiBatchFetcher) compressData(data []byte, codec compression.CompressionCodec) ([]byte, error) {
	// For Phase 5, implement basic compression support
	switch codec {
	case compression.None:
		return data, nil
	case compression.Gzip:
		// Implement actual GZIP compression
		var buf bytes.Buffer
		gzipWriter := gzip.NewWriter(&buf)

		if _, err := gzipWriter.Write(data); err != nil {
			gzipWriter.Close()
			return nil, fmt.Errorf("gzip compression write failed: %w", err)
		}

		if err := gzipWriter.Close(); err != nil {
			return nil, fmt.Errorf("gzip compression close failed: %w", err)
		}

		compressed := buf.Bytes()
		Debug("GZIP compression: %d bytes -> %d bytes (%.1f%% reduction)",
			len(data), len(compressed), 100.0*(1.0-float64(len(compressed))/float64(len(data))))

		return compressed, nil
	default:
		return nil, fmt.Errorf("unsupported compression codec: %d", codec)
	}
}

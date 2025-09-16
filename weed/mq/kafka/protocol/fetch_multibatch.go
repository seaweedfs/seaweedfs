package protocol

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/compression"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
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
	if startOffset >= highWaterMark {
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

	fmt.Printf("DEBUG: MultiBatch - partition:%d, startOffset:%d, highWaterMark:%d, maxBytes:%d\n",
		partitionID, startOffset, highWaterMark, maxBytes)

	var combinedBatches []byte
	currentOffset := startOffset
	totalSize := int32(0)
	batchCount := 0

	// Parameters for batch fetching - start smaller to respect maxBytes better
	recordsPerBatch := int32(10) // Start with smaller batch size
	maxBatchesPerFetch := 10     // Limit number of batches to avoid infinite loops

	for batchCount < maxBatchesPerFetch && currentOffset < highWaterMark {
		// Calculate remaining space
		remainingBytes := maxBytes - totalSize
		if remainingBytes < 100 { // Need at least 100 bytes for a minimal batch
			fmt.Printf("DEBUG: MultiBatch - insufficient space remaining: %d bytes\n", remainingBytes)
			break
		}

		// Adapt records per batch based on remaining space
		if remainingBytes < 1000 {
			recordsPerBatch = 10 // Smaller batches when space is limited
		}

		// Calculate how many records to fetch for this batch
		recordsAvailable := highWaterMark - currentOffset
		recordsToFetch := recordsPerBatch
		if int64(recordsToFetch) > recordsAvailable {
			recordsToFetch = int32(recordsAvailable)
		}

		// Fetch records for this batch
		smqRecords, err := f.handler.seaweedMQHandler.GetStoredRecords(topicName, partitionID, currentOffset, int(recordsToFetch))
		if err != nil || len(smqRecords) == 0 {
			fmt.Printf("DEBUG: MultiBatch - no more records available at offset %d\n", currentOffset)
			break
		}

		// Estimate batch size before construction to better respect maxBytes
		estimatedBatchSize := f.estimateBatchSize(smqRecords)

		// Note: we do not stop based on estimate; we will check actual size after constructing the batch

		// Construct record batch
		batch := f.constructSingleRecordBatch(currentOffset, smqRecords)
		batchSize := int32(len(batch))

		fmt.Printf("DEBUG: MultiBatch - constructed batch %d: %d records, %d bytes (estimated %d), offset %d\n",
			batchCount+1, len(smqRecords), batchSize, estimatedBatchSize, currentOffset)

		// Double-check actual size doesn't exceed maxBytes
		if totalSize+batchSize > maxBytes && batchCount > 0 {
			fmt.Printf("DEBUG: MultiBatch - actual batch would exceed limit (%d + %d > %d), stopping\n",
				totalSize, batchSize, maxBytes)
			break
		}

		// Add this batch to combined result
		combinedBatches = append(combinedBatches, batch...)
		totalSize += batchSize
		currentOffset += int64(len(smqRecords))
		batchCount++

		// If this is a small batch, we might be at the end
		if len(smqRecords) < int(recordsPerBatch) {
			fmt.Printf("DEBUG: MultiBatch - reached end with partial batch\n")
			break
		}
	}

	result := &FetchResult{
		RecordBatches: combinedBatches,
		NextOffset:    currentOffset,
		TotalSize:     totalSize,
		BatchCount:    batchCount,
	}

	fmt.Printf("DEBUG: MultiBatch - completed: %d batches, %d total bytes, next offset %d\n",
		result.BatchCount, result.TotalSize, result.NextOffset)

	return result, nil
}

// constructSingleRecordBatch creates a single record batch from SMQ records
func (f *MultiBatchFetcher) constructSingleRecordBatch(baseOffset int64, smqRecords []offset.SMQRecord) []byte {
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

	// Base timestamp (8 bytes) - use first record timestamp
	baseTimestamp := smqRecords[0].GetTimestamp()
	baseTimestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseTimestampBytes, uint64(baseTimestamp))
	batch = append(batch, baseTimestampBytes...)

	// Max timestamp (8 bytes) - use last record timestamp or same as base
	maxTimestamp := baseTimestamp
	if len(smqRecords) > 1 {
		maxTimestamp = smqRecords[len(smqRecords)-1].GetTimestamp()
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

		// Timestamp delta (varint) - calculate from base timestamp
		timestampDelta := smqRecord.GetTimestamp() - baseTimestamp
		recordBytes = append(recordBytes, encodeVarint(timestampDelta)...)

		// Offset delta (varint)
		offsetDelta := int64(i)
		recordBytes = append(recordBytes, encodeVarint(offsetDelta)...)

		// Key length and key (varint + data)
		key := smqRecord.GetKey()
		if key == nil {
			recordBytes = append(recordBytes, encodeVarint(-1)...) // null key
		} else {
			recordBytes = append(recordBytes, encodeVarint(int64(len(key)))...)
			recordBytes = append(recordBytes, key...)
		}

		// Value length and value (varint + data)
		value := smqRecord.GetValue()
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

	// Calculate CRC32 for the batch
	crcStartPos := crcPos + 4 // start after the CRC field
	crcData := batch[crcStartPos:]
	crc := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[crcPos:crcPos+4], crc)

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
	crcStartPos := crcPos + 4
	crcData := batch[crcStartPos:]
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
func (f *MultiBatchFetcher) CreateCompressedBatch(baseOffset int64, smqRecords []offset.SMQRecord, codec compression.CompressionCodec) (*CompressedBatchResult, error) {
	if codec == compression.None {
		// No compression requested
		batch := f.constructSingleRecordBatch(baseOffset, smqRecords)
		return &CompressedBatchResult{
			CompressedData: batch,
			OriginalSize:   int32(len(batch)),
			CompressedSize: int32(len(batch)),
			Codec:          compression.None,
		}, nil
	}

	// For Phase 5, implement basic GZIP compression support
	originalBatch := f.constructSingleRecordBatch(baseOffset, smqRecords)
	originalSize := int32(len(originalBatch))

	compressedData, err := f.compressData(originalBatch, codec)
	if err != nil {
		// Fall back to uncompressed if compression fails
		fmt.Printf("DEBUG: Compression failed, falling back to uncompressed: %v\n", err)
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

	// Calculate CRC32 for the batch (excluding the CRC field itself)
	crcStartPos := crcPos + 4
	crcData := batch[crcStartPos:]
	crc := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[crcPos:crcPos+4], crc)

	return batch
}

// estimateBatchSize estimates the size of a record batch before constructing it
func (f *MultiBatchFetcher) estimateBatchSize(smqRecords []offset.SMQRecord) int32 {
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

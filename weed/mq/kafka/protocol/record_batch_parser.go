package protocol

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/compression"
)

// RecordBatch represents a parsed Kafka record batch
type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC32                uint32
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	RecordCount          int32
	Records              []byte // Raw records data (may be compressed)
}

// RecordBatchParser handles parsing of Kafka record batches with compression support
type RecordBatchParser struct {
	// Add any configuration or state needed
}

// NewRecordBatchParser creates a new record batch parser
func NewRecordBatchParser() *RecordBatchParser {
	return &RecordBatchParser{}
}

// ParseRecordBatch parses a Kafka record batch from binary data
func (p *RecordBatchParser) ParseRecordBatch(data []byte) (*RecordBatch, error) {
	if len(data) < 61 { // Minimum record batch header size
		return nil, fmt.Errorf("record batch too small: %d bytes, need at least 61", len(data))
	}

	batch := &RecordBatch{}
	offset := 0

	// Parse record batch header
	batch.BaseOffset = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	batch.BatchLength = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	batch.PartitionLeaderEpoch = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	batch.Magic = int8(data[offset])
	offset += 1

	// Validate magic byte
	if batch.Magic != 2 {
		return nil, fmt.Errorf("unsupported record batch magic byte: %d, expected 2", batch.Magic)
	}

	batch.CRC32 = binary.BigEndian.Uint32(data[offset:])
	offset += 4

	batch.Attributes = int16(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	batch.LastOffsetDelta = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	batch.FirstTimestamp = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	batch.MaxTimestamp = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	batch.ProducerID = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	batch.ProducerEpoch = int16(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	batch.BaseSequence = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	batch.RecordCount = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Validate record count
	if batch.RecordCount < 0 || batch.RecordCount > 1000000 {
		return nil, fmt.Errorf("invalid record count: %d", batch.RecordCount)
	}

	// Extract records data (rest of the batch)
	if offset < len(data) {
		batch.Records = data[offset:]
	}

	return batch, nil
}

// GetCompressionCodec extracts the compression codec from the batch attributes
func (batch *RecordBatch) GetCompressionCodec() compression.CompressionCodec {
	return compression.ExtractCompressionCodec(batch.Attributes)
}

// IsCompressed returns true if the record batch is compressed
func (batch *RecordBatch) IsCompressed() bool {
	return batch.GetCompressionCodec() != compression.None
}

// DecompressRecords decompresses the records data if compressed
func (batch *RecordBatch) DecompressRecords() ([]byte, error) {
	if !batch.IsCompressed() {
		return batch.Records, nil
	}

	codec := batch.GetCompressionCodec()
	decompressed, err := compression.Decompress(codec, batch.Records)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress records with %s: %w", codec, err)
	}

	return decompressed, nil
}

// ValidateCRC32 validates the CRC32 checksum of the record batch
func (batch *RecordBatch) ValidateCRC32(originalData []byte) error {
	if len(originalData) < 17 { // Need at least up to CRC field
		return fmt.Errorf("data too small for CRC validation")
	}

	// CRC32 is calculated over the data starting after the CRC field
	// Skip: BaseOffset(8) + BatchLength(4) + PartitionLeaderEpoch(4) + Magic(1) + CRC(4) = 21 bytes
	// Kafka uses Castagnoli (CRC-32C) algorithm for record batch CRC
	dataForCRC := originalData[21:]

	calculatedCRC := crc32.Checksum(dataForCRC, crc32.MakeTable(crc32.Castagnoli))

	if calculatedCRC != batch.CRC32 {
		return fmt.Errorf("CRC32 mismatch: expected %x, got %x", batch.CRC32, calculatedCRC)
	}

	return nil
}

// ParseRecordBatchWithValidation parses and validates a record batch
func (p *RecordBatchParser) ParseRecordBatchWithValidation(data []byte, validateCRC bool) (*RecordBatch, error) {
	batch, err := p.ParseRecordBatch(data)
	if err != nil {
		return nil, err
	}

	if validateCRC {
		if err := batch.ValidateCRC32(data); err != nil {
			return nil, fmt.Errorf("CRC validation failed: %w", err)
		}
	}

	return batch, nil
}

// ExtractRecords extracts and decompresses individual records from the batch
func (batch *RecordBatch) ExtractRecords() ([]Record, error) {
	decompressedData, err := batch.DecompressRecords()
	if err != nil {
		return nil, err
	}

	// Parse individual records from decompressed data
	// This is a simplified implementation - full implementation would parse varint-encoded records
	records := make([]Record, 0, batch.RecordCount)

	// For now, create placeholder records
	// In a full implementation, this would parse the actual record format
	for i := int32(0); i < batch.RecordCount; i++ {
		record := Record{
			Offset:    batch.BaseOffset + int64(i),
			Key:       nil,                             // Would be parsed from record data
			Value:     decompressedData,                // Simplified - would be individual record value
			Headers:   nil,                             // Would be parsed from record data
			Timestamp: batch.FirstTimestamp + int64(i), // Simplified
		}
		records = append(records, record)
	}

	return records, nil
}

// Record represents a single Kafka record
type Record struct {
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
	Timestamp int64
}

// CompressRecordBatch compresses a record batch using the specified codec
func CompressRecordBatch(codec compression.CompressionCodec, records []byte) ([]byte, int16, error) {
	if codec == compression.None {
		return records, 0, nil
	}

	compressed, err := compression.Compress(codec, records)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to compress record batch: %w", err)
	}

	attributes := compression.SetCompressionCodec(0, codec)
	return compressed, attributes, nil
}

// CreateRecordBatch creates a new record batch with the given parameters
func CreateRecordBatch(baseOffset int64, records []byte, codec compression.CompressionCodec) ([]byte, error) {
	// Compress records if needed
	compressedRecords, attributes, err := CompressRecordBatch(codec, records)
	if err != nil {
		return nil, err
	}

	// Calculate batch length (everything after the batch length field)
	recordsLength := len(compressedRecords)
	batchLength := 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + recordsLength // Header + records

	// Build the record batch
	batch := make([]byte, 0, 61+recordsLength)

	// Base offset (8 bytes)
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(baseOffset))
	batch = append(batch, baseOffsetBytes...)

	// Batch length (4 bytes)
	batchLengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(batchLengthBytes, uint32(batchLength))
	batch = append(batch, batchLengthBytes...)

	// Partition leader epoch (4 bytes) - use 0 for simplicity
	batch = append(batch, 0, 0, 0, 0)

	// Magic byte (1 byte) - version 2
	batch = append(batch, 2)

	// CRC32 placeholder (4 bytes) - will be calculated later
	crcPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Attributes (2 bytes)
	attributesBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(attributesBytes, uint16(attributes))
	batch = append(batch, attributesBytes...)

	// Last offset delta (4 bytes) - assume single record for simplicity
	batch = append(batch, 0, 0, 0, 0)

	// First timestamp (8 bytes) - use current time
	// For simplicity, use 0
	batch = append(batch, 0, 0, 0, 0, 0, 0, 0, 0)

	// Max timestamp (8 bytes)
	batch = append(batch, 0, 0, 0, 0, 0, 0, 0, 0)

	// Producer ID (8 bytes) - use -1 for non-transactional
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	// Producer epoch (2 bytes) - use -1
	batch = append(batch, 0xFF, 0xFF)

	// Base sequence (4 bytes) - use -1
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Record count (4 bytes) - assume 1 for simplicity
	batch = append(batch, 0, 0, 0, 1)

	// Records data
	batch = append(batch, compressedRecords...)

	// Calculate and set CRC32
	// Kafka uses Castagnoli (CRC-32C) algorithm for record batch CRC
	dataForCRC := batch[21:] // Everything after CRC field
	crc := crc32.Checksum(dataForCRC, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[crcPos:crcPos+4], crc)

	return batch, nil
}

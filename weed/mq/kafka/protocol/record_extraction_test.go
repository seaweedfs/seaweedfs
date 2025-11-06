package protocol

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

// TestExtractAllRecords_RealKafkaFormat tests extracting records from a real Kafka v2 record batch
func TestExtractAllRecords_RealKafkaFormat(t *testing.T) {
	h := &Handler{} // Minimal handler for testing

	// Create a proper Kafka v2 record batch with 1 record
	// This mimics what Schema Registry or other Kafka clients would send

	// Build record batch header (61 bytes)
	batch := make([]byte, 0, 200)

	// BaseOffset (8 bytes)
	baseOffset := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffset, 0)
	batch = append(batch, baseOffset...)

	// BatchLength (4 bytes) - will set after we know total size
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// PartitionLeaderEpoch (4 bytes)
	batch = append(batch, 0, 0, 0, 0)

	// Magic (1 byte) - must be 2 for v2
	batch = append(batch, 2)

	// CRC32 (4 bytes) - will calculate and set later
	crcPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Attributes (2 bytes) - no compression
	batch = append(batch, 0, 0)

	// LastOffsetDelta (4 bytes)
	batch = append(batch, 0, 0, 0, 0)

	// FirstTimestamp (8 bytes)
	batch = append(batch, 0, 0, 0, 0, 0, 0, 0, 0)

	// MaxTimestamp (8 bytes)
	batch = append(batch, 0, 0, 0, 0, 0, 0, 0, 0)

	// ProducerID (8 bytes)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	// ProducerEpoch (2 bytes)
	batch = append(batch, 0xFF, 0xFF)

	// BaseSequence (4 bytes)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// RecordCount (4 bytes)
	batch = append(batch, 0, 0, 0, 1) // 1 record

	// Now add the actual record (varint-encoded)
	// Record format:
	// - length (signed zigzag varint)
	// - attributes (1 byte)
	// - timestampDelta (signed zigzag varint)
	// - offsetDelta (signed zigzag varint)
	// - keyLength (signed zigzag varint, -1 for null)
	// - key (bytes)
	// - valueLength (signed zigzag varint, -1 for null)
	// - value (bytes)
	// - headersCount (signed zigzag varint)

	record := make([]byte, 0, 50)

	// attributes (1 byte)
	record = append(record, 0)

	// timestampDelta (signed zigzag varint - 0)
	// 0 in zigzag is: (0 << 1) ^ (0 >> 63) = 0
	record = append(record, 0)

	// offsetDelta (signed zigzag varint - 0)
	record = append(record, 0)

	// keyLength (signed zigzag varint - -1 for null)
	// -1 in zigzag is: (-1 << 1) ^ (-1 >> 63) = -2 ^ -1 = 1
	record = append(record, 1)

	// key (none, because null with length -1)

	// valueLength (signed zigzag varint)
	testValue := []byte(`{"type":"string"}`)
	// Positive length N in zigzag is: (N << 1) = N*2
	valueLen := len(testValue)
	record = append(record, byte(valueLen<<1))

	// value
	record = append(record, testValue...)

	// headersCount (signed zigzag varint - 0)
	record = append(record, 0)

	// Prepend record length as zigzag-encoded varint
	recordLength := len(record)
	recordWithLength := make([]byte, 0, recordLength+5)
	// Zigzag encode the length: (n << 1) for positive n
	zigzagLength := byte(recordLength << 1)
	recordWithLength = append(recordWithLength, zigzagLength)
	recordWithLength = append(recordWithLength, record...)

	// Append record to batch
	batch = append(batch, recordWithLength...)

	// Calculate and set BatchLength (from PartitionLeaderEpoch to end)
	batchLength := len(batch) - 12 // Exclude BaseOffset(8) + BatchLength(4)
	binary.BigEndian.PutUint32(batch[batchLengthPos:batchLengthPos+4], uint32(batchLength))

	// Calculate and set CRC32 (from Attributes to end)
	// Kafka uses Castagnoli (CRC-32C) algorithm for record batch CRC
	crcData := batch[21:] // From Attributes onwards
	crc := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[crcPos:crcPos+4], crc)

	t.Logf("Created batch of %d bytes, record value: %s", len(batch), string(testValue))

	// Now test extraction
	results := h.extractAllRecords(batch)

	if len(results) == 0 {
		t.Fatalf("extractAllRecords returned 0 records, expected 1")
	}

	if len(results) != 1 {
		t.Fatalf("extractAllRecords returned %d records, expected 1", len(results))
	}

	result := results[0]

	// Key should be nil (we sent null key with varint -1)
	if result.Key != nil {
		t.Errorf("Expected nil key, got %v", result.Key)
	}

	// Value should match our test value
	if string(result.Value) != string(testValue) {
		t.Errorf("Value mismatch:\n  got:  %s\n  want: %s", string(result.Value), string(testValue))
	}

	t.Logf("Successfully extracted record with value: %s", string(result.Value))
}

// TestExtractAllRecords_CompressedBatch tests extracting records from a compressed batch
func TestExtractAllRecords_CompressedBatch(t *testing.T) {
	// This would test with actual compression, but for now we'll skip
	// as we need to ensure uncompressed works first
	t.Skip("Compressed batch test - implement after uncompressed works")
}

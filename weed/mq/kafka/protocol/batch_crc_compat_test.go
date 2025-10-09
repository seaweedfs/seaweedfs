package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
)

// TestBatchConstruction tests that our batch construction produces valid CRC
func TestBatchConstruction(t *testing.T) {
	// Create test data
	key := []byte("test-key")
	value := []byte("test-value")
	timestamp := time.Now()

	// Build batch using our implementation
	batch := constructTestBatch(0, timestamp, key, value)

	t.Logf("Batch size: %d bytes", len(batch))
	t.Logf("Batch hex:\n%s", hexDumpTest(batch))

	// Extract and verify CRC
	if len(batch) < 21 {
		t.Fatalf("Batch too short: %d bytes", len(batch))
	}

	storedCRC := binary.BigEndian.Uint32(batch[17:21])
	t.Logf("Stored CRC: 0x%08x", storedCRC)

	// Recalculate CRC from the data
	crcData := batch[21:]
	calculatedCRC := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	t.Logf("Calculated CRC: 0x%08x (over %d bytes)", calculatedCRC, len(crcData))

	if storedCRC != calculatedCRC {
		t.Errorf("CRC mismatch: stored=0x%08x calculated=0x%08x", storedCRC, calculatedCRC)

		// Debug: show what bytes the CRC is calculated over
		t.Logf("CRC data (first 100 bytes):")
		dumpSize := 100
		if len(crcData) < dumpSize {
			dumpSize = len(crcData)
		}
		for i := 0; i < dumpSize; i += 16 {
			end := i + 16
			if end > dumpSize {
				end = dumpSize
			}
			t.Logf("  %04d: %x", i, crcData[i:end])
		}
	} else {
		t.Log("CRC verification PASSED")
	}

	// Verify batch structure
	t.Log("\n=== Batch Structure ===")
	verifyField(t, "Base Offset", batch[0:8], binary.BigEndian.Uint64(batch[0:8]))
	verifyField(t, "Batch Length", batch[8:12], binary.BigEndian.Uint32(batch[8:12]))
	verifyField(t, "Leader Epoch", batch[12:16], int32(binary.BigEndian.Uint32(batch[12:16])))
	verifyField(t, "Magic", batch[16:17], batch[16])
	verifyField(t, "CRC", batch[17:21], binary.BigEndian.Uint32(batch[17:21]))
	verifyField(t, "Attributes", batch[21:23], binary.BigEndian.Uint16(batch[21:23]))
	verifyField(t, "Last Offset Delta", batch[23:27], binary.BigEndian.Uint32(batch[23:27]))
	verifyField(t, "Base Timestamp", batch[27:35], binary.BigEndian.Uint64(batch[27:35]))
	verifyField(t, "Max Timestamp", batch[35:43], binary.BigEndian.Uint64(batch[35:43]))
	verifyField(t, "Record Count", batch[57:61], binary.BigEndian.Uint32(batch[57:61]))

	// Verify the batch length field is correct
	expectedBatchLength := uint32(len(batch) - 12)
	actualBatchLength := binary.BigEndian.Uint32(batch[8:12])
	if expectedBatchLength != actualBatchLength {
		t.Errorf("Batch length mismatch: expected=%d actual=%d", expectedBatchLength, actualBatchLength)
	} else {
		t.Logf("Batch length correct: %d", actualBatchLength)
	}
}

// TestMultipleRecordsBatch tests batch construction with multiple records
func TestMultipleRecordsBatch(t *testing.T) {
	timestamp := time.Now()

	// We can't easily test multiple records without the full implementation
	// So let's test that our single record batch matches expected structure

	batch1 := constructTestBatch(0, timestamp, []byte("key1"), []byte("value1"))
	batch2 := constructTestBatch(1, timestamp, []byte("key2"), []byte("value2"))

	t.Logf("Batch 1 size: %d, CRC: 0x%08x", len(batch1), binary.BigEndian.Uint32(batch1[17:21]))
	t.Logf("Batch 2 size: %d, CRC: 0x%08x", len(batch2), binary.BigEndian.Uint32(batch2[17:21]))

	// Verify both batches have valid CRCs
	for i, batch := range [][]byte{batch1, batch2} {
		storedCRC := binary.BigEndian.Uint32(batch[17:21])
		calculatedCRC := crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli))

		if storedCRC != calculatedCRC {
			t.Errorf("Batch %d CRC mismatch: stored=0x%08x calculated=0x%08x", i+1, storedCRC, calculatedCRC)
		} else {
			t.Logf("Batch %d CRC valid", i+1)
		}
	}
}

// TestVarintEncoding tests our varint encoding implementation
func TestVarintEncoding(t *testing.T) {
	testCases := []struct {
		value    int64
		expected []byte
	}{
		{0, []byte{0x00}},
		{1, []byte{0x02}},
		{-1, []byte{0x01}},
		{5, []byte{0x0a}},
		{-5, []byte{0x09}},
		{127, []byte{0xfe, 0x01}},
		{128, []byte{0x80, 0x02}},
		{-127, []byte{0xfd, 0x01}},
		{-128, []byte{0xff, 0x01}},
	}

	for _, tc := range testCases {
		result := encodeVarint(tc.value)
		if !bytes.Equal(result, tc.expected) {
			t.Errorf("encodeVarint(%d) = %x, expected %x", tc.value, result, tc.expected)
		} else {
			t.Logf("encodeVarint(%d) = %x", tc.value, result)
		}
	}
}

// constructTestBatch builds a batch using our implementation
func constructTestBatch(baseOffset int64, timestamp time.Time, key, value []byte) []byte {
	batch := make([]byte, 0, 256)

	// Base offset (0-7)
	baseOffsetBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(baseOffsetBytes, uint64(baseOffset))
	batch = append(batch, baseOffsetBytes...)

	// Batch length placeholder (8-11)
	batchLengthPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Partition leader epoch (12-15)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Magic (16)
	batch = append(batch, 0x02)

	// CRC placeholder (17-20)
	crcPos := len(batch)
	batch = append(batch, 0, 0, 0, 0)

	// Attributes (21-22)
	batch = append(batch, 0, 0)

	// Last offset delta (23-26)
	batch = append(batch, 0, 0, 0, 0)

	// Base timestamp (27-34)
	timestampMs := timestamp.UnixMilli()
	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, uint64(timestampMs))
	batch = append(batch, timestampBytes...)

	// Max timestamp (35-42)
	batch = append(batch, timestampBytes...)

	// Producer ID (43-50)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF)

	// Producer epoch (51-52)
	batch = append(batch, 0xFF, 0xFF)

	// Base sequence (53-56)
	batch = append(batch, 0xFF, 0xFF, 0xFF, 0xFF)

	// Record count (57-60)
	recordCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordCountBytes, 1)
	batch = append(batch, recordCountBytes...)

	// Build record (61+)
	recordBody := []byte{}

	// Attributes
	recordBody = append(recordBody, 0)

	// Timestamp delta
	recordBody = append(recordBody, encodeVarint(0)...)

	// Offset delta
	recordBody = append(recordBody, encodeVarint(0)...)

	// Key length and key
	if key == nil {
		recordBody = append(recordBody, encodeVarint(-1)...)
	} else {
		recordBody = append(recordBody, encodeVarint(int64(len(key)))...)
		recordBody = append(recordBody, key...)
	}

	// Value length and value
	if value == nil {
		recordBody = append(recordBody, encodeVarint(-1)...)
	} else {
		recordBody = append(recordBody, encodeVarint(int64(len(value)))...)
		recordBody = append(recordBody, value...)
	}

	// Headers count
	recordBody = append(recordBody, encodeVarint(0)...)

	// Prepend record length
	recordLength := int64(len(recordBody))
	batch = append(batch, encodeVarint(recordLength)...)
	batch = append(batch, recordBody...)

	// Fill in batch length
	batchLength := uint32(len(batch) - 12)
	binary.BigEndian.PutUint32(batch[batchLengthPos:], batchLength)

	// Calculate CRC
	crcData := batch[21:]
	crc := crc32.Checksum(crcData, crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(batch[crcPos:], crc)

	return batch
}

// verifyField logs a field's value
func verifyField(t *testing.T, name string, bytes []byte, value interface{}) {
	t.Logf("  %s: %x (value: %v)", name, bytes, value)
}

// hexDump formats bytes as hex dump
func hexDumpTest(data []byte) string {
	var buf bytes.Buffer
	for i := 0; i < len(data); i += 16 {
		end := i + 16
		if end > len(data) {
			end = len(data)
		}
		buf.WriteString(fmt.Sprintf("  %04d: %x\n", i, data[i:end]))
	}
	return buf.String()
}

// TestClientSideCRCValidation mimics what a Kafka client does
func TestClientSideCRCValidation(t *testing.T) {
	// Build a batch
	batch := constructTestBatch(0, time.Now(), []byte("test-key"), []byte("test-value"))

	t.Logf("Constructed batch: %d bytes", len(batch))

	// Now pretend we're a Kafka client receiving this batch
	// Step 1: Read the batch header to get the CRC
	if len(batch) < 21 {
		t.Fatalf("Batch too short for client to read CRC")
	}

	clientReadCRC := binary.BigEndian.Uint32(batch[17:21])
	t.Logf("Client read CRC from header: 0x%08x", clientReadCRC)

	// Step 2: Calculate CRC over the data (from byte 21 onwards)
	clientCalculatedCRC := crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli))
	t.Logf("Client calculated CRC: 0x%08x", clientCalculatedCRC)

	// Step 3: Compare
	if clientReadCRC != clientCalculatedCRC {
		t.Errorf("CLIENT WOULD REJECT: CRC mismatch: read=0x%08x calculated=0x%08x",
			clientReadCRC, clientCalculatedCRC)
		t.Log("This is the error consumers are seeing!")
	} else {
		t.Log("CLIENT WOULD ACCEPT: CRC valid")
	}
}

// TestConcurrentBatchConstruction tests if there are race conditions
func TestConcurrentBatchConstruction(t *testing.T) {
	timestamp := time.Now()

	// Build multiple batches concurrently
	const numBatches = 10
	results := make(chan bool, numBatches)

	for i := 0; i < numBatches; i++ {
		go func(id int) {
			batch := constructTestBatch(int64(id), timestamp,
				[]byte(fmt.Sprintf("key-%d", id)),
				[]byte(fmt.Sprintf("value-%d", id)))

			// Validate CRC
			storedCRC := binary.BigEndian.Uint32(batch[17:21])
			calculatedCRC := crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli))

			results <- (storedCRC == calculatedCRC)
		}(i)
	}

	// Check all results
	allValid := true
	for i := 0; i < numBatches; i++ {
		if !<-results {
			allValid = false
			t.Errorf("Batch %d has invalid CRC", i)
		}
	}

	if allValid {
		t.Logf("All %d concurrent batches have valid CRCs", numBatches)
	}
}

// TestProductionBatchConstruction tests the actual production code
func TestProductionBatchConstruction(t *testing.T) {
	// Create a mock SMQ record
	mockRecord := &mockSMQRecord{
		key:       []byte("prod-key"),
		value:     []byte("prod-value"),
		timestamp: time.Now().UnixNano(),
	}

	// Create a mock handler
	mockHandler := &Handler{}

	// Create fetcher
	fetcher := NewMultiBatchFetcher(mockHandler)

	// Construct batch using production code
	batch := fetcher.constructSingleRecordBatch("test-topic", 0, []integration.SMQRecord{mockRecord})

	t.Logf("Production batch size: %d bytes", len(batch))

	// Validate CRC
	if len(batch) < 21 {
		t.Fatalf("Production batch too short: %d bytes", len(batch))
	}

	storedCRC := binary.BigEndian.Uint32(batch[17:21])
	calculatedCRC := crc32.Checksum(batch[21:], crc32.MakeTable(crc32.Castagnoli))

	t.Logf("Production batch CRC: stored=0x%08x calculated=0x%08x", storedCRC, calculatedCRC)

	if storedCRC != calculatedCRC {
		t.Errorf("PRODUCTION CODE CRC INVALID: stored=0x%08x calculated=0x%08x", storedCRC, calculatedCRC)
		t.Log("This means the production constructSingleRecordBatch has a bug!")
	} else {
		t.Log("PRODUCTION CODE CRC VALID")
	}
}

// mockSMQRecord implements the SMQRecord interface for testing
type mockSMQRecord struct {
	key       []byte
	value     []byte
	timestamp int64
}

func (m *mockSMQRecord) GetKey() []byte      { return m.key }
func (m *mockSMQRecord) GetValue() []byte    { return m.value }
func (m *mockSMQRecord) GetTimestamp() int64 { return m.timestamp }
func (m *mockSMQRecord) GetOffset() int64    { return 0 }

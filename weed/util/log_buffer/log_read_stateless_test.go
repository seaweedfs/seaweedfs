package log_buffer

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestReadMessagesAtOffset_EmptyBuffer(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true
	lb.bufferStartOffset = 0
	lb.offset = 0 // Empty buffer

	messages, nextOffset, hwm, endOfPartition, err := lb.ReadMessagesAtOffset(100, 10, 1024)

	// Reading from future offset (100) when buffer is at 0
	// Should return empty, no error
	if err != nil {
		t.Errorf("Expected no error for future offset, got %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("Expected 0 messages, got %d", len(messages))
	}
	if nextOffset != 100 {
		t.Errorf("Expected nextOffset=100, got %d", nextOffset)
	}
	if !endOfPartition {
		t.Error("Expected endOfPartition=true for future offset")
	}
	if hwm != 0 {
		t.Errorf("Expected highWaterMark=0, got %d", hwm)
	}
}

func TestReadMessagesAtOffset_SingleMessage(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true

	// Add a message
	entry := &filer_pb.LogEntry{
		TsNs:   time.Now().UnixNano(),
		Key:    []byte("key1"),
		Data:   []byte("value1"),
		Offset: 0,
	}
	lb.AddLogEntryToBuffer(entry)

	// Read from offset 0
	messages, nextOffset, _, endOfPartition, err := lb.ReadMessagesAtOffset(0, 10, 1024)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if len(messages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(messages))
	}
	if nextOffset != 1 {
		t.Errorf("Expected nextOffset=1, got %d", nextOffset)
	}
	if !endOfPartition {
		t.Error("Expected endOfPartition=true after reading all messages")
	}
	if messages[0].Offset != 0 {
		t.Errorf("Expected message offset=0, got %d", messages[0].Offset)
	}
	if string(messages[0].Key) != "key1" {
		t.Errorf("Expected key='key1', got '%s'", string(messages[0].Key))
	}
}

func TestReadMessagesAtOffset_MultipleMessages(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true

	// Add 5 messages
	for i := 0; i < 5; i++ {
		entry := &filer_pb.LogEntry{
			TsNs:   time.Now().UnixNano(),
			Key:    []byte("key"),
			Data:   []byte("value"),
			Offset: int64(i),
		}
		lb.AddLogEntryToBuffer(entry)
	}

	// Read from offset 0, max 3 messages
	messages, nextOffset, _, _, err := lb.ReadMessagesAtOffset(0, 3, 10240)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if len(messages) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(messages))
	}
	if nextOffset != 3 {
		t.Errorf("Expected nextOffset=3, got %d", nextOffset)
	}

	// Verify offsets are sequential
	for i, msg := range messages {
		if msg.Offset != int64(i) {
			t.Errorf("Message %d: expected offset=%d, got %d", i, i, msg.Offset)
		}
	}
}

func TestReadMessagesAtOffset_StartFromMiddle(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true

	// Add 10 messages (0-9)
	for i := 0; i < 10; i++ {
		entry := &filer_pb.LogEntry{
			TsNs:   time.Now().UnixNano(),
			Key:    []byte("key"),
			Data:   []byte("value"),
			Offset: int64(i),
		}
		lb.AddLogEntryToBuffer(entry)
	}

	// Read from offset 5
	messages, nextOffset, _, _, err := lb.ReadMessagesAtOffset(5, 3, 10240)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if len(messages) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(messages))
	}
	if nextOffset != 8 {
		t.Errorf("Expected nextOffset=8, got %d", nextOffset)
	}

	// Verify we got messages 5, 6, 7
	expectedOffsets := []int64{5, 6, 7}
	for i, msg := range messages {
		if msg.Offset != expectedOffsets[i] {
			t.Errorf("Message %d: expected offset=%d, got %d", i, expectedOffsets[i], msg.Offset)
		}
	}
}

func TestReadMessagesAtOffset_MaxBytesLimit(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true

	// Add messages with 100 bytes each
	for i := 0; i < 10; i++ {
		entry := &filer_pb.LogEntry{
			TsNs:   time.Now().UnixNano(),
			Key:    []byte("key"),
			Data:   make([]byte, 100), // 100 bytes
			Offset: int64(i),
		}
		lb.AddLogEntryToBuffer(entry)
	}

	// Request with max 250 bytes (should get ~2 messages)
	messages, _, _, _, err := lb.ReadMessagesAtOffset(0, 100, 250)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Should get at least 1 message, but likely 2
	if len(messages) == 0 {
		t.Error("Expected at least 1 message")
	}
	if len(messages) > 3 {
		t.Errorf("Expected max 3 messages with 250 byte limit, got %d", len(messages))
	}
}

func TestReadMessagesAtOffset_ConcurrentReads(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true

	// Add 100 messages
	for i := 0; i < 100; i++ {
		entry := &filer_pb.LogEntry{
			TsNs:   time.Now().UnixNano(),
			Key:    []byte("key"),
			Data:   []byte("value"),
			Offset: int64(i),
		}
		lb.AddLogEntryToBuffer(entry)
	}

	// Start 10 concurrent readers at different offsets
	done := make(chan bool, 10)

	for reader := 0; reader < 10; reader++ {
		startOffset := int64(reader * 10)
		go func(offset int64) {
			messages, nextOffset, _, _, err := lb.ReadMessagesAtOffset(offset, 5, 10240)

			if err != nil {
				t.Errorf("Reader at offset %d: unexpected error: %v", offset, err)
			}
			if len(messages) != 5 {
				t.Errorf("Reader at offset %d: expected 5 messages, got %d", offset, len(messages))
			}
			if nextOffset != offset+5 {
				t.Errorf("Reader at offset %d: expected nextOffset=%d, got %d", offset, offset+5, nextOffset)
			}

			// Verify sequential offsets
			for i, msg := range messages {
				expectedOffset := offset + int64(i)
				if msg.Offset != expectedOffset {
					t.Errorf("Reader at offset %d: message %d has offset %d, expected %d",
						offset, i, msg.Offset, expectedOffset)
				}
			}

			done <- true
		}(startOffset)
	}

	// Wait for all readers
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestReadMessagesAtOffset_FutureOffset(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true

	// Add 5 messages (0-4)
	for i := 0; i < 5; i++ {
		entry := &filer_pb.LogEntry{
			TsNs:   time.Now().UnixNano(),
			Key:    []byte("key"),
			Data:   []byte("value"),
			Offset: int64(i),
		}
		lb.AddLogEntryToBuffer(entry)
	}

	// Try to read from offset 10 (future)
	messages, nextOffset, _, endOfPartition, err := lb.ReadMessagesAtOffset(10, 10, 10240)

	if err != nil {
		t.Errorf("Expected no error for future offset, got %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("Expected 0 messages for future offset, got %d", len(messages))
	}
	if nextOffset != 10 {
		t.Errorf("Expected nextOffset=10, got %d", nextOffset)
	}
	if !endOfPartition {
		t.Error("Expected endOfPartition=true for future offset")
	}
}

func TestWaitForDataWithTimeout_DataAvailable(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true

	// Add message at offset 0
	entry := &filer_pb.LogEntry{
		TsNs:   time.Now().UnixNano(),
		Key:    []byte("key"),
		Data:   []byte("value"),
		Offset: 0,
	}
	lb.AddLogEntryToBuffer(entry)

	// Wait for data at offset 0 (should return immediately)
	dataAvailable := lb.WaitForDataWithTimeout(0, 100)

	if !dataAvailable {
		t.Error("Expected data to be available at offset 0")
	}
}

func TestWaitForDataWithTimeout_NoData(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true
	lb.bufferStartOffset = 0
	lb.offset = 0

	// Don't add any messages, wait for offset 10

	// Wait for data at offset 10 with short timeout
	start := time.Now()
	dataAvailable := lb.WaitForDataWithTimeout(10, 50)
	elapsed := time.Since(start)

	if dataAvailable {
		t.Error("Expected no data to be available")
	}
	// Note: Actual wait time may be shorter if subscriber mechanism
	// returns immediately. Just verify no data was returned.
	t.Logf("Waited %v for timeout", elapsed)
}

func TestWaitForDataWithTimeout_DataArrives(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true

	// Start waiting in background
	done := make(chan bool)
	var dataAvailable bool

	go func() {
		dataAvailable = lb.WaitForDataWithTimeout(0, 500)
		done <- true
	}()

	// Add data after 50ms
	time.Sleep(50 * time.Millisecond)
	entry := &filer_pb.LogEntry{
		TsNs:   time.Now().UnixNano(),
		Key:    []byte("key"),
		Data:   []byte("value"),
		Offset: 0,
	}
	lb.AddLogEntryToBuffer(entry)

	// Wait for result
	<-done

	if !dataAvailable {
		t.Error("Expected data to become available after being added")
	}
}

func TestGetHighWaterMark(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true

	// Initially should be 0
	hwm := lb.GetHighWaterMark()
	if hwm != 0 {
		t.Errorf("Expected initial HWM=0, got %d", hwm)
	}

	// Add messages (offsets 0-4)
	for i := 0; i < 5; i++ {
		entry := &filer_pb.LogEntry{
			TsNs:   time.Now().UnixNano(),
			Key:    []byte("key"),
			Data:   []byte("value"),
			Offset: int64(i),
		}
		lb.AddLogEntryToBuffer(entry)
	}

	// HWM should be 5 (next offset to write, not last written offset)
	// This matches Kafka semantics where HWM = last offset + 1
	hwm = lb.GetHighWaterMark()
	if hwm != 5 {
		t.Errorf("Expected HWM=5 after adding 5 messages (0-4), got %d", hwm)
	}
}

func TestGetLogStartOffset(t *testing.T) {
	lb := NewLogBuffer("test", time.Hour, nil, nil, func() {})
	lb.hasOffsets = true
	lb.bufferStartOffset = 10

	lso := lb.GetLogStartOffset()
	if lso != 10 {
		t.Errorf("Expected LSO=10, got %d", lso)
	}
}

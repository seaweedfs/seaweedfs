package offset

import (
	"testing"
	"time"
)

func TestLedger_BasicOperations(t *testing.T) {
	ledger := NewLedger()
	
	// Initially empty
	if earliest := ledger.GetEarliestOffset(); earliest != 0 {
		t.Errorf("earliest offset: got %d, want 0", earliest)
	}
	if latest := ledger.GetLatestOffset(); latest != 0 {
		t.Errorf("latest offset: got %d, want 0", latest)
	}
	if hwm := ledger.GetHighWaterMark(); hwm != 0 {
		t.Errorf("high water mark: got %d, want 0", hwm)
	}
	
	// Assign some offsets
	baseOffset1 := ledger.AssignOffsets(3)
	if baseOffset1 != 0 {
		t.Errorf("first base offset: got %d, want 0", baseOffset1)
	}
	
	baseOffset2 := ledger.AssignOffsets(2)
	if baseOffset2 != 3 {
		t.Errorf("second base offset: got %d, want 3", baseOffset2)
	}
	
	// High water mark should be updated
	if hwm := ledger.GetHighWaterMark(); hwm != 5 {
		t.Errorf("high water mark after assignment: got %d, want 5", hwm)
	}
	
	// But no records yet, so earliest/latest still 0
	if latest := ledger.GetLatestOffset(); latest != 0 {
		t.Errorf("latest offset with no records: got %d, want 0", latest)
	}
}

func TestLedger_AppendAndRetrieve(t *testing.T) {
	ledger := NewLedger()
	
	// Assign and append some records
	baseOffset := ledger.AssignOffsets(3)
	if baseOffset != 0 {
		t.Fatalf("unexpected base offset: %d", baseOffset)
	}
	
	// Append records with different timestamps
	timestamp1 := time.Now().UnixNano()
	timestamp2 := timestamp1 + 1000000 // +1ms
	timestamp3 := timestamp2 + 2000000 // +2ms
	
	if err := ledger.AppendRecord(0, timestamp1, 100); err != nil {
		t.Fatalf("append record 0: %v", err)
	}
	if err := ledger.AppendRecord(1, timestamp2, 200); err != nil {
		t.Fatalf("append record 1: %v", err)
	}
	if err := ledger.AppendRecord(2, timestamp3, 150); err != nil {
		t.Fatalf("append record 2: %v", err)
	}
	
	// Check earliest/latest
	if earliest := ledger.GetEarliestOffset(); earliest != 0 {
		t.Errorf("earliest offset: got %d, want 0", earliest)
	}
	if latest := ledger.GetLatestOffset(); latest != 2 {
		t.Errorf("latest offset: got %d, want 2", latest)
	}
	
	// Retrieve records
	ts, size, err := ledger.GetRecord(0)
	if err != nil {
		t.Fatalf("get record 0: %v", err)
	}
	if ts != timestamp1 {
		t.Errorf("record 0 timestamp: got %d, want %d", ts, timestamp1)
	}
	if size != 100 {
		t.Errorf("record 0 size: got %d, want 100", size)
	}
	
	ts, size, err = ledger.GetRecord(2)
	if err != nil {
		t.Fatalf("get record 2: %v", err)
	}
	if ts != timestamp3 {
		t.Errorf("record 2 timestamp: got %d, want %d", ts, timestamp3)
	}
	if size != 150 {
		t.Errorf("record 2 size: got %d, want 150", size)
	}
	
	// Try to get non-existent record
	_, _, err = ledger.GetRecord(5)
	if err == nil {
		t.Errorf("expected error for non-existent offset 5")
	}
}

func TestLedger_FindOffsetByTimestamp(t *testing.T) {
	ledger := NewLedger()
	
	// Add some records with known timestamps
	baseTime := time.Now().UnixNano()
	ledger.AssignOffsets(5)
	
	ledger.AppendRecord(0, baseTime, 100)
	ledger.AppendRecord(1, baseTime+1000000, 200)    // +1ms
	ledger.AppendRecord(2, baseTime+3000000, 150)    // +3ms
	ledger.AppendRecord(3, baseTime+10000000, 300)   // +10ms
	ledger.AppendRecord(4, baseTime+20000000, 250)   // +20ms
	
	// Find offset by timestamp
	testCases := []struct {
		timestamp      int64
		expectedOffset int64
		description    string
	}{
		{baseTime - 1000000, 0, "before first record"},           // Before any record
		{baseTime, 0, "exact first timestamp"},                   // Exact match first
		{baseTime + 500000, 1, "between first and second"},       // Between records
		{baseTime + 1000000, 1, "exact second timestamp"},        // Exact match middle
		{baseTime + 5000000, 3, "between records"},               // Between records
		{baseTime + 20000000, 4, "exact last timestamp"},         // Exact match last
		{baseTime + 30000000, 5, "after last record"},            // After all records (should return HWM)
	}
	
	for _, tc := range testCases {
		offset := ledger.FindOffsetByTimestamp(tc.timestamp)
		if offset != tc.expectedOffset {
			t.Errorf("%s: got offset %d, want %d", tc.description, offset, tc.expectedOffset)
		}
	}
}

func TestLedger_ErrorConditions(t *testing.T) {
	ledger := NewLedger()
	
	// Try to append without assigning offsets first
	err := ledger.AppendRecord(0, time.Now().UnixNano(), 100)
	if err == nil {
		t.Errorf("expected error when appending without assignment")
	}
	
	// Assign some offsets
	ledger.AssignOffsets(3)
	
	// Try to append out-of-range offset
	err = ledger.AppendRecord(5, time.Now().UnixNano(), 100)
	if err == nil {
		t.Errorf("expected error for out-of-range offset")
	}
	
	// Try negative offset
	err = ledger.AppendRecord(-1, time.Now().UnixNano(), 100)
	if err == nil {
		t.Errorf("expected error for negative offset")
	}
	
	// Append a record successfully
	timestamp := time.Now().UnixNano()
	if err := ledger.AppendRecord(0, timestamp, 100); err != nil {
		t.Fatalf("failed to append valid record: %v", err)
	}
	
	// Try to append the same offset again
	err = ledger.AppendRecord(0, timestamp+1000, 200)
	if err == nil {
		t.Errorf("expected error for duplicate offset")
	}
}

func TestLedger_ConcurrentAccess(t *testing.T) {
	ledger := NewLedger()
	
	// Test concurrent offset assignment
	done := make(chan bool, 2)
	
	go func() {
		for i := 0; i < 100; i++ {
			ledger.AssignOffsets(1)
		}
		done <- true
	}()
	
	go func() {
		for i := 0; i < 100; i++ {
			ledger.AssignOffsets(1)
		}
		done <- true
	}()
	
	// Wait for both goroutines
	<-done
	<-done
	
	// Should have assigned 200 total offsets
	hwm := ledger.GetHighWaterMark()
	if hwm != 200 {
		t.Errorf("concurrent assignment: got HWM %d, want 200", hwm)
	}
	
	// Test concurrent reads
	ledger.AssignOffsets(1)
	timestamp := time.Now().UnixNano()
	ledger.AppendRecord(200, timestamp, 100)
	
	// Multiple concurrent reads should work fine
	readDone := make(chan bool, 5)
	for i := 0; i < 5; i++ {
		go func() {
			ts, size, err := ledger.GetRecord(200)
			if err != nil || ts != timestamp || size != 100 {
				t.Errorf("concurrent read failed: ts=%d size=%d err=%v", ts, size, err)
			}
			readDone <- true
		}()
	}
	
	// Wait for all reads
	for i := 0; i < 5; i++ {
		<-readDone
	}
}

func TestLedger_GetStats(t *testing.T) {
	ledger := NewLedger()
	
	// Initially empty
	count, earliest, latest, next := ledger.GetStats()
	if count != 0 || earliest != 0 || latest != 0 || next != 0 {
		t.Errorf("initial stats: got count=%d earliest=%d latest=%d next=%d", count, earliest, latest, next)
	}
	
	// Add some data
	baseTime := time.Now().UnixNano()
	ledger.AssignOffsets(3)
	ledger.AppendRecord(0, baseTime, 100)
	ledger.AppendRecord(1, baseTime+1000000, 200)
	ledger.AppendRecord(2, baseTime+2000000, 150)
	
	count, earliest, latest, next = ledger.GetStats()
	if count != 3 {
		t.Errorf("entry count: got %d, want 3", count)
	}
	if earliest != baseTime {
		t.Errorf("earliest time: got %d, want %d", earliest, baseTime)
	}
	if latest != baseTime+2000000 {
		t.Errorf("latest time: got %d, want %d", latest, baseTime+2000000)
	}
	if next != 3 {
		t.Errorf("next offset: got %d, want 3", next)
	}
}

func TestLedger_EmptyLedgerTimestampLookup(t *testing.T) {
	ledger := NewLedger()
	
	// Empty ledger should handle timestamp queries gracefully
	offset := ledger.FindOffsetByTimestamp(time.Now().UnixNano())
	if offset != 0 {
		t.Errorf("empty ledger timestamp lookup: got %d, want 0", offset)
	}
	
	earliest, latest := ledger.GetTimestampRange()
	if earliest <= 0 || latest <= 0 {
		t.Errorf("empty ledger timestamp range: earliest=%d latest=%d", earliest, latest)
	}
	// For empty ledger, should return current time as both earliest and latest
	if earliest != latest {
		t.Errorf("empty ledger should have same earliest and latest time")
	}
}

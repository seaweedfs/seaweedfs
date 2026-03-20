package logbuffer

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func makeEntry(level string, msg string, ts time.Time) LogEntry {
	return LogEntry{
		Timestamp: ts,
		Level:     level,
		File:      "test.go",
		Line:      1,
		Message:   msg,
	}
}

func TestNewRingBuffer(t *testing.T) {
	rb := NewRingBuffer(100)
	if rb.cap != 100 {
		t.Errorf("expected capacity 100, got %d", rb.cap)
	}
	if rb.count != 0 {
		t.Errorf("expected count 0, got %d", rb.count)
	}
}

func TestNewRingBuffer_DefaultCapacity(t *testing.T) {
	rb := NewRingBuffer(0)
	if rb.cap != defaultCapacity {
		t.Errorf("expected default capacity %d, got %d", defaultCapacity, rb.cap)
	}

	rb = NewRingBuffer(-1)
	if rb.cap != defaultCapacity {
		t.Errorf("expected default capacity for negative, got %d", rb.cap)
	}
}

func TestRingBuffer_WriteAndSnapshot(t *testing.T) {
	rb := NewRingBuffer(5)
	now := time.Now()

	for i := 0; i < 3; i++ {
		rb.Write(makeEntry("INFO", fmt.Sprintf("msg-%d", i), now.Add(time.Duration(i)*time.Second)))
	}

	snap := rb.Snapshot(10)
	if len(snap) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(snap))
	}
	if snap[0].Message != "msg-0" {
		t.Errorf("expected first entry msg-0, got %q", snap[0].Message)
	}
	if snap[2].Message != "msg-2" {
		t.Errorf("expected last entry msg-2, got %q", snap[2].Message)
	}
}

func TestRingBuffer_Wraparound(t *testing.T) {
	rb := NewRingBuffer(3)
	now := time.Now()

	// Write 5 entries into buffer of 3 — oldest 2 should be gone
	for i := 0; i < 5; i++ {
		rb.Write(makeEntry("INFO", fmt.Sprintf("msg-%d", i), now.Add(time.Duration(i)*time.Second)))
	}

	if rb.count != 3 {
		t.Errorf("expected count 3 after wraparound, got %d", rb.count)
	}

	snap := rb.Snapshot(10)
	if len(snap) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(snap))
	}
	// Should have msg-2, msg-3, msg-4 (oldest first)
	if snap[0].Message != "msg-2" {
		t.Errorf("expected msg-2 after wraparound, got %q", snap[0].Message)
	}
	if snap[2].Message != "msg-4" {
		t.Errorf("expected msg-4 as newest, got %q", snap[2].Message)
	}
}

func TestRingBuffer_SnapshotEmpty(t *testing.T) {
	rb := NewRingBuffer(10)
	snap := rb.Snapshot(5)
	if snap != nil {
		t.Errorf("expected nil for empty buffer, got %v", snap)
	}
}

func TestRingBuffer_SnapshotZero(t *testing.T) {
	rb := NewRingBuffer(10)
	rb.Write(makeEntry("INFO", "msg", time.Now()))
	snap := rb.Snapshot(0)
	if snap != nil {
		t.Errorf("expected nil for n=0, got %v", snap)
	}
}

func TestRingBuffer_QueryByLevel(t *testing.T) {
	rb := NewRingBuffer(100)
	now := time.Now()

	rb.Write(makeEntry("INFO", "info-msg", now))
	rb.Write(makeEntry("WARNING", "warn-msg", now.Add(time.Second)))
	rb.Write(makeEntry("ERROR", "error-msg", now.Add(2*time.Second)))

	result := rb.Query(Filter{Level: "WARNING"})
	if result.Total != 2 {
		t.Errorf("expected 2 entries >= WARNING, got %d", result.Total)
	}
	for _, e := range result.Entries {
		if e.Level == "INFO" {
			t.Error("INFO should be filtered out for WARNING minimum")
		}
	}
}

func TestRingBuffer_QueryByTimeRange(t *testing.T) {
	rb := NewRingBuffer(100)
	base := time.Date(2026, 3, 18, 12, 0, 0, 0, time.UTC)

	rb.Write(makeEntry("INFO", "early", base))
	rb.Write(makeEntry("INFO", "middle", base.Add(5*time.Minute)))
	rb.Write(makeEntry("INFO", "late", base.Add(10*time.Minute)))

	result := rb.Query(Filter{
		Since: base.Add(3 * time.Minute),
		Until: base.Add(7 * time.Minute),
	})
	if result.Total != 1 {
		t.Errorf("expected 1 entry in time range, got %d", result.Total)
	}
	if len(result.Entries) > 0 && result.Entries[0].Message != "middle" {
		t.Errorf("expected 'middle', got %q", result.Entries[0].Message)
	}
}

func TestRingBuffer_QueryByPattern(t *testing.T) {
	rb := NewRingBuffer(100)
	now := time.Now()

	rb.Write(makeEntry("INFO", "user login successful", now))
	rb.Write(makeEntry("INFO", "file uploaded", now.Add(time.Second)))
	rb.Write(makeEntry("ERROR", "user login failed", now.Add(2*time.Second)))

	result := rb.Query(Filter{Pattern: "user login"})
	if result.Total != 2 {
		t.Errorf("expected 2 entries matching 'user login', got %d", result.Total)
	}
}

func TestRingBuffer_QueryByRegex(t *testing.T) {
	rb := NewRingBuffer(100)
	now := time.Now()

	rb.Write(makeEntry("INFO", "request took 150ms", now))
	rb.Write(makeEntry("INFO", "request took 2500ms", now.Add(time.Second)))
	rb.Write(makeEntry("INFO", "request took 50ms", now.Add(2*time.Second)))

	// Regex: find requests taking 1000ms+ (4+ digits before 'ms')
	result := rb.Query(Filter{Pattern: `\d{4,}ms`})
	if result.Total != 1 {
		t.Errorf("expected 1 entry with 4+ digit ms, got %d", result.Total)
	}
}

func TestRingBuffer_QueryInvalidRegex(t *testing.T) {
	rb := NewRingBuffer(100)
	now := time.Now()
	rb.Write(makeEntry("INFO", "test", now))

	// Invalid regex should return error or no panic
	result := rb.Query(Filter{Pattern: "[invalid"})
	// After fix: should return 0 results with no panic
	if result.Total < 0 {
		t.Error("query with invalid regex should not panic")
	}
}

func TestRingBuffer_QueryByFile(t *testing.T) {
	rb := NewRingBuffer(100)
	now := time.Now()

	e1 := makeEntry("INFO", "msg1", now)
	e1.File = "master_server.go"
	e2 := makeEntry("INFO", "msg2", now.Add(time.Second))
	e2.File = "volume_server.go"
	e3 := makeEntry("INFO", "msg3", now.Add(2*time.Second))
	e3.File = "master_grpc.go"

	rb.Write(e1)
	rb.Write(e2)
	rb.Write(e3)

	result := rb.Query(Filter{File: "master_*"})
	if result.Total != 2 {
		t.Errorf("expected 2 entries matching master_*, got %d", result.Total)
	}
}

func TestRingBuffer_QueryByRequestID(t *testing.T) {
	rb := NewRingBuffer(100)
	now := time.Now()

	e1 := makeEntry("INFO", "step1", now)
	e1.RequestID = "req-abc"
	e2 := makeEntry("INFO", "step2", now.Add(time.Second))
	e2.RequestID = "req-xyz"
	e3 := makeEntry("INFO", "step3", now.Add(2*time.Second))
	e3.RequestID = "req-abc"

	rb.Write(e1)
	rb.Write(e2)
	rb.Write(e3)

	result := rb.Query(Filter{RequestID: "req-abc"})
	if result.Total != 2 {
		t.Errorf("expected 2 entries for req-abc, got %d", result.Total)
	}
}

func TestRingBuffer_QueryPagination(t *testing.T) {
	rb := NewRingBuffer(100)
	now := time.Now()

	for i := 0; i < 20; i++ {
		rb.Write(makeEntry("INFO", fmt.Sprintf("msg-%d", i), now.Add(time.Duration(i)*time.Second)))
	}

	// Page 1: offset=0, limit=5
	r1 := rb.Query(Filter{Limit: 5, Offset: 0})
	if len(r1.Entries) != 5 {
		t.Errorf("page 1: expected 5 entries, got %d", len(r1.Entries))
	}
	if r1.Total != 20 {
		t.Errorf("expected total 20, got %d", r1.Total)
	}
	if !r1.HasMore {
		t.Error("expected HasMore=true for page 1")
	}
	if r1.Entries[0].Message != "msg-0" {
		t.Errorf("expected first entry msg-0, got %q", r1.Entries[0].Message)
	}

	// Page 2: offset=5, limit=5
	r2 := rb.Query(Filter{Limit: 5, Offset: 5})
	if len(r2.Entries) != 5 {
		t.Errorf("page 2: expected 5 entries, got %d", len(r2.Entries))
	}
	if r2.Entries[0].Message != "msg-5" {
		t.Errorf("expected page 2 first entry msg-5, got %q", r2.Entries[0].Message)
	}

	// Last page: offset=18, limit=5
	r3 := rb.Query(Filter{Limit: 5, Offset: 18})
	if len(r3.Entries) != 2 {
		t.Errorf("last page: expected 2 entries, got %d", len(r3.Entries))
	}
	if r3.HasMore {
		t.Error("expected HasMore=false for last page")
	}
}

func TestRingBuffer_Subscribe(t *testing.T) {
	rb := NewRingBuffer(100)

	ch, unsub := rb.Subscribe()
	defer unsub()

	go func() {
		rb.Write(makeEntry("INFO", "live-msg", time.Now()))
	}()

	select {
	case entry := <-ch:
		if entry.Message != "live-msg" {
			t.Errorf("expected 'live-msg', got %q", entry.Message)
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for subscriber message")
	}
}

func TestRingBuffer_SubscribeMultiple(t *testing.T) {
	rb := NewRingBuffer(100)

	ch1, unsub1 := rb.Subscribe()
	defer unsub1()
	ch2, unsub2 := rb.Subscribe()
	defer unsub2()

	rb.Write(makeEntry("INFO", "broadcast", time.Now()))

	for i, ch := range []<-chan LogEntry{ch1, ch2} {
		select {
		case entry := <-ch:
			if entry.Message != "broadcast" {
				t.Errorf("subscriber %d: expected 'broadcast', got %q", i, entry.Message)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("subscriber %d: timeout", i)
		}
	}
}

func TestRingBuffer_UnsubscribeStopsDelivery(t *testing.T) {
	rb := NewRingBuffer(100)

	ch, unsub := rb.Subscribe()
	unsub()

	// Write after unsubscribe — channel should be closed
	rb.Write(makeEntry("INFO", "after-unsub", time.Now()))

	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to be closed after unsubscribe")
		}
	case <-time.After(500 * time.Millisecond):
		// Channel closed, no more data — this is fine
	}
}

func TestRingBuffer_ConcurrentWriteAndQuery(t *testing.T) {
	rb := NewRingBuffer(1000)
	var wg sync.WaitGroup

	// Concurrent writers
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				rb.Write(makeEntry("INFO", fmt.Sprintf("w%d-msg-%d", id, i), time.Now()))
			}
		}(w)
	}

	// Concurrent readers
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				rb.Query(Filter{Limit: 10})
				rb.Snapshot(5)
			}
		}()
	}

	wg.Wait()

	// After all writes: 5 writers * 200 = 1000 entries, buffer is 1000
	if rb.count != 1000 {
		t.Errorf("expected 1000 entries, got %d", rb.count)
	}
}

func TestRingBuffer_QueryCombinedFilters(t *testing.T) {
	rb := NewRingBuffer(100)
	base := time.Date(2026, 3, 18, 12, 0, 0, 0, time.UTC)

	e1 := makeEntry("INFO", "user login ok", base)
	e1.File = "auth.go"
	e1.RequestID = "req-1"

	e2 := makeEntry("ERROR", "user login failed", base.Add(time.Second))
	e2.File = "auth.go"
	e2.RequestID = "req-2"

	e3 := makeEntry("ERROR", "disk full", base.Add(2*time.Second))
	e3.File = "storage.go"
	e3.RequestID = "req-3"

	rb.Write(e1)
	rb.Write(e2)
	rb.Write(e3)

	// Combine level + file + pattern
	result := rb.Query(Filter{
		Level:   "ERROR",
		File:    "auth*",
		Pattern: "login",
	})
	if result.Total != 1 {
		t.Errorf("expected 1 combined match, got %d", result.Total)
	}
	if len(result.Entries) > 0 && result.Entries[0].Message != "user login failed" {
		t.Errorf("expected 'user login failed', got %q", result.Entries[0].Message)
	}
}

// ---------- Stats / RecentErrors / TopFiles / ErrorRate ----------

func TestRingBuffer_Stats(t *testing.T) {
	rb := NewRingBuffer(100)
	now := time.Now()

	rb.Write(makeEntry("INFO", "a", now))
	rb.Write(makeEntry("INFO", "b", now))
	rb.Write(makeEntry("WARNING", "c", now))
	rb.Write(makeEntry("ERROR", "d", now))
	rb.Write(makeEntry("FATAL", "e", now))

	s := rb.Stats()
	if s.Capacity != 100 {
		t.Errorf("expected capacity 100, got %d", s.Capacity)
	}
	if s.Used != 5 {
		t.Errorf("expected used 5, got %d", s.Used)
	}
	if s.TotalWritten != 5 {
		t.Errorf("expected totalWritten 5, got %d", s.TotalWritten)
	}
	if s.ByLevel.Info != 2 {
		t.Errorf("expected 2 info, got %d", s.ByLevel.Info)
	}
	if s.ByLevel.Warning != 1 {
		t.Errorf("expected 1 warning, got %d", s.ByLevel.Warning)
	}
	if s.ByLevel.Error != 1 {
		t.Errorf("expected 1 error, got %d", s.ByLevel.Error)
	}
	if s.ByLevel.Fatal != 1 {
		t.Errorf("expected 1 fatal, got %d", s.ByLevel.Fatal)
	}
	if s.UsagePercent != 5.0 {
		t.Errorf("expected usage 5%%, got %.1f%%", s.UsagePercent)
	}
}

func TestRingBuffer_StatsDropped(t *testing.T) {
	rb := NewRingBuffer(100)
	ch, unsub := rb.Subscribe()
	defer unsub()

	// Fill subscriber channel (buffer=256) then overflow
	for i := 0; i < 300; i++ {
		rb.Write(makeEntry("INFO", fmt.Sprintf("msg-%d", i), time.Now()))
	}

	s := rb.Stats()
	if s.Dropped == 0 {
		t.Error("expected some dropped entries for slow subscriber")
	}
	if s.Subscribers != 1 {
		t.Errorf("expected 1 subscriber, got %d", s.Subscribers)
	}

	// Drain to avoid goroutine leak
	for len(ch) > 0 {
		<-ch
	}
}

func TestRingBuffer_RecentErrors(t *testing.T) {
	rb := NewRingBuffer(100)
	now := time.Now()

	rb.Write(makeEntry("INFO", "ok1", now))
	rb.Write(makeEntry("ERROR", "err1", now.Add(1*time.Second)))
	rb.Write(makeEntry("INFO", "ok2", now.Add(2*time.Second)))
	rb.Write(makeEntry("FATAL", "fatal1", now.Add(3*time.Second)))
	rb.Write(makeEntry("ERROR", "err2", now.Add(4*time.Second)))
	rb.Write(makeEntry("INFO", "ok3", now.Add(5*time.Second)))

	errs := rb.RecentErrors(10)
	if len(errs) != 3 {
		t.Fatalf("expected 3 errors, got %d", len(errs))
	}
	// Should be in chronological order
	if errs[0].Message != "err1" {
		t.Errorf("expected first error 'err1', got %q", errs[0].Message)
	}
	if errs[1].Message != "fatal1" {
		t.Errorf("expected second error 'fatal1', got %q", errs[1].Message)
	}
	if errs[2].Message != "err2" {
		t.Errorf("expected third error 'err2', got %q", errs[2].Message)
	}
}

func TestRingBuffer_RecentErrors_Limit(t *testing.T) {
	rb := NewRingBuffer(100)
	now := time.Now()

	for i := 0; i < 10; i++ {
		rb.Write(makeEntry("ERROR", fmt.Sprintf("err-%d", i), now.Add(time.Duration(i)*time.Second)))
	}

	errs := rb.RecentErrors(3)
	if len(errs) != 3 {
		t.Fatalf("expected 3 errors, got %d", len(errs))
	}
	// Should be the 3 MOST RECENT in chrono order
	if errs[0].Message != "err-7" {
		t.Errorf("expected err-7, got %q", errs[0].Message)
	}
	if errs[2].Message != "err-9" {
		t.Errorf("expected err-9, got %q", errs[2].Message)
	}
}

func TestRingBuffer_RecentErrors_Empty(t *testing.T) {
	rb := NewRingBuffer(100)
	errs := rb.RecentErrors(5)
	if errs != nil {
		t.Errorf("expected nil for empty buffer, got %v", errs)
	}
}

func TestRingBuffer_RecentErrors_NoErrors(t *testing.T) {
	rb := NewRingBuffer(100)
	rb.Write(makeEntry("INFO", "ok", time.Now()))
	rb.Write(makeEntry("WARNING", "warn", time.Now()))

	errs := rb.RecentErrors(5)
	if len(errs) != 0 {
		t.Errorf("expected 0 errors when only INFO/WARNING, got %d", len(errs))
	}
}

func TestRingBuffer_TopFiles(t *testing.T) {
	rb := NewRingBuffer(100)
	now := time.Now()

	// master_server.go: 5 entries
	for i := 0; i < 5; i++ {
		e := makeEntry("INFO", "msg", now)
		e.File = "master_server.go"
		rb.Write(e)
	}
	// volume_server.go: 3 entries
	for i := 0; i < 3; i++ {
		e := makeEntry("INFO", "msg", now)
		e.File = "volume_server.go"
		rb.Write(e)
	}
	// filer.go: 1 entry
	e := makeEntry("INFO", "msg", now)
	e.File = "filer.go"
	rb.Write(e)

	top := rb.TopFiles(2)
	if len(top) != 2 {
		t.Fatalf("expected 2 top files, got %d", len(top))
	}
	if top[0].File != "master_server.go" || top[0].Count != 5 {
		t.Errorf("expected master_server.go:5, got %s:%d", top[0].File, top[0].Count)
	}
	if top[1].File != "volume_server.go" || top[1].Count != 3 {
		t.Errorf("expected volume_server.go:3, got %s:%d", top[1].File, top[1].Count)
	}
}

func TestRingBuffer_TopFiles_Empty(t *testing.T) {
	rb := NewRingBuffer(100)
	top := rb.TopFiles(5)
	if len(top) != 0 {
		t.Errorf("expected 0 for empty buffer, got %d", len(top))
	}
}

func TestRingBuffer_ErrorRate(t *testing.T) {
	rb := NewRingBuffer(100)
	now := time.Now()

	// 8 INFO + 2 ERROR = 20% error rate
	for i := 0; i < 8; i++ {
		rb.Write(makeEntry("INFO", "ok", now.Add(-30*time.Second)))
	}
	rb.Write(makeEntry("ERROR", "e1", now.Add(-20*time.Second)))
	rb.Write(makeEntry("FATAL", "f1", now.Add(-10*time.Second)))

	r := rb.ErrorRate(1 * time.Minute)
	if r.TotalLogs != 10 {
		t.Errorf("expected 10 total, got %d", r.TotalLogs)
	}
	if r.ErrorCount != 2 {
		t.Errorf("expected 2 errors, got %d", r.ErrorCount)
	}
	if r.ErrorRate < 19.9 || r.ErrorRate > 20.1 {
		t.Errorf("expected ~20%% error rate, got %.1f%%", r.ErrorRate)
	}
}

func TestRingBuffer_ErrorRate_NoLogs(t *testing.T) {
	rb := NewRingBuffer(100)
	r := rb.ErrorRate(1 * time.Minute)
	if r.TotalLogs != 0 || r.ErrorRate != 0 {
		t.Errorf("expected zeros for empty buffer, got total=%d rate=%.1f", r.TotalLogs, r.ErrorRate)
	}
}

func TestRingBuffer_ErrorRate_OutsideWindow(t *testing.T) {
	rb := NewRingBuffer(100)
	// Write entries 10 minutes ago — outside 1 minute window
	old := time.Now().Add(-10 * time.Minute)
	rb.Write(makeEntry("ERROR", "old error", old))

	r := rb.ErrorRate(1 * time.Minute)
	if r.TotalLogs != 0 {
		t.Errorf("expected 0 logs in window, got %d", r.TotalLogs)
	}
}

func BenchmarkRingBuffer_Write(b *testing.B) {
	rb := NewRingBuffer(10000)
	entry := makeEntry("INFO", "benchmark message", time.Now())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Write(entry)
	}
}

func BenchmarkRingBuffer_Query(b *testing.B) {
	rb := NewRingBuffer(10000)
	now := time.Now()
	for i := 0; i < 10000; i++ {
		rb.Write(makeEntry("INFO", fmt.Sprintf("msg-%d", i), now.Add(time.Duration(i)*time.Millisecond)))
	}

	f := Filter{Level: "INFO", Limit: 100, Pattern: "msg-5"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Query(f)
	}
}

func BenchmarkParseLogLine(b *testing.B) {
	raw := []byte("I0318 12:34:56.123456 12345 master_server.go:123] request_id:abc123 operation completed successfully\n")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParseLogLine(0, raw)
	}
}

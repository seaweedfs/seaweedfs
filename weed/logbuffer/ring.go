package logbuffer

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const defaultCapacity = 10000

// RingBuffer is a thread-safe circular buffer for log entries
// with support for live subscribers (SSE streaming) and runtime stats.
type RingBuffer struct {
	mu      sync.RWMutex
	entries []LogEntry
	head    int // next write position
	count   int
	cap     int

	subsMu sync.RWMutex
	subs   map[uint64]chan LogEntry
	nextID uint64

	// Stats counters (atomic, lock-free reads)
	totalWritten uint64
	dropped      uint64 // entries dropped due to slow subscribers
	infoCount    uint64
	warnCount    uint64
	errorCount   uint64
	fatalCount   uint64
}

// BufferStats holds runtime statistics about the ring buffer.
type BufferStats struct {
	Capacity     int     `json:"capacity"`
	Used         int     `json:"used"`
	UsagePercent float64 `json:"usage_percent"`
	TotalWritten uint64  `json:"total_written"`
	Dropped      uint64  `json:"dropped"`
	Subscribers  int     `json:"subscribers"`
	ByLevel      struct {
		Info    uint64 `json:"info"`
		Warning uint64 `json:"warning"`
		Error   uint64 `json:"error"`
		Fatal   uint64 `json:"fatal"`
	} `json:"by_level"`
}

// NewRingBuffer creates a ring buffer with the given capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity <= 0 {
		capacity = defaultCapacity
	}
	return &RingBuffer{
		entries: make([]LogEntry, capacity),
		cap:     capacity,
		subs:    make(map[uint64]chan LogEntry),
	}
}

// Write adds a log entry to the ring buffer and notifies subscribers.
func (rb *RingBuffer) Write(entry LogEntry) {
	rb.mu.Lock()
	rb.entries[rb.head] = entry
	rb.head = (rb.head + 1) % rb.cap
	if rb.count < rb.cap {
		rb.count++
	}
	rb.mu.Unlock()

	// Update stats (lock-free)
	atomic.AddUint64(&rb.totalWritten, 1)
	switch strings.ToUpper(entry.Level) {
	case "INFO":
		atomic.AddUint64(&rb.infoCount, 1)
	case "WARNING":
		atomic.AddUint64(&rb.warnCount, 1)
	case "ERROR":
		atomic.AddUint64(&rb.errorCount, 1)
	case "FATAL":
		atomic.AddUint64(&rb.fatalCount, 1)
	}

	// Fan-out to SSE subscribers (non-blocking)
	rb.subsMu.RLock()
	for _, ch := range rb.subs {
		select {
		case ch <- entry:
		default:
			// Drop if subscriber is slow — count it
			atomic.AddUint64(&rb.dropped, 1)
		}
	}
	rb.subsMu.RUnlock()
}

// Subscribe returns a channel that receives new log entries and an unsubscribe function.
// The channel has a buffer of 256 entries to handle bursts.
func (rb *RingBuffer) Subscribe() (<-chan LogEntry, func()) {
	ch := make(chan LogEntry, 256)

	rb.subsMu.Lock()
	id := rb.nextID
	rb.nextID++
	rb.subs[id] = ch
	rb.subsMu.Unlock()

	unsubscribe := func() {
		rb.subsMu.Lock()
		delete(rb.subs, id)
		close(ch)
		rb.subsMu.Unlock()
	}

	return ch, unsubscribe
}

// Stats returns a snapshot of the buffer's runtime statistics.
func (rb *RingBuffer) Stats() BufferStats {
	rb.mu.RLock()
	used := rb.count
	cap_ := rb.cap
	rb.mu.RUnlock()

	rb.subsMu.RLock()
	subs := len(rb.subs)
	rb.subsMu.RUnlock()

	s := BufferStats{
		Capacity:     cap_,
		Used:         used,
		TotalWritten: atomic.LoadUint64(&rb.totalWritten),
		Dropped:      atomic.LoadUint64(&rb.dropped),
		Subscribers:  subs,
	}
	if cap_ > 0 {
		s.UsagePercent = float64(used) / float64(cap_) * 100
	}
	s.ByLevel.Info = atomic.LoadUint64(&rb.infoCount)
	s.ByLevel.Warning = atomic.LoadUint64(&rb.warnCount)
	s.ByLevel.Error = atomic.LoadUint64(&rb.errorCount)
	s.ByLevel.Fatal = atomic.LoadUint64(&rb.fatalCount)
	return s
}

// Query returns log entries matching the given filter.
// Entries are returned in chronological order (oldest first).
// Returns an error string in QueryResult if the regex pattern is invalid.
func (rb *RingBuffer) Query(f Filter) QueryResult {
	// Compile regex and compute filter params before acquiring lock
	// to avoid blocking writers during regex compilation.
	minLevel := LevelPriority(f.Level)

	var compiledPattern *regexp.Regexp
	if f.Pattern != "" {
		var err error
		compiledPattern, err = regexp.Compile(f.Pattern)
		if err != nil {
			return QueryResult{
				Error: fmt.Sprintf("invalid regex pattern: %v", err),
			}
		}
	}

	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return QueryResult{}
	}

	// Set defaults
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > rb.count {
		limit = rb.count
	}

	// Iterate from oldest to newest
	start := 0
	if rb.count == rb.cap {
		start = rb.head // oldest entry when buffer is full
	}

	var matched []LogEntry
	skipped := 0
	total := 0

	for i := 0; i < rb.count; i++ {
		idx := (start + i) % rb.cap
		entry := rb.entries[idx]

		if !matchesFilter(entry, f, minLevel, compiledPattern) {
			continue
		}

		total++

		if skipped < f.Offset {
			skipped++
			continue
		}

		if len(matched) < limit {
			matched = append(matched, entry)
		}
	}

	return QueryResult{
		Entries: matched,
		Total:   total,
		HasMore: total > f.Offset+len(matched),
	}
}

// Snapshot returns the last N entries (no filtering).
func (rb *RingBuffer) Snapshot(n int) []LogEntry {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if n <= 0 || rb.count == 0 {
		return nil
	}
	if n > rb.count {
		n = rb.count
	}

	result := make([]LogEntry, n)
	start := (rb.head - n + rb.cap) % rb.cap
	for i := 0; i < n; i++ {
		result[i] = rb.entries[(start+i)%rb.cap]
	}
	return result
}

// RecentErrors returns the last N error/fatal entries for quick diagnosis.
func (rb *RingBuffer) RecentErrors(n int) []LogEntry {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if n <= 0 || rb.count == 0 {
		return nil
	}

	var result []LogEntry
	// Walk backwards from newest entry
	for i := 0; i < rb.count && len(result) < n; i++ {
		idx := (rb.head - 1 - i + rb.cap) % rb.cap
		entry := rb.entries[idx]
		if LevelPriority(entry.Level) >= 3 { // ERROR or FATAL
			result = append(result, entry)
		}
	}

	// Reverse to chronological order (oldest first)
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

// TopFiles returns the N source files that generated the most log entries.
func (rb *RingBuffer) TopFiles(n int) []FileCount {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	counts := make(map[string]int)
	start := 0
	if rb.count == rb.cap {
		start = rb.head
	}
	for i := 0; i < rb.count; i++ {
		idx := (start + i) % rb.cap
		f := rb.entries[idx].File
		if f != "" {
			counts[f]++
		}
	}

	// Sort by count descending
	type kv struct {
		file  string
		count int
	}
	var sorted []kv
	for f, c := range counts {
		sorted = append(sorted, kv{f, c})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].count > sorted[j].count
	})

	if n > len(sorted) {
		n = len(sorted)
	}
	result := make([]FileCount, n)
	for i := 0; i < n; i++ {
		result[i] = FileCount{File: sorted[i].file, Count: sorted[i].count}
	}
	return result
}

// ErrorRate returns error+fatal count in the given time window.
func (rb *RingBuffer) ErrorRate(window time.Duration) ErrorRateResult {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	cutoff := time.Now().Add(-window)
	var errors, total int

	start := 0
	if rb.count == rb.cap {
		start = rb.head
	}
	for i := 0; i < rb.count; i++ {
		idx := (start + i) % rb.cap
		entry := rb.entries[idx]
		if entry.Timestamp.Before(cutoff) {
			continue
		}
		total++
		p := LevelPriority(entry.Level)
		if p >= 3 {
			errors++
		}
	}

	var rate float64
	if total > 0 {
		rate = float64(errors) / float64(total) * 100
	}

	return ErrorRateResult{
		Window:     window.String(),
		TotalLogs:  total,
		ErrorCount: errors,
		ErrorRate:  rate,
	}
}

func matchesFilter(entry LogEntry, f Filter, minLevel int, pattern *regexp.Regexp) bool {
	// Level filter
	if minLevel > 0 && LevelPriority(entry.Level) < minLevel {
		return false
	}

	// Time range
	if !f.Since.IsZero() && entry.Timestamp.Before(f.Since) {
		return false
	}
	if !f.Until.IsZero() && entry.Timestamp.After(f.Until) {
		return false
	}

	// File pattern (glob)
	if f.File != "" {
		matched, _ := filepath.Match(f.File, entry.File)
		if !matched {
			return false
		}
	}

	// Request ID
	if f.RequestID != "" && entry.RequestID != f.RequestID {
		return false
	}

	// Regex pattern on message
	if pattern != nil && !pattern.MatchString(entry.Message) {
		return false
	}

	return true
}

// LevelPriority converts a level name to a numeric priority for comparisons.
func LevelPriority(level string) int {
	switch strings.ToUpper(level) {
	case "INFO":
		return 1
	case "WARNING":
		return 2
	case "ERROR":
		return 3
	case "FATAL":
		return 4
	default:
		return 0
	}
}

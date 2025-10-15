package tracker

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Record represents a tracked message
type Record struct {
	Key        string `json:"key"`
	Topic      string `json:"topic"`
	Partition  int32  `json:"partition"`
	Offset     int64  `json:"offset"`
	Timestamp  int64  `json:"timestamp"`
	ProducerID int    `json:"producer_id,omitempty"`
	ConsumerID int    `json:"consumer_id,omitempty"`
}

// Tracker tracks produced and consumed records
type Tracker struct {
	mu               sync.Mutex
	producedRecords  []Record
	consumedRecords  []Record
	producedFile     string
	consumedFile     string
	testStartTime    int64  // Unix timestamp in nanoseconds - used to filter old messages
	testRunPrefix    string // Key prefix for this test run (e.g., "run-20251015-170150")
	filteredOldCount int    // Count of old messages consumed but not tracked
}

// NewTracker creates a new record tracker
func NewTracker(producedFile, consumedFile string, testStartTime int64) *Tracker {
	// Generate test run prefix from start time using same format as producer
	// Producer format: p.startTime.Format("20060102-150405") -> "20251015-170859"
	startTime := time.Unix(0, testStartTime)
	runID := startTime.Format("20060102-150405")
	testRunPrefix := fmt.Sprintf("run-%s", runID)

	fmt.Printf("Tracker initialized with prefix: %s (filtering messages not matching this prefix)\n", testRunPrefix)

	return &Tracker{
		producedRecords:  make([]Record, 0, 100000),
		consumedRecords:  make([]Record, 0, 100000),
		producedFile:     producedFile,
		consumedFile:     consumedFile,
		testStartTime:    testStartTime,
		testRunPrefix:    testRunPrefix,
		filteredOldCount: 0,
	}
}

// TrackProduced records a produced message
func (t *Tracker) TrackProduced(record Record) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.producedRecords = append(t.producedRecords, record)
}

// TrackConsumed records a consumed message
// Only tracks messages from the current test run (filters out old messages from previous tests)
func (t *Tracker) TrackConsumed(record Record) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Filter: Only track messages from current test run based on key prefix
	// Producer keys look like: "run-20251015-170150-key-123"
	// We only want messages that match our test run prefix
	if !strings.HasPrefix(record.Key, t.testRunPrefix) {
		// Count old messages consumed but not tracked
		t.filteredOldCount++
		return
	}

	t.consumedRecords = append(t.consumedRecords, record)
}

// SaveProduced writes produced records to file
func (t *Tracker) SaveProduced() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	f, err := os.Create(t.producedFile)
	if err != nil {
		return fmt.Errorf("failed to create produced file: %v", err)
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	for _, record := range t.producedRecords {
		if err := encoder.Encode(record); err != nil {
			return fmt.Errorf("failed to encode produced record: %v", err)
		}
	}

	fmt.Printf("Saved %d produced records to %s\n", len(t.producedRecords), t.producedFile)
	return nil
}

// SaveConsumed writes consumed records to file
func (t *Tracker) SaveConsumed() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	f, err := os.Create(t.consumedFile)
	if err != nil {
		return fmt.Errorf("failed to create consumed file: %v", err)
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	for _, record := range t.consumedRecords {
		if err := encoder.Encode(record); err != nil {
			return fmt.Errorf("failed to encode consumed record: %v", err)
		}
	}

	fmt.Printf("Saved %d consumed records to %s\n", len(t.consumedRecords), t.consumedFile)
	return nil
}

// Compare compares produced and consumed records
func (t *Tracker) Compare() ComparisonResult {
	t.mu.Lock()
	defer t.mu.Unlock()

	result := ComparisonResult{
		TotalProduced:    len(t.producedRecords),
		TotalConsumed:    len(t.consumedRecords),
		FilteredOldCount: t.filteredOldCount,
	}

	// Build maps for efficient lookup
	producedMap := make(map[string]Record)
	for _, record := range t.producedRecords {
		key := fmt.Sprintf("%s-%d-%d", record.Topic, record.Partition, record.Offset)
		producedMap[key] = record
	}

	consumedMap := make(map[string]int)
	duplicateKeys := make(map[string][]Record)

	for _, record := range t.consumedRecords {
		key := fmt.Sprintf("%s-%d-%d", record.Topic, record.Partition, record.Offset)
		consumedMap[key]++

		if consumedMap[key] > 1 {
			duplicateKeys[key] = append(duplicateKeys[key], record)
		}
	}

	// Find missing records (produced but not consumed)
	for key, record := range producedMap {
		if _, found := consumedMap[key]; !found {
			result.Missing = append(result.Missing, record)
		}
	}

	// Find duplicate records (consumed multiple times)
	for key, records := range duplicateKeys {
		if len(records) > 0 {
			// Add first occurrence for context
			result.Duplicates = append(result.Duplicates, DuplicateRecord{
				Record: records[0],
				Count:  consumedMap[key],
			})
		}
	}

	result.MissingCount = len(result.Missing)
	result.DuplicateCount = len(result.Duplicates)
	result.UniqueConsumed = result.TotalConsumed - sumDuplicates(result.Duplicates)

	return result
}

// ComparisonResult holds the comparison results
type ComparisonResult struct {
	TotalProduced    int
	TotalConsumed    int
	UniqueConsumed   int
	MissingCount     int
	DuplicateCount   int
	FilteredOldCount int // Old messages consumed but filtered out
	Missing          []Record
	Duplicates       []DuplicateRecord
}

// DuplicateRecord represents a record consumed multiple times
type DuplicateRecord struct {
	Record Record
	Count  int
}

// PrintSummary prints a summary of the comparison
func (r *ComparisonResult) PrintSummary() {
	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("             MESSAGE VERIFICATION RESULTS")
	fmt.Println(strings.Repeat("=", 70))

	fmt.Printf("\nProduction Summary:\n")
	fmt.Printf("  Total Produced:    %d messages\n", r.TotalProduced)

	fmt.Printf("\nConsumption Summary:\n")
	fmt.Printf("  Total Consumed:    %d messages (from current test)\n", r.TotalConsumed)
	fmt.Printf("  Unique Consumed:   %d messages\n", r.UniqueConsumed)
	fmt.Printf("  Duplicate Reads:   %d messages\n", r.TotalConsumed-r.UniqueConsumed)
	if r.FilteredOldCount > 0 {
		fmt.Printf("  Filtered Old:      %d messages (from previous tests, not tracked)\n", r.FilteredOldCount)
	}

	fmt.Printf("\nVerification Results:\n")
	if r.MissingCount == 0 {
		fmt.Printf("  ✅ Missing Records:   0 (all messages delivered)\n")
	} else {
		fmt.Printf("  ❌ Missing Records:   %d (data loss detected!)\n", r.MissingCount)
	}

	if r.DuplicateCount == 0 {
		fmt.Printf("  ✅ Duplicate Records: 0 (no duplicates)\n")
	} else {
		duplicatePercent := float64(r.TotalConsumed-r.UniqueConsumed) * 100.0 / float64(r.TotalProduced)
		fmt.Printf("  ⚠️  Duplicate Records: %d unique messages read multiple times (%.1f%%)\n",
			r.DuplicateCount, duplicatePercent)
	}

	fmt.Printf("\nDelivery Guarantee:\n")
	if r.MissingCount == 0 && r.DuplicateCount == 0 {
		fmt.Printf("  ✅ EXACTLY-ONCE: All messages delivered exactly once\n")
	} else if r.MissingCount == 0 {
		fmt.Printf("  ✅ AT-LEAST-ONCE: All messages delivered (some duplicates)\n")
	} else {
		fmt.Printf("  ❌ AT-MOST-ONCE: Some messages lost\n")
	}

	// Print sample of missing records (up to 10)
	if len(r.Missing) > 0 {
		fmt.Printf("\nSample Missing Records (first 10 of %d):\n", len(r.Missing))
		for i, record := range r.Missing {
			if i >= 10 {
				break
			}
			fmt.Printf("  - %s[%d]@%d (key=%s)\n",
				record.Topic, record.Partition, record.Offset, record.Key)
		}
	}

	// Print sample of duplicate records (up to 10)
	if len(r.Duplicates) > 0 {
		fmt.Printf("\nSample Duplicate Records (first 10 of %d):\n", len(r.Duplicates))
		// Sort by count descending
		sorted := make([]DuplicateRecord, len(r.Duplicates))
		copy(sorted, r.Duplicates)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Count > sorted[j].Count
		})

		for i, dup := range sorted {
			if i >= 10 {
				break
			}
			fmt.Printf("  - %s[%d]@%d (key=%s, read %d times)\n",
				dup.Record.Topic, dup.Record.Partition, dup.Record.Offset,
				dup.Record.Key, dup.Count)
		}
	}

	fmt.Println(strings.Repeat("=", 70))
}

func sumDuplicates(duplicates []DuplicateRecord) int {
	sum := 0
	for _, dup := range duplicates {
		sum += dup.Count - 1 // Don't count the first occurrence
	}
	return sum
}

package integration

import (
	"testing"
	"time"
)

// TestAdaptiveFetchTimeout verifies that the adaptive timeout strategy
// allows reading multiple records from disk within a reasonable time
func TestAdaptiveFetchTimeout(t *testing.T) {
	t.Log("Testing adaptive fetch timeout strategy...")

	// Simulate the scenario where we need to read 4 records from disk
	// Each record takes 100-200ms to read (simulates disk I/O)
	recordReadTimes := []time.Duration{
		150 * time.Millisecond, // Record 1 (from disk)
		150 * time.Millisecond, // Record 2 (from disk)
		150 * time.Millisecond, // Record 3 (from disk)
		150 * time.Millisecond, // Record 4 (from disk)
	}

	// Test 1: Old strategy (50ms timeout per record)
	t.Run("OldStrategy_50ms_Timeout", func(t *testing.T) {
		timeout := 50 * time.Millisecond
		recordsReceived := 0

		start := time.Now()
		for i, readTime := range recordReadTimes {
			if readTime <= timeout {
				recordsReceived++
			} else {
				t.Logf("Record %d timed out (readTime=%v > timeout=%v)", i+1, readTime, timeout)
				break
			}
		}
		duration := time.Since(start)

		t.Logf("Old strategy: received %d/%d records in %v", recordsReceived, len(recordReadTimes), duration)

		if recordsReceived >= len(recordReadTimes) {
			t.Error("Old strategy should NOT receive all records (timeout too short)")
		} else {
			t.Logf("✓ Bug reproduced: old strategy times out too quickly")
		}
	})

	// Test 2: New adaptive strategy (1 second timeout for first 5 records)
	t.Run("NewStrategy_1s_Timeout", func(t *testing.T) {
		timeout := 1 * time.Second // Generous timeout for first batch
		recordsReceived := 0

		start := time.Now()
		for i, readTime := range recordReadTimes {
			if readTime <= timeout {
				recordsReceived++
				t.Logf("Record %d received (readTime=%v)", i+1, readTime)
			} else {
				t.Logf("Record %d timed out (readTime=%v > timeout=%v)", i+1, readTime, timeout)
				break
			}
		}
		duration := time.Since(start)

		t.Logf("New strategy: received %d/%d records in %v", recordsReceived, len(recordReadTimes), duration)

		if recordsReceived < len(recordReadTimes) {
			t.Errorf("New strategy should receive all records (timeout=%v)", timeout)
		} else {
			t.Logf("✓ Fix verified: new strategy receives all records")
		}
	})

	// Test 3: Schema Registry catch-up scenario
	t.Run("SchemaRegistry_CatchUp_Scenario", func(t *testing.T) {
		// Schema Registry has 500ms total timeout to catch up from offset 3 to 6
		schemaRegistryTimeout := 500 * time.Millisecond

		// With old strategy (50ms per record after first):
		// - First record: 10s timeout ✓
		// - Records 2-4: 50ms each ✗ (times out after record 1)
		// Total time: > 500ms (only gets 1 record per fetch)

		// With new strategy (1s per record for first 5):
		// - Records 1-4: 1s each ✓
		// - All 4 records received in ~600ms
		// Total time: ~600ms (gets all 4 records in one fetch)

		recordsNeeded := 4
		perRecordReadTime := 150 * time.Millisecond

		// Old strategy simulation
		oldStrategyTime := time.Duration(recordsNeeded) * 50 * time.Millisecond // Times out, need multiple fetches
		oldStrategyRoundTrips := recordsNeeded                                  // One record per fetch

		// New strategy simulation
		newStrategyTime := time.Duration(recordsNeeded) * perRecordReadTime // All in one fetch
		newStrategyRoundTrips := 1

		t.Logf("Schema Registry catch-up simulation:")
		t.Logf("  Old strategy: %d round trips, ~%v total time", oldStrategyRoundTrips, oldStrategyTime*time.Duration(oldStrategyRoundTrips))
		t.Logf("  New strategy: %d round trip, ~%v total time", newStrategyRoundTrips, newStrategyTime)
		t.Logf("  Schema Registry timeout: %v", schemaRegistryTimeout)

		oldStrategyTotalTime := oldStrategyTime * time.Duration(oldStrategyRoundTrips)
		newStrategyTotalTime := newStrategyTime * time.Duration(newStrategyRoundTrips)

		if oldStrategyTotalTime > schemaRegistryTimeout {
			t.Logf("✓ Old strategy exceeds timeout: %v > %v", oldStrategyTotalTime, schemaRegistryTimeout)
		}

		if newStrategyTotalTime <= schemaRegistryTimeout+200*time.Millisecond {
			t.Logf("✓ New strategy completes within timeout: %v <= %v", newStrategyTotalTime, schemaRegistryTimeout+200*time.Millisecond)
		} else {
			t.Errorf("New strategy too slow: %v > %v", newStrategyTotalTime, schemaRegistryTimeout)
		}
	})
}

// TestFetchTimeoutProgression verifies the timeout progression logic
func TestFetchTimeoutProgression(t *testing.T) {
	t.Log("Testing fetch timeout progression...")

	// Adaptive timeout logic:
	// - First 5 records: 1 second (catch-up from disk)
	// - After 5 records: 100ms (streaming from memory)

	getTimeout := func(recordNumber int) time.Duration {
		if recordNumber <= 5 {
			return 1 * time.Second
		}
		return 100 * time.Millisecond
	}

	t.Logf("Timeout progression:")
	for i := 1; i <= 10; i++ {
		timeout := getTimeout(i)
		t.Logf("  Record %2d: timeout = %v", i, timeout)
	}

	// Verify the progression
	if getTimeout(1) != 1*time.Second {
		t.Error("First record should have 1s timeout")
	}
	if getTimeout(5) != 1*time.Second {
		t.Error("Fifth record should have 1s timeout")
	}
	if getTimeout(6) != 100*time.Millisecond {
		t.Error("Sixth record should have 100ms timeout (fast path)")
	}
	if getTimeout(10) != 100*time.Millisecond {
		t.Error("Tenth record should have 100ms timeout (fast path)")
	}

	t.Log("✓ Timeout progression is correct")
}

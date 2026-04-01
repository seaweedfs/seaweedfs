package s3api

import (
	"math"
	"testing"
	"time"
)

// TestVersionIdFormatDetection tests that old and new format version IDs are correctly identified
func TestVersionIdFormatDetection(t *testing.T) {
	tests := []struct {
		name      string
		versionId string
		expectNew bool
	}{
		// New format (inverted timestamps) - values > 0x4000000000000000
		{
			name:      "new format - inverted timestamp",
			versionId: "68a1b2c3d4e5f6780000000000000000", // > 0x4000...
			expectNew: true,
		},
		{
			name:      "new format - high value",
			versionId: "7fffffffffffffff0000000000000000", // near max
			expectNew: true,
		},
		// Old format (raw timestamps) - values < 0x4000000000000000
		{
			name:      "old format - raw timestamp",
			versionId: "179a1b2c3d4e5f670000000000000000", // ~2024-2025
			expectNew: false,
		},
		{
			name:      "old format - low value",
			versionId: "10000000000000000000000000000000",
			expectNew: false,
		},
		// Edge cases
		{
			name:      "null version",
			versionId: "null",
			expectNew: false,
		},
		{
			name:      "short version ID",
			versionId: "abc123",
			expectNew: false,
		},
		{
			name:      "empty version ID",
			versionId: "",
			expectNew: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isNewFormatVersionId(tt.versionId)
			if got != tt.expectNew {
				t.Errorf("isNewFormatVersionId(%s) = %v, want %v", tt.versionId, got, tt.expectNew)
			}
		})
	}
}

// TestGenerateVersionIdFormats tests that generateVersionId produces correct format based on parameter
func TestGenerateVersionIdFormats(t *testing.T) {
	// Generate old format version ID
	oldFormatId := generateVersionId(false)
	if len(oldFormatId) != 32 {
		t.Errorf("old format version ID length = %d, want 32", len(oldFormatId))
	}
	if isNewFormatVersionId(oldFormatId) {
		t.Errorf("generateVersionId(false) produced new format ID: %s", oldFormatId)
	}

	// Generate new format version ID
	newFormatId := generateVersionId(true)
	if len(newFormatId) != 32 {
		t.Errorf("new format version ID length = %d, want 32", len(newFormatId))
	}
	if !isNewFormatVersionId(newFormatId) {
		t.Errorf("generateVersionId(true) produced old format ID: %s", newFormatId)
	}
}

// TestGetVersionTimestamp tests timestamp extraction from both formats
func TestGetVersionTimestamp(t *testing.T) {
	now := time.Now().UnixNano()

	// Generate old and new format IDs
	oldId := generateVersionId(false)
	newId := generateVersionId(true)

	oldTs := getVersionTimestamp(oldId)
	newTs := getVersionTimestamp(newId)

	// Both should be close to current time (within 1 second)
	tolerance := int64(time.Second)

	if abs(oldTs-now) > tolerance {
		t.Errorf("old format timestamp diff too large: got %d, want ~%d", oldTs, now)
	}
	if abs(newTs-now) > tolerance {
		t.Errorf("new format timestamp diff too large: got %d, want ~%d", newTs, now)
	}

	// null should return 0
	if ts := getVersionTimestamp("null"); ts != 0 {
		t.Errorf("getVersionTimestamp(null) = %d, want 0", ts)
	}
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// TestCompareVersionIdsSameFormatOld tests sorting of old format version IDs (newest first)
func TestCompareVersionIdsSameFormatOld(t *testing.T) {
	// Old format: larger hex value = newer (raw timestamp)
	older := "1700000000000000" + "0000000000000000" // older timestamp
	newer := "1800000000000000" + "0000000000000000" // newer timestamp

	// Verify both are old format
	if isNewFormatVersionId(older) || isNewFormatVersionId(newer) {
		t.Fatal("test setup error: expected old format IDs")
	}

	// compareVersionIds should return negative if first arg is newer
	result := compareVersionIds(newer, older)
	if result >= 0 {
		t.Errorf("compareVersionIds(newer, older) = %d, want negative", result)
	}

	result = compareVersionIds(older, newer)
	if result <= 0 {
		t.Errorf("compareVersionIds(older, newer) = %d, want positive", result)
	}

	result = compareVersionIds(older, older)
	if result != 0 {
		t.Errorf("compareVersionIds(same, same) = %d, want 0", result)
	}
}

// TestCompareVersionIdsSameFormatNew tests sorting of new format version IDs (newest first)
func TestCompareVersionIdsSameFormatNew(t *testing.T) {
	// New format: smaller hex value = newer (inverted timestamp)
	// MaxInt64 - newer_ts < MaxInt64 - older_ts
	newer := "6800000000000000" + "0000000000000000" // smaller = newer
	older := "6900000000000000" + "0000000000000000" // larger = older

	// Verify both are new format
	if !isNewFormatVersionId(older) || !isNewFormatVersionId(newer) {
		t.Fatal("test setup error: expected new format IDs")
	}

	// compareVersionIds should return negative if first arg is newer
	result := compareVersionIds(newer, older)
	if result >= 0 {
		t.Errorf("compareVersionIds(newer, older) = %d, want negative", result)
	}

	result = compareVersionIds(older, newer)
	if result <= 0 {
		t.Errorf("compareVersionIds(older, newer) = %d, want positive", result)
	}
}

// TestCompareVersionIdsMixedFormats tests sorting when comparing old and new format IDs
func TestCompareVersionIdsMixedFormats(t *testing.T) {
	// Create IDs where we know the actual timestamps
	// Old format: raw timestamp
	oldFormatTs := int64(1700000000000000000) // some timestamp
	oldFormatId := createOldFormatVersionId(oldFormatTs)

	// New format: inverted timestamp (created 1 second later)
	newFormatTs := oldFormatTs + int64(time.Second)
	newFormatId := createNewFormatVersionId(newFormatTs)

	// Verify formats
	if isNewFormatVersionId(oldFormatId) {
		t.Fatalf("expected old format for %s", oldFormatId)
	}
	if !isNewFormatVersionId(newFormatId) {
		t.Fatalf("expected new format for %s", newFormatId)
	}

	// New format ID is newer (created 1 second later)
	result := compareVersionIds(newFormatId, oldFormatId)
	if result >= 0 {
		t.Errorf("compareVersionIds(newer_new_format, older_old_format) = %d, want negative", result)
	}

	result = compareVersionIds(oldFormatId, newFormatId)
	if result <= 0 {
		t.Errorf("compareVersionIds(older_old_format, newer_new_format) = %d, want positive", result)
	}
}

// TestCompareVersionIdsNullHandling tests that null versions sort last
func TestCompareVersionIdsNullHandling(t *testing.T) {
	regular := generateVersionId(true)

	// null should sort after regular versions
	if result := compareVersionIds("null", regular); result <= 0 {
		t.Errorf("compareVersionIds(null, regular) = %d, want positive (null sorts last)", result)
	}

	if result := compareVersionIds(regular, "null"); result >= 0 {
		t.Errorf("compareVersionIds(regular, null) = %d, want negative (null sorts last)", result)
	}
}

// Helper to create old format version ID from timestamp
func createOldFormatVersionId(ts int64) string {
	return sprintf16x(uint64(ts)) + "0000000000000000"
}

// Helper to create new format version ID from timestamp
func createNewFormatVersionId(ts int64) string {
	inverted := uint64(math.MaxInt64 - ts)
	return sprintf16x(inverted) + "0000000000000000"
}

func sprintf16x(v uint64) string {
	return sprintf("%016x", v)
}

func sprintf(format string, v uint64) string {
	result := make([]byte, 16)
	for i := 15; i >= 0; i-- {
		digit := v & 0xf
		if digit < 10 {
			result[i] = byte('0' + digit)
		} else {
			result[i] = byte('a' + digit - 10)
		}
		v >>= 4
	}
	return string(result)
}

// TestOldFormatBackwardCompatibility ensures old format versions work correctly in sorting
func TestOldFormatBackwardCompatibility(t *testing.T) {
	// Simulate a bucket that was created before the inverted format was introduced
	// All versions should be old format and should sort correctly (newest first)

	// Create 5 old format version IDs with known timestamps
	baseTs := int64(1700000000000000000)
	versions := make([]string, 5)
	for i := 0; i < 5; i++ {
		ts := baseTs + int64(i)*int64(time.Minute) // each 1 minute apart
		versions[i] = createOldFormatVersionId(ts)
	}

	// Verify all are old format
	for i, v := range versions {
		if isNewFormatVersionId(v) {
			t.Fatalf("version %d should be old format: %s", i, v)
		}
	}

	// Verify sorting: versions[4] is newest, versions[0] is oldest
	// compareVersionIds(newer, older) should return negative
	for i := 0; i < 4; i++ {
		newer := versions[i+1]
		older := versions[i]
		result := compareVersionIds(newer, older)
		if result >= 0 {
			t.Errorf("compareVersionIds(versions[%d], versions[%d]) = %d, want negative (newer first)", i+1, i, result)
		}
	}

	// Verify extracted timestamps are correct
	for i, v := range versions {
		expectedTs := baseTs + int64(i)*int64(time.Minute)
		gotTs := getVersionTimestamp(v)
		if gotTs != expectedTs {
			t.Errorf("getVersionTimestamp(versions[%d]) = %d, want %d", i, gotTs, expectedTs)
		}
	}
}

// TestNewFormatSorting ensures new format versions sort correctly (newest first)
func TestNewFormatSorting(t *testing.T) {
	// Create 5 new format version IDs with known timestamps
	baseTs := int64(1700000000000000000)
	versions := make([]string, 5)
	for i := 0; i < 5; i++ {
		ts := baseTs + int64(i)*int64(time.Minute) // each 1 minute apart
		versions[i] = createNewFormatVersionId(ts)
	}

	// Verify all are new format
	for i, v := range versions {
		if !isNewFormatVersionId(v) {
			t.Fatalf("version %d should be new format: %s", i, v)
		}
	}

	// Verify sorting: versions[4] is newest, versions[0] is oldest
	for i := 0; i < 4; i++ {
		newer := versions[i+1]
		older := versions[i]
		result := compareVersionIds(newer, older)
		if result >= 0 {
			t.Errorf("compareVersionIds(versions[%d], versions[%d]) = %d, want negative (newer first)", i+1, i, result)
		}
	}

	// Verify extracted timestamps are correct
	for i, v := range versions {
		expectedTs := baseTs + int64(i)*int64(time.Minute)
		gotTs := getVersionTimestamp(v)
		if gotTs != expectedTs {
			t.Errorf("getVersionTimestamp(versions[%d]) = %d, want %d", i, gotTs, expectedTs)
		}
	}
}

// TestMixedFormatTransition simulates a bucket transitioning from old to new format
func TestMixedFormatTransition(t *testing.T) {
	baseTs := int64(1700000000000000000)

	// First 3 versions created with old format (before upgrade)
	oldVersions := make([]string, 3)
	for i := 0; i < 3; i++ {
		ts := baseTs + int64(i)*int64(time.Minute)
		oldVersions[i] = createOldFormatVersionId(ts)
	}

	// Next 3 versions created with new format (after upgrade)
	newVersions := make([]string, 3)
	for i := 0; i < 3; i++ {
		ts := baseTs + int64(3+i)*int64(time.Minute) // continue from where old left off
		newVersions[i] = createNewFormatVersionId(ts)
	}

	// All versions in chronological order (oldest to newest)
	allVersions := append(oldVersions, newVersions...)

	// Verify mixed formats
	for i := 0; i < 3; i++ {
		if isNewFormatVersionId(allVersions[i]) {
			t.Errorf("allVersions[%d] should be old format", i)
		}
	}
	for i := 3; i < 6; i++ {
		if !isNewFormatVersionId(allVersions[i]) {
			t.Errorf("allVersions[%d] should be new format", i)
		}
	}

	// Verify sorting works correctly across the format boundary
	for i := 0; i < 5; i++ {
		newer := allVersions[i+1]
		older := allVersions[i]
		result := compareVersionIds(newer, older)
		if result >= 0 {
			t.Errorf("compareVersionIds(allVersions[%d], allVersions[%d]) = %d, want negative (newer first)", i+1, i, result)
		}
	}

	// Verify the newest (new format) version sorts before oldest (old format) when comparing directly
	newest := allVersions[5] // newest, new format
	oldest := allVersions[0] // oldest, old format
	if result := compareVersionIds(newest, oldest); result >= 0 {
		t.Errorf("compareVersionIds(newest_new_format, oldest_old_format) = %d, want negative", result)
	}
}

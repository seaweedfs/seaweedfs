package s3tables

import (
	"strings"
	"testing"
)

func TestGetTableLocationMappingEntryPathPerTable(t *testing.T) {
	tableLocationBucket := "shared-location--table-s3"
	tablePathA := GetTablePath("warehouse", "analytics", "orders")
	tablePathB := GetTablePath("warehouse", "analytics", "customers")

	entryPathA := GetTableLocationMappingEntryPath(tableLocationBucket, tablePathA)
	entryPathARepeat := GetTableLocationMappingEntryPath(tableLocationBucket, tablePathA)
	entryPathB := GetTableLocationMappingEntryPath(tableLocationBucket, tablePathB)

	if entryPathA != entryPathARepeat {
		t.Fatalf("mapping entry path should be deterministic: %q != %q", entryPathA, entryPathARepeat)
	}
	if entryPathA == entryPathB {
		t.Fatalf("mapping entry path should differ per table path: %q == %q", entryPathA, entryPathB)
	}

	expectedPrefix := GetTableLocationMappingPath(tableLocationBucket) + "/"
	if !strings.HasPrefix(entryPathA, expectedPrefix) {
		t.Fatalf("mapping entry path %q should start with %q", entryPathA, expectedPrefix)
	}
	if strings.TrimPrefix(entryPathA, expectedPrefix) == "" {
		t.Fatalf("mapping entry name should not be empty: %q", entryPathA)
	}
}

func TestTableBucketPathFromTablePath(t *testing.T) {
	testCases := []struct {
		name      string
		tablePath string
		expected  string
		ok        bool
	}{
		{
			name:      "valid table path",
			tablePath: GetTablePath("warehouse", "analytics", "orders"),
			expected:  GetTableBucketPath("warehouse"),
			ok:        true,
		},
		{
			name:      "valid table bucket root",
			tablePath: GetTableBucketPath("warehouse"),
			expected:  GetTableBucketPath("warehouse"),
			ok:        true,
		},
		{
			name:      "invalid non-tables path",
			tablePath: "/tmp/warehouse/analytics/orders",
			expected:  "",
			ok:        false,
		},
		{
			name:      "invalid empty bucket segment",
			tablePath: "/buckets/",
			expected:  "",
			ok:        false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, ok := tableBucketPathFromTablePath(tc.tablePath)
			if ok != tc.ok {
				t.Fatalf("tableBucketPathFromTablePath(%q) ok=%v, want %v", tc.tablePath, ok, tc.ok)
			}
			if actual != tc.expected {
				t.Fatalf("tableBucketPathFromTablePath(%q)=%q, want %q", tc.tablePath, actual, tc.expected)
			}
		})
	}
}

package mount

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

// TestPermissionCheckPerformance simulates permission checks during a large copy operation.
// Shows that with caching, even 50,000 permission checks only trigger 1 system lookup.
func TestPermissionCheckPerformance(t *testing.T) {
	oldLookupSupplementaryGroupIDs := lookupSupplementaryGroupIDs
	lookupCount := 0
	lookupSupplementaryGroupIDs = func(uid uint32) ([]string, error) {
		lookupCount++
		return []string{"456", "789", "1011"}, nil
	}
	defer func() {
		lookupSupplementaryGroupIDs = oldLookupSupplementaryGroupIDs
	}()

	// Simulate copying 10,000 files as non-root user.
	// Each file access requires multiple permission checks.
	// Without caching, this would trigger 50,000 system lookups!
	fileCount := 10000
	checksPerFile := 5

	clearSupplementaryGroupCache()
	start := time.Now()
	for i := 0; i < fileCount; i++ {
		for j := 0; j < checksPerFile; j++ {
			gid := uint32(100 + (i % 10))
			hasAccess(999, 999, 123, gid, 0o040, fuse.R_OK|fuse.W_OK)
		}
	}
	elapsed := time.Since(start)

	totalChecks := fileCount * checksPerFile
	if lookupCount != 1 {
		t.Fatalf("Expected 1 system lookup (cache hit for UID 999), got %d", lookupCount)
	}

	opsPerSecond := float64(totalChecks) / elapsed.Seconds()
	fmt.Printf("\n=== Permission Check Performance Test (WITH CACHE) ===\n")
	fmt.Printf("Files simulated: %d\n", fileCount)
	fmt.Printf("Checks per file: %d\n", checksPerFile)
	fmt.Printf("Total checks: %d\n", totalChecks)
	fmt.Printf("System lookups: %d (would be %d without cache)\n", lookupCount, totalChecks)
	fmt.Printf("Lookups eliminated: %d (%.1f%% reduction)\n", totalChecks-lookupCount,
		(1-float64(lookupCount)/float64(totalChecks))*100)
	fmt.Printf("Time elapsed: %v\n", elapsed)
	fmt.Printf("Throughput: %.0f checks/sec\n", opsPerSecond)
}

// BenchmarkPermissionCheckScaling shows how performance scales with unique users.
func BenchmarkPermissionCheckScaling(b *testing.B) {
	oldLookupSupplementaryGroupIDs := lookupSupplementaryGroupIDs
	lookupSupplementaryGroupIDs = func(uid uint32) ([]string, error) {
		return []string{"456", "789", "1011"}, nil
	}
	clearSupplementaryGroupCache()
	defer func() {
		lookupSupplementaryGroupIDs = oldLookupSupplementaryGroupIDs
		clearSupplementaryGroupCache()
	}()

	// Simulate repeated permission checks for the same user
	// (typical single-user copy operation)
	b.Run("single-user", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			hasAccess(999, 999, 123, 456, 0o040, fuse.R_OK|fuse.W_OK)
		}
	})

	b.Run("multi-user-10", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			uid := uint32(1000 + (i % 10))
			hasAccess(uid, 100, 123, 456, 0o040, fuse.R_OK|fuse.W_OK)
		}
	})

	b.Run("multi-user-100", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			uid := uint32(1000 + (i % 100))
			hasAccess(uid, 100, 123, 456, 0o040, fuse.R_OK|fuse.W_OK)
		}
	})
}

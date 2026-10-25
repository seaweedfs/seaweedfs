package mount

import (
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

// BenchmarkCachedLookupSupplementaryGroupIDs measures cache hit performance.
func BenchmarkCachedLookupSupplementaryGroupIDs(b *testing.B) {
	oldLookupSupplementaryGroupIDs := lookupSupplementaryGroupIDs
	lookupSupplementaryGroupIDs = func(uid uint32) ([]string, error) {
		return []string{"456", "789", "1011"}, nil
	}
	clearSupplementaryGroupCache()
	defer func() {
		lookupSupplementaryGroupIDs = oldLookupSupplementaryGroupIDs
		clearSupplementaryGroupCache()
	}()

	cachedLookupSupplementaryGroupIDs(999)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cachedLookupSupplementaryGroupIDs(999)
	}
}

// BenchmarkHasAccessPermissionCheck measures permission checks with the cache.
// Simulates repeated permission checks for the same user against different files.
func BenchmarkHasAccessPermissionCheck(b *testing.B) {
	oldLookupSupplementaryGroupIDs := lookupSupplementaryGroupIDs
	lookupSupplementaryGroupIDs = func(uid uint32) ([]string, error) {
		return []string{"456", "789", "1011"}, nil
	}
	clearSupplementaryGroupCache()
	defer func() {
		lookupSupplementaryGroupIDs = oldLookupSupplementaryGroupIDs
		clearSupplementaryGroupCache()
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hasAccess(999, 999, 123, uint32(i%10), 0o040, fuse.R_OK|fuse.W_OK)
	}
}

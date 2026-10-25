package wdclient

import (
	"testing"
)

// TestInvalidateCacheValidFileId tests cache invalidation with a valid file ID
func TestInvalidateCacheValidFileId(t *testing.T) {
	// Create a simple vidMapClient (can use nil provider for this test)
	vc := &vidMapClient{
		vidMap: newVidMap("", DefaultVidMapCacheSize),
	}

	// Add some locations to the cache
	vid := uint32(456)
	vc.addLocation(vid, Location{Url: "http://server1:8080"})

	if _, found := vc.GetLocations(vid); !found {
		t.Fatal("Location should exist before invalidation")
	}

	// Call InvalidateCache with a properly formatted file ID
	fileId := "456,abcdef123456"
	vc.InvalidateCache(fileId)

	// Verify the locations were removed
	if _, found := vc.GetLocations(vid); found {
		t.Errorf("Expected locations for vid %d to be removed after InvalidateCache", vid)
	}
}

// TestInvalidateCacheInvalidFileId tests cache invalidation with invalid file IDs
func TestInvalidateCacheInvalidFileId(t *testing.T) {
	testCases := []struct {
		name   string
		fileId string
	}{
		{"empty file ID", ""},
		{"no comma separator", "12345"},
		{"non-numeric vid", "abc,defg"},
		{"negative vid", "-1,abcd"},
		{"oversized vid", "999999999999999999999,abcd"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			vc := &vidMapClient{
				vidMap: newVidMap("", DefaultVidMapCacheSize),
			}

			// Add a location to ensure the cache isn't empty
			vc.addLocation(1, Location{Url: "http://server:8080"})

			// This should not panic or cause errors
			vc.InvalidateCache(tc.fileId)

			// Verify the existing location is still there (not affected)
			if _, found := vc.GetLocations(1); !found {
				t.Errorf("InvalidateCache with invalid fileId '%s' should not affect other entries", tc.fileId)
			}
		})
	}
}

// TestInvalidateCacheWithHistory tests that invalidation also drops locations
// that were learned before the last reset and are still being retained.
func TestInvalidateCacheWithHistory(t *testing.T) {
	vid := uint32(789)

	vc := &vidMapClient{
		vidMap: newVidMap("", DefaultVidMapCacheSize),
	}

	// Learned from an earlier master, then kept across a reset
	vc.addLocation(vid, Location{Url: "http://server1:8080"})
	vc.resetVidMap()

	if _, found := vc.GetLocations(vid); !found {
		t.Fatal("Retained location should still be readable after a reset")
	}

	vc.InvalidateCache("789,xyz123")

	if _, found := vc.GetLocations(vid); found {
		t.Error("Expected retained location to be dropped by InvalidateCache")
	}
}

// TestDeleteVidDropsRetainedGenerations tests that deleteVid removes a volume
// no matter which generation it was last refreshed in.
func TestDeleteVidDropsRetainedGenerations(t *testing.T) {
	vid := uint32(999)

	vm := newVidMap("", DefaultVidMapCacheSize)
	vm.addLocation(vid, Location{Url: "http://server1:8080"})
	vm.addEcLocation(vid, Location{Url: "http://server2:8080"})
	vm.reset()
	vm.reset()

	if _, found := vm.GetLocations(vid); !found {
		t.Fatal("Volume should still be readable before deletion")
	}

	vm.deleteVid(vid)

	if _, found := vm.GetLocations(vid); found {
		t.Error("Expected volume to be gone after deleteVid")
	}
	if vm.hasVolumeServer("http://server1:8080") || vm.hasVolumeServer("http://server2:8080") {
		t.Error("Expected deleteVid to release the server references it held")
	}
}

// TestGetLocationsEmptyArrayNoFallback tests that a volume known to have no
// locations reports a miss instead of serving what an earlier generation held.
// Covers the bug where a volume pod restarts, the vid map holds an empty array,
// and lookups fall back to the stale locations from before the restart.
func TestGetLocationsEmptyArrayNoFallback(t *testing.T) {
	vm := newVidMap("", DefaultVidMapCacheSize)
	vid := uint32(10)
	oldLocation := Location{Url: "10.131.1.28:8081"}
	newLocation := Location{Url: "10.131.1.65:8081"}

	// Volume initially has its old location, which is then retained across a reset
	vm.addLocation(vid, oldLocation)
	vm.reset()

	locs, found := vm.GetLocations(vid)
	if !found || len(locs) != 1 || locs[0].Url != oldLocation.Url {
		t.Fatalf("Expected to find old location, got found=%v locs=%v", found, locs)
	}

	// Volume server restarts and the old location is deleted
	vm.deleteLocation(vid, oldLocation)

	locs, found = vm.GetLocations(vid)
	if found {
		t.Errorf("Expected found=false for empty location array, got found=true with locs=%v", locs)
	}
	if locs != nil {
		t.Errorf("Expected nil locations for empty array, got %v (should not serve stale history!)", locs)
	}

	// When the new location is added, it should be returned (not the stale one)
	vm.addLocation(vid, newLocation)
	locs, found = vm.GetLocations(vid)
	if !found || len(locs) != 1 {
		t.Fatalf("Expected to find new location, got found=%v locs=%v", found, locs)
	}
	if locs[0].Url != newLocation.Url {
		t.Errorf("Expected new location %s, got %s (got stale history!)", newLocation.Url, locs[0].Url)
	}
}

// TestGetLocationsRetainedAcrossReset tests that a volume the new master has
// not mentioned yet is still served from what the previous one told us.
func TestGetLocationsRetainedAcrossReset(t *testing.T) {
	vm := newVidMap("", DefaultVidMapCacheSize)
	vid := uint32(99)
	retained := Location{Url: "cache-server:8081"}

	vm.addLocation(vid, retained)
	vm.reset()

	locs, found := vm.GetLocations(vid)
	if !found || len(locs) != 1 {
		t.Fatalf("Expected to find retained location after reset, got found=%v locs=%v", found, locs)
	}
	if locs[0].Url != retained.Url {
		t.Errorf("Expected retained location %s, got %s", retained.Url, locs[0].Url)
	}
}

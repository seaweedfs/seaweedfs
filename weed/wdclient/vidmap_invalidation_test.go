package wdclient

import (
	"testing"
)

// TestInvalidateCacheValidFileId tests cache invalidation with a valid file ID
func TestInvalidateCacheValidFileId(t *testing.T) {
	// Create a simple vidMapClient (can use nil provider for this test)
	vc := &vidMapClient{
		vidMap:          newVidMap(""),
		vidMapCacheSize: 5,
	}

	// Add some locations to the cache
	vid := uint32(456)
	vc.vidMap.Lock()
	vc.vidMap.vid2Locations[vid] = []Location{{Url: "http://server1:8080"}}
	vc.vidMap.Unlock()

	// Verify location exists
	vc.vidMap.RLock()
	_, found := vc.vidMap.vid2Locations[vid]
	vc.vidMap.RUnlock()

	if !found {
		t.Fatal("Location should exist before invalidation")
	}

	// Call InvalidateCache with a properly formatted file ID
	fileId := "456,abcdef123456"
	vc.InvalidateCache(fileId)

	// Verify the locations were removed
	vc.vidMap.RLock()
	_, foundAfter := vc.vidMap.vid2Locations[vid]
	vc.vidMap.RUnlock()

	if foundAfter {
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
				vidMap:          newVidMap(""),
				vidMapCacheSize: 5,
			}

			// Add a location to ensure the cache isn't empty
			vc.vidMap.Lock()
			vc.vidMap.vid2Locations[1] = []Location{{Url: "http://server:8080"}}
			vc.vidMap.Unlock()

			// This should not panic or cause errors
			vc.InvalidateCache(tc.fileId)

			// Verify the existing location is still there (not affected)
			vc.vidMap.RLock()
			_, found := vc.vidMap.vid2Locations[1]
			vc.vidMap.RUnlock()

			if !found {
				t.Errorf("InvalidateCache with invalid fileId '%s' should not affect other entries", tc.fileId)
			}
		})
	}
}

// TestInvalidateCacheWithHistory tests that invalidation propagates through cache history
func TestInvalidateCacheWithHistory(t *testing.T) {
	vid := uint32(789)

	// Create first vidMap with the volume
	vm1 := newVidMap("")
	vm1.Lock()
	vm1.vid2Locations[vid] = []Location{{Url: "http://server1:8080"}}
	vm1.Unlock()

	// Create second vidMap with the cached first one
	vm2 := newVidMap("")
	vm2.cache.Store(vm1) // vm1 becomes the cache/history
	vm2.Lock()
	vm2.vid2Locations[vid] = []Location{{Url: "http://server2:8080"}}
	vm2.Unlock()

	// Create vidMapClient with vm2 as current
	vc := &vidMapClient{
		vidMap:          vm2,
		vidMapCacheSize: 5,
	}

	// Verify both have the vid before invalidation
	vm2.RLock()
	_, foundInCurrent := vm2.vid2Locations[vid]
	vm2.RUnlock()

	vm1.RLock()
	_, foundInCache := vm1.vid2Locations[vid]
	vm1.RUnlock()

	if !foundInCurrent || !foundInCache {
		t.Fatal("Both maps should have the vid before invalidation")
	}

	// Invalidate the cache
	fileId := "789,xyz123"
	vc.InvalidateCache(fileId)

	// Check that current map doesn't have the vid
	vm2.RLock()
	_, foundInCurrentAfter := vm2.vid2Locations[vid]
	vm2.RUnlock()

	if foundInCurrentAfter {
		t.Error("Expected vid to be removed from current vidMap after InvalidateCache")
	}

	// Check that cache doesn't have the vid either (recursive deletion)
	vm1.RLock()
	_, foundInCacheAfter := vm1.vid2Locations[vid]
	vm1.RUnlock()

	if foundInCacheAfter {
		t.Error("Expected vid to be removed from cached vidMap as well (recursive deletion)")
	}
}

// TestDeleteVidRecursion tests the deleteVid method removes from history chain
func TestDeleteVidRecursion(t *testing.T) {
	vid := uint32(999)

	// Create a chain: vm3 -> vm2 -> vm1
	vm1 := newVidMap("")
	vm1.Lock()
	vm1.vid2Locations[vid] = []Location{{Url: "http://server1:8080"}}
	vm1.Unlock()

	vm2 := newVidMap("")
	vm2.cache.Store(vm1)
	vm2.Lock()
	vm2.vid2Locations[vid] = []Location{{Url: "http://server2:8080"}}
	vm2.Unlock()

	vm3 := newVidMap("")
	vm3.cache.Store(vm2)
	vm3.Lock()
	vm3.vid2Locations[vid] = []Location{{Url: "http://server3:8080"}}
	vm3.Unlock()

	// Verify all have the vid
	vm3.RLock()
	_, found3 := vm3.vid2Locations[vid]
	vm3.RUnlock()

	vm2.RLock()
	_, found2 := vm2.vid2Locations[vid]
	vm2.RUnlock()

	vm1.RLock()
	_, found1 := vm1.vid2Locations[vid]
	vm1.RUnlock()

	if !found1 || !found2 || !found3 {
		t.Fatal("All maps should have the vid before deletion")
	}

	// Delete from vm3 (should cascade)
	vm3.deleteVid(vid)

	// Verify it's gone from all
	vm3.RLock()
	_, found3After := vm3.vid2Locations[vid]
	vm3.RUnlock()

	vm2.RLock()
	_, found2After := vm2.vid2Locations[vid]
	vm2.RUnlock()

	vm1.RLock()
	_, found1After := vm1.vid2Locations[vid]
	vm1.RUnlock()

	if found3After {
		t.Error("Expected vid to be removed from vm3")
	}
	if found2After {
		t.Error("Expected vid to be removed from vm2 (cascaded)")
	}
	if found1After {
		t.Error("Expected vid to be removed from vm1 (cascaded)")
	}
}

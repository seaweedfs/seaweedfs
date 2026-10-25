package wdclient

import (
	"runtime"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func urlsOf(locations []Location) []string {
	urls := make([]string, 0, len(locations))
	for _, loc := range locations {
		urls = append(urls, loc.Url)
	}
	return urls
}

// A volume that moved while we were talking to the previous master must answer
// with where it is now, not with both servers: the first write of a generation
// replaces the entry instead of merging into it.
func TestAddLocationReplacesEarlierGeneration(t *testing.T) {
	vm := newVidMap("", DefaultVidMapCacheSize)
	vid := uint32(3)
	movedFrom := Location{Url: "10.0.0.1:8080"}
	movedTo := Location{Url: "10.0.0.2:8080"}

	vm.addLocation(vid, movedFrom)
	vm.reset()
	vm.addLocation(vid, movedTo)

	locs, found := vm.GetLocations(vid)
	if !found || len(locs) != 1 || locs[0].Url != movedTo.Url {
		t.Fatalf("expected only %s after the move, got %v", movedTo.Url, urlsOf(locs))
	}
	if vm.hasVolumeServer(pb.ServerAddress(movedFrom.Url)) {
		t.Errorf("server %s should no longer be referenced after the volume moved", movedFrom.Url)
	}
	if !vm.hasVolumeServer(pb.ServerAddress(movedTo.Url)) {
		t.Errorf("server %s should be referenced after the volume moved", movedTo.Url)
	}
}

// Replicas reported within one generation accumulate, and a repeated report of
// the same server is not counted twice.
func TestAddLocationMergesWithinGeneration(t *testing.T) {
	vm := newVidMap("", DefaultVidMapCacheSize)
	vid := uint32(4)
	first := Location{Url: "10.0.0.1:8080"}
	second := Location{Url: "10.0.0.2:8080"}

	vm.addLocation(vid, first)
	vm.addLocation(vid, second)
	vm.addLocation(vid, first)

	locs, found := vm.GetLocations(vid)
	if !found || len(locs) != 2 {
		t.Fatalf("expected both replicas, got %v", urlsOf(locs))
	}

	// One delete must be enough to evict the server: the duplicate report of
	// `first` must not have taken a second reference.
	vm.deleteLocation(vid, first)
	if vm.hasVolumeServer(pb.ServerAddress(first.Url)) {
		t.Errorf("server %s should be evicted after a single delete", first.Url)
	}
}

// An entry survives exactly retainGenerations resets without being relearned.
func TestResetRetainsUntilWindowExpires(t *testing.T) {
	const retain = 2
	vm := newVidMap("", retain)
	vid := uint32(5)
	location := Location{Url: "10.0.0.1:8080"}
	vm.addLocation(vid, location)

	for i := 0; i < retain; i++ {
		vm.reset()
		if _, found := vm.GetLocations(vid); !found {
			t.Fatalf("location should still be retained after %d reset(s)", i+1)
		}
	}

	vm.reset()
	if _, found := vm.GetLocations(vid); found {
		t.Errorf("location should be dropped after %d resets", retain+1)
	}
	if vm.hasVolumeServer(pb.ServerAddress(location.Url)) {
		t.Errorf("expiry should release the server reference for %s", location.Url)
	}
	if len(vm.serverRefCount) != 0 {
		t.Errorf("expiry left %d dangling server refcounts", len(vm.serverRefCount))
	}
}

// Relearning an entry restarts its retention window.
func TestResetWindowRestartsOnRelearn(t *testing.T) {
	const retain = 2
	vm := newVidMap("", retain)
	vid := uint32(6)
	location := Location{Url: "10.0.0.1:8080"}

	for i := 0; i < retain+3; i++ {
		vm.addLocation(vid, location)
		vm.reset()
	}

	if _, found := vm.GetLocations(vid); !found {
		t.Error("a location relearned every generation must never expire")
	}
}

// A delete names one location; it must not make the rest of a stale entry look
// freshly learned and so outlive its retention window.
func TestDeleteLocationDoesNotRefreshGeneration(t *testing.T) {
	const retain = 2
	vm := newVidMap("", retain)
	vid := uint32(7)
	first := Location{Url: "10.0.0.1:8080"}
	second := Location{Url: "10.0.0.2:8080"}

	vm.addLocation(vid, first)
	vm.addLocation(vid, second)
	for i := 0; i < retain; i++ {
		vm.reset()
	}
	vm.deleteLocation(vid, first)

	locs, found := vm.GetLocations(vid)
	if !found || len(locs) != 1 || locs[0].Url != second.Url {
		t.Fatalf("expected %s to remain, got found=%v %v", second.Url, found, urlsOf(locs))
	}

	vm.reset()
	if _, found := vm.GetLocations(vid); found {
		t.Error("the surviving location was learned in the expired generation and should be dropped")
	}
}

// Encoding a volume must stop the regular copies a previous generation knew
// from answering for it, and decoding it must stop its shards from answering.
func TestNewestGenerationWinsAcrossEcTransition(t *testing.T) {
	regular := Location{Url: "10.0.0.1:8080"}
	ecShard := Location{Url: "10.0.0.2:8080"}

	t.Run("encoded", func(t *testing.T) {
		vm := newVidMap("", DefaultVidMapCacheSize)
		vm.addLocation(1, regular)
		vm.reset()
		vm.addEcLocation(1, ecShard)

		locs, found := vm.GetLocations(1)
		if !found || len(locs) != 1 || locs[0].Url != ecShard.Url {
			t.Fatalf("expected the freshly learned EC shard, got found=%v %v", found, urlsOf(locs))
		}
	})

	t.Run("decoded", func(t *testing.T) {
		vm := newVidMap("", DefaultVidMapCacheSize)
		vm.addEcLocation(1, ecShard)
		vm.reset()
		vm.addLocation(1, regular)

		locs, found := vm.GetLocations(1)
		if !found || len(locs) != 1 || locs[0].Url != regular.Url {
			t.Fatalf("expected the freshly learned regular copy, got found=%v %v", found, urlsOf(locs))
		}
	})

	t.Run("same generation prefers regular", func(t *testing.T) {
		vm := newVidMap("", DefaultVidMapCacheSize)
		vm.addEcLocation(1, ecShard)
		vm.addLocation(1, regular)

		locs, found := vm.GetLocations(1)
		if !found || len(locs) != 1 || locs[0].Url != regular.Url {
			t.Fatalf("expected the regular copy, got found=%v %v", found, urlsOf(locs))
		}
	})
}

// Losing the last location drops the entry: a client that never resets should
// not accumulate one empty entry per volume it has ever seen deleted.
func TestDeleteLastLocationDropsEntry(t *testing.T) {
	vm := newVidMap("", DefaultVidMapCacheSize)
	vid := uint32(12)
	location := Location{Url: "10.0.0.1:8080"}

	vm.addLocation(vid, location)
	vm.deleteLocation(vid, location)

	if _, found := vm.GetLocations(vid); found {
		t.Error("a volume with no locations left must not resolve")
	}
	if len(vm.vid2Locations) != 0 {
		t.Errorf("expected the emptied entry to be dropped, got %v", vm.vid2Locations)
	}
	if len(vm.serverRefCount) != 0 {
		t.Errorf("server refcounts leaked: %v", vm.serverRefCount)
	}
}

// EC locations follow the same rules as regular ones, and back a volume whose
// regular locations are gone.
func TestEcLocationsFollowGenerationRules(t *testing.T) {
	vm := newVidMap("", DefaultVidMapCacheSize)
	vid := uint32(8)
	regular := Location{Url: "10.0.0.1:8080"}
	ecShard := Location{Url: "10.0.0.2:8080"}
	movedEcShard := Location{Url: "10.0.0.3:8080"}

	vm.addLocation(vid, regular)
	vm.addEcLocation(vid, ecShard)

	locs, found := vm.GetLocations(vid)
	if !found || len(locs) != 1 || locs[0].Url != regular.Url {
		t.Fatalf("regular locations should win while they exist, got %v", urlsOf(locs))
	}

	// Regular copy goes away: the EC shards still serve the volume.
	vm.deleteLocation(vid, regular)
	locs, found = vm.GetLocations(vid)
	if !found || len(locs) != 1 || locs[0].Url != ecShard.Url {
		t.Fatalf("expected EC shard location, got found=%v %v", found, urlsOf(locs))
	}

	// EC shards move under a new master: no merging with the old report.
	vm.reset()
	vm.addEcLocation(vid, movedEcShard)
	locs, found = vm.GetLocations(vid)
	if !found || len(locs) != 1 || locs[0].Url != movedEcShard.Url {
		t.Fatalf("expected only the moved EC shard, got found=%v %v", found, urlsOf(locs))
	}
}

// Locations handed to a caller are never rewritten underneath it.
func TestGetLocationsResultIsStable(t *testing.T) {
	vm := newVidMap("", DefaultVidMapCacheSize)
	vid := uint32(9)
	first := Location{Url: "10.0.0.1:8080"}
	second := Location{Url: "10.0.0.2:8080"}

	vm.addLocation(vid, first)
	vm.addLocation(vid, second)

	locs, found := vm.GetLocations(vid)
	if !found || len(locs) != 2 {
		t.Fatalf("expected both replicas, got %v", urlsOf(locs))
	}
	snapshot := append([]Location(nil), locs...)

	vm.deleteLocation(vid, first)
	vm.addLocation(vid, Location{Url: "10.0.0.3:8080"})

	for i := range snapshot {
		if locs[i] != snapshot[i] {
			t.Errorf("location %d changed under the caller: %v became %v", i, snapshot[i], locs[i])
		}
	}
}

func TestNewVidMapDefaultsRetention(t *testing.T) {
	for _, retain := range []int{0, -1} {
		if got := newVidMap("", retain).retainGenerations; got != DefaultVidMapCacheSize {
			t.Errorf("newVidMap(%d) retention = %d, want %d", retain, got, DefaultVidMapCacheSize)
		}
	}
}

// Readers must never see a spurious miss for a volume that stays live, and the
// bookkeeping must survive concurrent writers. Run with -race.
func TestConcurrentResetAndUpdates(t *testing.T) {
	vm := newVidMap("", DefaultVidMapCacheSize)
	live := Location{Url: "10.0.0.1:8080"}
	churn := Location{Url: "10.0.0.2:8080"}
	const liveVid, churnVid = 1, 2

	vm.addLocation(liveVid, live)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if _, found := vm.GetLocations(liveVid); !found {
						t.Error("a volume that is relearned every generation must always resolve")
						return
					}
					vm.hasVolumeServer(pb.ServerAddress(live.Url))
					vm.GetLocationsClone(churnVid)
					runtime.Gosched()
				}
			}
		}()
	}

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				vm.addEcLocation(churnVid, churn)
				vm.deleteEcLocation(churnVid, churn)
				vm.deleteVid(churnVid)
			}
		}()
	}

	for i := 0; i < 300; i++ {
		vm.reset()
		vm.addLocation(liveVid, live)
	}
	close(stop)
	wg.Wait()

	if _, found := vm.GetLocations(liveVid); !found {
		t.Fatal("live volume lost after the churn")
	}
	vm.deleteVid(liveVid)
	if len(vm.serverRefCount) != 0 {
		t.Errorf("server refcounts leaked: %v", vm.serverRefCount)
	}
}

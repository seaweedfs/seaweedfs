package wdclient

import (
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// When the master reports a tier transition (local ↔ remote) for an existing
// replica on the same server URL, the cached DataInRemote must update. Otherwise
// reads keep preferring a remote-backed replica or skip a newly-restored local
// one. The server refcount must stay stable because the server key only depends
// on the URL/grpc port.
func TestAddLocationUpdatesDataInRemoteOnTransition(t *testing.T) {
	vm := newVidMap("", DefaultVidMapCacheSize)
	vid := uint32(11)
	server := Location{Url: "10.0.0.1:8080", DataCenter: "dc1", GrpcPort: 18080}

	// First seen as local.
	vm.addLocation(vid, server)
	locs, found := vm.GetLocations(vid)
	if !found || len(locs) != 1 || locs[0].DataInRemote {
		t.Fatalf("expected single local replica, got %+v", locs)
	}

	// Same URL flips to remote.
	tiered := server
	tiered.DataInRemote = true
	vm.addLocation(vid, tiered)

	locs, found = vm.GetLocations(vid)
	if !found {
		t.Fatalf("tier transition dropped the replica")
	}
	if len(locs) != 1 {
		t.Fatalf("tier transition should replace in place, got %d entries: %+v", len(locs), locs)
	}
	if !locs[0].DataInRemote {
		t.Errorf("DataInRemote did not flip to remote on tier-out: %+v", locs[0])
	}
	if locs[0].Url != server.Url || locs[0].DataCenter != server.DataCenter {
		t.Errorf("replaced entry lost non-DataInRemote fields: %+v", locs[0])
	}
	if !vm.hasVolumeServer(pb.ServerAddress(server.Url)) {
		t.Errorf("server ref should remain stable across DataInRemote flip")
	}

	// Restored locally: same URL, DataInRemote back to false.
	restored := server
	vm.addLocation(vid, restored)

	locs, found = vm.GetLocations(vid)
	if !found {
		t.Fatalf("restore transition dropped the replica")
	}
	if len(locs) != 1 {
		t.Fatalf("restore should replace in place, got %d entries: %+v", len(locs), locs)
	}
	if locs[0].DataInRemote {
		t.Errorf("DataInRemote did not flip back to local on restore: %+v", locs[0])
	}
}

// LookupVolumeServerUrl must reflect the latest DataInRemote so the local-first
// ordering picks up newly-restored local replicas on the next read.
func TestLookupVolumeServerUrlReflectsRemoteTransition(t *testing.T) {
	vm := newVidMap("dc1", DefaultVidMapCacheSize)
	vid := uint32(12)

	remote := Location{Url: "10.0.0.1:8080", DataCenter: "dc1", DataInRemote: true}
	vm.addLocation(vid, remote)

	urls, err := vm.LookupVolumeServerUrl("12")
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if len(urls) != 1 || urls[0] != "10.0.0.1:8080" {
		t.Fatalf("expected only the remote replica, got %v", urls)
	}

	// Tier restored: same URL, DataInRemote=false.
	local := Location{Url: "10.0.0.1:8080", DataCenter: "dc1"}
	vm.addLocation(vid, local)

	urls, err = vm.LookupVolumeServerUrl("12")
	if err != nil {
		t.Fatalf("lookup after restore failed: %v", err)
	}
	if len(urls) != 1 || urls[0] != "10.0.0.1:8080" {
		t.Fatalf("expected only the restored replica, got %v", urls)
	}
}

// A tier flip must not write into the slice a concurrent lookup is still
// walking: GetLocations hands out the entry's own slice and the caller reads
// it after the lock is dropped.
func TestTierFlipDoesNotRaceWithLookup(t *testing.T) {
	vm := newVidMap("dc1", DefaultVidMapCacheSize)
	vid := uint32(13)
	vm.addLocation(vid, Location{Url: "10.0.0.1:8080", DataCenter: "dc1"})
	vm.addLocation(vid, Location{Url: "10.0.0.2:8080", DataCenter: "dc1"})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			vm.addLocation(vid, Location{Url: "10.0.0.1:8080", DataCenter: "dc1", DataInRemote: i%2 == 0})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			if _, err := vm.LookupVolumeServerUrl("13"); err != nil {
				t.Errorf("lookup failed mid-flip: %v", err)
				return
			}
		}
	}()
	wg.Wait()
}

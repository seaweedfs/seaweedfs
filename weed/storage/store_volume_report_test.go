package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func reportingStore(t *testing.T, vids ...needle.VolumeId) *Store {
	t.Helper()
	store := newTestStore(t, 1)
	for _, vid := range vids {
		mountTestVolume(t, store.Locations[0], vid)
	}
	return store
}

// Until the master says it compares digests it may be one that reads a partial
// list as the whole truth, so it keeps getting the whole list.
func TestHeartbeatSendsFullListUntilTheMasterAccepts(t *testing.T) {
	store := reportingStore(t, 1, 2)
	store.ResetVolumeReporting()

	for i := 0; i < 3; i++ {
		heartbeat := store.CollectHeartbeat()
		if len(heartbeat.Volumes) != 2 {
			t.Fatalf("heartbeat %d carried %d volumes, want the full list of 2", i, len(heartbeat.Volumes))
		}
		if len(heartbeat.ChangedVolumes) != 0 {
			t.Fatalf("heartbeat %d sent changes to a master that never accepted them", i)
		}
	}
}

// The one that would be catastrophic: a heartbeat with nothing to report must
// not look like a server that has lost every volume.
func TestQuietHeartbeatDoesNotLookLikeAnEmptyServer(t *testing.T) {
	store := reportingStore(t, 1, 2)
	store.ResetVolumeReporting()
	store.AcceptVolumeChanges()
	store.CollectHeartbeat()

	heartbeat := store.CollectHeartbeat()
	if len(heartbeat.Volumes) != 0 || len(heartbeat.ChangedVolumes) != 0 {
		t.Fatalf("expected nothing to report, got %d volumes and %d changes",
			len(heartbeat.Volumes), len(heartbeat.ChangedVolumes))
	}
	if heartbeat.HasNoVolumes {
		t.Error("a heartbeat with nothing to report claimed the server holds no volumes")
	}
}

// The digest covers everything held, not just what was sent, or a master that
// applied the changes could never confirm it is current.
func TestQuietHeartbeatStillDigestsEverythingHeld(t *testing.T) {
	store := reportingStore(t, 1, 2)
	store.ResetVolumeReporting()
	full := store.CollectHeartbeat()
	store.AcceptVolumeChanges()

	quiet := store.CollectHeartbeat()
	if quiet.GetVolumeDigest() != full.GetVolumeDigest() {
		t.Errorf("digest changed with nothing to report: %d then %d",
			full.GetVolumeDigest(), quiet.GetVolumeDigest())
	}
}

func TestHeartbeatReportsOnlyWhatChanged(t *testing.T) {
	store := reportingStore(t, 1, 2)
	store.ResetVolumeReporting()
	store.AcceptVolumeChanges()
	store.CollectHeartbeat()

	mountTestVolume(t, store.Locations[0], 3)
	heartbeat := store.CollectHeartbeat()

	if len(heartbeat.Volumes) != 0 {
		t.Errorf("a changed-only heartbeat carried a full list of %d", len(heartbeat.Volumes))
	}
	if len(heartbeat.ChangedVolumes) != 1 || heartbeat.ChangedVolumes[0].Id != 3 {
		t.Errorf("expected only the new volume, got %v", heartbeat.ChangedVolumes)
	}
}

func TestHeartbeatReturnsToTheFullListOnRequest(t *testing.T) {
	store := reportingStore(t, 1, 2)
	store.ResetVolumeReporting()
	store.AcceptVolumeChanges()
	store.CollectHeartbeat()

	store.RequestFullVolumeList()
	heartbeat := store.CollectHeartbeat()
	if len(heartbeat.Volumes) != 2 {
		t.Errorf("after a resend request the heartbeat carried %d volumes, want 2", len(heartbeat.Volumes))
	}
	if len(heartbeat.ChangedVolumes) != 0 {
		t.Error("a resend should carry the list, not changes")
	}

	if next := store.CollectHeartbeat(); len(next.Volumes) != 0 {
		t.Errorf("the resend repeated instead of returning to changes: %d volumes", len(next.Volumes))
	}
}

// A reconnect may reach a master that knows nothing about this server.
func TestReconnectingSendsTheFullListAgain(t *testing.T) {
	store := reportingStore(t, 1, 2)
	store.ResetVolumeReporting()
	store.AcceptVolumeChanges()
	store.CollectHeartbeat()

	store.ResetVolumeReporting()
	heartbeat := store.CollectHeartbeat()
	if len(heartbeat.Volumes) != 2 {
		t.Errorf("after reconnecting the heartbeat carried %d volumes, want the full list of 2", len(heartbeat.Volumes))
	}
}

// A volume that goes and comes back has to be reported again, so forgetting it
// while it is away is what makes that work.
func TestRemountedVolumeIsReportedAgain(t *testing.T) {
	store := reportingStore(t, 1)
	store.ResetVolumeReporting()
	store.AcceptVolumeChanges()
	store.CollectHeartbeat()

	store.Locations[0].UnloadVolume(needle.VolumeId(1))
	store.CollectHeartbeat()

	mountTestVolume(t, store.Locations[0], 1)
	heartbeat := store.CollectHeartbeat()
	if len(heartbeat.ChangedVolumes) != 1 {
		t.Errorf("a remounted volume was not reported: %v", heartbeat.ChangedVolumes)
	}
}

// A request that lands while a heartbeat is being built asked about a later
// state than that heartbeat carries, so it must survive being committed over.
func TestFullListRequestDuringCollectionSurvives(t *testing.T) {
	store := reportingStore(t, 1, 2)
	store.ResetVolumeReporting()
	store.AcceptVolumeChanges()
	store.CollectHeartbeat()

	full, generation := store.volumeReport.begin()
	if full {
		t.Fatal("expected to be past the first full list")
	}
	store.RequestFullVolumeList()
	store.volumeReport.commit(map[volumeReportKey]reportedVolume{}, generation)

	if heartbeat := store.CollectHeartbeat(); len(heartbeat.Volumes) != 2 {
		t.Errorf("a resend request made during collection was lost: %d volumes sent", len(heartbeat.Volumes))
	}
}

// A delta says nothing through silence, so a volume that disappears must be
// named or the master keeps counting it until a digest mismatch buys a full
// list. This is what keeps a collection delete from leaving phantom volumes
// in the master's free-slot accounting.
func TestDepartedVolumeIsNamedInTheDelta(t *testing.T) {
	store := reportingStore(t, 1, 2)
	store.ResetVolumeReporting()
	store.AcceptVolumeChanges()
	store.CollectHeartbeat()

	store.Locations[0].UnloadVolume(needle.VolumeId(2))
	heartbeat := store.CollectHeartbeat()
	if len(heartbeat.DeletedVolumes) != 1 || heartbeat.DeletedVolumes[0].Id != 2 {
		t.Fatalf("expected volume 2 to be named as departed, got %v", heartbeat.DeletedVolumes)
	}

	// Named once: the next quiet heartbeat has nothing left to say about it.
	if next := store.CollectHeartbeat(); len(next.DeletedVolumes) != 0 {
		t.Errorf("a departure was reported twice: %v", next.DeletedVolumes)
	}
}

// A full list is already the whole truth; naming departures beside it would
// tell the master to remove what the list already excludes.
func TestFullListCarriesNoDepartures(t *testing.T) {
	store := reportingStore(t, 1, 2)
	store.ResetVolumeReporting()
	store.AcceptVolumeChanges()
	store.CollectHeartbeat()

	store.Locations[0].UnloadVolume(needle.VolumeId(2))
	store.RequestFullVolumeList()
	heartbeat := store.CollectHeartbeat()
	if len(heartbeat.Volumes) != 1 {
		t.Fatalf("full list carried %d volumes, want 1", len(heartbeat.Volumes))
	}
	if len(heartbeat.DeletedVolumes) != 0 {
		t.Errorf("a full list named departures: %v", heartbeat.DeletedVolumes)
	}
}

// A volume that moved disks is still held; naming it as departed would have
// the master unregister a volume the same heartbeat re-adds.
func TestMovedVolumeIsNotADeparture(t *testing.T) {
	store := newTestStore(t, 2)
	mountTestVolume(t, store.Locations[0], 1)
	store.ResetVolumeReporting()
	store.AcceptVolumeChanges()
	store.CollectHeartbeat()

	store.Locations[0].UnloadVolume(needle.VolumeId(1))
	// mountTestVolume leaves diskId at zero; the real mount path stamps the
	// destination disk, which is what makes the copy a different report key.
	moved, err := NewVolume(store.Locations[1].Directory, store.Locations[1].IdxDirectory, "", 1,
		NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	moved.diskId = 1
	store.Locations[1].SetVolume(needle.VolumeId(1), moved)
	heartbeat := store.CollectHeartbeat()
	if len(heartbeat.DeletedVolumes) != 0 {
		t.Errorf("a moved volume was named as departed: %v", heartbeat.DeletedVolumes)
	}
	if len(heartbeat.ChangedVolumes) != 1 || heartbeat.ChangedVolumes[0].Id != 1 {
		t.Errorf("the moved volume was not reported as changed: %v", heartbeat.ChangedVolumes)
	}
}

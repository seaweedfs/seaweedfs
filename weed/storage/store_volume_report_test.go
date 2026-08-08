package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
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

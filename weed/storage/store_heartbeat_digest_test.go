package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func mountTestVolume(t *testing.T, loc *DiskLocation, vid needle.VolumeId) {
	t.Helper()
	v, err := NewVolume(loc.Directory, loc.IdxDirectory, "", vid, NeedleMapInMemory,
		&super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	loc.SetVolume(vid, v)
}

// The digest has to cover exactly the volumes the heartbeat carries. A volume
// reported but left out of the digest, or the reverse, makes the master's
// comparison disagree forever.
func TestCollectHeartbeatDigestsExactlyWhatItReports(t *testing.T) {
	store := newTestStore(t, 2)
	mountTestVolume(t, store.Locations[0], 1)
	mountTestVolume(t, store.Locations[0], 2)
	mountTestVolume(t, store.Locations[1], 3)

	heartbeat := store.CollectHeartbeat()
	if heartbeat.VolumeDigest == nil {
		t.Fatal("heartbeat carried no digest")
	}
	if len(heartbeat.Volumes) != 3 {
		t.Fatalf("expected 3 volumes reported, got %d", len(heartbeat.Volumes))
	}

	var want uint64
	for _, m := range heartbeat.Volumes {
		vi, err := NewVolumeInfo(m)
		if err != nil {
			t.Fatal(err)
		}
		want ^= vi.ReportHash()
	}
	if got := heartbeat.GetVolumeDigest(); got != want {
		t.Errorf("digest %d does not cover the reported volumes (%d)", got, want)
	}
}

// A server holding nothing reports a digest of 0, which is why the field needs
// explicit presence: it must stay distinguishable from a server that computes
// no digest at all.
func TestCollectHeartbeatDigestsAnEmptyStore(t *testing.T) {
	store := newTestStore(t, 1)

	heartbeat := store.CollectHeartbeat()
	if heartbeat.VolumeDigest == nil {
		t.Fatal("an empty store still has to report a digest, or the master cannot tell it from an old server")
	}
	if got := heartbeat.GetVolumeDigest(); got != 0 {
		t.Errorf("expected an empty store to digest to 0, got %d", got)
	}
	if !heartbeat.HasNoVolumes {
		t.Error("expected has_no_volumes on an empty store")
	}
}

func TestCollectHeartbeatDigestFollowsVolumeChanges(t *testing.T) {
	store := newTestStore(t, 1)
	mountTestVolume(t, store.Locations[0], 1)
	first := store.CollectHeartbeat().GetVolumeDigest()

	if second := store.CollectHeartbeat().GetVolumeDigest(); second != first {
		t.Errorf("an unchanged store reported a different digest: %d then %d", first, second)
	}

	mountTestVolume(t, store.Locations[0], 2)
	if grown := store.CollectHeartbeat().GetVolumeDigest(); grown == first {
		t.Error("mounting a volume left the digest unchanged")
	}
}

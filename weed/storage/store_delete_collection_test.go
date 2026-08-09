package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

func mountCollectionVolume(t *testing.T, loc *DiskLocation, vid needle.VolumeId, collection string) {
	t.Helper()
	v, err := NewVolume(loc.Directory, loc.IdxDirectory, collection, vid, NeedleMapInMemory,
		&super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	loc.SetVolume(vid, v)
}

// A collection deleted between two heartbeats is the deletion no report would
// ever name: its volumes can be grown and destroyed without a single list
// mentioning them, so nothing but this would tell the master their slots came
// free.
func TestDeleteCollectionNamesTheVolumesItDestroyed(t *testing.T) {
	store := newTestStore(t, 1)
	mountCollectionVolume(t, store.Locations[0], 1, "books")
	mountCollectionVolume(t, store.Locations[0], 2, "books")
	mountCollectionVolume(t, store.Locations[0], 3, "movies")

	if err := store.DeleteCollection("books"); err != nil {
		t.Fatal(err)
	}

	named := make(map[uint32]string)
	for len(store.DeletedVolumesChan) > 0 {
		m := <-store.DeletedVolumesChan
		named[m.Id] = m.Collection
	}
	if len(named) != 2 || named[1] != "books" || named[2] != "books" {
		t.Fatalf("delete named %v, want volumes 1 and 2 of books", named)
	}

	if _, found := store.Locations[0].FindVolume(3); !found {
		t.Error("another collection's volume was destroyed")
	}
}

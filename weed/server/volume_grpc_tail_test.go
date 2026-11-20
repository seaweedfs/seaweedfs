package weed_server

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestWriteTailedNeedleSizeZeroRecords(t *testing.T) {
	store := storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{t.TempDir()}, []int32{10}, []util.MinFreeSpace{{}}, "", storage.NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType}, nil, 0, stats.DefaultDiskIOProbeConfig())
	t.Cleanup(store.Close)
	if err := store.AddVolume(1, "", storage.NeedleMapInMemory, "000", "", 0, needle.Version3, 0, types.HardDriveType, 0); err != nil {
		t.Fatalf("add volume: %v", err)
	}
	const live, deleted, fresh = types.NeedleId(1), types.NeedleId(2), types.NeedleId(3)
	for _, n := range []*needle.Needle{
		{Id: live, Cookie: 7, Data: []byte("live")},
		{Id: deleted, Cookie: 7, Data: []byte("deleted")},
	} {
		if err := writeTailedNeedle(store, 1, n); err != nil {
			t.Fatalf("write %d: %v", n.Id, err)
		}
	}
	if _, err := store.DeleteVolumeNeedle(1, &needle.Needle{Id: deleted}, false); err != nil {
		t.Fatalf("delete %d: %v", deleted, err)
	}
	v := store.GetVolume(1)
	fileCount, deletedCount := v.FileCount(), v.DeletedCount()

	for _, id := range []types.NeedleId{live, deleted, fresh} {
		if err := writeTailedNeedle(store, 1, &needle.Needle{Id: id, Cookie: 7}); err != nil {
			t.Fatalf("tail size-0 record for %d: %v", id, err)
		}
	}

	read := func(id types.NeedleId) (int, error) {
		return store.ReadVolumeNeedle(1, &needle.Needle{Id: id}, nil, nil)
	}
	if _, err := read(live); !errors.Is(err, storage.ErrorDeleted) {
		t.Errorf("size-0 record after a write: read err = %v, want deleted", err)
	}
	if _, err := read(deleted); !errors.Is(err, storage.ErrorDeleted) {
		t.Errorf("size-0 record after a delete: read err = %v, want deleted", err)
	}
	if size, err := read(fresh); err != nil || size != 0 {
		t.Errorf("size-0 record for a new key: read = %d, %v, want an empty needle", size, err)
	}
	if got, want := v.FileCount(), fileCount+1; got != want {
		t.Errorf("file count = %d, want %d", got, want)
	}
	if got, want := v.DeletedCount(), deletedCount+1; got != want {
		t.Errorf("deleted count = %d, want %d", got, want)
	}

	if err := writeTailedNeedle(store, 1, &needle.Needle{Id: fresh, Cookie: 7}); err != nil {
		t.Fatalf("second size-0 record for %d: %v", fresh, err)
	}
	if _, err := read(fresh); !errors.Is(err, storage.ErrorDeleted) {
		t.Errorf("second size-0 record: read err = %v, want deleted", err)
	}
}

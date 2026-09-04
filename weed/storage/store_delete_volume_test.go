package storage

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestDeleteVolumeErrorsAreInspectable(t *testing.T) {
	store := newTestStore(t, 1)
	mountCollectionVolume(t, store.Locations[0], 5, "")
	n := &needle.Needle{Id: types.Uint64ToNeedleId(1), Data: []byte("keep")}
	if _, err := store.WriteVolumeNeedle(5, n, false, false); err != nil {
		t.Fatal(err)
	}

	err := store.DeleteVolume(5, true, false)
	if !errors.Is(err, ErrVolumeNotEmpty) {
		t.Fatalf("only-empty delete of a non-empty volume = %v, want ErrVolumeNotEmpty", err)
	}
	if _, found := store.Locations[0].FindVolume(5); !found {
		t.Fatal("refused delete removed the volume")
	}

	err = store.DeleteVolume(99, false, false)
	if !errors.Is(err, ErrVolumeNotFound) {
		t.Fatalf("delete of an absent volume = %v, want ErrVolumeNotFound", err)
	}

	if err := store.DeleteVolume(5, false, false); err != nil {
		t.Fatalf("forced delete: %v", err)
	}
}

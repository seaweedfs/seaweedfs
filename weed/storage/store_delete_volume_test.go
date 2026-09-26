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

	err := store.DeleteVolume(5, true, false, false)
	if !errors.Is(err, ErrVolumeNotEmpty) {
		t.Fatalf("only-empty delete of a non-empty volume = %v, want ErrVolumeNotEmpty", err)
	}
	if _, found := store.Locations[0].FindVolume(5); !found {
		t.Fatal("refused delete removed the volume")
	}

	err = store.DeleteVolume(99, false, false, false)
	if !errors.Is(err, ErrVolumeNotFound) {
		t.Fatalf("delete of an absent volume = %v, want ErrVolumeNotFound", err)
	}

	if err := store.DeleteVolume(5, false, false, false); err != nil {
		t.Fatalf("forced delete: %v", err)
	}
}

func TestDeleteVolumeOnlyGarbage(t *testing.T) {
	store := newTestStore(t, 1)
	mountCollectionVolume(t, store.Locations[0], 5, "")
	n := &needle.Needle{Id: types.Uint64ToNeedleId(1), Data: []byte("keep")}
	if _, err := store.WriteVolumeNeedle(5, n, false, false); err != nil {
		t.Fatal(err)
	}

	// A live needle refuses, and the volume stays mounted.
	err := store.DeleteVolume(5, false, true, false)
	if !errors.Is(err, ErrVolumeNotEmpty) {
		t.Fatalf("only-garbage delete of a live volume = %v, want ErrVolumeNotEmpty", err)
	}
	if _, found := store.Locations[0].FindVolume(5); !found {
		t.Fatal("refused delete removed the volume")
	}

	if _, err := store.DeleteVolumeNeedle(5, n); err != nil {
		t.Fatal(err)
	}
	// The shell sends both flags so a pre-upgrade server still refuses; either
	// check passing must suffice here.
	if err := store.DeleteVolume(5, true, true, false); err != nil {
		t.Fatalf("delete of a fully deleted volume: %v", err)
	}
	if _, found := store.Locations[0].FindVolume(5); found {
		t.Fatal("fully deleted volume survived")
	}
}

package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func newVidStampStore(t *testing.T, dir string) *Store {
	t.Helper()
	return NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{dir}, []int32{100}, []util.MinFreeSpace{{}}, "",
		NeedleMapInMemory, []types.DiskType{types.HardDriveType}, nil, 3,
		stats.DefaultDiskIOProbeConfig())
}

func TestVidStampWrittenOnBoundIncrease(t *testing.T) {
	dir := t.TempDir()
	store := newVidStampStore(t, dir)
	defer store.Close()

	store.UpdateMaxVolumeIdSeen(needle.VolumeId(42))

	data, err := os.ReadFile(filepath.Join(dir, vidStampFileName))
	if err != nil {
		t.Fatalf("read stamp: %v", err)
	}
	stamp, err := readVidStamp(dir)
	if err != nil {
		t.Fatalf("parse stamp: %v", err)
	}
	if stamp.MaxVolumeId != 42 {
		t.Fatalf("stamp max = %d, want 42", stamp.MaxVolumeId)
	}
	if len(data) == 0 {
		t.Fatal("empty stamp")
	}
}

func TestVidStampSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	store := newVidStampStore(t, dir)
	store.UpdateMaxVolumeIdSeen(needle.VolumeId(42))
	store.UpdateLastSeenTopologyId("cluster-a")
	store.Close()

	restarted := newVidStampStore(t, dir)
	defer restarted.Close()
	if got := restarted.MaxVolumeIdSeen(); got != needle.VolumeId(42) {
		t.Fatalf("restored bound = %d, want 42", got)
	}
	if got := restarted.LastSeenTopologyId(); got != "cluster-a" {
		t.Fatalf("restored topology id = %q, want cluster-a", got)
	}
}

func TestVidStampMonotonic(t *testing.T) {
	dir := t.TempDir()
	store := newVidStampStore(t, dir)
	defer store.Close()

	store.UpdateMaxVolumeIdSeen(needle.VolumeId(10))
	store.UpdateMaxVolumeIdSeen(needle.VolumeId(5))
	if got := store.MaxVolumeIdSeen(); got != needle.VolumeId(10) {
		t.Fatalf("bound regressed to %d, want 10", got)
	}
}

func TestVidStampBootstrapFromVolumes(t *testing.T) {
	dir := t.TempDir()

	store := newVidStampStore(t, dir)
	if err := store.AddVolume(17, "", NeedleMapInMemory, "000", "", 0, needle.GetCurrentVersion(), 0, types.HardDriveType, 0); err != nil {
		t.Fatalf("add volume: %v", err)
	}
	store.Close()

	// upgrading a pre-stamp dir: volumes exist but no stamp yet
	if err := os.Remove(filepath.Join(dir, vidStampFileName)); err != nil {
		t.Fatalf("remove stamp: %v", err)
	}

	restarted := newVidStampStore(t, dir)
	defer restarted.Close()
	if got := restarted.MaxVolumeIdSeen(); got != needle.VolumeId(17) {
		t.Fatalf("bootstrapped bound = %d, want 17", got)
	}
}

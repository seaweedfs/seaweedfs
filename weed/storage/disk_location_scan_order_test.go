package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// A volume has both an .idx and a .vif, and the scan hands over whichever the
// filesystem returned first. Which one it was must not decide whether the
// volume loads -- it used to, through the .vif guard, and only a sorted
// listing kept the .idx in front.
func TestScanOrderDoesNotDecideWhetherAVolumeLoads(t *testing.T) {
	loaded := make(map[string]bool)
	for _, ext := range []string{".idx", ".vif"} {
		store := newTestStore(t, 1)
		location := store.Locations[0]
		mountTestVolume(t, location, 9, "")
		location.UnloadVolume(needle.VolumeId(9))
		// A .dat with data in it and a stray .ecx with no shards beside it: an
		// interrupted encode, which the EC validation below the guard reclaims.
		if err := os.Truncate(filepath.Join(location.Directory, "9.dat"), 4096); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(location.Directory, "9.ecx"), make([]byte, 20), 0644); err != nil {
			t.Fatal(err)
		}

		location.loadExistingVolume("9"+ext, NeedleMapInMemory, true, 0, 0, false)
		_, found := location.FindVolume(needle.VolumeId(9))
		loaded[ext] = found
	}

	if loaded[".idx"] != loaded[".vif"] {
		t.Errorf("scanning the .idx loaded=%v but scanning the .vif loaded=%v", loaded[".idx"], loaded[".vif"])
	}
	if !loaded[".idx"] {
		t.Error("a volume with an .idx beside its .vif did not load")
	}
}

// A volume mid-copy must not be picked up by a reload: the copy clears and
// rewrites its files, so registering now pairs a mounted volume with data
// that is still changing, and the .note cleanup would unlink the copy's
// own output.
func TestLoadExistingVolumeSkipsACopyInFlight(t *testing.T) {
	store := newTestStore(t, 1)
	location := store.Locations[0]
	mountTestVolume(t, location, 9, "")
	location.UnloadVolume(needle.VolumeId(9))

	location.loadGuard = func(vid needle.VolumeId) (func(), bool) {
		return func() {}, true
	}
	location.loadExistingVolume("9.idx", NeedleMapInMemory, true, 0, 0, false)
	if _, found := location.FindVolume(needle.VolumeId(9)); found {
		t.Fatal("a volume the guard skips was loaded")
	}

	// A release the guard hands out is held through the whole load and only
	// dropped on return.
	releaseRan := false
	location.loadGuard = func(vid needle.VolumeId) (func(), bool) {
		return func() { releaseRan = true }, false
	}
	location.loadExistingVolume("9.idx", NeedleMapInMemory, true, 0, 0, false)
	if _, found := location.FindVolume(needle.VolumeId(9)); !found {
		t.Fatal("a clear guard pass did not load the volume")
	}
	if !releaseRan {
		t.Fatal("the guard release was not called when the load returned")
	}
}

// The claim holder's own mount must not consult the guard: VolumeCopy mounts
// while its claim still stands, and VolumeMount/VolumeConfigure hold the
// guard's lock through the mount. The skip is for scan-driven loads; an
// explicit LoadVolume is serialized by the caller's own hold.
func TestLoadVolumeIgnoresTheGuardForTheHoldersOwnMount(t *testing.T) {
	store := newTestStore(t, 1)
	location := store.Locations[0]
	mountTestVolume(t, location, 9, "")
	location.UnloadVolume(needle.VolumeId(9))

	location.loadGuard = func(vid needle.VolumeId) (func(), bool) {
		return func() {}, true
	}
	if !location.LoadVolume(0, needle.VolumeId(9), NeedleMapInMemory, nil, true) {
		t.Fatal("the claim holder's own mount was skipped by its own guard")
	}
	if _, found := location.FindVolume(needle.VolumeId(9)); !found {
		t.Fatal("explicit mount did not register the volume")
	}

	location.UnloadVolume(needle.VolumeId(9))
	if location.LoadVolume(0, needle.VolumeId(9), NeedleMapInMemory, nil, false) {
		t.Fatal("an unguarded mount slipped past the claim's skip")
	}
}

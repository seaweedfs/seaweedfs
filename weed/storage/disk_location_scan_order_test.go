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

		location.loadExistingVolume("9"+ext, NeedleMapInMemory, true, 0, 0)
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

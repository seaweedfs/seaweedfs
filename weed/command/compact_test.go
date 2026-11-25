package command

import (
	"bytes"
	"os"
	"slices"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func runCompactOnFixTestVolume(t *testing.T, dir, method string) {
	t.Helper()
	savedDir, savedCollection, savedId, savedMethod, savedStatus := *compactVolumePath, *compactVolumeCollection, *compactVolumeId, *compactMethod, commandExitStatus
	t.Cleanup(func() {
		*compactVolumePath, *compactVolumeCollection, *compactVolumeId, *compactMethod, commandExitStatus = savedDir, savedCollection, savedId, savedMethod, savedStatus
	})
	*compactVolumePath, *compactVolumeCollection, *compactVolumeId, *compactMethod, commandExitStatus = dir, "fix", 7, method, 0
	runCompact(cmdCompact, nil)
}

// compactedKeys returns the keys in the .cpx, and those whose .cpd record
// holds the data writeFixTestVolume wrote for them.
func compactedKeys(t *testing.T, base string) (indexed, intact []uint64) {
	t.Helper()
	datFile, err := os.Open(base + ".cpd")
	if err != nil {
		t.Fatal(err)
	}
	dat := backend.NewDiskFile(datFile)
	defer dat.Close()
	idxFile, err := os.Open(base + ".cpx")
	if err != nil {
		t.Fatal(err)
	}
	defer idxFile.Close()
	if err := idx.WalkIndexFile(idxFile, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		indexed = append(indexed, uint64(key))
		n := new(needle.Needle)
		if n.ReadData(dat, offset.ToActualOffset(), size, needle.GetCurrentVersion()) == nil && bytes.Equal(n.Data, bytes.Repeat([]byte{byte(key)}, 1000*int(key))) {
			intact = append(intact, uint64(key))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(indexed)
	slices.Sort(intact)
	return indexed, intact
}

// A damaged header in the middle of the .dat: -method data copies every live
// record past it and drops the damaged one, where -method index copies it as is.
func TestCompactDataMethodSkipsUnreadableRecord(t *testing.T) {
	sizeAt := int64(types.NeedleIdSize + types.CookieSize)
	for _, tc := range []struct {
		name   string
		at     int64
		damage []byte
	}{
		{"garbage header", 0, bytes.Repeat([]byte{0xa5}, 16)},
		{"size past the end", sizeAt, []byte{0x7f, 0xff, 0xff, 0xf0}},
		{"size into the next record", sizeAt, []byte{0x00, 0x00, 0x00, 0x10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, method := range []string{"data", "index"} {
				dir := t.TempDir()
				base, offsets := writeFixTestVolume(t, dir, 5)
				doFixOneVolume(dir, "fix_7", "fix", 7, true)
				overwriteAt(t, base+".dat", offsets[2]+tc.at, tc.damage)

				runCompactOnFixTestVolume(t, dir, method)

				if CommandExitStatus() != 0 {
					t.Fatalf("-method %s: exit status %d", method, CommandExitStatus())
				}
				indexed, intact := compactedKeys(t, base)
				if !slices.Equal(intact, []uint64{1, 2, 4, 5}) {
					t.Errorf("-method %s: intact keys %v, want [1 2 4 5]", method, intact)
				}
				if method == "data" && !slices.Equal(indexed, intact) {
					t.Errorf("-method data: indexed keys %v, want only the intact ones", indexed)
				}
			}
		})
	}
}

func TestCompactFailureLeavesNoOutput(t *testing.T) {
	dir := t.TempDir()
	base, _ := writeFixTestVolume(t, dir, 5)
	doFixOneVolume(dir, "fix_7", "fix", 7, true)
	if err := os.Mkdir(base+".cpd", 0755); err != nil {
		t.Fatal(err)
	}

	runCompactOnFixTestVolume(t, dir, "data")

	if CommandExitStatus() == 0 {
		t.Error("expected a non-zero exit status")
	}
	for _, ext := range []string{".cpd", ".cpx"} {
		if _, err := os.Stat(base + ext); !os.IsNotExist(err) {
			t.Errorf("%s left behind: %v", ext, err)
		}
	}
}

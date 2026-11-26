package command

import (
	"bytes"
	"os"
	"slices"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
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
// holds the data writeFixTestVolume wrote for them. The .cpd must hold no
// record the .cpx does not list.
func compactedKeys(t *testing.T, base string) (indexed, intact []uint64) {
	t.Helper()
	datFile, err := os.Open(base + ".cpd")
	if err != nil {
		t.Fatal(err)
	}
	dat := backend.NewDiskFile(datFile)
	defer dat.Close()
	sb, err := super_block.ReadSuperBlock(dat)
	if err != nil {
		t.Fatal(err)
	}
	idxFile, err := os.Open(base + ".cpx")
	if err != nil {
		t.Fatal(err)
	}
	defer idxFile.Close()
	listed := int64(sb.BlockSize())
	if err := idx.WalkIndexFile(idxFile, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		indexed = append(indexed, uint64(key))
		listed += needle.GetActualSize(size, sb.Version)
		n := new(needle.Needle)
		if n.ReadData(dat, offset.ToActualOffset(), size, sb.Version) == nil && bytes.Equal(n.Data, bytes.Repeat([]byte{byte(key)}, 1000*int(key))) {
			intact = append(intact, uint64(key))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if datSize, _, err := dat.GetStat(); err != nil || datSize != listed {
		t.Errorf(".cpd holds %d bytes, its index lists %d: %v", datSize, listed, err)
	}
	slices.Sort(indexed)
	slices.Sort(intact)
	return indexed, intact
}

func damageDat(at int64, b []byte) func(t *testing.T, base string, offsets []int64) {
	return func(t *testing.T, base string, offsets []int64) {
		overwriteAt(t, base+".dat", offsets[2]+at, b)
	}
}

// Damage to the middle record of five: both methods drop it and keep the
// others. -method data trusts an intact record over a wrong index size.
func TestCompactDataMethodSkipsUnreadableRecord(t *testing.T) {
	sizeAt := int64(types.NeedleIdSize + types.CookieSize)
	others := []uint64{1, 2, 4, 5}
	for _, tc := range []struct {
		name      string
		corrupt   func(t *testing.T, base string, offsets []int64)
		wantData  []uint64
		wantIndex []uint64
	}{
		{"garbage header", damageDat(0, bytes.Repeat([]byte{0xa5}, 16)), others, others},
		{"size past the end", damageDat(sizeAt, []byte{0x7f, 0xff, 0xff, 0xf0}), others, others},
		{"size into the next record", damageDat(sizeAt, []byte{0x00, 0x00, 0x00, 0x10}), others, others},
		{"size onto a later record", func(t *testing.T, base string, offsets []int64) {
			span := offsets[4] - offsets[2]
			size := types.Size(span)
			for needle.GetActualSize(size, needle.GetCurrentVersion()) > span {
				size--
			}
			b := make([]byte, types.SizeSize)
			types.SizeToBytes(b, size)
			overwriteAt(t, base+".dat", offsets[2]+sizeAt, b)
		}, others, others},
		{"data", damageDat(types.NeedleHeaderSize+10, []byte{0xee}), others, others},
		{"index size", func(t *testing.T, base string, _ []int64) {
			row := int64(slices.Index(indexedKeys(t, base+".idx"), 3))
			overwriteAt(t, base+".idx", row*types.NeedleMapEntrySize+types.NeedleIdSize+types.OffsetSize, []byte{0x00, 0x00, 0x00, 0x01})
		}, []uint64{1, 2, 3, 4, 5}, others},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for method, want := range map[string][]uint64{"data": tc.wantData, "index": tc.wantIndex} {
				dir := t.TempDir()
				base, offsets := writeFixTestVolume(t, dir, 5)
				doFixOneVolume(dir, "fix_7", "fix", 7, true)
				tc.corrupt(t, base, offsets)

				runCompactOnFixTestVolume(t, dir, method)

				if CommandExitStatus() != 0 {
					t.Fatalf("-method %s: exit status %d", method, CommandExitStatus())
				}
				indexed, intact := compactedKeys(t, base)
				if !slices.Equal(indexed, want) || !slices.Equal(intact, want) {
					t.Errorf("-method %s: indexed keys %v, intact keys %v, want %v", method, indexed, intact, want)
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

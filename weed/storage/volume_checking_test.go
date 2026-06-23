package storage

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestScrubVolumeData(t *testing.T) {
	testCases := []struct {
		name      string
		dataPath  string
		indexPath string
		version   needle.Version
		want      int64
		wantErrs  []error
	}{
		{
			name:      "healthy volume",
			dataPath:  "./test_files/healthy_volume.dat",
			indexPath: "./test_files/healthy_volume.idx",
			version:   needle.Version3,
			want:      27,
			wantErrs:  []error{},
		},
		{
			name:      "zero-size volume without index",
			dataPath:  "./test_files/empty_volume.dat",
			indexPath: "./test_files/empty_volume.idx",
			version:   needle.Version3,
			want:      0,
			wantErrs:  []error{},
		},
		{
			name:      "zero-size volume with index",
			dataPath:  "./test_files/empty_volume.dat",
			indexPath: "./test_files/healthy_volume.idx",
			version:   needle.Version3,
			want:      27,
			wantErrs: []error{
				fmt.Errorf("failed to read needle 3 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 4 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 8 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 9 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 11 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 18 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 20 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 21 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 25 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 29 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 31 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 33 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 35 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 39 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 45 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 46 on volume 0: EOF"),
				fmt.Errorf("failed to read needle 48 on volume 0: EOF"),
				fmt.Errorf("data file for volume 0 is smaller (8) than the 27 needles it contains (942856)"),
			},
		},
		{
			name:      "volume without index",
			dataPath:  "./test_files/healthy_volume.dat",
			indexPath: "./test_files/empty_volume.idx",
			version:   needle.Version3,
			want:      0,
			wantErrs: []error{
				fmt.Errorf("data file size for volume 0 (942856) doesn't match the size for 0 needles read (8)"),
			},
		},
		{
			name:      "bitrot volume",
			dataPath:  "./test_files/bitrot_volume.dat",
			indexPath: "./test_files/bitrot_volume.idx",
			version:   needle.Version3,
			want:      27,
			wantErrs: []error{
				fmt.Errorf("failed to read needle 3 on volume 0: invalid CRC for needle 3 (got 0b243a0d, want 4af853fb), data on disk corrupted: needle data corrupted"),
				fmt.Errorf("failed to read needle 48 on volume 0: invalid CRC for needle 30 (got 3c40e8d5, want 5077fea1), data on disk corrupted: needle data corrupted"),
				fmt.Errorf("data file size for volume 0 (942864) doesn't match the size for 27 needles read (942856)"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			datFile, err := os.OpenFile(tc.dataPath, os.O_RDONLY, 0)
			if err != nil {
				t.Fatalf("failed to open data file: %v", err)
			}
			defer datFile.Close()

			idxFile, err := os.OpenFile(tc.indexPath, os.O_RDONLY, 0)
			if err != nil {
				t.Fatalf("failed to open index file: %v", err)
			}
			defer idxFile.Close()

			idxStat, err := idxFile.Stat()
			if err != nil {
				t.Fatalf("failed to stat index file: %v", err)
			}

			v := Volume{
				DataBackend: backend.NewDiskFile(datFile),
				volumeInfo: &volume_server_pb.VolumeInfo{
					Version: uint32(tc.version),
				},
			}

			got, gotErrs := v.scrubVolumeData(idxFile, idxStat.Size())

			if got != tc.want {
				t.Errorf("expected %d files processed, got %d", tc.want, got)
			}
			if !reflect.DeepEqual(gotErrs, tc.wantErrs) {
				t.Errorf("expected errors %v, got %v", tc.wantErrs, gotErrs)
			}
		})
	}
}

// TestCheckVolumeDataIntegrityWithDeletionTombstone guards the size argument
// passed to ReadData when verifying a trailing deletion tombstone. The .idx
// entry carries TombstoneFileSize (-1) but the appended tombstone needle in
// .dat carries Size=0. Forwarding the .idx size into ReadData fails the size
// check, activates the 32GB 4-byte-offset wrap-around retry, and surfaces EOF;
// CheckVolumeDataIntegrity then treats the volume as corrupt and the loader
// marks every such volume read-only on startup.
func TestCheckVolumeDataIntegrityWithDeletionTombstone(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	for i := 1; i <= 3; i++ {
		n := newRandomNeedle(uint64(i))
		if _, _, _, err := v.writeNeedle2(n, true, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}
	// Delete a live needle so a tombstone (Size=0 on disk, -1 in idx) is the
	// last entry the integrity check examines.
	if _, err := v.doDeleteRequest(newEmptyNeedle(2)); err != nil {
		t.Fatalf("delete needle: %v", err)
	}
	if err := v.DataBackend.Sync(); err != nil {
		t.Fatalf("sync .dat: %v", err)
	}
	if err := v.nm.Sync(); err != nil {
		t.Fatalf("sync .idx: %v", err)
	}

	idxFile, err := os.OpenFile(v.FileName(".idx"), os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("open .idx: %v", err)
	}
	defer idxFile.Close()

	lastAppendAtNs, err := CheckVolumeDataIntegrity(v, idxFile)
	if err != nil {
		t.Fatalf("CheckVolumeDataIntegrity should succeed with a trailing deletion tombstone: %v", err)
	}
	// V3 tombstones still record AppendAtNs; a zero return means ReadData
	// bailed out before populating the needle and would mask future regressions
	// that quietly skip the tombstone body.
	if lastAppendAtNs == 0 {
		t.Errorf("expected non-zero lastAppendAtNs for V3 deletion tombstone, got 0")
	}
}

// TestCheckVolumeDataIntegritySortedIndex reproduces issue #9688: after the
// .idx is rebuilt sorted by key — what weed fix and other rebuilds emitted via
// needle_map.AscendingVisit — the last file-position entry is the highest-key
// needle, not the needle at the .dat tail. The integrity check compared that
// mid-file needle's tail against the full .dat size, so fileSize > fileTailOffset
// tripped and flipped the volume read-only on every load even though the .dat
// was intact. The check must locate and verify the needle at the .dat tail
// (the maximum offset) regardless of .idx ordering.
func TestCheckVolumeDataIntegritySortedIndex(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	// Write keys in descending order so the highest key lands at the lowest
	// .dat offset; the last-written needle (lowest key) sits at the tail.
	for _, id := range []uint64{30, 20, 10} {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(id), true, false); err != nil {
			t.Fatalf("write needle %d: %v", id, err)
		}
	}
	if err := v.DataBackend.Sync(); err != nil {
		t.Fatalf("sync .dat: %v", err)
	}
	if err := v.nm.Sync(); err != nil {
		t.Fatalf("sync .idx: %v", err)
	}

	// Rebuild the .idx sorted ascending by key, the shape weed fix's SaveToIdx
	// emitted. The last entry is now key 30, which is physically first in the
	// .dat — far from the .dat tail.
	rewriteIdxSortedByKey(t, v.FileName(".idx"))

	idxFile, err := os.OpenFile(v.FileName(".idx"), os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("open .idx: %v", err)
	}
	defer idxFile.Close()

	if _, err := CheckVolumeDataIntegrity(v, idxFile); err != nil {
		t.Fatalf("CheckVolumeDataIntegrity should pass for a key-sorted .idx: %v", err)
	}
}

// TestCheckVolumeDataIntegrityVerifiesDeletionTail ensures a deletion tombstone
// at the .dat tail is verified rather than skipped: unindexed bytes appended
// past it must flip the volume read-only, not be silently tolerated.
func TestCheckVolumeDataIntegrityVerifiesDeletionTail(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	for i := 1; i <= 3; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(uint64(i)), true, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}
	// Delete a needle so a tombstone is the last (highest-offset) record.
	if _, err := v.doDeleteRequest(newEmptyNeedle(2)); err != nil {
		t.Fatalf("delete needle: %v", err)
	}
	// Append unindexed bytes past the tombstone tail.
	datSize, _, err := v.DataBackend.GetStat()
	if err != nil {
		t.Fatalf("stat .dat: %v", err)
	}
	if _, err := v.DataBackend.WriteAt(make([]byte, 64), datSize); err != nil {
		t.Fatalf("append trailing bytes: %v", err)
	}
	if err := v.DataBackend.Sync(); err != nil {
		t.Fatalf("sync .dat: %v", err)
	}
	if err := v.nm.Sync(); err != nil {
		t.Fatalf("sync .idx: %v", err)
	}

	idxFile, err := os.OpenFile(v.FileName(".idx"), os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("open .idx: %v", err)
	}
	defer idxFile.Close()

	if _, err := CheckVolumeDataIntegrity(v, idxFile); err == nil {
		t.Fatal("CheckVolumeDataIntegrity should detect bytes past a deletion-tombstone tail")
	}
}

// TestVolumeLoadStaysWritableWithKeySortedIndex reproduces issue #9688 end to
// end through the real volume load path: a volume whose .idx is sorted by key
// (the on-disk state weed fix left behind) must reload writable, not flip to
// read-only — and a live needle must still be readable afterward.
func TestVolumeLoadStaysWritableWithKeySortedIndex(t *testing.T) {
	dir := t.TempDir()

	// Build a volume: keys written descending so the highest key (30) lands at
	// the lowest .dat offset, then delete one so a tombstone sits at the tail —
	// the layout in the bug report (highest-key needle mid-file, tombstone last).
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	wanted := newRandomNeedle(10)
	for _, n := range []*needle.Needle{newRandomNeedle(30), newRandomNeedle(20), wanted} {
		if _, _, _, err := v.writeNeedle2(n, true, false); err != nil {
			t.Fatalf("write needle %d: %v", n.Id, err)
		}
	}
	if _, err := v.doDeleteRequest(newEmptyNeedle(20)); err != nil {
		t.Fatalf("delete needle: %v", err)
	}
	idxName := v.FileName(".idx")
	v.Close()

	// Rewrite the .idx sorted by key — the shape old weed fix produced.
	rewriteIdxSortedByKey(t, idxName)

	// Reload through the real load path; the integrity check must not flip it.
	reloaded, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	defer reloaded.Close()
	if reloaded.noWriteOrDelete {
		t.Fatal("volume flipped read-only after reload with a key-sorted .idx (issue #9688)")
	}

	// The surviving needle is still readable, and new writes still succeed.
	rn := &needle.Needle{Id: wanted.Id}
	if _, err := reloaded.readNeedle(rn, nil, nil); err != nil {
		t.Fatalf("read surviving needle after reload: %v", err)
	}
	if _, _, _, err := reloaded.writeNeedle2(newRandomNeedle(40), true, false); err != nil {
		t.Fatalf("write after reload should succeed: %v", err)
	}
}

// rewriteIdxSortedByKey rewrites the .idx in ascending key order — the on-disk
// shape weed fix's SaveToIdx emitted (needle_map.AscendingVisit) — so a test can
// exercise an .idx whose file order differs from .dat append order.
func rewriteIdxSortedByKey(t *testing.T, idxPath string) {
	t.Helper()
	f, err := os.OpenFile(idxPath, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("open .idx for read: %v", err)
	}
	var entries []needle_map.NeedleValue
	if walkErr := idx.WalkIndexFile(f, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		entries = append(entries, needle_map.NeedleValue{Key: key, Offset: offset, Size: size})
		return nil
	}); walkErr != nil {
		f.Close()
		t.Fatalf("walk .idx: %v", walkErr)
	}
	f.Close()

	// Sort by key, then by offset, so a key's live entry precedes its tombstone
	// — the order weed fix's AscendingVisit emitted.
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Key != entries[j].Key {
			return entries[i].Key < entries[j].Key
		}
		return entries[i].Offset.ToActualOffset() < entries[j].Offset.ToActualOffset()
	})

	out, err := os.OpenFile(idxPath, os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatalf("open .idx for rewrite: %v", err)
	}
	defer out.Close()
	for _, e := range entries {
		if _, err := out.Write(e.ToBytes()); err != nil {
			t.Fatalf("write idx entry: %v", err)
		}
	}
}

// TestMaxNeedleEnd ensures the needle map's MaxNeedleEnd accumulator lets
// volume.load() detect an .idx that references bytes past the end of the .dat
// — the deeper-than-tail corruption shape from issue #8928 that the existing
// last-10-entries scan cannot see. The check is populated by the load walk
// and read by volume.load() to flip the volume read-only.
func TestMaxNeedleEnd(t *testing.T) {
	dir := t.TempDir()

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()

	// A handful of healthy needles establishes a baseline .dat/.idx.
	for i := 1; i <= 4; i++ {
		n := newRandomNeedle(uint64(i))
		if _, _, _, err := v.writeNeedle2(n, true, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}
	if err := v.DataBackend.Sync(); err != nil {
		t.Fatalf("sync .dat: %v", err)
	}
	if err := v.nm.Sync(); err != nil {
		t.Fatalf("sync .idx: %v", err)
	}

	datSize, _, err := v.DataBackend.GetStat()
	if err != nil {
		t.Fatalf("stat .dat: %v", err)
	}

	// Sanity: a fresh load over the healthy .idx puts MaxNeedleEnd inside
	// the .dat.
	idxFile, err := os.OpenFile(v.FileName(".idx"), os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("open .idx: %v", err)
	}
	healthyNm, err := LoadCompactNeedleMap(idxFile, v.Version())
	idxFile.Close()
	if err != nil {
		t.Fatalf("load healthy .idx: %v", err)
	}
	healthyEnd := healthyNm.MaxNeedleEnd()
	if healthyEnd <= 0 || healthyEnd > datSize {
		t.Fatalf("healthy volume should have MaxNeedleEnd (%d) in (0, dat_size=%d]", healthyEnd, datSize)
	}

	// Inject a dangling entry by appending to the .idx, then reload. The
	// walk should observe MaxNeedleEnd past dat_size — exactly the signal
	// volume.load uses to mark the volume read-only.
	bogusOffset := types.ToOffset(datSize + 4*1024*1024)
	if err := v.nm.Put(types.Uint64ToNeedleId(9999), bogusOffset, types.Size(1024)); err != nil {
		t.Fatalf("inject dangling idx entry: %v", err)
	}
	if err := v.nm.Sync(); err != nil {
		t.Fatalf("sync .idx after inject: %v", err)
	}

	idxFile, err = os.OpenFile(v.FileName(".idx"), os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("reopen .idx: %v", err)
	}
	defer idxFile.Close()
	badNm, err := LoadCompactNeedleMap(idxFile, v.Version())
	if err != nil {
		t.Fatalf("reload .idx after inject: %v", err)
	}
	badEnd := badNm.MaxNeedleEnd()
	if badEnd <= datSize {
		t.Fatalf("after dangling-entry inject MaxNeedleEnd (%d) should exceed dat_size (%d)", badEnd, datSize)
	}
}

package command

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

// TestSaveToIdxWritesOffsetOrder guards that weed fix rebuilds the .idx as an
// append-ordered log (sorted by .dat offset), not sorted by key. A key-sorted
// .idx puts the highest-key needle last instead of the .dat-tail needle, which
// flipped volumes read-only on load (issue #9688). It must also carry every
// tombstone — including one whose live needle is gone — so the last entry is
// the real .dat tail.
func TestSaveToIdxWritesOffsetOrder(t *testing.T) {
	nm := needle_map.NewMemDb()
	defer nm.Close()
	nmDeleted := needle_map.NewMemDb()
	defer nmDeleted.Close()

	// Live needles with high keys at low offsets (as if written first).
	for _, e := range []struct {
		key    uint64
		offset int64
	}{{30, 8}, {20, 128}, {10, 256}} {
		if err := nm.Set(types.Uint64ToNeedleId(e.key), types.ToOffset(e.offset), types.Size(100)); err != nil {
			t.Fatalf("nm.Set: %v", err)
		}
	}
	// A tombstone at the .dat tail whose live needle is gone; the old SaveToIdx
	// dropped these because the key was absent from the live map.
	if err := nmDeleted.Set(types.Uint64ToNeedleId(5), types.ToOffset(384), types.TombstoneFileSize); err != nil {
		t.Fatalf("nmDeleted.Set: %v", err)
	}

	scanner := &VolumeFileScanner4Fix{nm: nm, nmDeleted: nmDeleted, includeDeleted: true}

	idxPath := filepath.Join(t.TempDir(), "v.idx")
	if err := SaveToIdx(scanner, idxPath); err != nil {
		t.Fatalf("SaveToIdx: %v", err)
	}

	f, err := os.Open(idxPath)
	if err != nil {
		t.Fatalf("open idx: %v", err)
	}
	defer f.Close()

	var offsets []int64
	var lastKey types.NeedleId
	if err := idx.WalkIndexFile(f, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		offsets = append(offsets, offset.ToActualOffset())
		lastKey = key
		return nil
	}); err != nil {
		t.Fatalf("walk idx: %v", err)
	}

	if len(offsets) != 4 {
		t.Fatalf("expected 4 entries (3 live + 1 tombstone), got %d", len(offsets))
	}
	for i := 1; i < len(offsets); i++ {
		if offsets[i] < offsets[i-1] {
			t.Fatalf("entries not in offset order: %v", offsets)
		}
	}
	// The .dat-tail needle (the orphan tombstone at the highest offset) must be last.
	if lastKey != types.Uint64ToNeedleId(5) {
		t.Errorf("last entry should be the .dat-tail tombstone (key 5), got key %v", lastKey)
	}
}

// writeFixTestVolume writes a .dat holding needles 1..count and returns its
// base path and the offset of each needle, in key order.
func writeFixTestVolume(t *testing.T, dir string, count int) (string, []int64) {
	t.Helper()
	base := filepath.Join(dir, "fix_7")
	version := needle.GetCurrentVersion()
	f, err := os.OpenFile(base+".dat", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}
	datBackend := backend.NewDiskFile(f)
	defer datBackend.Close()
	sb := super_block.SuperBlock{Version: version, ReplicaPlacement: &super_block.ReplicaPlacement{}, Ttl: &needle.TTL{}}
	if _, err := datBackend.WriteAt(sb.Bytes(), 0); err != nil {
		t.Fatal(err)
	}
	var offsets []int64
	for i := 1; i <= count; i++ {
		n := &needle.Needle{Id: types.Uint64ToNeedleId(uint64(i)), Data: bytes.Repeat([]byte{byte(i)}, 1000*i)}
		n.Checksum = needle.NewCRC(n.Data)
		offset, _, _, err := n.Append(datBackend, version)
		if err != nil {
			t.Fatalf("append needle %d: %v", i, err)
		}
		offsets = append(offsets, int64(offset))
	}
	return base, offsets
}

func indexedKeys(t *testing.T, idxPath string) []uint64 {
	t.Helper()
	f, err := os.Open(idxPath)
	if err != nil {
		t.Fatalf("open idx: %v", err)
	}
	defer f.Close()
	var keys []uint64
	if err := idx.WalkIndexFile(f, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		keys = append(keys, uint64(key))
		return nil
	}); err != nil {
		t.Fatalf("walk idx: %v", err)
	}
	return keys
}

func overwriteAt(t *testing.T, path string, offset int64, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(b, offset); err != nil {
		t.Fatal(err)
	}
}

func TestFixSkipsRecordPastEndOfDat(t *testing.T) {
	savedStatus := commandExitStatus
	defer func() { commandExitStatus = savedStatus }()
	sizeAt := types.NeedleIdSize + types.CookieSize
	for _, tc := range []struct {
		name    string
		corrupt func(t *testing.T, dat string, off int64)
	}{
		{"truncated", func(t *testing.T, dat string, off int64) {
			if err := os.Truncate(dat, off+100); err != nil {
				t.Fatal(err)
			}
		}},
		{"huge size", func(t *testing.T, dat string, off int64) {
			overwriteAt(t, dat, off+int64(sizeAt), []byte{0x7f, 0xff, 0xff, 0xf0})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			base, offsets := writeFixTestVolume(t, dir, 5)
			tc.corrupt(t, base+".dat", offsets[4])
			commandExitStatus = 0
			doFixOneVolume(dir, "fix_7", "fix", 7, true)
			if CommandExitStatus() == 0 {
				t.Error("expected a non-zero exit status")
			}
			if got := indexedKeys(t, base+".idx"); !slices.Equal(got, []uint64{1, 2, 3, 4}) {
				t.Errorf("indexed keys %v, want [1 2 3 4]", got)
			}
		})
	}
}

func TestFixIgnoreErrorReportsIncompleteScan(t *testing.T) {
	savedIgnore, savedStatus := *fixIgnoreError, commandExitStatus
	*fixIgnoreError = true
	defer func() { *fixIgnoreError, commandExitStatus = savedIgnore, savedStatus }()

	t.Run("bad header", func(t *testing.T) {
		dir := t.TempDir()
		base, offsets := writeFixTestVolume(t, dir, 5)
		overwriteAt(t, base+".dat", offsets[2], bytes.Repeat([]byte{0xa5}, 16))
		commandExitStatus = 0
		doFixOneVolume(dir, "fix_7", "fix", 7, false)
		if CommandExitStatus() == 0 {
			t.Error("expected a non-zero exit status for a scan that stopped before the end of the .dat")
		}
		if got := indexedKeys(t, base+".idx"); !slices.Equal(got, []uint64{1, 2}) {
			t.Errorf("indexed keys %v, want [1 2]", got)
		}
	})

	t.Run("bad super block keeps the existing index", func(t *testing.T) {
		dir := t.TempDir()
		base, _ := writeFixTestVolume(t, dir, 3)
		overwriteAt(t, base+".dat", 0, bytes.Repeat([]byte{0xa5}, 8))
		if err := os.WriteFile(base+".idx", []byte("previous index"), 0644); err != nil {
			t.Fatal(err)
		}
		commandExitStatus = 0
		doFixOneVolume(dir, "fix_7", "fix", 7, true)
		if CommandExitStatus() == 0 {
			t.Error("expected a non-zero exit status for an unreadable super block")
		}
		if b, err := os.ReadFile(base + ".idx"); err != nil || string(b) != "previous index" {
			t.Errorf("existing .idx changed: %q, %v", b, err)
		}
	})
}

// liveKeys applies the .idx entries in order and returns the keys left live.
func liveKeys(t *testing.T, idxPath string) []uint64 {
	t.Helper()
	f, err := os.Open(idxPath)
	if err != nil {
		t.Fatalf("open idx: %v", err)
	}
	defer f.Close()
	live := map[uint64]bool{}
	if err := idx.WalkIndexFile(f, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
		live[uint64(key)] = !offset.IsZero() && size.IsValid()
		return nil
	}); err != nil {
		t.Fatalf("walk idx: %v", err)
	}
	var keys []uint64
	for key, ok := range live {
		if ok {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys
}

// Damaged needles, and a damaged newer copy of a key, are indexed unless
// -verifyCrc reads the data; then they are indexed as deleted and the older
// copy of the key does not come back. Needle 5's data size is zeroed, which
// skips the CRC.
func TestFixVerifyCrcIndexesDamagedNeedlesAsDeleted(t *testing.T) {
	savedVerify, savedStatus := *fixVerifyCrc, commandExitStatus
	defer func() { *fixVerifyCrc, commandExitStatus = savedVerify, savedStatus }()
	dir := t.TempDir()
	base, offsets := writeFixTestVolume(t, dir, 5)
	f, err := os.OpenFile(base+".dat", os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	datBackend := backend.NewDiskFile(f)
	rewrite := &needle.Needle{Id: types.Uint64ToNeedleId(2), Data: bytes.Repeat([]byte{0x22}, 500)}
	rewrite.Checksum = needle.NewCRC(rewrite.Data)
	offset, _, _, err := rewrite.Append(datBackend, needle.GetCurrentVersion())
	datBackend.Close()
	if err != nil {
		t.Fatal(err)
	}
	dataAt := int64(types.NeedleHeaderSize + types.DataSizeSize)
	overwriteAt(t, base+".dat", offsets[3]+dataAt, []byte{0xee})
	overwriteAt(t, base+".dat", int64(offset)+dataAt, []byte{0xee})
	overwriteAt(t, base+".dat", offsets[4]+types.NeedleHeaderSize, make([]byte, types.DataSizeSize))

	for _, tc := range []struct {
		verify bool
		want   []uint64
		status int
	}{
		{false, []uint64{1, 2, 3, 4, 5}, 0},
		{true, []uint64{1, 3}, 1},
	} {
		for _, includeDeleted := range []bool{false, true} {
			*fixVerifyCrc, commandExitStatus = tc.verify, 0
			doFixOneVolume(dir, "fix_7", "fix", 7, includeDeleted)
			if got := liveKeys(t, base+".idx"); !slices.Equal(got, tc.want) {
				t.Errorf("verifyCrc=%v includeDeleted=%v: live keys %v, want %v", tc.verify, includeDeleted, got, tc.want)
			}
			if CommandExitStatus() != tc.status {
				t.Errorf("verifyCrc=%v includeDeleted=%v: exit status %d, want %d", tc.verify, includeDeleted, CommandExitStatus(), tc.status)
			}
		}
	}
}

// Loading a volume repaired with -verifyCrc must not bring back the older
// copy of a key whose newer copy, the last record, was damaged.
func TestFixVerifyCrcDamagedTailStaysDeletedOnLoad(t *testing.T) {
	savedVerify, savedStatus := *fixVerifyCrc, commandExitStatus
	defer func() { *fixVerifyCrc, commandExitStatus = savedVerify, savedStatus }()
	dir := t.TempDir()
	base := filepath.Join(dir, "fix_7")
	f, err := os.OpenFile(base+".dat", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}
	datBackend := backend.NewDiskFile(f)
	sb := super_block.SuperBlock{Version: needle.GetCurrentVersion(), ReplicaPlacement: &super_block.ReplicaPlacement{}, Ttl: &needle.TTL{}}
	if _, err := datBackend.WriteAt(sb.Bytes(), 0); err != nil {
		t.Fatal(err)
	}
	var offset uint64
	for i, key := range []uint64{1, 2, 2} {
		n := &needle.Needle{Id: types.Uint64ToNeedleId(key), Data: bytes.Repeat([]byte{byte(i + 1)}, 500), AppendAtNs: uint64(i + 1)}
		n.Checksum = needle.NewCRC(n.Data)
		if offset, _, _, err = n.Append(datBackend, needle.GetCurrentVersion()); err != nil {
			t.Fatal(err)
		}
	}
	datBackend.Close()
	overwriteAt(t, base+".dat", int64(offset)+types.NeedleHeaderSize+types.DataSizeSize, []byte{0xee})
	datBefore, err := os.Stat(base + ".dat")
	if err != nil {
		t.Fatal(err)
	}

	*fixVerifyCrc, commandExitStatus = true, 0
	doFixOneVolume(dir, "fix_7", "fix", 7, true)

	v, err := storage.NewVolume(dir, dir, "fix", 7, storage.NeedleMapInMemory, nil, nil, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if _, found, _ := v.LookupNeedle(types.Uint64ToNeedleId(2)); found {
		t.Error("key 2 is live after loading the repaired volume")
	}
	if _, found, _ := v.LookupNeedle(types.Uint64ToNeedleId(1)); !found {
		t.Error("key 1 is missing after loading the repaired volume")
	}
	datAfter, err := os.Stat(base + ".dat")
	if err != nil {
		t.Fatal(err)
	}
	if datAfter.Size() != datBefore.Size() {
		t.Errorf(".dat changed on load: %d bytes before, %d after", datBefore.Size(), datAfter.Size())
	}
}

type failingReadAt struct {
	backend.BackendStorageFile
	at int64
}

func (f failingReadAt) ReadAt(p []byte, off int64) (int, error) {
	if off == f.at {
		return 0, syscall.EIO
	}
	return f.BackendStorageFile.ReadAt(p, off)
}

// A needle -verifyCrc cannot read fails the scan instead of being dropped.
func TestFixVerifyCrcFailsOnReadError(t *testing.T) {
	dir := t.TempDir()
	base, offsets := writeFixTestVolume(t, dir, 3)
	f, err := os.Open(base + ".dat")
	if err != nil {
		t.Fatal(err)
	}
	dat := backend.NewDiskFile(f)
	defer dat.Close()
	nm, nmDeleted := needle_map.NewMemDb(), needle_map.NewMemDb()
	defer nm.Close()
	defer nmDeleted.Close()
	scanner := &VolumeFileScanner4Fix{nm: nm, nmDeleted: nmDeleted, verifyDat: failingReadAt{dat, offsets[1]}}
	if err := storage.ScanVolumeFile(dir, "fix", 7, storage.NeedleMapInMemory, scanner); !errors.Is(err, syscall.EIO) {
		t.Fatalf("scan error %v, want EIO", err)
	}
}

func TestLocalDatSizeIgnoresRemoteTier(t *testing.T) {
	dir := t.TempDir()
	base, _ := writeFixTestVolume(t, dir, 2)
	info, err := os.Stat(base + ".dat")
	if err != nil {
		t.Fatal(err)
	}
	if got := localDatSize(dir, "fix_7"); got != info.Size() {
		t.Fatalf("localDatSize = %d, want %d", got, info.Size())
	}
	vi := &volume_server_pb.VolumeInfo{Files: []*volume_server_pb.RemoteFile{{BackendType: "s3", Key: "fix_7.dat", FileSize: uint64(info.Size()) * 2}}}
	if err := volume_info.SaveVolumeInfo(base+".vif", vi); err != nil {
		t.Fatal(err)
	}
	if got := localDatSize(dir, "fix_7"); got != 0 {
		t.Errorf("localDatSize with a remote .dat = %d, want 0", got)
	}
}

func TestFixTruncatedDeleteKeepsKeyDeleted(t *testing.T) {
	savedStatus := commandExitStatus
	defer func() { commandExitStatus = savedStatus }()
	dir := t.TempDir()
	base, _ := writeFixTestVolume(t, dir, 3)
	f, err := os.OpenFile(base+".dat", os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	datBackend := backend.NewDiskFile(f)
	deletion := &needle.Needle{Id: types.Uint64ToNeedleId(2)}
	offset, _, _, err := deletion.Append(datBackend, needle.GetCurrentVersion())
	datBackend.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(base+".dat", int64(offset)+types.NeedleHeaderSize+2); err != nil {
		t.Fatal(err)
	}

	for _, includeDeleted := range []bool{true, false} {
		doFixOneVolume(dir, "fix_7", "fix", 7, includeDeleted)
		f, err := os.Open(base + ".idx")
		if err != nil {
			t.Fatal(err)
		}
		live := map[uint64]bool{}
		err = idx.WalkIndexFile(f, 0, func(key types.NeedleId, offset types.Offset, size types.Size) error {
			live[uint64(key)] = !offset.IsZero() && size.IsValid()
			return nil
		})
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if live[2] || !live[1] || !live[3] {
			t.Errorf("includeDeleted=%v: live keys %v, want 1 and 3 only", includeDeleted, live)
		}
	}
}

func TestSaveToIdxReportsOpenError(t *testing.T) {
	nm := needle_map.NewMemDb()
	defer nm.Close()
	idxPath := filepath.Join(t.TempDir(), "fix_7.idx")
	if err := os.Mkdir(idxPath, 0755); err != nil {
		t.Fatal(err)
	}
	if err := SaveToIdx(&VolumeFileScanner4Fix{nm: nm}, idxPath); err == nil {
		t.Error("expected an error when the .idx path cannot be opened")
	}
	if _, err := os.Stat(idxPath); err != nil {
		t.Errorf("a failed open must leave the existing .idx path alone: %v", err)
	}
}

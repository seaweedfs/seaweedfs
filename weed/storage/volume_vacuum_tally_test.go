package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func TestLoadCompactSnapshot(t *testing.T) {
	k1, k2, k3, k4 := types.NeedleId(1), types.NeedleId(2), types.NeedleId(3), types.NeedleId(4)
	rows := []struct {
		key    types.NeedleId
		offset int64
		size   types.Size
	}{
		{k1, 8, 100},
		{k2, 16, 200},
		{k1, 24, 300},                    // overwrite
		{k2, 0, types.TombstoneFileSize}, // delete
		{k3, 0, 50},                      // zero offset never becomes live
		{k4, 32, 400},
		{k2, 40, 250}, // written again after the delete
	}
	idxPath := filepath.Join(t.TempDir(), "1.idx")
	var buf []byte
	for _, r := range rows {
		buf = append(buf, needle_map.ToBytes(r.key, types.ToOffset(r.offset), r.size)...)
	}
	if err := os.WriteFile(idxPath, buf, 0644); err != nil {
		t.Fatal(err)
	}

	nm := needle_map.NewMemDb()
	defer nm.Close()
	snap, err := loadCompactSnapshot(idxPath, nm)
	if err != nil {
		t.Fatalf("loadCompactSnapshot: %v", err)
	}
	if want := (compactSnapshot{liveCount: 3, liveBytes: 950}); snap != want {
		t.Fatalf("snapshot = %+v, want %+v", snap, want)
	}
	if v, ok := nm.Get(k1); !ok || v.Size != 300 {
		t.Fatalf("k1 = %+v %v, want size 300", v, ok)
	}
	if _, ok := nm.Get(k3); ok {
		t.Fatalf("k3 with a zero offset must not be in the snapshot")
	}
}

func TestCompactTallyVerify(t *testing.T) {
	cases := []struct {
		name    string
		snap    compactSnapshot
		tally   compactTally
		dst     int64
		wantErr string
	}{
		{"empty volume", compactSnapshot{}, compactTally{}, 8, ""},
		{"all copied", compactSnapshot{3, 300}, compactTally{copiedCount: 3, copiedBytes: 300}, 400, ""},
		{"drops accounted", compactSnapshot{3, 300}, compactTally{copiedCount: 1, copiedBytes: 100, expiredCount: 1, expiredBytes: 100, unreadableCount: 1, unreadableBytes: 100}, 150, ""},
		{"traversal loss", compactSnapshot{40, 4000}, compactTally{copiedCount: 20, copiedBytes: 2000}, 2100, "accounted for 20 of 40"},
		{"short write", compactSnapshot{2, 2000}, compactTally{copiedCount: 2, copiedBytes: 2000}, 1500, "unexpected new data size"},
		{"drops exceed", compactSnapshot{1, 100}, compactTally{expiredCount: 1, expiredBytes: 200}, 8, "more than the 100 live bytes"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.tally.verify(needle.VolumeId(1), c.snap, c.dst)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("verify: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("verify error = %v, want it to contain %q", err, c.wantErr)
			}
		})
	}
}

// TestCompactByIndex_TraversalLossIsRejected injects the #5215 failure: the
// snapshot traversal silently skips live entries. Vacuum must refuse to
// produce a volume that lacks them.
func TestCompactByIndex_TraversalLossIsRejected(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()
	for i := uint64(1); i <= 40; i++ {
		n := newRandomNeedle(i)
		n.Data = make([]byte, 50000)
		n.Checksum = needle.NewCRC(n.Data)
		if _, _, _, err := v.writeNeedle2(n, true, false, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}

	original := visitCompactSnapshot
	defer func() { visitCompactSnapshot = original }()
	visitCompactSnapshot = func(nm *needle_map.MemDb, visit func(needle_map.NeedleValue) error) error {
		i := 0
		return original(nm, func(value needle_map.NeedleValue) error {
			i++
			if i%2 == 0 {
				return nil
			}
			return visit(value)
		})
	}

	err = v.CompactByIndex(nil)
	if err == nil || !strings.Contains(err.Error(), "accounted for 20 of 40") {
		t.Fatalf("CompactByIndex error = %v, want the traversal loss to be rejected", err)
	}
	for i := uint64(1); i <= 40; i++ {
		if _, err := v.readNeedle(&needle.Needle{Id: types.Uint64ToNeedleId(i)}, nil, nil); err != nil {
			t.Fatalf("needle %d unreadable after a refused vacuum: %v", i, err)
		}
	}
}

// TestCompactByIndex_ExpiredNeedlesOnTtlVolumeAreAccounted checks that the
// size check, now active on TTL volumes, accounts for expired needles.
func TestCompactByIndex_ExpiredNeedlesOnTtlVolumeAreAccounted(t *testing.T) {
	volumeTtl, err := needle.ReadTTL("1m")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, volumeTtl, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()
	write := func(id uint64, lastModified time.Time) {
		n := newRandomNeedle(id)
		n.Ttl = needle.EMPTY_TTL // an upload without ?ttl=, stamped with the volume TTL
		n.LastModified = uint64(lastModified.Unix())
		n.SetHasLastModifiedDate()
		if _, _, _, err := v.writeNeedle2(n, true, false, false); err != nil {
			t.Fatalf("write needle %d: %v", id, err)
		}
	}
	for i := uint64(1); i <= 5; i++ {
		write(i, time.Date(2023, 11, 14, 0, 0, 0, 0, time.UTC))
	}
	write(6, time.Now())

	if err := v.CompactByIndex(nil); err != nil {
		t.Fatalf("CompactByIndex: %v", err)
	}
	if err := v.CommitCompact(); err != nil {
		t.Fatalf("CommitCompact: %v", err)
	}
	if got := v.FileCount(); got != 1 {
		t.Fatalf("FileCount = %d, want 1", got)
	}
}

func TestCompactByIndex_InterruptIsNotAnAccountingError(t *testing.T) {
	dir := t.TempDir()
	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, &needle.TTL{}, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	defer v.Close()
	for i := uint64(1); i <= 10; i++ {
		if _, _, _, err := v.writeNeedle2(newRandomNeedle(i), true, false, false); err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
	}
	calls := 0
	err = v.CompactByIndex(&CompactOptions{ProgressCallback: func(int64) bool {
		calls++
		return calls < 3
	}})
	if err == nil || !strings.Contains(err.Error(), "interrupted") || strings.Contains(err.Error(), "accounted for") {
		t.Fatalf("CompactByIndex error = %v, want a plain interruption", err)
	}
}

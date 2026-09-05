package storage

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestVolumeTtlClockSurvivesDeletes reproduces issue #11160: deletes append a
// tombstone to the .dat, which moves the file's mtime, and the loader read the
// TTL clock back from that mtime. A volume taking delete traffic therefore had
// expired() re-armed for another full TTL on every restart and was never
// reclaimed. The clock has to come from the newest write instead.
func TestVolumeTtlClockSurvivesDeletes(t *testing.T) {
	dir := t.TempDir()
	ttl, err := needle.ReadTTL("5m")
	if err != nil {
		t.Fatalf("read ttl: %v", err)
	}

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, ttl, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}

	// Backdate the writes on disk so the last one sits well outside the TTL,
	// while the tombstones below leave the .dat mtime at now.
	lastWriteNs := uint64(time.Now().Add(-2 * time.Hour).UnixNano())
	for i := 1; i <= 3; i++ {
		n := newRandomNeedle(uint64(i))
		offset, _, _, err := v.writeNeedle2(n, true, false, false)
		if err != nil {
			t.Fatalf("write needle %d: %v", i, err)
		}
		backdateAppendAtNs(t, v, int64(offset), n.Size, lastWriteNs)
	}
	// More than one tombstone: the scan has to walk back over the whole run of
	// them to reach a write.
	for _, id := range []uint64{2, 3} {
		if _, err := v.doDeleteRequest(newEmptyNeedle(id)); err != nil {
			t.Fatalf("delete needle %d: %v", id, err)
		}
	}
	contentSize := v.ContentSize()
	v.Close()

	reloaded, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, ttl, 0, needle.GetCurrentVersion(), 0, 0)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	defer reloaded.Close()

	if got, want := reloaded.lastModifiedTsSeconds, lastWriteNs/uint64(time.Second); got != want {
		t.Errorf("TTL clock recovered as %d, want the last write at %d", got, want)
	}
	if !reloaded.expired(contentSize, 1024*1024) {
		t.Error("a TTL volume whose last write is 2h old must be expired after a reload (issue #11160)")
	}
}

// TestVolumeTtlClockKeepsMtimeWithoutRecoverableWrite covers a TTL volume whose
// needles carry no append timestamp: the loader must stay on the .dat mtime
// rather than treat the volume as written at the epoch and drop it on sight.
func TestVolumeTtlClockKeepsMtimeWithoutRecoverableWrite(t *testing.T) {
	dir := t.TempDir()
	ttl, err := needle.ReadTTL("5m")
	if err != nil {
		t.Fatalf("read ttl: %v", err)
	}

	v, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, ttl, 0, needle.Version2, 0, 0)
	if err != nil {
		t.Fatalf("volume creation: %v", err)
	}
	if _, _, _, err := v.writeNeedle2(newRandomNeedle(1), true, false, false); err != nil {
		t.Fatalf("write needle: %v", err)
	}
	contentSize := v.ContentSize()
	v.Close()

	reloaded, err := NewVolume(dir, dir, "", 1, NeedleMapInMemory, &super_block.ReplicaPlacement{}, ttl, 0, needle.Version2, 0, 0)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	defer reloaded.Close()

	if reloaded.expired(contentSize, 1024*1024) {
		t.Error("a just-written volume must not be expired after a reload")
	}
}

func backdateAppendAtNs(t *testing.T, v *Volume, offset int64, size types.Size, appendAtNs uint64) {
	t.Helper()
	stamp := make([]byte, types.TimestampSize)
	util.Uint64toBytes(stamp, appendAtNs)
	tsOffset := offset + types.NeedleHeaderSize + int64(size) + needle.NeedleChecksumSize
	if _, err := v.DataBackend.WriteAt(stamp, tsOffset); err != nil {
		t.Fatalf("backdate the needle at offset %d: %v", offset, err)
	}
}

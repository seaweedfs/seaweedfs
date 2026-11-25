package storage

import (
	"fmt"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// compactSnapshot is what the pre-compaction .idx says is live. It is counted
// while the snapshot is loaded, with point lookups only, so a fault in the
// traversal that later copies the snapshot cannot hide from it (#5215).
type compactSnapshot struct {
	liveCount int
	liveBytes uint64
}

// loadCompactSnapshot fills nm from idxPath with the same filtering as
// MemDb.LoadFromIdx (a zero offset or a deleted size removes the key) and
// counts the live entries and bytes on the way.
func loadCompactSnapshot(idxPath string, nm *needle_map.MemDb) (snap compactSnapshot, err error) {
	idxFile, err := os.OpenFile(idxPath, os.O_RDONLY, 0644)
	if err != nil {
		return snap, err
	}
	defer idxFile.Close()
	err = idx.WalkIndexFile(idxFile, 0, func(key NeedleId, offset Offset, size Size) error {
		if old, ok := nm.Get(key); ok {
			snap.liveCount--
			snap.liveBytes -= uint64(old.Size)
		}
		if offset.IsZero() || size.IsDeleted() {
			return nm.Delete(key)
		}
		snap.liveCount++
		snap.liveBytes += uint64(size)
		return nm.Set(key, offset, size)
	})
	return snap, err
}

// compactTally accounts for every live snapshot entry the copy loop visits.
type compactTally struct {
	copiedCount, expiredCount, unreadableCount int
	copiedBytes, expiredBytes, unreadableBytes uint64
}

func (t *compactTally) copied(size Size) {
	t.copiedCount++
	if size.IsValid() {
		t.copiedBytes += uint64(size)
	}
}

func (t *compactTally) expired(size Size) {
	t.expiredCount++
	if size.IsValid() {
		t.expiredBytes += uint64(size)
	}
}

func (t *compactTally) unreadable(size Size) {
	t.unreadableCount++
	if size.IsValid() {
		t.unreadableBytes += uint64(size)
	}
}

// visitCompactSnapshot walks the snapshot the copy loop reads from. It is a
// variable so a test can inject a traversal that loses entries.
var visitCompactSnapshot = func(nm *needle_map.MemDb, visit func(needle_map.NeedleValue) error) error {
	return nm.AscendingVisit(visit)
}

// verify checks that every live snapshot entry was copied, expired or found
// unreadable, and that the new .dat holds at least the bytes left to copy.
func (t compactTally) verify(vid needle.VolumeId, snap compactSnapshot, dstDatSize int64) error {
	if visited := t.copiedCount + t.expiredCount + t.unreadableCount; visited != snap.liveCount {
		return fmt.Errorf("volume %s compaction accounted for %d of %d live index entries (copied %d, expired %d, unreadable %d)",
			vid, visited, snap.liveCount, t.copiedCount, t.expiredCount, t.unreadableCount)
	}
	dropped := t.expiredBytes + t.unreadableBytes
	if dropped > snap.liveBytes {
		return fmt.Errorf("volume %s compaction dropped %d bytes, more than the %d live bytes in the snapshot",
			vid, dropped, snap.liveBytes)
	}
	if expected := snap.liveBytes - dropped; expected > uint64(dstDatSize) {
		return fmt.Errorf("volume %s unexpected new data size: %d does not match expected live content size %d from the pre-compaction snapshot",
			vid, dstDatSize, expected)
	}
	return nil
}

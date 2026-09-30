package storage

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/stats"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

var ErrInsufficientSpace = fmt.Errorf("insufficient free space")

func (s *Store) CheckCompactVolume(volumeId needle.VolumeId) (garbageRatio float64, diskSpaceLow bool, err error) {
	if v := s.findVolume(volumeId); v != nil {
		glog.V(3).Infof("volume %d garbage level: %f", volumeId, v.garbageLevel())
		// diskSpaceLow only counts when it is the sole read-only cause — an
		// operator mark or I/O quarantine still shields the volume.
		_, noWriteOrDelete, noWriteCanDelete, isLow := v.ReadOnlyReasons()
		return v.garbageLevel(), isLow && !noWriteOrDelete && !noWriteCanDelete, nil
	}
	return 0, false, fmt.Errorf("volume id %d is not found during check compact: %w", volumeId, ErrVolumeNotFound)
}

func (s *Store) CompactVolume(vid needle.VolumeId, preallocate int64, compactionBytePerSecond int64, progressFn ProgressFunc) error {
	if v := s.findVolume(vid); v != nil {
		if err := ensureCompactVolumeSpace(v, preallocate); err != nil {
			return err
		}
		return v.CompactByIndex(&CompactOptions{
			PreallocateBytes:  preallocate,
			MaxBytesPerSecond: compactionBytePerSecond,
			ProgressCallback:  progressFn,
		})
	}
	return fmt.Errorf("volume id %d is not found during compact: %w", vid, ErrVolumeNotFound)
}

func (s *Store) CommitCompactVolume(vid needle.VolumeId) (bool, int64, error) {
	if s.isStopping.Load() {
		return false, 0, fmt.Errorf("volume id %d skips compact because volume is stopping", vid)
	}
	if v := s.findVolume(vid); v != nil {
		isReadOnly := v.IsReadOnly()
		err := v.CommitCompact()
		var volumeSize int64 = 0
		if err == nil && v.DataBackend != nil {
			volumeSize, _, _ = v.DataBackend.GetStat()
		}
		return isReadOnly, volumeSize, err
	}
	return false, 0, fmt.Errorf("volume id %d is not found during commit compact: %w", vid, ErrVolumeNotFound)
}

func (s *Store) CommitCleanupVolume(vid needle.VolumeId) error {
	if v := s.findVolume(vid); v != nil {
		return v.cleanupCompact()
	}
	return fmt.Errorf("volume id %d is not found during cleaning up: %w", vid, ErrVolumeNotFound)
}

// compactionDiskFree reports the free bytes on the disk holding dir. It is a
// variable so a test can stand in for a full disk without filling one.
var compactionDiskFree = func(dir string) uint64 {
	return stats.NewDiskStatus(dir).Free
}

// compactionSpaceNeeded estimates what CompactByIndex will write for v: the
// live needles with their on-disk framing behind a superblock, and a rebuilt
// index with one entry per live needle. The volume's current size is the
// wrong yardstick: the more garbage a volume holds, the less its compaction
// writes, and a store that filled up until its volumes went read-only is
// exactly where the all-garbage volumes must still compact to give the space
// back (issue #11516). The estimate never exceeds the current volume, and
// preallocate wins when it is larger, because the new .dat is preallocated
// to that length.
func compactionSpaceNeeded(v *Volume, preallocate int64) (spaceNeeded, liveBytes, indexBytes int64) {
	datSize, idxSize, _ := v.FileStat()
	liveBytes, indexBytes = int64(datSize), int64(idxSize)

	liveCount := int64(v.FileCount()) - int64(v.DeletedCount())
	liveContent := int64(v.ContentSize()) - int64(v.DeletedSize())
	// A .sdx converted back to .idx carries no deleted sizes (see
	// garbageLevel), and counters that disagree mean the metric is off;
	// either way the whole volume stays the estimate.
	deletedSizeKnown := v.DeletedCount() == 0 || v.DeletedSize() > 0
	if deletedSizeKnown && liveCount >= 0 && liveContent >= 0 {
		// GetActualSize(0) is the framing of an empty needle; another
		// padding unit covers the worst case for any other size.
		perNeedle := needle.GetActualSize(0, v.Version()) + types.NeedlePaddingSize
		if estimate := super_block.SuperBlockSize + liveContent + liveCount*perNeedle; estimate < liveBytes {
			liveBytes = estimate
		}
		if estimate := liveCount * types.NeedleMapEntrySize; estimate < indexBytes {
			indexBytes = estimate
		}
	}

	spaceNeeded = liveBytes + indexBytes
	if preallocate > spaceNeeded {
		spaceNeeded = preallocate
	}
	return spaceNeeded, liveBytes, indexBytes
}

func ensureCompactVolumeSpace(v *Volume, preallocate int64) error {
	spaceNeeded, liveBytes, indexBytes := compactionSpaceNeeded(v, preallocate)
	volumeSize, indexSize, _ := v.FileStat()
	free := compactionDiskFree(v.dir)
	if int64(free) < spaceNeeded {
		return fmt.Errorf("insufficient free space for compaction: need %d bytes (live: %d, index: %d, current volume: %d, current index: %d), but only %d bytes available: %w",
			spaceNeeded, liveBytes, indexBytes, volumeSize, indexSize, free, ErrInsufficientSpace)
	}
	spaceNeeded += spaceNeeded / 10

	glog.V(1).Infof("volume %d compaction space check: live=%d, index=%d, current volume=%d, space_needed=%d, free_space=%d",
		v.Id, liveBytes, indexBytes, volumeSize, spaceNeeded, free)

	return nil
}

func (s *Store) CompactVolumeFiles(vid needle.VolumeId, collection string, location *DiskLocation, needleMapKind NeedleMapKind, ldbTimeout int64, preallocate int64, compactionBytePerSecond int64) (err error) {
	if location == nil {
		return fmt.Errorf("volume %d compaction location is nil", vid)
	}

	tempVolume, err := loadVolumeWithoutWorker(location.Directory, location.IdxDirectory, collection, vid, needleMapKind, ldbTimeout)
	if err != nil {
		return fmt.Errorf("load volume %d for offline compaction: %w", vid, err)
	}
	tempVolume.location = location

	defer func() {
		if tempVolume.tmpNm != nil {
			tempVolume.tmpNm.Close()
			tempVolume.tmpNm = nil
		}
		tempVolume.doClose()
	}()

	if err := ensureCompactVolumeSpace(tempVolume, preallocate); err != nil {
		return err
	}

	if err := tempVolume.CompactByIndex(&CompactOptions{
		PreallocateBytes:  preallocate,
		MaxBytesPerSecond: compactionBytePerSecond,
	}); err != nil {
		if cleanupErr := tempVolume.cleanupCompact(); cleanupErr != nil {
			return fmt.Errorf("compact volume %d: %v (cleanup failed: %v)", vid, err, cleanupErr)
		}
		return fmt.Errorf("compact volume %d: %w", vid, err)
	}

	if err := tempVolume.CommitCompact(); err != nil {
		if cleanupErr := tempVolume.cleanupCompact(); cleanupErr != nil {
			return fmt.Errorf("commit compact volume %d: %v (cleanup failed: %v)", vid, err, cleanupErr)
		}
		return fmt.Errorf("commit compact volume %d: %w", vid, err)
	}

	return nil
}

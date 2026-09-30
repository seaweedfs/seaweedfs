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

// estimatedCompactedSize is what compaction writes: a superblock, the live
// needles with their on-disk framing, and an index with live entries only.
// Deleted bytes do not carry over, so a mostly-garbage volume needs far less
// space than it occupies.
func estimatedCompactedSize(v *Volume) int64 {
	liveCount := v.FileCount()
	if deleted := v.DeletedCount(); deleted < liveCount {
		liveCount -= deleted
	} else {
		liveCount = 0
	}
	liveBytes := v.ContentSize()
	if deleted := v.DeletedSize(); deleted < liveBytes {
		liveBytes -= deleted
	} else {
		liveBytes = 0
	}
	perNeedle := needle.GetActualSize(0, v.Version()) + types.NeedlePaddingSize + types.NeedleMapEntrySize
	return super_block.SuperBlockSize + int64(liveCount)*perNeedle + int64(liveBytes)
}

func ensureCompactVolumeSpace(v *Volume, preallocate int64) error {
	volumeSize, indexSize, _ := v.FileStat()

	// The compacted output holds live needles only, so measure against the
	// estimated compacted size — otherwise a disk full of garbage can never
	// reclaim itself.
	estimatedCompactSize := estimatedCompactedSize(v)
	spaceNeeded := preallocate
	if estimatedCompactSize > preallocate {
		spaceNeeded = estimatedCompactSize
	}
	spaceNeeded += spaceNeeded / 10

	diskStatus := stats.NewDiskStatus(v.dir)
	if int64(diskStatus.Free) < spaceNeeded {
		return fmt.Errorf("insufficient free space for compaction: need %d bytes (volume: %d, index: %d), but only %d bytes available: %w",
			spaceNeeded, volumeSize, indexSize, diskStatus.Free, ErrInsufficientSpace)
	}

	glog.V(1).Infof("volume %d compaction space check: volume=%d, index=%d, space_needed=%d, free_space=%d",
		v.Id, volumeSize, indexSize, spaceNeeded, diskStatus.Free)

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

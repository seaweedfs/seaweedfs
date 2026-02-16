package storage

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/stats"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func (s *Store) CheckCompactVolume(volumeId needle.VolumeId) (float64, error) {
	if v := s.findVolume(volumeId); v != nil {
		glog.V(3).Infof("volume %d garbage level: %f", volumeId, v.garbageLevel())
		return v.garbageLevel(), nil
	}
	return 0, fmt.Errorf("volume id %d is not found during check compact", volumeId)
}

func (s *Store) CompactVolume(vid needle.VolumeId, preallocate int64, compactionBytePerSecond int64, progressFn ProgressFunc) error {
	if v := s.findVolume(vid); v != nil {
		// Get current volume size for space calculation
		volumeSize, indexSize, _ := v.FileStat()

		// Calculate space needed for compaction:
		// 1. Space for the new compacted volume (approximately same as current volume size)
		// 2. Use the larger of preallocate or estimated volume size
		estimatedCompactSize := int64(volumeSize + indexSize)
		spaceNeeded := preallocate
		if estimatedCompactSize > preallocate {
			spaceNeeded = estimatedCompactSize
		}

		diskStatus := stats.NewDiskStatus(v.dir)
		if int64(diskStatus.Free) < spaceNeeded {
			return fmt.Errorf("insufficient free space for compaction: need %d bytes (volume: %d, index: %d, buffer: 10%%), but only %d bytes available",
				spaceNeeded, volumeSize, indexSize, diskStatus.Free)
		}

		glog.V(1).Infof("volume %d compaction space check: volume=%d, index=%d, space_needed=%d, free_space=%d",
			vid, volumeSize, indexSize, spaceNeeded, diskStatus.Free)

		return v.CompactByIndex(&CompactOptions{
			PreallocateBytes:  preallocate,
			MaxBytesPerSecond: compactionBytePerSecond,
			ProgressCallback:  progressFn,
		})
	}
	return fmt.Errorf("volume id %d is not found during compact", vid)
}

func (s *Store) CommitCompactVolume(vid needle.VolumeId) (bool, int64, error) {
	if s.isStopping {
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
	return false, 0, fmt.Errorf("volume id %d is not found during commit compact", vid)
}

func (s *Store) CommitCleanupVolume(vid needle.VolumeId) error {
	if v := s.findVolume(vid); v != nil {
		return v.cleanupCompact()
	}
	return fmt.Errorf("volume id %d is not found during cleaning up", vid)
}

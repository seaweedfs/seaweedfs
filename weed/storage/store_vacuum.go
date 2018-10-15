package storage

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

func (s *Store) CheckCompactVolume(volumeId VolumeId) (float64, error) {
	if v := s.findVolume(volumeId); v != nil {
		glog.V(3).Infof("volumd %d garbage level: %f", volumeId, v.garbageLevel())
		return v.garbageLevel(), nil
	}
	return 0, fmt.Errorf("volume id %d is not found during check compact", volumeId)
}
func (s *Store) CompactVolume(vid VolumeId, preallocate int64) error {
	if v := s.findVolume(vid); v != nil {
		return v.Compact(preallocate)
	}
	return fmt.Errorf("volume id %d is not found during compact", vid)
}
func (s *Store) CommitCompactVolume(vid VolumeId) error {
	if v := s.findVolume(vid); v != nil {
		return v.commitCompact()
	}
	return fmt.Errorf("volume id %d is not found during commit compact", vid)
}
func (s *Store) CommitCleanupVolume(vid VolumeId) error {
	if v := s.findVolume(vid); v != nil {
		return v.cleanupCompact()
	}
	return fmt.Errorf("volume id %d is not found during cleaning up", vid)
}

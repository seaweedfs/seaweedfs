package storage

import (
	"fmt"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

func (s *Store) CheckCompactVolume(volumeIdString string, garbageThresholdString string) (error, bool) {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer", volumeIdString), false
	}
	garbageThreshold, e := strconv.ParseFloat(garbageThresholdString, 32)
	if e != nil {
		return fmt.Errorf("garbageThreshold %s is not a valid float number", garbageThresholdString), false
	}
	if v := s.findVolume(vid); v != nil {
		glog.V(3).Infoln(vid, "garbage level is", v.garbageLevel())
		return nil, garbageThreshold < v.garbageLevel()
	}
	return fmt.Errorf("volume id %d is not found during check compact", vid), false
}
func (s *Store) CompactVolume(volumeIdString string, preallocate int64) error {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer", volumeIdString)
	}
	if v := s.findVolume(vid); v != nil {
		return v.Compact(preallocate)
	}
	return fmt.Errorf("volume id %d is not found during compact", vid)
}
func (s *Store) CommitCompactVolume(volumeIdString string) error {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer", volumeIdString)
	}
	if v := s.findVolume(vid); v != nil {
		return v.commitCompact()
	}
	return fmt.Errorf("volume id %d is not found during commit compact", vid)
}
func (s *Store) CommitCleanupVolume(volumeIdString string) error {
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("Volume Id %s is not a valid unsigned integer", volumeIdString)
	}
	if v := s.findVolume(vid); v != nil {
		return v.cleanupCompact()
	}
	return fmt.Errorf("volume id %d is not found during cleaning up", vid)
}

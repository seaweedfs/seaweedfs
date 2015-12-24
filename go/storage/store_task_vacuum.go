package storage

import (
	"fmt"
	"net/url"
)

type VacuumTask struct {
	V *Volume
}

func NewVacuumTask(s *Store, args url.Values) (*VacuumTask, error) {
	volumeIdString := args.Get("volumme")
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return nil, fmt.Errorf("Volume Id %s is not a valid unsigned integer", volumeIdString)
	}
	v := s.findVolume(vid)
	if v == nil {
		return nil, fmt.Errorf("volume id %d is not found", vid)
	}
	return &VacuumTask{V: v}, nil
}

func (t *VacuumTask) Run() error {
	return t.V.Compact()
}

func (t *VacuumTask) Commit() error {
	return t.V.commitCompact()
}

func (t *VacuumTask) Clean() error {
	return t.V.cleanCompact()
}

func (t *VacuumTask) Info() url.Values {
	//TODO
	return url.Values{}
}

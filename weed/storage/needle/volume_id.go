package needle

import (
	"strconv"
)

type VolumeId uint32

func NewVolumeId(vid string) (VolumeId, error) {
	volumeId, err := strconv.ParseUint(vid, 10, 64)
	return VolumeId(volumeId), err
}
func (vid VolumeId) String() string {
	return strconv.FormatUint(uint64(vid), 10)
}
func (vid VolumeId) Next() VolumeId {
	return VolumeId(uint32(vid) + 1)
}

package storage

import ()

type VolumeInfo struct {
	Id      VolumeId
	Size    int64
	RepType ReplicationType
}
type ReplicationType byte

const (
	Copy00               = ReplicationType(00) // single copy
	Copy01               = ReplicationType(01) // 2 copies, each on different racks, same data center
	Copy10               = ReplicationType(10) // 2 copies, each on different data center
	Copy11               = ReplicationType(11) // 3 copies, 2 on different racks and local data center, 1 on different data center
	Copy20               = ReplicationType(20) // 3 copies, each on dffereint data center
	LengthRelicationType = 5
	CopyNil              = ReplicationType(255) // nil value
)

func NewReplicationType(t string) ReplicationType {
	switch t {
	case "00":
		return Copy00
	case "01":
		return Copy01
	case "10":
		return Copy10
	case "11":
		return Copy11
	case "20":
		return Copy20
	}
	return Copy00
}
func (r *ReplicationType) String() string {
	switch *r {
	case Copy00:
		return "00"
	case Copy01:
		return "01"
	case Copy10:
		return "10"
	case Copy11:
		return "11"
	case Copy20:
		return "20"
	}
	return "00"
}

func GetReplicationLevelIndex(v *VolumeInfo) int {
	switch v.RepType {
	case Copy00:
		return 0
	case Copy01:
		return 1
	case Copy10:
		return 2
	case Copy11:
		return 3
	case Copy20:
		return 4
	}
	return -1
}
func GetCopyCount(v *VolumeInfo) int {
	switch v.RepType {
	case Copy00:
		return 1
	case Copy01:
		return 2
	case Copy10:
		return 2
	case Copy11:
		return 3
	case Copy20:
		return 3
	}
	return 0
}

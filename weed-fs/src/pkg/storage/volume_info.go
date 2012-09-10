package storage

import ()

type VolumeInfo struct {
	Id              VolumeId
	Size            int64
	ReplicationType ReplicationType
}
type ReplicationType int

const (
  Copy00 = ReplicationType(00) // single copy
  Copy01 = ReplicationType(01) // 2 copies, each on different racks, same data center
  Copy10 = ReplicationType(10) // 2 copies, each on different data center
  Copy11 = ReplicationType(11) // 3 copies, 2 on different racks and local data center, 1 on different data center
  Copy20 = ReplicationType(20) // 3 copies, each on dffereint data center
  LengthRelicationType = 5
)

func GetReplicationLevelIndex(v *VolumeInfo) int {
  switch v.ReplicationType {
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
  switch v.ReplicationType {
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

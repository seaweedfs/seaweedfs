package storage

import (
  "errors"
)

type VolumeInfo struct {
	Id      VolumeId
	Size    int64
	RepType ReplicationType
	FileCount int
	DeleteCount int
}
type ReplicationType string

const (
	Copy000               = ReplicationType("000") // single copy
	Copy001               = ReplicationType("001") // 2 copies, both on the same racks,  and same data center
  Copy010               = ReplicationType("010") // 2 copies, both on different racks, but same data center
	Copy100               = ReplicationType("100") // 2 copies, each on different data center
	Copy110               = ReplicationType("110") // 3 copies, 2 on different racks and local data center, 1 on different data center
	Copy200               = ReplicationType("200") // 3 copies, each on dffereint data center
	LengthRelicationType = 6
	CopyNil              = ReplicationType(255) // nil value
)

func NewReplicationTypeFromString(t string) (ReplicationType, error) {
	switch t {
	case "000":
		return Copy000, nil
	case "001":
		return Copy001, nil
	case "010":
		return Copy010, nil
  case "100":
    return Copy100, nil
	case "110":
		return Copy110, nil
	case "200":
		return Copy200, nil
	}
	return Copy000, errors.New("Unknown Replication Type:"+t)
}
func NewReplicationTypeFromByte(b byte) (ReplicationType, error) {
  switch b {
  case byte(000):
    return Copy000, nil
  case byte(001):
    return Copy001, nil
  case byte(010):
    return Copy010, nil
  case byte(100):
    return Copy100, nil
  case byte(110):
    return Copy110, nil
  case byte(200):
    return Copy200, nil
  }
  return Copy000, errors.New("Unknown Replication Type:"+string(b))
}

func (r *ReplicationType) String() string {
	switch *r {
	case Copy000:
		return "000"
	case Copy001:
		return "001"
  case Copy010:
    return "010"
	case Copy100:
		return "100"
	case Copy110:
		return "110"
	case Copy200:
		return "200"
	}
	return "000"
}
func (r *ReplicationType) Byte() byte {
  switch *r {
  case Copy000:
    return byte(000)
  case Copy001:
    return byte(001)
  case Copy010:
    return byte(010)
  case Copy100:
    return byte(100)
  case Copy110:
    return byte(110)
  case Copy200:
    return byte(200)
  }
  return byte(000)
}

func (repType ReplicationType)GetReplicationLevelIndex() int {
	switch repType {
	case Copy000:
		return 0
	case Copy001:
		return 1
  case Copy010:
    return 2
	case Copy100:
		return 3
	case Copy110:
		return 4
	case Copy200:
		return 5
	}
	return -1
}
func (repType ReplicationType)GetCopyCount() int {
	switch repType {
	case Copy000:
		return 1
	case Copy001:
		return 2
  case Copy010:
    return 2
	case Copy100:
		return 2
	case Copy110:
		return 3
	case Copy200:
		return 3
	}
	return 0
}

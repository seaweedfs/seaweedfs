package super_block

import (
	"errors"
	"fmt"
)

type ReplicaPlacement struct {
	SameRackCount       int `json:"node,omitempty"`
	DiffRackCount       int `json:"rack,omitempty"`
	DiffDataCenterCount int `json:"dc,omitempty"`
}

func NewReplicaPlacementFromString(t string) (*ReplicaPlacement, error) {
	rp := &ReplicaPlacement{}
	for i, c := range t {
		count := int(c - '0')
		if 0 <= count && count <= 2 {
			switch i {
			case 0:
				rp.DiffDataCenterCount = count
			case 1:
				rp.DiffRackCount = count
			case 2:
				rp.SameRackCount = count
			}
		} else {
			return rp, errors.New("Unknown Replication Type:" + t)
		}
	}
	return rp, nil
}

func NewReplicaPlacementFromByte(b byte) (*ReplicaPlacement, error) {
	return NewReplicaPlacementFromString(fmt.Sprintf("%03d", b))
}

func (rp *ReplicaPlacement) Byte() byte {
	if rp == nil {
		return 0
	}
	ret := rp.DiffDataCenterCount*100 + rp.DiffRackCount*10 + rp.SameRackCount
	return byte(ret)
}

func (rp *ReplicaPlacement) String() string {
	b := make([]byte, 3)
	b[0] = byte(rp.DiffDataCenterCount + '0')
	b[1] = byte(rp.DiffRackCount + '0')
	b[2] = byte(rp.SameRackCount + '0')
	return string(b)
}

func (rp *ReplicaPlacement) GetCopyCount() int {
	return rp.DiffDataCenterCount + rp.DiffRackCount + rp.SameRackCount + 1
}

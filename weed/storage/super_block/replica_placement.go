package super_block

import (
	"fmt"
)

type ReplicaPlacement struct {
	SameRackCount       int `json:"node,omitempty"`
	DiffRackCount       int `json:"rack,omitempty"`
	DiffDataCenterCount int `json:"dc,omitempty"`
}

func NewReplicaPlacementFromString(t string) (*ReplicaPlacement, error) {
	rp := &ReplicaPlacement{}
	switch len(t) {
	case 0:
		t = "000"
	case 1:
		t = "00" + t
	case 2:
		t = "0" + t
	}
	for i, c := range t {
		count := int(c - '0')
		if count < 0 {
			return rp, fmt.Errorf("unknown replication type: %s", t)
		}
		switch i {
		case 0:
			rp.DiffDataCenterCount = count
		case 1:
			rp.DiffRackCount = count
		case 2:
			rp.SameRackCount = count
		}
	}
	value := rp.DiffDataCenterCount*100 + rp.DiffRackCount*10 + rp.SameRackCount
	if value > 255 {
		return rp, fmt.Errorf("unexpected replication type: %s", t)
	}
	return rp, nil
}

func NewReplicaPlacementFromByte(b byte) (*ReplicaPlacement, error) {
	return NewReplicaPlacementFromString(fmt.Sprintf("%03d", b))
}

func (rp *ReplicaPlacement) HasReplication() bool {
	return rp.DiffDataCenterCount != 0 || rp.DiffRackCount != 0 || rp.SameRackCount != 0
}

func (a *ReplicaPlacement) Equals(b *ReplicaPlacement) bool {
	if a == nil || b == nil {
		return false
	}
	return (a.SameRackCount == b.SameRackCount &&
		a.DiffRackCount == b.DiffRackCount &&
		a.DiffDataCenterCount == b.DiffDataCenterCount)
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

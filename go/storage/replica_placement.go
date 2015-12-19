package storage

import (
	"encoding/json"
	"errors"
	"fmt"
)

type ReplicaPlacement struct {
	SameRackCount       int
	DiffRackCount       int
	DiffDataCenterCount int
}

type ReplicaPlacements struct {
	settings map[string]*ReplicaPlacement
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

func (rp *ReplicaPlacement) Equal(rp1 *ReplicaPlacement) bool {
	return rp.SameRackCount == rp1.SameRackCount &&
		rp.DiffRackCount == rp1.DiffRackCount &&
		rp.DiffDataCenterCount == rp1.DiffDataCenterCount
}

func NewReplicaPlacements(defaultRP string) *ReplicaPlacements {
	rp, e := NewReplicaPlacementFromString(defaultRP)
	if e != nil {
		rp, _ = NewReplicaPlacementFromString("000")
	}
	rps := &ReplicaPlacements{settings: make(map[string]*ReplicaPlacement)}
	rps.settings[""] = rp
	return rps
}

func NewReplicaPlacementsFromJson(s string) *ReplicaPlacements {
	m := make(map[string]*ReplicaPlacement)
	if json.Unmarshal([]byte(s), m) == nil {
		m[""], _ = NewReplicaPlacementFromString("000")
	}
	return &ReplicaPlacements{settings: m}
}

func (rps *ReplicaPlacements) Get(collection string) *ReplicaPlacement {
	if rp, ok := rps.settings[collection]; ok {
		return rp
	}
	return rps.settings[""]
}

func (rps *ReplicaPlacements) Set(collection, t string) error {
	rp, e := NewReplicaPlacementFromString(t)
	if e == nil {
		rps.settings[collection] = rp
	}
	return e
}

func (rps *ReplicaPlacements) Marshal() string {
	buf, _ := json.Marshal(rps.settings)
	return string(buf)
}

package topology

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

type VolumeLocationList struct {
	list []*DataNode
}

func NewVolumeLocationList() *VolumeLocationList {
	return &VolumeLocationList{}
}

func (dnll *VolumeLocationList) String() string {
	return fmt.Sprintf("%v", dnll.list)
}

func (dnll *VolumeLocationList) Copy() *VolumeLocationList {
	list := make([]*DataNode, len(dnll.list))
	copy(list, dnll.list)
	return &VolumeLocationList{
		list: list,
	}
}

func (dnll *VolumeLocationList) Head() *DataNode {
	//mark first node as master volume
	if dnll.Length() == 0 {
		return nil
	}
	return dnll.list[0]
}

func (dnll *VolumeLocationList) Rest() []*DataNode {
	//mark first node as master volume
	return dnll.list[1:]
}

func (dnll *VolumeLocationList) Length() int {
	if dnll == nil {
		return 0
	}
	return len(dnll.list)
}

// Set adds loc, or replaces the entry at the same address, returning the node it
// displaced. Two volume servers can share an address -- GetOrCreateDataNode keys
// on the reported id and refuses to merge a new id onto an address an older node
// still claims -- so the displaced node is not necessarily loc, and callers
// tracking which node a volume is reachable through must move their bookkeeping
// off it rather than assume loc already owned the entry.
func (dnll *VolumeLocationList) Set(loc *DataNode) (displaced *DataNode) {
	for i := 0; i < len(dnll.list); i++ {
		if loc.Ip == dnll.list[i].Ip && loc.Port == dnll.list[i].Port {
			displaced = dnll.list[i]
			dnll.list[i] = loc
			return displaced
		}
	}
	dnll.list = append(dnll.list, loc)
	return nil
}

// Remove drops the entry at loc's address and returns the node removed, or nil
// if the volume was not reachable there. As with Set, the removed node is
// matched by address and need not be loc.
func (dnll *VolumeLocationList) Remove(loc *DataNode) (removed *DataNode) {
	for i, dnl := range dnll.list {
		if loc.Ip == dnl.Ip && loc.Port == dnl.Port {
			dnll.list = append(dnll.list[:i], dnll.list[i+1:]...)
			return dnl
		}
	}
	return nil
}

func (dnll *VolumeLocationList) Refresh(freshThreshHold int64) {
	var changed bool
	for _, dnl := range dnll.list {
		if dnl.LastSeen < freshThreshHold {
			changed = true
			break
		}
	}
	if changed {
		var l []*DataNode
		for _, dnl := range dnll.list {
			if dnl.LastSeen >= freshThreshHold {
				l = append(l, dnl)
			}
		}
		dnll.list = l
	}
}

// Stats returns logic size and count
func (dnll *VolumeLocationList) Stats(vid needle.VolumeId, freshThreshHold int64) (size uint64, fileCount int) {
	for _, dnl := range dnll.list {
		if dnl.LastSeen < freshThreshHold {
			vinfo, err := dnl.GetVolumesById(vid)
			if err == nil {
				return (vinfo.Size - vinfo.DeletedByteCount), vinfo.FileCount - vinfo.DeleteCount
			}
		}
	}
	return 0, 0
}

package topology

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/storage"
)

type VolumeLocationList struct {
	list []*DataNode
}

func NewVolumeLocationList() *VolumeLocationList {
	return &VolumeLocationList{}
}

func (vll *VolumeLocationList) String() string {
	return fmt.Sprintf("%v", vll.list)
}

func (vll *VolumeLocationList) Head() *DataNode {
	// mark first node as master volume
	return vll.list[0]
}

func (vll *VolumeLocationList) Length() int {
	return len(vll.list)
}

func (vll *VolumeLocationList) FindDataNodeByIpPort(ip string, port int) (*DataNode, int) {
	for i := 0; i < len(vll.list); i++ {
		if ip == vll.list[i].Ip && port == vll.list[i].Port {
			return vll.list[i], i
		}
	}

	return nil, -1
}
func (vll *VolumeLocationList) Set(loc *DataNode) {
	dataNode, i := vll.FindDataNodeByIpPort(loc.Ip, loc.Port)
	if dataNode != nil {
		vll.list[i] = loc
	} else {
		vll.list = append(vll.list, loc)
	}
}

func (vll *VolumeLocationList) Remove(loc *DataNode) bool {
	dataNode, i := vll.FindDataNodeByIpPort(loc.Ip, loc.Port)
	if dataNode != nil {
		vll.list = append(vll.list[:i], vll.list[i+1:]...)
		return true
	}
	return false
}

func (vll *VolumeLocationList) Refresh(freshThreshHold int64) {
	var changed bool
	for _, dnl := range vll.list {
		if dnl.LastSeen < freshThreshHold {
			changed = true
			break
		}
	}
	if changed {
		var l []*DataNode
		for _, dnl := range vll.list {
			if dnl.LastSeen >= freshThreshHold {
				l = append(l, dnl)
			}
		}
		vll.list = l
	}
}

func (vll *VolumeLocationList) Stats(vid storage.VolumeId, freshThreshHold int64) (size uint64, fileCount int) {
	for _, dnl := range vll.list {
		if dnl.LastSeen < freshThreshHold {
			vinfo, err := dnl.GetVolumesById(vid)
			if err == nil {
				return vinfo.Size - vinfo.DeletedByteCount, vinfo.FileCount - vinfo.DeleteCount
			}
		}
	}
	return 0, 0
}

func (vll *VolumeLocationList) HasReadOnly(id storage.VolumeId) (hasReadOnly, hasValue bool) {
	for _, dn := range vll.list {
		vi, err := dn.GetVolumesById(id)
		if err == nil {
			hasValue = true
		}

		if vi.IsReadOnly() {
			hasReadOnly = true
			break
		}
	}

	return
}

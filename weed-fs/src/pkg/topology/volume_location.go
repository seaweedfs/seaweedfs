package topology

import ()

type VolumeLocationList struct {
	list []*DataNode
}

func NewVolumeLocationList() *VolumeLocationList {
	return &VolumeLocationList{}
}

func (dnll *VolumeLocationList) Head() *DataNode {
	return dnll.list[0]
}

func (dnll *VolumeLocationList) Length() int {
  return len(dnll.list)
}

func (dnll *VolumeLocationList) Add(loc *DataNode) bool {
	for _, dnl := range dnll.list {
		if loc.Ip == dnl.Ip && loc.Port == dnl.Port {
			return false
		}
	}
	dnll.list = append(dnll.list, loc)
	return true
}

func (dnll *VolumeLocationList) Remove(loc *DataNode) bool {
  for i, dnl := range dnll.list {
    if loc.Ip == dnl.Ip && loc.Port == dnl.Port {
      dnll.list = append(dnll.list[:i],dnll.list[i+1:]...)
      return true
    }
  }
  return false
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

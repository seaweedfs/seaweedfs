package topology

import ()

type DataNodeLocationList struct {
	list []*DataNode
}

func NewDataNodeLocationList() *DataNodeLocationList {
	return &DataNodeLocationList{}
}

func (dnll *DataNodeLocationList) Head() *DataNode {
	return dnll.list[0]
}

func (dnll *DataNodeLocationList) Add(loc *DataNode) bool {
	for _, dnl := range dnll.list {
		if loc.Ip == dnl.Ip && loc.Port == dnl.Port {
			return false
		}
	}
	dnll.list = append(dnll.list, loc)
	return true
}

func (dnll *DataNodeLocationList) Refresh(freshThreshHold int64) {
	var changed bool
	for _, dnl := range dnll.list {
		if dnl.lastSeen < freshThreshHold {
			changed = true
			break
		}
	}
	if changed {
		var l []*DataNode
		for _, dnl := range dnll.list {
			if dnl.lastSeen >= freshThreshHold {
				l = append(l, dnl)
			}
		}
		dnll.list = l
	}
}

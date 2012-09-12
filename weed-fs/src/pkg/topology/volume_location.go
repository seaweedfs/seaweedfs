package topology

import ()

type DataNodeLocationList struct {
	list []*DataNode
}

func NewDataNodeLocationList() *DataNodeLocationList {
	return &DataNodeLocationList{}
}

func (dnll *DataNodeLocationList) Add(loc *DataNode) {
	for _, dnl := range dnll.list {
		if loc.Ip == dnl.Ip && loc.Port == dnl.Port {
			break
		}
	}
	dnll.list = append(dnll.list, loc)
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

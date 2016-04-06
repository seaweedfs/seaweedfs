package topology

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/storage"
	"math/rand"
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

func (dnll *VolumeLocationList) Head() *DataNode {
	//mark first node as master volume
	return dnll.list[0]
}

func (dnll *VolumeLocationList) PickForRead() *DataNode {
	return dnll.list[rand.Intn(len(dnll.list))]
}

func (dnll *VolumeLocationList) AllDataNode() []*DataNode {
	return dnll.list
}

func (dnll *VolumeLocationList) Duplicate() *VolumeLocationList {
	l := make([]*DataNode, len(dnll.list))
	copy(l, dnll.list)
	return &VolumeLocationList{list: l}
}

func (dnll *VolumeLocationList) Length() int {
	return len(dnll.list)
}

func (dnll *VolumeLocationList) Set(loc *DataNode) {
	for i := 0; i < len(dnll.list); i++ {
		if loc.Ip == dnll.list[i].Ip && loc.Port == dnll.list[i].Port {
			dnll.list[i] = loc
			return
		}
	}
	dnll.list = append(dnll.list, loc)
}

func (dnll *VolumeLocationList) Remove(loc *DataNode) bool {
	for i, dnl := range dnll.list {
		if loc.Ip == dnl.Ip && loc.Port == dnl.Port {
			dnll.list = append(dnll.list[:i], dnll.list[i+1:]...)
			return true
		}
	}
	return false
}

func (dnll *VolumeLocationList) Refresh(freshThreshHold int64) {
	var changed bool
	for _, dnl := range dnll.list {
		if dnl.LastSeen() < freshThreshHold {
			changed = true
			break
		}
	}
	if changed {
		var l []*DataNode
		for _, dnl := range dnll.list {
			if dnl.LastSeen() >= freshThreshHold {
				l = append(l, dnl)
			}
		}
		dnll.list = l
	}
}

// return all data centers, first is main data center
func (dnll *VolumeLocationList) DiffDataCenters() []Node {
	m := make(map[*DataCenter]int)
	maxCount := 0
	var mainDC *DataCenter
	for _, dn := range dnll.list {
		var dc *DataCenter
		if dc = dn.GetDataCenter(); dc == nil {
			continue
		}
		m[dc] = m[dc] + 1
		if m[dc] > maxCount {
			mainDC = dc
			maxCount = m[dc]
		}
	}
	dataCenters := make([]Node, 0, len(m))
	if mainDC != nil {
		dataCenters = append(dataCenters, mainDC)
	}
	for dc := range m {
		if dc != mainDC {
			dataCenters = append(dataCenters, dc)
		}
	}
	return dataCenters
}

// return all racks if data center set nil
func (dnll *VolumeLocationList) DiffRacks(mainDC *DataCenter) []Node {
	m := make(map[*Rack]int)
	maxCount := 0
	var mainRack *Rack
	for _, dn := range dnll.list {
		if mainDC != nil && dn.GetDataCenter() != mainDC {
			continue
		}
		var rack *Rack
		if rack = dn.GetRack(); rack == nil {
			continue
		}
		m[rack] = m[rack] + 1
		if m[rack] > maxCount {
			mainRack = rack
			maxCount = m[rack]
		}
	}
	racks := make([]Node, 0, len(m))
	if mainRack != nil {
		racks = append(racks, mainRack)
	}
	for rack := range m {
		if rack != mainRack {
			racks = append(racks, rack)
		}
	}
	return racks
}

func (dnll *VolumeLocationList) SameServers(mainRack *Rack) (servers []Node) {
	for _, dn := range dnll.list {
		if mainRack != nil && dn.GetRack() != mainRack {
			continue
		}
		var rack *Rack
		if rack = dn.GetRack(); rack == nil {
			continue
		}
		servers = append(servers, dn)
	}
	return servers
}

func (dnll *VolumeLocationList) CalcReplicaPlacement() (rp *storage.ReplicaPlacement) {
	var dcs, rs, ss []Node
	dcs = dnll.DiffDataCenters()
	if len(dcs) > 0 {
		rs = dnll.DiffRacks(dcs[0].(*DataCenter))
		if len(rs) > 0 {
			ss = dnll.SameServers(rs[0].(*Rack))
		}
	}

	rp = &storage.ReplicaPlacement{
		SameRackCount:       len(ss) - 1,
		DiffRackCount:       len(rs) - 1,
		DiffDataCenterCount: len(dcs) - 1,
	}
	return
}

func (dnll *VolumeLocationList) ContainsDataNode(n *DataNode) bool {
	for _, dn := range dnll.list {
		if dn == n {
			return true
		}
	}
	return false
}

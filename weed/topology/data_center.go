package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"slices"
	"strings"
)

type DataCenter struct {
	NodeImpl
}

func NewDataCenter(id string) *DataCenter {
	dc := &DataCenter{}
	dc.id = NodeId(id)
	dc.nodeType = "DataCenter"
	dc.diskUsages = newDiskUsages()
	dc.children = make(map[NodeId]Node)
	dc.NodeImpl.value = dc
	return dc
}

func (dc *DataCenter) GetOrCreateRack(rackName string) *Rack {
	dc.Lock()
	defer dc.Unlock()
	for _, c := range dc.children {
		rack := c.(*Rack)
		if string(rack.Id()) == rackName {
			return rack
		}
	}
	rack := NewRack(rackName)
	dc.doLinkChildNode(rack)
	return rack
}

type DataCenterInfo struct {
	Id    NodeId     `json:"Id"`
	Racks []RackInfo `json:"Racks"`
}

func (dc *DataCenter) ToInfo() (info DataCenterInfo) {
	info.Id = dc.Id()
	var racks []RackInfo
	for _, c := range dc.Children() {
		rack := c.(*Rack)
		racks = append(racks, rack.ToInfo())
	}

	slices.SortFunc(racks, func(a, b RackInfo) int {
		return strings.Compare(string(a.Id), string(b.Id))
	})
	info.Racks = racks
	return
}

func (dc *DataCenter) ToDataCenterInfo() *master_pb.DataCenterInfo {
	m := &master_pb.DataCenterInfo{
		Id:        string(dc.Id()),
		DiskInfos: dc.diskUsages.ToDiskInfo(),
	}
	for _, c := range dc.Children() {
		rack := c.(*Rack)
		m.RackInfos = append(m.RackInfos, rack.ToRackInfo())
	}
	return m
}

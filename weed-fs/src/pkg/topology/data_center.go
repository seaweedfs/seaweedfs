package topology

import (
)

type DataCenter struct {
	NodeImpl
	ipRange *IpRange
}

func NewDataCenter(id string) *DataCenter {
	dc := &DataCenter{}
	dc.id = NodeId(id)
	dc.nodeType = "DataCenter"
	dc.children = make(map[NodeId]Node)
  dc.NodeImpl.value = dc
	return dc
}

func (dc *DataCenter) MatchLocationRange(ip string) bool {
	if dc.ipRange == nil {
		return true
	}
	return dc.ipRange.Match(ip)
}

func (dc *DataCenter) GetOrCreateRack(ip string) *Rack {
  for _, c := range dc.Children() {
    rack := c.(*Rack)
    if rack.MatchLocationRange(ip) {
      return rack
    }
  }
  rack := NewRack("DefaultRack")
  dc.LinkChildNode(rack)
  return rack
}

func (dc *DataCenter) ToMap() interface{}{
  m := make(map[string]interface{})
  m["Max"] = dc.GetMaxVolumeCount()
  m["Free"] = dc.FreeSpace()
  var racks []interface{}
  for _, c := range dc.Children() {
    rack := c.(*Rack)
    racks = append(racks, rack.ToMap())
  }
  m["Racks"] = racks
  return m
}

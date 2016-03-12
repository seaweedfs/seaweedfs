package topology

type DataCenter struct {
	NodeImpl
}

func NewDataCenter(id string) *DataCenter {
	dc := &DataCenter{}
	dc.id = NodeId(id)
	dc.nodeType = "DataCenter"
	dc.children = make(map[NodeId]Node)
	dc.NodeImpl.value = dc
	return dc
}

func (dc *DataCenter) GetOrCreateRack(rackName string) *Rack {
	n := dc.GetChildren(NodeId(rackName))
	if n != nil {
		return n.(*Rack)
	}
	rack := NewRack(rackName)
	dc.LinkChildNode(rack)
	return rack
}

func (dc *DataCenter) ToMap() interface{} {
	m := make(map[string]interface{})
	m["Id"] = dc.Id()
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

package topology

func (t *Topology) ToMap() interface{} {
	m := make(map[string]interface{})
	m["Max"] = t.GetMaxVolumeCount()
	m["Free"] = t.FreeSpace()
	var dcs []interface{}
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		dcs = append(dcs, dc.ToMap())
	}
	m["DataCenters"] = dcs
	var layouts []interface{}
	for i1 := range t.collectionMap.IterItems() {
		c := i1.Value.(*Collection)
		for i2 := range c.storageType2VolumeLayout.IterItems() {
			if i2.Value == nil {
				continue
			}
			tmp := i2.Value.(*VolumeLayout).ToMap()
			tmp["collection"] = c.Name
			layouts = append(layouts, tmp)
		}
	}
	m["layouts"] = layouts
	return m
}

func (t *Topology) ToVolumeMap() interface{} {
	m := make(map[string]interface{})
	m["Max"] = t.GetMaxVolumeCount()
	m["Free"] = t.FreeSpace()
	dcs := make(map[NodeId]interface{})
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		racks := make(map[NodeId]interface{})
		for _, r := range dc.Children() {
			rack := r.(*Rack)
			dataNodes := make(map[NodeId]interface{})
			for _, d := range rack.Children() {
				dn := d.(*DataNode)
				var volumes []interface{}
				for _, v := range dn.Volumes() {
					volumes = append(volumes, v)
				}
				dataNodes[d.Id()] = volumes
			}
			racks[r.Id()] = dataNodes
		}
		dcs[dc.Id()] = racks
	}
	m["DataCenters"] = dcs
	return m
}

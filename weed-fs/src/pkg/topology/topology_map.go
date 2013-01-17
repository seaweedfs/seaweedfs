package topology

import (
)

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
	for _, layout := range t.replicaType2VolumeLayout {
		if layout != nil {
			layouts = append(layouts, layout.ToMap())
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
				for _, v := range dn.volumes {
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

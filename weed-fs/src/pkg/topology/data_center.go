package topology

import (
	"pkg/storage"
)

type DataCenterId string
type DataCenter struct {
	Id      DataCenterId
	racks   map[RackId]*Rack
	ipRange IpRange

	//transient
	allocation StorageCapacity
	topology   *Topology
}

func (d *DataCenter) CreateOneVolume(r int, vid storage.VolumeId) storage.VolumeId {
	for _, rack := range d.racks {
		freeSpace := rack.allocation.maxVolumeCount - rack.allocation.countVolumeCount
		if r > freeSpace {
			r -= freeSpace
		} else {
			rack.CreateOneVolume(r, vid)
		}
	}
	return vid
}
func (d *DataCenter) AddVolume(rack *Rack, v *storage.VolumeInfo) {
	d.allocation.countVolumeCount += 1
	d.topology.AddVolume(d, v)
}
func (d *DataCenter) AddNode(rack *Rack, n *Node) {
	d.allocation.countVolumeCount += len(n.volumes)
	d.allocation.maxVolumeCount += n.maxVolumeCount
	d.topology.AddNode(d, n)
}
func (d *DataCenter) RemoveNode(rack *Rack, n *Node) {
	d.allocation.countVolumeCount -= len(n.volumes)
	d.allocation.maxVolumeCount -= n.maxVolumeCount
	d.topology.RemoveNode(d, n)
}
func (d *DataCenter) AddRack(rack *Rack) {
	d.racks[rack.Id] = rack
	d.allocation.countVolumeCount += rack.allocation.countVolumeCount
	d.allocation.maxVolumeCount += rack.allocation.maxVolumeCount
	d.topology.AddRack(d, rack)
}
func (d *DataCenter) RemoveRack(rack *Rack) {
	delete(d.racks, rack.Id)
	d.allocation.countVolumeCount -= rack.allocation.countVolumeCount
	d.allocation.maxVolumeCount -= rack.allocation.maxVolumeCount
	d.topology.AddRack(d, rack)
}

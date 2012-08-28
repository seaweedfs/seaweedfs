package topology

import (
	"math/rand"
	"pkg/storage"
)

type Topology struct {
	datacenters map[DataCenterId]*DataCenter

	//transient
	allocation StorageCapacity
}

//FIXME
func (t *Topology) RandomlyCreateOneVolume() storage.VolumeId {
	r := rand.Intn(t.allocation.maxVolumeCount-t.allocation.countVolumeCount)
  vid := t.nextVolumeId()
	for _, d := range t.datacenters {
	  freeSpace := d.allocation.maxVolumeCount-d.allocation.countVolumeCount
	  if r>freeSpace{
	    r -= freeSpace
	  }else{
	    d.CreateOneVolume(r, vid)
	    return vid
	  }
	}
	return storage.VolumeId(0) //FIXME
}
func (t *Topology) nextVolumeId() storage.VolumeId {
	return storage.VolumeId(0)
}
func (t *Topology) AddVolume(d *DataCenter, v *storage.VolumeInfo) {
	t.allocation.countVolumeCount += 1
}
func (t *Topology) AddNode(d *DataCenter, n *Node){
  t.allocation.countVolumeCount += len(n.volumes)
  t.allocation.maxVolumeCount += n.maxVolumeCount
}
func (t *Topology) RemoveNode(d *DataCenter, n *Node){
  t.allocation.countVolumeCount -= len(n.volumes)
  t.allocation.maxVolumeCount -= n.maxVolumeCount
}
func (t *Topology) AddRack(d *DataCenter, rack *Rack){
  t.allocation.countVolumeCount += rack.allocation.countVolumeCount
  t.allocation.maxVolumeCount += rack.allocation.maxVolumeCount
}
func (t *Topology) RemoveRack(d *DataCenter, rack *Rack){
  t.allocation.countVolumeCount -= rack.allocation.countVolumeCount
  t.allocation.maxVolumeCount -= rack.allocation.maxVolumeCount
}
func (t *Topology) AddDataCenter(d *DataCenter) {
  t.datacenters[d.Id] = d
	t.allocation.countVolumeCount += d.allocation.countVolumeCount
	t.allocation.maxVolumeCount += d.allocation.maxVolumeCount
}
func (t *Topology) RemoveDataCenter(d *DataCenter) {
  delete(t.datacenters,d.Id)
  t.allocation.countVolumeCount -= d.allocation.countVolumeCount
  t.allocation.maxVolumeCount -= d.allocation.maxVolumeCount
}

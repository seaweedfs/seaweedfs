package topology

import (
  "pkg/storage"
)

type RackId uint32
type Rack struct {
	Id      RackId
	nodes   map[NodeId]*Node
	ipRange IpRange
	
  //transient
  allocation StorageCapacity
  dataCenter *DataCenter
}
func (rack *Rack) CreateOneVolume(r int, vid storage.VolumeId) storage.VolumeId {
  for _, node := range rack.nodes {
    freeSpace := node.maxVolumeCount - len(node.volumes)
    if r > freeSpace {
      r -= freeSpace
    } else {
      node.CreateOneVolume(r, vid)
    }
  }
  return vid
}
func (r *Rack) AddVolume(n *Node, v *storage.VolumeInfo){
  r.allocation.countVolumeCount += 1
  r.dataCenter.AddVolume(r,v)
}
func (r *Rack) AddNode(n *Node){
  r.nodes[n.Ip] = n
  r.allocation.countVolumeCount += len(n.volumes)
  r.allocation.maxVolumeCount += n.maxVolumeCount
  r.dataCenter.AddNode(r,n)
}
func (r *Rack) RemoveNode(n *Node){
  delete(r.nodes,n.Ip)
  r.allocation.countVolumeCount -= len(n.volumes)
  r.allocation.maxVolumeCount -= n.maxVolumeCount
  r.dataCenter.RemoveNode(r,n)
}

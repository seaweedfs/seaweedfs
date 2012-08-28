package topology

import (
  "pkg/storage"
)

type NodeId string
type Node struct {
	volumes     map[storage.VolumeId]*storage.VolumeInfo
	maxVolumeCount int
	Ip          NodeId
	Port        int
	PublicUrl   string
	
	//transient
	rack *Rack
}
func (n *Node) CreateOneVolume(r int, vid storage.VolumeId) storage.VolumeId {
  n.AddVolume(&storage.VolumeInfo{Id:vid, Size: 32*1024*1024*1024})
  return vid
}
func (n *Node) AddVolume(v *storage.VolumeInfo){
  n.volumes[v.Id] = v
  n.rack.AddVolume(n,v)
}

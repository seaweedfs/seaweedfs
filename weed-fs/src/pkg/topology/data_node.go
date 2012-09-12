package topology

import (
	_ "fmt"
	"pkg/storage"
)

type DataNode struct {
	NodeImpl
	volumes   map[storage.VolumeId]*storage.VolumeInfo
	Ip        string
	Port      int
	PublicUrl string
	lastSeen  int64 // unix time in seconds
}

func NewDataNode(id string) *DataNode {
	s := &DataNode{}
	s.id = NodeId(id)
	s.nodeType = "DataNode"
	s.volumes = make(map[storage.VolumeId]*storage.VolumeInfo)
	return s
}
func (dn *DataNode) CreateOneVolume(r int, vid storage.VolumeId) storage.VolumeId {
	dn.AddVolume(&storage.VolumeInfo{Id: vid, Size: 32 * 1024 * 1024 * 1024})
	return vid
}
func (dn *DataNode) AddVolume(v *storage.VolumeInfo) {
	dn.volumes[v.Id] = v
	dn.UpAdjustActiveVolumeCountDelta(1)
	dn.UpAdjustMaxVolumeId(v.Id)
	dn.GetTopology().RegisterVolume(v,dn)
}
func (dn *DataNode) GetTopology() *Topology {
  p := dn.parent
  for p.Parent()!=nil{
    p = p.Parent()
  }
  t := p.(*Topology)
  return t
}

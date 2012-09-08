package topology

import (
	_ "fmt"
	"pkg/storage"
)

type DataNode struct {
	NodeImpl
	volumes   map[storage.VolumeId]*storage.VolumeInfo
}

func NewDataNode(id string) *DataNode {
	s := &DataNode{}
	s.id = NodeId(id)
	s.nodeType = "DataNode"
	s.volumes = make(map[storage.VolumeId]*storage.VolumeInfo)
	return s
}
func (s *DataNode) CreateOneVolume(r int, vid storage.VolumeId) storage.VolumeId {
	s.AddVolume(&storage.VolumeInfo{Id: vid, Size: 32 * 1024 * 1024 * 1024})
	return vid
}
func (s *DataNode) AddVolume(v *storage.VolumeInfo) {
	s.volumes[v.Id] = v
	s.UpAdjustActiveVolumeCountDelta(1)
	s.UpAdjustMaxVolumeId(v.Id)
}

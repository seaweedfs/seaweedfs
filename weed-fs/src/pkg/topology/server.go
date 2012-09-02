package topology

import (
	_ "fmt"
	"pkg/storage"
)

type Server struct {
	NodeImpl
	volumes   map[storage.VolumeId]*storage.VolumeInfo
	Ip        NodeId
	Port      int
	PublicUrl string
}

func NewServer(id string) *Server {
	s := &Server{}
	s.id = NodeId(id)
	s.nodeType = "Server"
	s.volumes = make(map[storage.VolumeId]*storage.VolumeInfo)
	return s
}
func (s *Server) CreateOneVolume(r int, vid storage.VolumeId) storage.VolumeId {
	s.AddVolume(&storage.VolumeInfo{Id: vid, Size: 32 * 1024 * 1024 * 1024})
	return vid
}
func (s *Server) AddVolume(v *storage.VolumeInfo) {
	s.volumes[v.Id] = v
	s.UpAdjustActiveVolumeCountDelta(1)
	s.UpAdjustMaxVolumeId(v.Id)
}

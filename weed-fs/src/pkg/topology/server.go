package topology

import (
  "pkg/storage"
)

type Server struct {
  Node
	volumes     map[storage.VolumeId]*storage.VolumeInfo
	Ip          NodeId
	Port        int
	PublicUrl   string
}
func (s *Server) CreateOneVolume(r int, vid storage.VolumeId) storage.VolumeId {
  s.AddVolume(&storage.VolumeInfo{Id:vid, Size: 32*1024*1024*1024})
  return vid
}
func (s *Server) AddVolume(v *storage.VolumeInfo){
  s.volumes[v.Id] = v
  s.Node.AddVolume(v)
}

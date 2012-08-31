package topology

import (
  "pkg/storage"
  _ "fmt"
)

type Server struct {
  Node
	volumes     map[storage.VolumeId]*storage.VolumeInfo
	Ip          NodeId
	Port        int
	PublicUrl   string
}
func NewServer(id NodeId) *Server{
  s := &Server{}
  s.Node.Id = id
  s.volumes = make(map[storage.VolumeId]*storage.VolumeInfo)
  return s
}
func (s *Server) CreateOneVolume(r int, vid storage.VolumeId) storage.VolumeId {
  s.AddVolume(&storage.VolumeInfo{Id:vid, Size: 32*1024*1024*1024})
  return vid
}
func (s *Server) AddVolume(v *storage.VolumeInfo){
  s.volumes[v.Id] = v
  s.Node.AddVolume(v)
}

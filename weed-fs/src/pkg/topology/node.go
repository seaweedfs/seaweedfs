package topology

import (
  "pkg/storage"
)

type NodeId uint32
type Node struct {
	volumes     map[storage.VolumeId]storage.VolumeInfo
	volumeLimit int
	Ip          string
	Port        int
	PublicUrl   string
	
	//transient
	allocation *Allocation
}
type Allocation struct {
  count int
  limit int
}

func (n *Node) GetAllocation() *Allocation{
  if n.allocation == nil {
    n.allocation = &Allocation{count:len(n.volumes), limit : n.volumeLimit}
  }
  return n.allocation
}


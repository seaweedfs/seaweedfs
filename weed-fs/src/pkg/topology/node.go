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
}

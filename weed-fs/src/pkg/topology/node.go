package topology

import ()

type NodeId uint32
type VolumeId uint32
type VolumeInfo struct {
	Id   VolumeId
	Size int64
}
type Node struct {
	volumes     map[VolumeId]VolumeInfo
	volumeLimit int
	Ip          string
	Port        int
	PublicUrl   string
}

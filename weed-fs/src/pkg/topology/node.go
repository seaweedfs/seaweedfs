package topology

import (

)

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

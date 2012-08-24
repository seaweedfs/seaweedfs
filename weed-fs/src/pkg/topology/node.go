package topology

import (

)

type VolumeInfo struct {
  Id   uint32
  Size int64
}
type Node struct {
  volumes     map[uint64]VolumeInfo
  volumeLimit int
  Port        int
  PublicUrl   string
}

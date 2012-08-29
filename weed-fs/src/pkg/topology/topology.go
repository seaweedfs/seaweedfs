package topology

import (
  "math/rand"
	"pkg/storage"
)

type Topology struct {
  Node
}

func (t *Topology) RandomlyReserveOneVolume() (bool,storage.VolumeId) {
  r := rand.Intn(t.Node.maxVolumeCount-t.Node.countVolumeCount-t.Node.reservedVolumeCount)
  vid := t.nextVolumeId()
  return t.Node.ReserveOneVolume(r,vid), vid
}

func (t *Topology) nextVolumeId() storage.VolumeId {
  vid := t.Node.GetMaxVolumeId()
	return vid.Next()
}

package topology

import (
  "math/rand"
	"pkg/storage"
	"fmt"
)

type Topology struct {
  Node
}
func NewTopology(id NodeId) *Topology{
  t := &Topology{}
  t.Node = *NewNode()
  t.Node.Id = id
  return t
}

func (t *Topology) RandomlyReserveOneVolume() (bool, *Node, storage.VolumeId) {
  slots := t.Node.maxVolumeCount-t.Node.activeVolumeCount
  r := rand.Intn(slots)
  fmt.Println("slots:", slots, "random :", r)
  vid := t.nextVolumeId()
  ret, node := t.Node.ReserveOneVolume(r,vid)
  return ret, node, vid
}

func (t *Topology) nextVolumeId() storage.VolumeId {
  vid := t.Node.GetMaxVolumeId()
	return vid.Next()
}

package topology

import (
  "math/rand"
	"pkg/storage"
	_ "fmt"
)

type Topology struct {
  NodeImpl
}

func NewTopology(id string) *Topology {
  t := &Topology{}
  t.id = NodeId(id)
  t.nodeType = "Topology"
  t.children = make(map[NodeId]Node)
  return t
}
func (t *Topology) RandomlyReserveOneVolume() (bool, Node, storage.VolumeId) {
  slots := t.maxVolumeCount-t.activeVolumeCount
  r := rand.Intn(slots)
  vid := t.nextVolumeId()
  ret, node := t.ReserveOneVolume(r,vid)
  return ret, node, vid
}

func (t *Topology) nextVolumeId() storage.VolumeId {
  vid := t.GetMaxVolumeId()
	return vid.Next()
}

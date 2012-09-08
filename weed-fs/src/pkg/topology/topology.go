package topology

import (
	_ "fmt"
	"math/rand"
	"pkg/storage"
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
func (t *Topology) RandomlyReserveOneVolume() (bool, *DataNode, storage.VolumeId) {
	vid := t.NextVolumeId()
	ret, node := t.ReserveOneVolume(rand.Intn(t.FreeSpace()), vid)
  return ret, node, vid
}

func (t *Topology) RandomlyReserveOneVolumeExcept(except []Node) (bool, *DataNode, storage.VolumeId) {
  freeSpace := t.FreeSpace()
  for _, node := range except {
    freeSpace -= node.FreeSpace()
  }
  vid := t.NextVolumeId()
  ret, node := t.ReserveOneVolume(rand.Intn(freeSpace), vid)
  return ret, node, vid
}

func (t *Topology) NextVolumeId() storage.VolumeId {
	vid := t.GetMaxVolumeId()
	return vid.Next()
}

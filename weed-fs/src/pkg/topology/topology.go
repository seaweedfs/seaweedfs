package topology

import (
	"fmt"
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
func (t *Topology) RandomlyReserveOneVolume() (bool, *Server, storage.VolumeId) {
	vid := t.nextVolumeId()
	ret, node := t.ReserveOneVolume(rand.Intn(t.FreeSpace()), vid)
  fmt.Println("node.IsServer", node.IsServer())
  return ret, node, vid
}

func (t *Topology) nextVolumeId() storage.VolumeId {
	vid := t.GetMaxVolumeId()
	return vid.Next()
}

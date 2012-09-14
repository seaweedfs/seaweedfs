package topology

import (
	_ "fmt"
	"math/rand"
	"pkg/sequence"
	"pkg/storage"
)

type Topology struct {
	NodeImpl

	//transient vid~servers mapping for each replication type
	replicaType2VolumeLayout []*VolumeLayout

	pulse int64

	volumeSizeLimit uint64

	sequence sequence.Sequencer
}

func NewTopology(id string, dirname string, filename string, volumeSizeLimit uint64, pulse int) *Topology {
	t := &Topology{}
	t.id = NodeId(id)
	t.nodeType = "Topology"
	t.children = make(map[NodeId]Node)
	t.replicaType2VolumeLayout = make([]*VolumeLayout, storage.LengthRelicationType)
	t.pulse = int64(pulse)
	t.volumeSizeLimit = volumeSizeLimit
	t.sequence = sequence.NewSequencer(dirname, filename)
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

func (t *Topology) registerVolumeLayout(v *storage.VolumeInfo, dn *DataNode) {
  replicationTypeIndex := storage.GetReplicationLevelIndex(v)
  if t.replicaType2VolumeLayout[replicationTypeIndex] == nil {
    t.replicaType2VolumeLayout[replicationTypeIndex] = NewVolumeLayout(t.volumeSizeLimit, t.pulse)
  }
  t.replicaType2VolumeLayout[replicationTypeIndex].RegisterVolume(v, dn)
}

func (t *Topology) RegisterVolume(v *storage.VolumeInfo, ip string, port int, publicUrl string) {
	dc := t.GetOrCreateDataCenter(ip)
	rack := dc.GetOrCreateRack(ip)
	dn := rack.GetOrCreateDataNode(ip, port, publicUrl)
	dn.AddVolume(v)
  t.registerVolumeLayout(v,dn)
}

func (t *Topology) GetOrCreateDataCenter(ip string) *DataCenter{
  for _, c := range t.Children() {
    dc := c.(*DataCenter)
    if dc.MatchLocationRange(ip) {
      return dc
    }
  }
  dc := NewDataCenter("DefaultDataCenter")
  t.LinkChildNode(dc)
  return dc
}

package topology

import (
	"errors"
	"fmt"
	"math/rand"
	"pkg/directory"
	"pkg/sequence"
	"pkg/storage"
	"time"
)

type Topology struct {
	NodeImpl

	//transient vid~servers mapping for each replication type
	replicaType2VolumeLayout []*VolumeLayout

	pulse int64

	volumeSizeLimit uint64

	sequence sequence.Sequencer

	chanDeadDataNodes      chan *DataNode
	chanRecoveredDataNodes chan *DataNode
	chanFullVolumes        chan *storage.VolumeInfo
	chanIncomplemteVolumes chan *storage.VolumeInfo
	chanRecoveredVolumes   chan *storage.VolumeInfo
}

func NewTopology(id string, dirname string, filename string, volumeSizeLimit uint64, pulse int) *Topology {
	t := &Topology{}
	t.id = NodeId(id)
	t.nodeType = "Topology"
	t.NodeImpl.value = t
	t.children = make(map[NodeId]Node)
	t.replicaType2VolumeLayout = make([]*VolumeLayout, storage.LengthRelicationType)
	t.pulse = int64(pulse)
	t.volumeSizeLimit = volumeSizeLimit

	t.sequence = sequence.NewSequencer(dirname, filename)

	t.chanDeadDataNodes = make(chan *DataNode)
	t.chanRecoveredDataNodes = make(chan *DataNode)
	t.chanFullVolumes = make(chan *storage.VolumeInfo)
	t.chanIncomplemteVolumes = make(chan *storage.VolumeInfo)
	t.chanRecoveredVolumes = make(chan *storage.VolumeInfo)

	return t
}

func (t *Topology) RandomlyReserveOneVolume() (bool, *DataNode, *storage.VolumeId) {
	if t.FreeSpace() <= 0 {
		return false, nil, nil
	}
	vid := t.NextVolumeId()
	ret, node := t.ReserveOneVolume(rand.Intn(t.FreeSpace()), vid)
	return ret, node, &vid
}

func (t *Topology) RandomlyReserveOneVolumeExcept(except []Node) (bool, *DataNode, *storage.VolumeId) {
	freeSpace := t.FreeSpace()
	for _, node := range except {
		freeSpace -= node.FreeSpace()
	}
	if freeSpace <= 0 {
		return false, nil, nil
	}
	vid := t.NextVolumeId()
	ret, node := t.ReserveOneVolume(rand.Intn(freeSpace), vid)
	return ret, node, &vid
}

func (t *Topology) NextVolumeId() storage.VolumeId {
	vid := t.GetMaxVolumeId()
	return vid.Next()
}

func (t *Topology) PickForWrite(repType storage.ReplicationType, count int) (string, int, *DataNode, error) {
	replicationTypeIndex := repType.GetReplicationLevelIndex()
	if t.replicaType2VolumeLayout[replicationTypeIndex] == nil {
		t.replicaType2VolumeLayout[replicationTypeIndex] = NewVolumeLayout(repType, t.volumeSizeLimit, t.pulse)
	}
	vid, count, datanodes, err := t.replicaType2VolumeLayout[replicationTypeIndex].PickForWrite(count)
	if err != nil {
		return "", 0, nil, errors.New("No writable volumes avalable!")
	}
	fileId, count := t.sequence.NextFileId(count)
	return directory.NewFileId(*vid, fileId, rand.Uint32()).String(), count, datanodes.Head(), nil
}

func (t *Topology) GetVolumeLayout(repType storage.ReplicationType) *VolumeLayout {
	replicationTypeIndex := repType.GetReplicationLevelIndex()
	if t.replicaType2VolumeLayout[replicationTypeIndex] == nil {
		t.replicaType2VolumeLayout[replicationTypeIndex] = NewVolumeLayout(repType, t.volumeSizeLimit, t.pulse)
	}
	return t.replicaType2VolumeLayout[replicationTypeIndex]
}

func (t *Topology) RegisterVolumeLayout(v *storage.VolumeInfo, dn *DataNode) {
	t.GetVolumeLayout(v.RepType).RegisterVolume(v, dn)
}

func (t *Topology) RegisterVolumes(volumeInfos []storage.VolumeInfo, ip string, port int, publicUrl string, maxVolumeCount int) {
	dc := t.GetOrCreateDataCenter(ip)
	rack := dc.GetOrCreateRack(ip)
	dn := rack.GetOrCreateDataNode(ip, port, publicUrl, maxVolumeCount)
	for _, v := range volumeInfos {
		dn.AddOrUpdateVolume(v)
		t.RegisterVolumeLayout(&v, dn)
	}
}

func (t *Topology) GetOrCreateDataCenter(ip string) *DataCenter {
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

func (t *Topology) ToMap() interface{} {
	m := make(map[string]interface{})
	m["Free"] = t.FreeSpace()
	var dcs []interface{}
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		dcs = append(dcs, dc.ToMap())
	}
	m["DataCenters"] = dcs
	var layouts []interface{}
	for _, layout := range t.replicaType2VolumeLayout {
		if layout != nil {
			layouts = append(layouts, layout.ToMap())
		}
	}
	m["layouts"] = layouts
	return m
}

func (t *Topology) StartRefreshWritableVolumes() {
	go func() {
		for {
			freshThreshHold := time.Now().Unix() - 3*t.pulse //5 times of sleep interval
			t.CollectDeadNodeAndFullVolumes(freshThreshHold, t.volumeSizeLimit)
			time.Sleep(time.Duration(float32(t.pulse*1e3)*(1+rand.Float32())) * time.Millisecond)
		}
	}()
	go func() {
		for {
			select {
			case v := <-t.chanIncomplemteVolumes:
				fmt.Println("Volume", v, "is incomplete!")
			case v := <-t.chanRecoveredVolumes:
				fmt.Println("Volume", v, "is recovered!")
			case v := <-t.chanFullVolumes:
				t.SetVolumeReadOnly(v)
				fmt.Println("Volume", v, "is full!")
			case dn := <-t.chanRecoveredDataNodes:
				t.RegisterRecoveredDataNode(dn)
				fmt.Println("DataNode", dn, "is back alive!")
			case dn := <-t.chanDeadDataNodes:
				t.UnRegisterDataNode(dn)
				fmt.Println("DataNode", dn, "is dead!")
			}
		}
	}()
}
func (t *Topology) SetVolumeReadOnly(volumeInfo *storage.VolumeInfo) {
	vl := t.GetVolumeLayout(volumeInfo.RepType)
	vl.SetVolumeReadOnly(volumeInfo.Id)
}
func (t *Topology) SetVolumeWritable(volumeInfo *storage.VolumeInfo) {
	vl := t.GetVolumeLayout(volumeInfo.RepType)
	vl.SetVolumeWritable(volumeInfo.Id)
}
func (t *Topology) UnRegisterDataNode(dn *DataNode) {
	for _, v := range dn.volumes {
		fmt.Println("Removing Volume", v.Id, "from the dead volume server", dn)
		t.SetVolumeReadOnly(&v)
	}
}
func (t *Topology) RegisterRecoveredDataNode(dn *DataNode) {
	for _, v := range dn.volumes {
		if uint64(v.Size) < t.volumeSizeLimit {
			t.SetVolumeWritable(&v)
		}
	}
}

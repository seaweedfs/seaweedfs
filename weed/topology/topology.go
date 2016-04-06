package topology

import (
	"errors"
	"io/ioutil"
	"math/rand"

	"strconv"
	"time"

	"github.com/chrislusf/raft"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/sequence"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/weedpb"
)

type Topology struct {
	NodeImpl

	collectionMap      *util.ConcurrentMap
	pulse              int64
	volumeSizeLimit    uint64
	joinKey            string
	Sequence           sequence.Sequencer
	CollectionSettings *storage.CollectionSettings
	configuration      *Configuration
	raftServer         raft.Server

	chanDeadDataNodes      chan *DataNode
	chanRecoveredDataNodes chan *DataNode
	chanFullVolumes        chan storage.VolumeInfo
}

func NewTopology(id string, confFile string, cs *storage.CollectionSettings, seq sequence.Sequencer, volumeSizeLimit uint64, pulse int) (*Topology, error) {
	t := &Topology{}
	t.id = NodeId(id)
	t.nodeType = "Topology"
	t.NodeImpl.value = t
	t.children = make(map[NodeId]Node)
	t.collectionMap = util.NewConcurrentMap()
	t.pulse = int64(pulse)
	t.volumeSizeLimit = volumeSizeLimit
	t.CollectionSettings = cs
	t.ReGenJoinKey()

	t.Sequence = seq

	t.chanDeadDataNodes = make(chan *DataNode)
	t.chanRecoveredDataNodes = make(chan *DataNode)
	t.chanFullVolumes = make(chan storage.VolumeInfo)

	err := t.loadConfiguration(confFile)

	return t, err
}

func (t *Topology) GetRaftServer() raft.Server {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.raftServer
}

func (t *Topology) SetRaftServer(s raft.Server) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.raftServer = s
}

func (t *Topology) IsLeader() bool {
	if leader, e := t.Leader(); e == nil {
		return leader == t.GetRaftServer().Name()
	}
	return false
}

func (t *Topology) Leader() (string, error) {
	l := ""
	if t.GetRaftServer() != nil {
		l = t.GetRaftServer().Leader()
	} else {
		return "", errors.New("Raft Server not ready yet!")
	}

	if l == "" {
		// We are a single node cluster, we are the leader
		return t.GetRaftServer().Name(), errors.New("Raft Server not initialized!")
	}

	return l, nil
}

func (t *Topology) GetJoinKey() string {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.joinKey
}

func (t *Topology) ReGenJoinKey() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.joinKey = strconv.FormatInt(time.Now().UnixNano(), 16)
}

func (t *Topology) GetVolumeSizeLimit() uint64 {
	// volumeSizeLimit is only for read
	//t.mutex.RLock()
	//defer t.mutex.RUnlock()
	return t.volumeSizeLimit
}

func (t *Topology) loadConfiguration(configurationFile string) error {
	b, e := ioutil.ReadFile(configurationFile)
	if e == nil {
		t.configuration, e = NewConfiguration(b)
		return e
	}
	glog.V(0).Infoln("Using default configurations.")
	return nil
}

func (t *Topology) Lookup(collection string, vid storage.VolumeId) (vl *VolumeLocationList) {
	//maybe an issue if lots of collections?
	if collection == "" {
		t.collectionMap.Walk(func(k string, c interface{}) (e error) {
			if list := c.(*Collection).Lookup(vid); list != nil {
				vl = list
				return util.ErrBreakWalk
			}
			return nil
		})

	} else {
		if c, _ := t.collectionMap.Get(collection); c != nil {
			return c.(*Collection).Lookup(vid)
		}
	}
	return
}

func (t *Topology) NextVolumeId() storage.VolumeId {
	vid := t.GetMaxVolumeId()
	next := vid.Next()
	go t.GetRaftServer().Do(NewMaxVolumeIdCommand(next))
	return next
}

func (t *Topology) HasWritableVolume(option *VolumeGrowOption) bool {
	vl := t.GetVolumeLayout(option.Collection, option.Ttl)
	return vl.GetActiveVolumeCount(option) > 0
}

func (t *Topology) PickForWrite(count uint64, option *VolumeGrowOption) (string, uint64, *DataNode, error) {
	vid, count, dataNodes, err := t.GetVolumeLayout(option.Collection, option.Ttl).PickForWrite(count, option)
	if err != nil || dataNodes.Length() == 0 {
		return "", 0, nil, errors.New("No writable volumes available!")
	}
	fileId, count := t.Sequence.NextFileId(count)
	return storage.NewFileId(*vid, fileId, rand.Uint32()).String(), count, dataNodes.Head(), nil
}

func (t *Topology) GetVolumeLayout(collectionName string, ttl *storage.TTL) *VolumeLayout {
	return t.collectionMap.GetOrNew(collectionName, func() interface{} {
		return NewCollection(collectionName, t.CollectionSettings.GetReplicaPlacement(collectionName), t.volumeSizeLimit)
	}).(*Collection).GetOrCreateVolumeLayout(ttl)
}

func (t *Topology) GetCollection(collectionName string) (*Collection, bool) {
	c, hasCollection := t.collectionMap.Get(collectionName)
	return c.(*Collection), hasCollection
}

func (t *Topology) DeleteCollection(collectionName string) {
	t.collectionMap.Delete(collectionName)
}

func (t *Topology) RegisterVolumeLayout(v *storage.VolumeInfo, dn *DataNode) {
	t.GetVolumeLayout(v.Collection, v.Ttl).RegisterVolume(v, dn)
}
func (t *Topology) UnRegisterVolumeLayout(v *storage.VolumeInfo, dn *DataNode) {
	glog.Infof("removing volume info:%+v", v)
	t.GetVolumeLayout(v.Collection, v.Ttl).UnRegisterVolume(v, dn)
}

func (t *Topology) ProcessJoinMessage(joinMessage *weedpb.JoinMessage) {
	t.Sequence.SetMax(joinMessage.MaxFileKey)
	dcName, rackName := t.configuration.Locate(joinMessage.Ip, joinMessage.DataCenter, joinMessage.Rack)
	dc := t.GetOrCreateDataCenter(dcName)
	rack := dc.GetOrCreateRack(rackName)
	dn := rack.FindDataNode(joinMessage.Ip, int(joinMessage.Port))
	if joinMessage.IsInit && dn != nil {
		t.UnRegisterDataNode(dn)
	}
	dn = rack.GetOrCreateDataNode(joinMessage.Ip,
		int(joinMessage.Port), joinMessage.PublicUrl,
		int(joinMessage.MaxVolumeCount))
	var volumeInfos []*storage.VolumeInfo
	for _, v := range joinMessage.Volumes {
		if vi, err := storage.NewVolumeInfo(v); err == nil {
			volumeInfos = append(volumeInfos, vi)
		} else {
			glog.V(0).Infoln("Fail to convert joined volume information:", err.Error())
		}
	}

	deletedVolumes := dn.UpdateVolumes(volumeInfos)
	for _, v := range volumeInfos {
		t.RegisterVolumeLayout(v, dn)
	}
	for _, v := range deletedVolumes {
		t.UnRegisterVolumeLayout(v, dn)
	}

}

func (t *Topology) ProcessJoinMessageV2(joinMsgV2 *weedpb.JoinMessageV2) {
	t.Sequence.SetMax(joinMsgV2.MaxFileKey)
	dcName, rackName := t.configuration.Locate(joinMsgV2.Ip, joinMsgV2.DataCenter, joinMsgV2.Rack)
	dc := t.GetOrCreateDataCenter(dcName)
	rack := dc.GetOrCreateRack(rackName)
	dn := rack.FindDataNode(joinMsgV2.Ip, int(joinMsgV2.Port))
	if joinMsgV2.JoinKey == "" && dn != nil {
		t.UnRegisterDataNode(dn)
	}
	dn = rack.GetOrCreateDataNode(joinMsgV2.Ip,
		int(joinMsgV2.Port), joinMsgV2.PublicUrl,
		int(joinMsgV2.MaxVolumeCount))
	var volumeInfos []*storage.VolumeInfo
	for _, v := range joinMsgV2.Volumes {
		if vi, err := storage.NewVolumeInfo(v); err == nil {
			volumeInfos = append(volumeInfos, vi)
		} else {
			glog.V(0).Infoln("Fail to convert joined volume information:", err.Error())
		}
	}

	deletedVolumes := dn.UpdateVolumes(volumeInfos)
	for _, v := range volumeInfos {
		t.RegisterVolumeLayout(v, dn)
	}
	for _, v := range deletedVolumes {
		t.UnRegisterVolumeLayout(v, dn)
	}

}

func (t *Topology) GetOrCreateDataCenter(dcName string) *DataCenter {
	n := t.GetChildren(NodeId(dcName))
	if n != nil {
		return n.(*DataCenter)
	}
	dc := NewDataCenter(dcName)
	t.LinkChildNode(dc)
	return dc
}

type DataNodeWalker func(dn *DataNode) (e error)

func (t *Topology) WalkDataNode(walker DataNodeWalker) error {
	for _, c := range t.Children() {
		for _, rack := range c.(*DataCenter).Children() {
			for _, dn := range rack.(*Rack).Children() {
				if e := walker(dn.(*DataNode)); e != nil {
					return e
				}
			}
		}
	}
	return nil
}

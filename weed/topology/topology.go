package topology

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	backoff "github.com/cenkalti/backoff/v4"

	hashicorpRaft "github.com/hashicorp/raft"
	"github.com/seaweedfs/raft"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type Topology struct {
	vacuumLockCounter int64
	NodeImpl

	collectionMap  *util.ConcurrentReadMap
	ecShardMap     map[needle.VolumeId]*EcShardLocations
	ecShardMapLock sync.RWMutex

	pulse int64

	volumeSizeLimit  uint64
	replicationAsMin bool
	isDisableVacuum  bool

	Sequence sequence.Sequencer

	chanFullVolumes    chan storage.VolumeInfo
	chanCrowdedVolumes chan storage.VolumeInfo

	Configuration *Configuration

	RaftServer           raft.Server
	RaftServerAccessLock sync.RWMutex
	HashicorpRaft        *hashicorpRaft.Raft
	barrierLock          sync.Mutex
	barrierDone          bool

	UuidAccessLock sync.RWMutex
	UuidMap        map[string][]string

	LastLeaderChangeTime time.Time
}

func NewTopology(id string, seq sequence.Sequencer, volumeSizeLimit uint64, pulse int, replicationAsMin bool) *Topology {
	t := &Topology{}
	t.id = NodeId(id)
	t.nodeType = "Topology"
	t.NodeImpl.value = t
	t.diskUsages = newDiskUsages()
	t.children = make(map[NodeId]Node)
	t.collectionMap = util.NewConcurrentReadMap()
	t.ecShardMap = make(map[needle.VolumeId]*EcShardLocations)
	t.pulse = int64(pulse)
	t.volumeSizeLimit = volumeSizeLimit
	t.replicationAsMin = replicationAsMin

	t.Sequence = seq

	t.chanFullVolumes = make(chan storage.VolumeInfo)
	t.chanCrowdedVolumes = make(chan storage.VolumeInfo)

	t.Configuration = &Configuration{}

	return t
}

func (t *Topology) IsChildLocked() (bool, error) {
	if t.IsLocked() {
		return true, errors.New("topology is locked")
	}
	for _, dcNode := range t.Children() {
		if dcNode.IsLocked() {
			return true, fmt.Errorf("topology child %s is locked", dcNode.String())
		}
		for _, rackNode := range dcNode.Children() {
			if rackNode.IsLocked() {
				return true, fmt.Errorf("dc %s child %s is locked", dcNode.String(), rackNode.String())
			}
			for _, dataNode := range rackNode.Children() {
				if dataNode.IsLocked() {
					return true, fmt.Errorf("rack %s child %s is locked", rackNode.String(), dataNode.Id())
				}
			}
		}
	}
	return false, nil
}

func (t *Topology) IsLeader() bool {
	t.RaftServerAccessLock.RLock()
	defer t.RaftServerAccessLock.RUnlock()

	if t.RaftServer != nil {
		if t.RaftServer.State() == raft.Leader {
			return true
		}
		if leader, err := t.Leader(); err == nil {
			if pb.ServerAddress(t.RaftServer.Name()) == leader {
				return true
			}
		}
	} else if t.HashicorpRaft != nil {
		if t.HashicorpRaft.State() == hashicorpRaft.Leader {
			return true
		}
	}
	return false
}

func (t *Topology) IsLeaderAndCanRead() bool {
	if t.RaftServer != nil {
		return t.IsLeader()
	} else if t.HashicorpRaft != nil {
		return t.IsLeader() && t.DoBarrier()
	} else {
		return false
	}
}

func (t *Topology) DoBarrier() bool {
	t.barrierLock.Lock()
	defer t.barrierLock.Unlock()
	if t.barrierDone {
		return true
	}

	glog.V(0).Infof("raft do barrier")
	barrier := t.HashicorpRaft.Barrier(2 * time.Minute)
	if err := barrier.Error(); err != nil {
		glog.Errorf("failed to wait for barrier, error %s", err)
		return false

	}

	t.barrierDone = true
	glog.V(0).Infof("raft do barrier success")
	return true
}

func (t *Topology) BarrierReset() {
	t.barrierLock.Lock()
	defer t.barrierLock.Unlock()
	t.barrierDone = false
}

func (t *Topology) Leader() (l pb.ServerAddress, err error) {
	exponentialBackoff := backoff.NewExponentialBackOff()
	exponentialBackoff.InitialInterval = 100 * time.Millisecond
	exponentialBackoff.MaxElapsedTime = 20 * time.Second
	leaderNotSelected := errors.New("leader not selected yet")
	l, err = backoff.RetryWithData(
		func() (l pb.ServerAddress, err error) {
			l, err = t.MaybeLeader()
			if err == nil && l == "" {
				err = leaderNotSelected
			}
			return l, err
		},
		exponentialBackoff)
	if err == leaderNotSelected {
		l = ""
	}
	return l, err
}

func (t *Topology) MaybeLeader() (l pb.ServerAddress, err error) {
	t.RaftServerAccessLock.RLock()
	defer t.RaftServerAccessLock.RUnlock()

	if t.RaftServer != nil {
		l = pb.ServerAddress(t.RaftServer.Leader())
	} else if t.HashicorpRaft != nil {
		l = pb.ServerAddress(t.HashicorpRaft.Leader())
	} else {
		err = errors.New("Raft Server not ready yet!")
	}

	return
}

func (t *Topology) Lookup(collection string, vid needle.VolumeId) (dataNodes []*DataNode) {
	// maybe an issue if lots of collections?
	if collection == "" {
		for _, c := range t.collectionMap.Items() {
			if list := c.(*Collection).Lookup(vid); list != nil {
				return list
			}
		}
	} else {
		if c, ok := t.collectionMap.Find(collection); ok {
			return c.(*Collection).Lookup(vid)
		}
	}

	if locations, found := t.LookupEcShards(vid); found {
		for _, loc := range locations.Locations {
			dataNodes = append(dataNodes, loc...)
		}
		return dataNodes
	}

	return nil
}

func (t *Topology) NextVolumeId() (needle.VolumeId, error) {
	if !t.IsLeaderAndCanRead() {
		return 0, fmt.Errorf("as leader can not read yet")

	}
	vid := t.GetMaxVolumeId()
	next := vid.Next()

	t.RaftServerAccessLock.RLock()
	defer t.RaftServerAccessLock.RUnlock()

	if t.RaftServer != nil {
		if _, err := t.RaftServer.Do(NewMaxVolumeIdCommand(next)); err != nil {
			return 0, err
		}
	} else if t.HashicorpRaft != nil {
		b, err := json.Marshal(NewMaxVolumeIdCommand(next))
		if err != nil {
			return 0, fmt.Errorf("failed marshal NewMaxVolumeIdCommand: %+v", err)
		}
		if future := t.HashicorpRaft.Apply(b, time.Second); future.Error() != nil {
			return 0, future.Error()
		}
	}
	return next, nil
}

func (t *Topology) PickForWrite(requestedCount uint64, option *VolumeGrowOption, volumeLayout *VolumeLayout) (fileId string, count uint64, volumeLocationList *VolumeLocationList, shouldGrow bool, err error) {
	var vid needle.VolumeId
	vid, count, volumeLocationList, shouldGrow, err = volumeLayout.PickForWrite(requestedCount, option)
	if err != nil {
		return "", 0, nil, shouldGrow, fmt.Errorf("failed to find writable volumes for collection:%s replication:%s ttl:%s error: %v", option.Collection, option.ReplicaPlacement.String(), option.Ttl.String(), err)
	}
	if volumeLocationList == nil || volumeLocationList.Length() == 0 {
		return "", 0, nil, shouldGrow, fmt.Errorf("%s available for collection:%s replication:%s ttl:%s", NoWritableVolumes, option.Collection, option.ReplicaPlacement.String(), option.Ttl.String())
	}
	nextFileId := t.Sequence.NextFileId(requestedCount)
	fileId = needle.NewFileId(vid, nextFileId, rand.Uint32()).String()
	return fileId, count, volumeLocationList, shouldGrow, nil
}

func (t *Topology) GetVolumeLayout(collectionName string, rp *super_block.ReplicaPlacement, ttl *needle.TTL, diskType types.DiskType) *VolumeLayout {
	return t.collectionMap.Get(collectionName, func() interface{} {
		return NewCollection(collectionName, t.volumeSizeLimit, t.replicationAsMin)
	}).(*Collection).GetOrCreateVolumeLayout(rp, ttl, diskType)
}

func (t *Topology) ListCollections(includeNormalVolumes, includeEcVolumes bool) (ret []string) {

	mapOfCollections := make(map[string]bool)
	for _, c := range t.collectionMap.Items() {
		mapOfCollections[c.(*Collection).Name] = true
	}

	if includeEcVolumes {
		t.ecShardMapLock.RLock()
		for _, ecVolumeLocation := range t.ecShardMap {
			mapOfCollections[ecVolumeLocation.Collection] = true
		}
		t.ecShardMapLock.RUnlock()
	}

	for k := range mapOfCollections {
		ret = append(ret, k)
	}
	return ret
}

func (t *Topology) FindCollection(collectionName string) (*Collection, bool) {
	c, hasCollection := t.collectionMap.Find(collectionName)
	if !hasCollection {
		return nil, false
	}
	return c.(*Collection), hasCollection
}

func (t *Topology) DeleteCollection(collectionName string) {
	t.collectionMap.Delete(collectionName)
}

func (t *Topology) DeleteLayout(collectionName string, rp *super_block.ReplicaPlacement, ttl *needle.TTL, diskType types.DiskType) {
	collection, found := t.FindCollection(collectionName)
	if !found {
		return
	}
	collection.DeleteVolumeLayout(rp, ttl, diskType)
	if len(collection.storageType2VolumeLayout.Items()) == 0 {
		t.DeleteCollection(collectionName)
	}
}

func (t *Topology) RegisterVolumeLayout(v storage.VolumeInfo, dn *DataNode) {
	diskType := types.ToDiskType(v.DiskType)
	vl := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
	vl.RegisterVolume(&v, dn)
	vl.EnsureCorrectWritables(&v)
}
func (t *Topology) UnRegisterVolumeLayout(v storage.VolumeInfo, dn *DataNode) {
	glog.Infof("removing volume info: %+v from %v", v, dn.id)
	if v.ReplicaPlacement.GetCopyCount() > 1 {
		stats.MasterReplicaPlacementMismatch.WithLabelValues(v.Collection, v.Id.String()).Set(0)
	}
	diskType := types.ToDiskType(v.DiskType)
	volumeLayout := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
	volumeLayout.UnRegisterVolume(&v, dn)
	if volumeLayout.isEmpty() {
		t.DeleteLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
	}
}

func (t *Topology) DataCenterExists(dcName string) bool {
	return dcName == "" || t.GetDataCenter(dcName) != nil
}

func (t *Topology) GetDataCenter(dcName string) (dc *DataCenter) {
	t.RLock()
	defer t.RUnlock()
	for _, c := range t.children {
		dc = c.(*DataCenter)
		if string(dc.Id()) == dcName {
			return dc
		}
	}
	return dc
}

func (t *Topology) GetOrCreateDataCenter(dcName string) *DataCenter {
	t.Lock()
	defer t.Unlock()
	for _, c := range t.children {
		dc := c.(*DataCenter)
		if string(dc.Id()) == dcName {
			return dc
		}
	}
	dc := NewDataCenter(dcName)
	t.doLinkChildNode(dc)
	return dc
}

func (t *Topology) ListDataCenters() (dcs []string) {
	t.RLock()
	defer t.RUnlock()
	for _, c := range t.children {
		dcs = append(dcs, string(c.(*DataCenter).Id()))
	}
	return dcs
}

func (t *Topology) ListDCAndRacks() (dcs map[NodeId][]NodeId) {
	t.RLock()
	defer t.RUnlock()
	dcs = make(map[NodeId][]NodeId)
	for _, dcNode := range t.children {
		dcNodeId := dcNode.(*DataCenter).Id()
		for _, rackNode := range dcNode.Children() {
			dcs[dcNodeId] = append(dcs[dcNodeId], rackNode.(*Rack).Id())
		}
	}
	return dcs
}

func (t *Topology) SyncDataNodeRegistration(volumes []*master_pb.VolumeInformationMessage, dn *DataNode) (newVolumes, deletedVolumes []storage.VolumeInfo) {
	// convert into in memory struct storage.VolumeInfo
	var volumeInfos []storage.VolumeInfo
	for _, v := range volumes {
		if vi, err := storage.NewVolumeInfo(v); err == nil {
			volumeInfos = append(volumeInfos, vi)
		} else {
			glog.V(0).Infof("Fail to convert joined volume information: %v", err)
		}
	}
	// find out the delta volumes
	var changedVolumes []storage.VolumeInfo
	newVolumes, deletedVolumes, changedVolumes = dn.UpdateVolumes(volumeInfos)
	for _, v := range newVolumes {
		t.RegisterVolumeLayout(v, dn)
	}
	for _, v := range deletedVolumes {
		t.UnRegisterVolumeLayout(v, dn)
	}
	for _, v := range changedVolumes {
		diskType := types.ToDiskType(v.DiskType)
		vl := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
		vl.EnsureCorrectWritables(&v)
	}
	return
}

func (t *Topology) IncrementalSyncDataNodeRegistration(newVolumes, deletedVolumes []*master_pb.VolumeShortInformationMessage, dn *DataNode) {
	var newVis, oldVis []storage.VolumeInfo
	for _, v := range newVolumes {
		vi, err := storage.NewVolumeInfoFromShort(v)
		if err != nil {
			glog.V(0).Infof("NewVolumeInfoFromShort %v: %v", v, err)
			continue
		}
		newVis = append(newVis, vi)
	}
	for _, v := range deletedVolumes {
		vi, err := storage.NewVolumeInfoFromShort(v)
		if err != nil {
			glog.V(0).Infof("NewVolumeInfoFromShort %v: %v", v, err)
			continue
		}
		oldVis = append(oldVis, vi)
	}
	dn.DeltaUpdateVolumes(newVis, oldVis)

	for _, vi := range newVis {
		t.RegisterVolumeLayout(vi, dn)
	}
	for _, vi := range oldVis {
		t.UnRegisterVolumeLayout(vi, dn)
	}

	return
}

func (t *Topology) DataNodeRegistration(dcName, rackName string, dn *DataNode) {
	if dn.Parent() != nil {
		return
	}
	// registration to topo
	dc := t.GetOrCreateDataCenter(dcName)
	rack := dc.GetOrCreateRack(rackName)
	rack.LinkChildNode(dn)
	glog.Infof("[%s] reLink To topo  ", dn.Id())
}

func (t *Topology) DisableVacuum() {
	glog.V(0).Infof("DisableVacuum")
	t.isDisableVacuum = true
}

func (t *Topology) EnableVacuum() {
	glog.V(0).Infof("EnableVacuum")
	t.isDisableVacuum = false
}

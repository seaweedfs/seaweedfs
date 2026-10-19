package topology

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
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

const (
	// WarmupPulseMultiplier is the number of heartbeat intervals to wait after
	// a leader change before treating volume lookup misses as definitive.
	WarmupPulseMultiplier = 3
)

type Topology struct {
	vacuumLockCounter int64
	NodeImpl

	collectionMap  *util.ConcurrentReadMap
	ecShardMap     map[needle.VolumeId]*EcShardLocations
	ecShardMapLock sync.RWMutex

	pulse int64

	volumeSizeLimit          uint64
	replicationAsMin         bool
	vacuumDisabledByOperator atomic.Bool // true when operator manually disables vacuum
	vacuumDisabledByPlugin   atomic.Bool // true when disabled by the vacuum plugin monitor
	adminServerConnectedFunc func() bool // optional callback to check admin server presence

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

	topologyId     string
	topologyIdLock sync.RWMutex

	lastLeaderChangeTime     time.Time
	hadVolumesAtLeaderChange bool
	lastLeaderChangeTimeLock sync.RWMutex

	// dataNodeIndex is an address -> *DataNode lookup so callers (e.g. the
	// Ping admission gate) do not have to walk every dc/rack/node tier on
	// every request. Keys use the canonical http form returned by
	// pb.ServerAddress.ToHttpAddress so a target like "1.2.3.4:8080" finds
	// the same node whether or not the grpc port suffix is present.
	dataNodeIndex     map[string]*DataNode
	dataNodeIndexLock sync.RWMutex

	maxVolumeIdFile          string
	maxVolumeIdFileLock      sync.Mutex
	lastPersistedMaxVolumeId atomic.Uint32
	lastPersistedTopologyId  atomic.Value

	growthFrozen          atomic.Bool
	growthFrozenAt        atomic.Int64
	freezeOnMismatch      atomic.Bool
	stateLossRecoveryWait atomic.Int64
	allocationNotBefore   atomic.Int64
	topologyInitPending   atomic.Bool
}

func NewTopology(id string, seq sequence.Sequencer, volumeSizeLimit uint64, pulse int, replicationAsMin bool) *Topology {
	t := &Topology{}
	t.id = NodeId(id)
	t.nodeType = "Topology"
	t.NodeImpl.value = t
	t.diskUsages = newDiskUsages()
	t.children = make(map[NodeId]Node)
	t.capacityReservations = newCapacityReservations()
	t.collectionMap = util.NewConcurrentReadMap()
	t.ecShardMap = make(map[needle.VolumeId]*EcShardLocations)
	t.pulse = int64(pulse)
	t.volumeSizeLimit = volumeSizeLimit
	t.replicationAsMin = replicationAsMin

	t.Sequence = seq

	t.chanFullVolumes = make(chan storage.VolumeInfo)
	t.chanCrowdedVolumes = make(chan storage.VolumeInfo)

	t.Configuration = &Configuration{}
	t.dataNodeIndex = make(map[string]*DataNode)

	return t
}

// LookupDataNodeByAddress returns the registered DataNode that serves addr,
// or nil if no such node has been observed. Lookup is O(1) and uses the
// canonical http form of the address so callers that pass either
// "host:port" or "host:port.grpc" find the same node.
func (t *Topology) LookupDataNodeByAddress(addr pb.ServerAddress) *DataNode {
	if addr == "" {
		return nil
	}
	t.dataNodeIndexLock.RLock()
	defer t.dataNodeIndexLock.RUnlock()
	if t.dataNodeIndex == nil {
		return nil
	}
	return t.dataNodeIndex[addr.ToHttpAddress()]
}

// registerDataNodeAddress records dn in the address index under its current
// http address. Callers must invoke unregisterDataNodeAddress with the prior
// address whenever a node's Ip or Port changes (e.g. k8s pod reschedule).
func (t *Topology) registerDataNodeAddress(dn *DataNode) {
	if dn == nil {
		return
	}
	key := dn.ServerAddress().ToHttpAddress()
	if key == "" {
		return
	}
	t.dataNodeIndexLock.Lock()
	defer t.dataNodeIndexLock.Unlock()
	if t.dataNodeIndex == nil {
		t.dataNodeIndex = make(map[string]*DataNode)
	}
	t.dataNodeIndex[key] = dn
}

// unregisterDataNodeAddress removes the index entry for addr, but only when
// the entry still points at dn. The conditional guard avoids dropping a
// freshly re-registered node whose address happens to alias the one being
// removed (e.g. legacy id transitions or a fast restart).
func (t *Topology) unregisterDataNodeAddress(addr pb.ServerAddress, dn *DataNode) {
	if addr == "" {
		return
	}
	key := addr.ToHttpAddress()
	if key == "" {
		return
	}
	t.dataNodeIndexLock.Lock()
	defer t.dataNodeIndexLock.Unlock()
	if existing, ok := t.dataNodeIndex[key]; ok && (dn == nil || existing == dn) {
		delete(t.dataNodeIndex, key)
	}
}

// FreeBytes sums what every volume server reports as free on its filesystems.
// reported is false unless all of them answered: the one that stayed quiet may
// be the one holding the room, and a partial sum would read as a cluster with
// none left.
func (t *Topology) FreeBytes() (freeBytes uint64, reported bool) {
	for _, dcNode := range t.Children() {
		for _, rackNode := range dcNode.Children() {
			for _, dataNode := range rackNode.Children() {
				nodeFreeBytes, nodeReported := dataNode.GetDiskUsages().FreeBytes()
				if !nodeReported {
					return 0, false
				}
				freeBytes += nodeFreeBytes
			}
		}
	}
	return freeBytes, true
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

// SetLastLeaderChangeTime records the time of the most recent leader transition.
// It also snapshots whether the topology already had known volumes at that
// moment. IsWarmingUp uses the snapshot instead of the live MaxVolumeId so a
// fresh cluster that happens to grow its first volume inside the warmup window
// does not retroactively flip into "warming up" state — there is no prior
// topology to wait for on a bootstrap.
func (t *Topology) SetLastLeaderChangeTime(ts time.Time) {
	hadVolumes := t.GetMaxVolumeId() > 0
	t.lastLeaderChangeTimeLock.Lock()
	defer t.lastLeaderChangeTimeLock.Unlock()
	t.lastLeaderChangeTime = ts
	t.hadVolumesAtLeaderChange = hadVolumes
}

// GetLastLeaderChangeTime returns the time of the most recent leader transition.
func (t *Topology) GetLastLeaderChangeTime() time.Time {
	t.lastLeaderChangeTimeLock.RLock()
	defer t.lastLeaderChangeTimeLock.RUnlock()
	return t.lastLeaderChangeTime
}

// IsWarmingUp returns true if the master recently became leader and may not yet
// have a complete topology. After a leader change or restart, volume servers need
// up to WarmupPulseMultiplier heartbeat intervals to reconnect and report their volumes.
// Returns false on a fresh cluster start — i.e. when no volumes existed at the
// time of the leader change — since there is no prior topology state to wait for.
// Checking the *live* MaxVolumeId here would make a bootstrapping cluster flip
// into warming-up the moment its first volume is grown, which manifested as a
// 15-second window of spurious Unavailable errors on AssignVolume for workloads
// that start writing immediately (see #8777).
func (t *Topology) IsWarmingUp() bool {
	t.lastLeaderChangeTimeLock.RLock()
	lastChange := t.lastLeaderChangeTime
	hadVolumes := t.hadVolumesAtLeaderChange
	t.lastLeaderChangeTimeLock.RUnlock()
	if !hadVolumes || lastChange.IsZero() {
		return false
	}
	return time.Since(lastChange) < t.WarmupDuration()
}

// WarmupDuration returns the configured warmup duration based on pulse interval.
func (t *Topology) WarmupDuration() time.Duration {
	return time.Duration(t.pulse*WarmupPulseMultiplier) * time.Second
}

// RemainingWarmupDuration returns how much warmup time is left, or 0 if not warming up.
func (t *Topology) RemainingWarmupDuration() time.Duration {
	if !t.IsWarmingUp() {
		return 0
	}
	remaining := t.WarmupDuration() - time.Since(t.GetLastLeaderChangeTime())
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (t *Topology) IsLeader() bool {
	t.RaftServerAccessLock.RLock()
	defer t.RaftServerAccessLock.RUnlock()

	if t.RaftServer != nil {
		if t.RaftServer.State() == raft.Leader {
			return true
		}
		// Directly check leader to avoid re-acquiring lock via MaybeLeader()
		leader := pb.ServerAddress(t.RaftServer.Leader())
		if leader != "" {
			if pb.ServerAddress(t.RaftServer.Name()).Equals(leader) {
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
		if l == "" && t.RaftServer.State() == raft.Leader {
			l = pb.ServerAddress(t.RaftServer.Name())
		}
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
	for t.topologyInitPending.Load() {
		if !t.IsLeaderAndCanRead() {
			return 0, fmt.Errorf("as leader can not read yet")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if t.IsGrowthFrozen() {
		return 0, fmt.Errorf("volume growth is frozen after master state loss; verify the cluster inventory, then run: volume.growth.resume")
	}
	if wait := t.allocationDelay(); wait > 0 {
		glog.V(0).Infof("deferring volume id allocation for %v", wait)
		time.Sleep(wait)
		if t.IsGrowthFrozen() {
			return 0, fmt.Errorf("volume growth is frozen after master state loss; verify the cluster inventory, then run: volume.growth.resume")
		}
	}
	vid := t.GetMaxVolumeId()
	next := vid.Next()

	if err := t.PersistMaxVolumeId(next); err != nil {
		return 0, fmt.Errorf("persist max volume id %d: %w", next, err)
	}

	t.RaftServerAccessLock.RLock()
	defer t.RaftServerAccessLock.RUnlock()

	if t.RaftServer != nil {
		if _, err := t.RaftServer.Do(NewMaxVolumeIdCommand(next, t.GetTopologyId())); err != nil {
			return 0, err
		}
	} else if t.HashicorpRaft != nil {
		b, err := json.Marshal(NewMaxVolumeIdCommand(next, t.GetTopologyId()))
		if err != nil {
			return 0, fmt.Errorf("failed marshal NewMaxVolumeIdCommand: %+v", err)
		}
		if future := t.HashicorpRaft.Apply(b, time.Second); future.Error() != nil {
			return 0, future.Error()
		}
	}
	return next, nil
}

// DefaultNeedleSizeEstimate is the fallback per-file-ID size estimate when
// the client does not provide an expected data size.
const DefaultNeedleSizeEstimate uint64 = 1024 * 1024 // 1 MB

func (t *Topology) PickForWrite(requestedCount uint64, option *VolumeGrowOption, volumeLayout *VolumeLayout, expectedDataSize uint64) (fileId string, count uint64, volumeLocationList *VolumeLocationList, shouldGrow bool, err error) {
	var vid needle.VolumeId
	vid, count, volumeLocationList, shouldGrow, err = volumeLayout.PickForWrite(requestedCount, option)
	if err != nil {
		return "", 0, nil, shouldGrow, fmt.Errorf("failed to find writable volumes for collection:%s replication:%s ttl:%s error: %v", option.Collection, option.ReplicaPlacement.String(), option.Ttl.String(), err)
	}
	if volumeLocationList == nil || volumeLocationList.Length() == 0 {
		return "", 0, nil, shouldGrow, fmt.Errorf("%s available for collection:%s replication:%s ttl:%s", NoWritableVolumes, option.Collection, option.ReplicaPlacement.String(), option.Ttl.String())
	}
	// Track estimated assigned bytes to spread load between heartbeats. A flat
	// fallback overcharges a small-file workload enough to mark near-empty
	// volumes full, so prefer the volume's own average.
	sizePerFile := DefaultNeedleSizeEstimate
	if expectedDataSize > 0 {
		sizePerFile = expectedDataSize
	} else if vi, infoErr := volumeLocationList.Head().GetVolumesById(vid); infoErr == nil && vi.FileCount > 0 {
		if avg := vi.Size / uint64(vi.FileCount); avg > 0 {
			sizePerFile = avg
		}
	}
	pendingBytes := min(uint64(count)*sizePerFile, uint64(math.MaxInt64))
	if volumeLayout.RecordAssign(vid, int64(pendingBytes)) {
		volumeLayout.AdjustActiveVolumeCountForFull(vid)
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

// FindVolumeLayout is the read-only counterpart of GetVolumeLayout: it returns
// nil when the collection or its layout does not exist, instead of creating
// one. Lets callers check a layout's state without polluting the collection
// map with an empty collection.
func (t *Topology) FindVolumeLayout(collectionName string, rp *super_block.ReplicaPlacement, ttl *needle.TTL, diskType types.DiskType) *VolumeLayout {
	c, ok := t.collectionMap.Find(collectionName)
	if !ok {
		return nil
	}
	vl, ok := c.(*Collection).GetVolumeLayout(rp, ttl, diskType)
	if !ok {
		return nil
	}
	return vl
}

// DecayQuietVolumeSizes decays pending assign estimates across every layout.
// A volume that changed reports within a pulse, so two quiet pulses mean the
// size on record is the size there is.
func (t *Topology) DecayQuietVolumeSizes() {
	quietCutoff := time.Duration(2*t.pulse) * time.Second
	for _, c := range t.collectionMap.Items() {
		for _, vl := range c.(*Collection).GetAllVolumeLayouts() {
			vl.DecayQuietVolumeSizes(quietCutoff)
		}
	}
}

// CollectionVolumeStats aggregates stats across all volume layouts and EC
// volumes of one collection, or across every collection when collectionName is
// empty.
func (t *Topology) CollectionVolumeStats(collectionName string) *VolumeLayoutStats {
	ret := &VolumeLayoutStats{}
	var collections []*Collection
	if collectionName == "" {
		for _, c := range t.collectionMap.Items() {
			collections = append(collections, c.(*Collection))
		}
	} else if c, found := t.FindCollection(collectionName); found {
		collections = append(collections, c)
	}
	for _, c := range collections {
		for _, vl := range c.GetAllVolumeLayouts() {
			stats := vl.Stats()
			ret.TotalSize += stats.TotalSize
			ret.UsedSize += stats.UsedSize
			ret.LogicalUsedSize += stats.LogicalUsedSize
			ret.FileCount += stats.FileCount
		}
	}
	// EC volumes live outside collectionMap, so a collection whose volumes are
	// all encoded has no layout left to report them
	ecStats := t.CollectionEcVolumeStats(collectionName)
	ret.TotalSize += ecStats.TotalSize
	ret.UsedSize += ecStats.UsedSize
	ret.LogicalUsedSize += ecStats.LogicalUsedSize
	ret.FileCount += ecStats.FileCount
	return ret
}

func (t *Topology) ListCollections(includeNormalVolumes, includeEcVolumes bool) (ret []string) {
	found := make(map[string]bool)

	if includeNormalVolumes {
		t.collectionMap.RLock()
		for _, c := range t.collectionMap.Items() {
			found[c.(*Collection).Name] = true
		}
		t.collectionMap.RUnlock()
	}

	if includeEcVolumes {
		t.ecShardMapLock.RLock()
		for _, ecVolumeLocation := range t.ecShardMap {
			found[ecVolumeLocation.Collection] = true
		}
		t.ecShardMapLock.RUnlock()
	}

	for k := range found {
		ret = append(ret, k)
	}
	slices.Sort(ret)

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
	// The layouts vanish with the collection, but every location they served
	// holds a bit in its node's lookup digest. Left in place, those bits keep
	// the node's held and servable digests apart forever, and the master asks
	// for the full volume list on every heartbeat from then on.
	// Unpublish first so a racing registration re-resolves into a fresh collection.
	collection, found := t.collectionMap.Delete(collectionName)
	if !found {
		return
	}
	for _, vl := range collection.(*Collection).GetAllVolumeLayouts() {
		vl.releaseLookupOwnership()
	}
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
	for {
		vl := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
		if vl.RegisterVolume(&v, dn) {
			vl.EnsureCorrectWritables(&v)
			return
		}
		// Dropped with its collection; the next lookup creates a fresh one.
	}
}

func (t *Topology) UnRegisterVolumeLayout(v storage.VolumeInfo, dn *DataNode) {
	glog.Infof("removing volume info: %+v from %v", v, dn.id)
	diskType := types.ToDiskType(v.DiskType)
	volumeLayout := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
	volumeLayout.UnRegisterVolume(&v, dn)
	// Drop the series only after the last placement is gone. Deleting while
	// another data node still holds v would hide under-replication until the
	// next CollectDeadNodeAndFullVolumes cycle recreates the label.
	if v.ReplicaPlacement.GetCopyCount() > 1 && len(t.Lookup(v.Collection, v.Id)) == 0 {
		stats.MasterReplicaPlacementMismatch.DeleteLabelValues(v.Collection, v.Id.String())
	}
	if volumeLayout.isEmpty() {
		t.DeleteLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
	}
}

func (t *Topology) DataCenterExists(dcName string) bool {
	return dcName == "" || t.GetDataCenter(dcName) != nil
}

// RackExists reports whether a rack with the id exists under dcName; an
// empty dcName searches every data center, an empty rackName always matches.
func (t *Topology) RackExists(dcName, rackName string) bool {
	if rackName == "" {
		return true
	}
	t.RLock()
	defer t.RUnlock()
	for _, c := range t.children {
		if dcName != "" && string(c.Id()) != dcName {
			continue
		}
		for _, r := range c.Children() {
			if string(r.Id()) == rackName {
				return true
			}
		}
	}
	return false
}

// DataNodeExists reports whether a data node with the id exists under
// dcName/rackName; empty parents search every data center and rack, an empty
// nodeId always matches.
func (t *Topology) DataNodeExists(dcName, rackName, nodeId string) bool {
	if nodeId == "" {
		return true
	}
	t.RLock()
	defer t.RUnlock()
	for _, c := range t.children {
		if dcName != "" && string(c.Id()) != dcName {
			continue
		}
		for _, r := range c.Children() {
			if rackName != "" && string(r.Id()) != rackName {
				continue
			}
			for _, dn := range r.Children() {
				if string(dn.Id()) == nodeId {
					return true
				}
			}
		}
	}
	return false
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

func (t *Topology) SyncDataNodeRegistration(volumes []*master_pb.VolumeInformationMessage, dn *DataNode) (newVolumes, deletedVolumes, changedVolumes []storage.VolumeInfo) {
	// convert into in memory struct storage.VolumeInfo
	volumeInfos := make([]storage.VolumeInfo, 0, len(volumes))
	for _, v := range volumes {
		if vi, err := storage.NewVolumeInfo(v); err == nil {
			volumeInfos = append(volumeInfos, vi)
		} else {
			glog.V(0).Infof("Fail to convert joined volume information: %v", err)
		}
	}
	// find out the delta volumes
	newVolumes, deletedVolumes, changedVolumes = dn.UpdateVolumes(volumeInfos)
	for _, v := range newVolumes {
		t.RegisterVolumeLayout(v, dn)
	}
	for _, v := range deletedVolumes {
		t.UnRegisterVolumeLayout(v, dn)
	}
	for _, v := range changedVolumes {
		if v.ReplicaPlacement == nil {
			continue
		}
		t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, types.ToDiskType(v.DiskType)).SetReplicaReadOnlyFlag(dn, v.Id, v.ReadOnly)
	}
	// Update effective sizes for all reported volumes (decay pending estimates).
	// If decay brings a volume eagerly removed by RecordAssign back under the
	// writable threshold, restore the matching activeVolumeCount.
	for _, v := range volumeInfos {
		if v.ReplicaPlacement == nil {
			continue
		}
		diskType := types.ToDiskType(v.DiskType)
		vl := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
		// Self-heal: a volume reported by the data node but missing from the
		// lookup index is re-registered. This repairs the split left by a
		// disconnect/reconnect race, where UnRegisterDataNode dropped the volume
		// from vid2location but the reconnecting full heartbeat skipped it
		// (still in the disk map, so UpdateVolumes did not report it as new).
		// Without this, the volume stays visible in volume.list/admin UI yet
		// LookupVolume returns "volume id not found".
		if !vl.HasDataNode(v.Id, dn) {
			for !vl.RegisterVolume(&v, dn) {
				// Dropped with its collection; the next lookup creates a fresh
				// one, which the calls below must use too.
				vl = t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
			}
			// Volumes new to the disk map were registered above, so reaching
			// here means only the lookup index had lost it. Clients were told
			// it went when the node dropped out, so the repair has to tell them
			// it is back.
			newVolumes = append(newVolumes, v)
		}
		vl.UpdateOversizedState(&v, dn)
		if vl.UpdateVolumeSize(v.Id, v.Size, v.CompactRevision, true) {
			vl.AdjustActiveVolumeCountAfterRecovery(v.Id)
		}
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

	type layoutKey struct {
		id               needle.VolumeId
		collection       string
		replicaPlacement byte
		ttl              uint32
		diskType         types.DiskType
	}
	key := func(vi storage.VolumeInfo) layoutKey {
		return layoutKey{
			id:               vi.Id,
			collection:       vi.Collection,
			replicaPlacement: vi.ReplicaPlacement.Byte(),
			ttl:              vi.Ttl.ToUint32(),
			diskType:         types.ToDiskType(vi.DiskType),
		}
	}
	replacements := make(map[layoutKey]struct{}, len(newVis))
	for _, vi := range newVis {
		replacements[key(vi)] = struct{}{}
		t.RegisterVolumeLayout(vi, dn)
	}
	for _, oldVi := range oldVis {
		if _, replaced := replacements[key(oldVi)]; !replaced {
			t.UnRegisterVolumeLayout(oldVi, dn)
		}
	}

	return
}

// ApplyVolumeChanges records the volumes a heartbeat reported as changed and
// returns the ones the node did not already have. Only the named volumes are
// touched: unlike a full report, silence about a volume says nothing about
// whether the server still has it.
//
// Most changes are a volume growing, which moves no location, so returning
// only the arrivals keeps a busy cluster from telling every client about
// volumes they can already reach. The arrival set also includes replicas whose
// IsRemote() classification flipped on tier transition: the volume is still
// servable from the same node, but every connected client has stale replica
// priority and must be told to refresh.
func (t *Topology) ApplyVolumeChanges(changed []*master_pb.VolumeInformationMessage, dn *DataNode) (newVolumes []storage.VolumeInfo) {
	volumeInfos := make([]storage.VolumeInfo, 0, len(changed))
	for _, v := range changed {
		vi, err := storage.NewVolumeInfo(v)
		if err != nil {
			glog.V(0).Infof("Fail to convert changed volume information: %v", err)
			continue
		}
		volumeInfos = append(volumeInfos, vi)
	}

	for _, vi := range volumeInfos {
		isNew, isChanged, tierTransition := dn.AddOrUpdateVolume(vi)
		if vi.ReplicaPlacement == nil {
			if isNew {
				newVolumes = append(newVolumes, vi)
			}
			continue
		}
		vl := t.GetVolumeLayout(vi.Collection, vi.ReplicaPlacement, vi.Ttl, types.ToDiskType(vi.DiskType))
		// Reaching the lookup index is what makes a volume servable, so a
		// volume only that index had lost is an arrival as far as clients are
		// concerned: they were told it went when the node dropped out.
		becameServable := !vl.HasDataNode(vi.Id, dn)
		for becameServable && !vl.RegisterVolume(&vi, dn) {
			// Dropped with its collection; the next lookup creates a fresh one.
			vl = t.GetVolumeLayout(vi.Collection, vi.ReplicaPlacement, vi.Ttl, types.ToDiskType(vi.DiskType))
		}
		if isNew || becameServable || tierTransition || isChanged {
			newVolumes = append(newVolumes, vi)
		}
		if isChanged {
			vl.SetReplicaReadOnlyFlag(dn, vi.Id, vi.ReadOnly)
		}
		vl.UpdateOversizedState(&vi, dn)
		if vl.UpdateVolumeSize(vi.Id, vi.Size, vi.CompactRevision, true) {
			vl.AdjustActiveVolumeCountAfterRecovery(vi.Id)
		}
		vl.EnsureCorrectWritables(&vi)
	}
	return newVolumes
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

// IsVacuumDisabled returns true if vacuum is disabled by either the
// operator or the plugin monitor.
func (t *Topology) IsVacuumDisabled() bool {
	return t.vacuumDisabledByOperator.Load() || t.vacuumDisabledByPlugin.Load()
}

// DisableVacuum is called by the operator (shell command / manual RPC).
// Only sets the operator flag; does not affect the plugin flag.
func (t *Topology) DisableVacuum() {
	glog.V(0).Infof("DisableVacuum (by operator)")
	t.vacuumDisabledByOperator.Store(true)
}

// EnableVacuum is called by the operator (shell command / manual RPC).
// Only clears the operator flag; does not affect the plugin flag.
func (t *Topology) EnableVacuum() {
	glog.V(0).Infof("EnableVacuum (by operator)")
	t.vacuumDisabledByOperator.Store(false)
}

// DisableVacuumByPlugin is called by the admin server's vacuum monitor
// when a vacuum plugin worker connects. Only sets the plugin flag.
func (t *Topology) DisableVacuumByPlugin() {
	glog.V(0).Infof("DisableVacuum (by plugin worker)")
	t.vacuumDisabledByPlugin.Store(true)
}

// EnableVacuumByPlugin is called by the admin server's vacuum monitor
// when a vacuum plugin worker disconnects. Only clears the plugin flag.
func (t *Topology) EnableVacuumByPlugin() {
	glog.V(0).Infof("EnableVacuum (by plugin worker)")
	t.vacuumDisabledByPlugin.Store(false)
}

// IsVacuumDisabledByPlugin returns whether the plugin monitor has disabled vacuum.
func (t *Topology) IsVacuumDisabledByPlugin() bool {
	return t.vacuumDisabledByPlugin.Load()
}

// SetAdminServerConnectedFunc sets an optional callback used by the vacuum
// safety net to detect when the admin server has disconnected.
func (t *Topology) SetAdminServerConnectedFunc(f func() bool) {
	t.adminServerConnectedFunc = f
}

func (t *Topology) GetTopologyId() string {
	t.topologyIdLock.RLock()
	defer t.topologyIdLock.RUnlock()
	return t.topologyId
}

func (t *Topology) SetTopologyId(topologyId string) {
	t.topologyIdLock.Lock()
	defer t.topologyIdLock.Unlock()
	if topologyId == "" {
		return
	}
	if t.topologyId == "" {
		t.topologyId = topologyId
		return
	}
	if t.topologyId != topologyId {
		glog.Fatalf("Split-brain detected! Current TopologyId is %s, but received %s. Stopping to prevent data corruption.", t.topologyId, topologyId)
	}
}

func (t *Topology) SetGrowthFreezeConfig(freezeOnMismatch bool, recoveryWait time.Duration) {
	t.freezeOnMismatch.Store(freezeOnMismatch)
	t.stateLossRecoveryWait.Store(int64(recoveryWait))
}

// CheckReportedTopologyId freezes growth when a volume server reports a
// topology id different from this cluster's: the topology id survives raft
// state cleanup, so a mismatch means the master's state was rebuilt (or a
// foreign disk was cloned in).
func (t *Topology) CheckReportedTopologyId(reported string) {
	if reported == "" || !t.freezeOnMismatch.Load() {
		return
	}
	own := t.GetTopologyId()
	if own == "" || reported == own {
		return
	}
	t.growthFrozenAt.Store(time.Now().UnixNano())
	if t.growthFrozen.CompareAndSwap(false, true) {
		stats.MasterVolumeGrowthFrozen.Set(1)
		glog.Errorf("volume growth frozen: a volume server reports topology id %s but this cluster is %s; the master state was rebuilt. Verify the cluster inventory, then run: volume.growth.resume", reported, own)
		t.applyGrowthFreeze(true)
	}
}

func (t *Topology) IsGrowthFrozen() bool {
	if !t.growthFrozen.Load() {
		return false
	}
	if wait := t.stateLossRecoveryWait.Load(); wait > 0 {
		if time.Duration(time.Now().UnixNano()-t.growthFrozenAt.Load()) > time.Duration(wait) {
			if t.growthFrozen.CompareAndSwap(true, false) {
				stats.MasterVolumeGrowthFrozen.Set(0)
				glog.V(0).Infof("volume growth unfrozen after %v", time.Duration(wait))
			}
			return false
		}
	}
	return true
}

func (t *Topology) ResumeVolumeGrowth() bool {
	if !t.growthFrozen.CompareAndSwap(true, false) {
		return false
	}
	stats.MasterVolumeGrowthFrozen.Set(0)
	glog.V(0).Infof("volume growth resumed by operator")
	t.applyGrowthFreeze(false)
	return true
}

// SetGrowthFrozen applies a replicated freeze state on every master. The
// timestamp lands before the flag so a reader can never see frozen with a
// zero time and misjudge the auto-resume window.
func (t *Topology) SetGrowthFrozen(frozen bool) {
	if frozen {
		t.growthFrozenAt.Store(time.Now().UnixNano())
		if t.growthFrozen.CompareAndSwap(false, true) {
			stats.MasterVolumeGrowthFrozen.Set(1)
			glog.V(0).Infof("volume growth freeze applied from raft")
		}
		return
	}
	if t.growthFrozen.CompareAndSwap(true, false) {
		stats.MasterVolumeGrowthFrozen.Set(0)
	}
}

// applyGrowthFreeze replicates the freeze flag so followers keep it across a
// leader change. No raft server (single-master test topologies) just sets it.
func (t *Topology) applyGrowthFreeze(frozen bool) {
	cmd := NewMaxVolumeIdCommand(t.GetMaxVolumeId(), t.GetTopologyId())
	cmd.GrowthFrozen = &frozen
	t.RaftServerAccessLock.RLock()
	defer t.RaftServerAccessLock.RUnlock()
	if t.RaftServer != nil {
		if _, err := t.RaftServer.Do(cmd); err != nil {
			glog.Warningf("apply growth freeze %v: %v", frozen, err)
		}
	} else if t.HashicorpRaft != nil {
		if b, err := json.Marshal(cmd); err == nil {
			t.HashicorpRaft.Apply(b, time.Second)
		}
	} else {
		t.SetGrowthFrozen(frozen)
	}
}

// SetTopologyInitPending gates allocation while a topology id is being
// minted, so growth cannot slip in before the warmup window is installed.
func (t *Topology) SetTopologyInitPending(pending bool) {
	t.topologyInitPending.Store(pending)
}

// DeferAllocation holds off new id allocation for d. Called when the leader
// mints a fresh topology id: on a rebuilt master that window gives veteran
// servers a pulse to raise the bound or trip the freeze.
func (t *Topology) DeferAllocation(d time.Duration) {
	t.allocationNotBefore.Store(time.Now().Add(d).UnixNano())
}

func (t *Topology) allocationDelay() time.Duration {
	return time.Until(time.Unix(0, t.allocationNotBefore.Load()))
}

// SetMaxVolumeIdFile mirrors the allocation bound to a file outside raft, so
// a master whose raft state is gone still restores a safe boundary. An
// existing file seeds both the bound and the topology id.
func (t *Topology) SetMaxVolumeIdFile(file string) {
	t.maxVolumeIdFile = file
	data, err := os.ReadFile(file)
	if err != nil {
		return
	}
	cmd := &MaxVolumeIdCommand{}
	if err := json.Unmarshal(data, cmd); err != nil {
		glog.Warningf("corrupt max volume id file %s: %v", file, err)
		return
	}
	t.lastPersistedMaxVolumeId.Store(uint32(cmd.MaxVolumeId))
	t.lastPersistedTopologyId.Store(cmd.TopologyId)
	t.UpAdjustMaxVolumeId(cmd.MaxVolumeId)
	if cmd.TopologyId != "" {
		t.SetTopologyId(cmd.TopologyId)
	}
	if cmd.GrowthFrozen != nil {
		t.SetGrowthFrozen(*cmd.GrowthFrozen)
	}
}

// PersistMaxVolumeId writes the bound to the mirror file. Called by the
// leader before a raft apply and by both raft implementations on apply and
// recovery, so follower mirrors stay current too.
func (t *Topology) PersistMaxVolumeId(vid needle.VolumeId) error {
	topoId := t.GetTopologyId()
	mirrored := func() bool {
		return t.lastPersistedMaxVolumeId.Load() >= uint32(vid) && t.lastPersistedTopologyId.Load() == topoId
	}
	if mirrored() {
		return nil
	}
	t.maxVolumeIdFileLock.Lock()
	defer t.maxVolumeIdFileLock.Unlock()
	if mirrored() {
		return nil
	}
	if t.maxVolumeIdFile == "" {
		t.lastPersistedMaxVolumeId.Store(uint32(vid))
		t.lastPersistedTopologyId.Store(topoId)
		return nil
	}
	cmd := NewMaxVolumeIdCommand(vid, topoId)
	if t.growthFrozen.Load() {
		cmd.GrowthFrozen = new(bool)
		*cmd.GrowthFrozen = true
	}
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	tmp := t.maxVolumeIdFile + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, t.maxVolumeIdFile); err != nil {
		return err
	}
	if err := util.FsyncDir(filepath.Dir(t.maxVolumeIdFile)); err != nil {
		return err
	}
	t.lastPersistedMaxVolumeId.Store(uint32(vid))
	t.lastPersistedTopologyId.Store(topoId)
	return nil
}

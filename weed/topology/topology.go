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
	"strings"
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
	maxWritableVolumes       int
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

	maxVolumeIdFile           string
	maxVolumeIdFileLock       sync.Mutex
	lastPersistedMaxVolumeId  atomic.Uint32
	lastPersistedTopologyId   atomic.Value
	lastPersistedGrowthFrozen atomic.Bool

	growthFrozen            atomic.Bool
	growthFrozenAt          atomic.Int64
	metaReadOnly            atomic.Bool
	freezeOnMismatch        atomic.Bool
	stateLossRecoveryWait   atomic.Int64
	allocationNotBefore     atomic.Int64
	allocationEvidenceSince atomic.Int64
	allocationDeferral      atomic.Int64
	topologyInitPending     atomic.Bool

	// releaseAllocOnReport lifts a post-mint allocation hold as soon as a full
	// volume-server inventory has been applied, for single-reporter setups
	// (weed mini) whose embedded volume server is the only possible source of
	// prior cluster state. inventoryApplied records that at least one full
	// volume list has been synced into this topology.
	releaseAllocOnReport atomic.Bool
	inventoryApplied     atomic.Bool
	singleReporter       atomic.Value // string

	reportedTopologyIdsLock    sync.Mutex
	pendingReportedTopologyIds map[string]struct{}

	// quarantinedVolumeIds are (node, vid) pairs force-read-only because the
	// vid collided across layouts. Kept separate from the reported flag so
	// later heartbeats from the same server cannot quietly undo it.
	quarantinedVolumeIds sync.Map
	// quarantineLock serializes each quarantine record update with the layout
	// mark it justifies: a release pass and a heartbeat read-only flag update
	// each observe record and mark as one state, never a mark whose record
	// was already deleted or a cleared mark whose record is not yet visible.
	quarantineLock sync.Mutex
	// quarantinePushLocks serialize the remote mark/unmark calls per replica
	// so a push decided under an older record state cannot land on the
	// volume server after a newer one. Keyed by replica so a slow server
	// does not stall pushes to the others.
	quarantinePushLocks   map[quarantinedVolume]*quarantinePushLock
	quarantinePushLocksMu sync.Mutex

	// duplicateFlagChecks tracks in-flight flagDuplicateVolumeId passes so
	// tests can wait for the async checks a registration started.
	duplicateFlagChecks sync.WaitGroup

	// QuarantineVolumeFn pushes the quarantine onto the volume server itself
	// (VolumeMarkReadonly/VolumeMarkWritable). nil keeps quarantine at the
	// layout level only.
	QuarantineVolumeFn func(dn *DataNode, vid needle.VolumeId, readOnly bool) error
}

type quarantinedVolume struct {
	dnId NodeId
	vid  needle.VolumeId
}

type quarantinedVolumeState struct {
	reported bool
	enforced bool
	retry    bool
}

type quarantinePushLock struct {
	sync.Mutex
	refs int
}

func (t *Topology) isQuarantined(dn *DataNode, vid needle.VolumeId) bool {
	_, ok := t.quarantinedVolumeIds.Load(quarantinedVolume{dn.Id(), vid})
	return ok
}

// pushQuarantine sends the remote mark only while it still matches the
// record: a read-only push requires the pair to exist, a writable push
// requires it to be gone. The bool reports whether the remote call was
// actually made, so callers can tell a skipped stale push from a failed one.
func (t *Topology) pushQuarantine(dn *DataNode, vid needle.VolumeId, readOnly bool) (bool, error) {
	unlock := t.lockQuarantinePush(quarantinedVolume{dn.Id(), vid})
	defer unlock()
	return t.sendQuarantine(dn, vid, readOnly)
}

// sendQuarantine runs the record check and remote mark; the caller holds
// the replica's push lock.
func (t *Topology) sendQuarantine(dn *DataNode, vid needle.VolumeId, readOnly bool) (bool, error) {
	t.quarantineLock.Lock()
	_, exists := t.quarantinedVolumeIds.Load(quarantinedVolume{dn.Id(), vid})
	t.quarantineLock.Unlock()
	if exists != readOnly {
		return false, nil
	}
	return true, t.QuarantineVolumeFn(dn, vid, readOnly)
}

// releaseQuarantineWritable pushes the writable mark and re-arms the record
// on failure, all under the replica's push lock so a queued stale push cannot
// slip between the failure and the restore and undo the re-armed record.
func (t *Topology) releaseQuarantineWritable(dn *DataNode, vid needle.VolumeId) {
	unlock := t.lockQuarantinePush(quarantinedVolume{dn.Id(), vid})
	defer unlock()
	if _, err := t.sendQuarantine(dn, vid, false); err != nil {
		t.restoreQuarantineRecord(dn, vid)
	}
}

// lockQuarantinePush returns the release for q's push lock, creating the lock
// on first use and dropping it once the last holder lets go.
func (t *Topology) lockQuarantinePush(q quarantinedVolume) func() {
	t.quarantinePushLocksMu.Lock()
	if t.quarantinePushLocks == nil {
		t.quarantinePushLocks = make(map[quarantinedVolume]*quarantinePushLock)
	}
	l, ok := t.quarantinePushLocks[q]
	if !ok {
		l = &quarantinePushLock{}
		t.quarantinePushLocks[q] = l
	}
	l.refs++
	t.quarantinePushLocksMu.Unlock()
	l.Lock()
	return func() {
		t.quarantinePushLocksMu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(t.quarantinePushLocks, q)
		}
		t.quarantinePushLocksMu.Unlock()
		l.Unlock()
	}
}

// takeQuarantineRetryLocked disarms a pending release retry and reports
// whether one was armed. The caller holds quarantineLock.
func (t *Topology) takeQuarantineRetryLocked(q quarantinedVolume) bool {
	v, ok := t.quarantinedVolumeIds.Load(q)
	if !ok {
		return false
	}
	state := v.(quarantinedVolumeState)
	if !state.retry {
		return false
	}
	state.retry = false
	t.quarantinedVolumeIds.Store(q, state)
	return true
}

// retryPendingQuarantineReleases re-runs the release pass for every record a
// failed writable push re-armed. The retry in enforceQuarantine only fires
// while a heartbeat keeps carrying the volume, and an idle replica drops out
// of the delta reports once its stored flag matches the server's, so pending
// retries also run off the leader pulse.
func (t *Topology) retryPendingQuarantineReleases() {
	vids := make(map[needle.VolumeId]struct{})
	t.quarantineLock.Lock()
	t.quarantinedVolumeIds.Range(func(key, _ any) bool {
		q := key.(quarantinedVolume)
		if t.takeQuarantineRetryLocked(q) {
			vids[q.vid] = struct{}{}
		}
		return true
	})
	t.quarantineLock.Unlock()
	for vid := range vids {
		go t.releaseResolvedQuarantine(vid)
	}
}

// enforceQuarantine keeps the latest reported read-only flag for a quarantined
// copy so a later release restores the server's true state. The stored report
// keeps the server's flag: quarantine lives at the layout level, and folding
// it into the record here would make the master's volume digest permanently
// disagree with the server's and force a full volume list on every heartbeat.
func (t *Topology) enforceQuarantine(dn *DataNode, vi storage.VolumeInfo) storage.VolumeInfo {
	q := quarantinedVolume{dn.Id(), vi.Id}
	t.quarantineLock.Lock()
	v, ok := t.quarantinedVolumeIds.Load(q)
	if !ok {
		t.quarantineLock.Unlock()
		return vi
	}
	if t.takeQuarantineRetryLocked(q) {
		// A failed writable push re-armed this record; hand the release pass
		// one retry per report. Clearing it first keeps a surviving conflict
		// on the normal enforce path afterwards.
		t.quarantineLock.Unlock()
		go t.releaseResolvedQuarantine(vi.Id)
		return vi
	}
	state := v.(quarantinedVolumeState)
	if !state.enforced {
		state.reported = vi.ReadOnly
	}
	if t.QuarantineVolumeFn == nil || (state.enforced && vi.ReadOnly) {
		t.quarantinedVolumeIds.Store(q, state)
		t.quarantineLock.Unlock()
		return vi
	}
	t.quarantineLock.Unlock()

	// The mark never landed on the server, or the server lost it (restart,
	// manual unquarantine); push it while the conflict stands. The call is
	// remote, so it runs outside the lock.
	pushed, err := t.pushQuarantine(dn, vi.Id, true)
	if pushed && err == nil {
		state.enforced = true
	}
	t.quarantineLock.Lock()
	cur, still := t.quarantinedVolumeIds.Load(q)
	if still {
		latest := cur.(quarantinedVolumeState)
		if !latest.enforced {
			latest.reported = state.reported
		}
		latest.enforced = latest.enforced || state.enforced
		t.quarantinedVolumeIds.Store(q, latest)
		t.quarantineLock.Unlock()
		return vi
	}
	t.quarantineLock.Unlock()
	if state.enforced && !state.reported {
		// The conflict resolved while the push was in flight; leave the
		// server consistent with the released layout.
		t.releaseQuarantineWritable(dn, vi.Id)
	}
	return vi
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

	t.allocationDeferral.Store(-1)

	t.Configuration = &Configuration{}
	t.dataNodeIndex = make(map[string]*DataNode)
	t.quarantinePushLocks = make(map[quarantinedVolume]*quarantinePushLock)

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

// SetMetaReadOnly disables volume id allocation: -mdir is on a read-only
// filesystem and raft runs from a copy that does not survive a restart.
func (t *Topology) SetMetaReadOnly(readOnly bool) {
	t.metaReadOnly.Store(readOnly)
}

func (t *Topology) NextVolumeId() (needle.VolumeId, error) {
	if t.metaReadOnly.Load() {
		return 0, fmt.Errorf("volume growth disabled: -mdir is on a read-only filesystem")
	}
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
	}
	for wait := t.allocationDelay(); wait > 0; wait = t.allocationDelay() {
		if wait > 500*time.Millisecond {
			wait = 500 * time.Millisecond
		}
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
		c := NewCollection(collectionName, t.volumeSizeLimit, t.replicationAsMin)
		c.maxWritableVolumes = t.maxWritableVolumes
		return c
	}).(*Collection).GetOrCreateVolumeLayout(rp, ttl, diskType)
}

// SetMaxWritableVolumes caps how many writable volumes in a volume layout
// (collection + replica placement + ttl + disk type) assignments are spread
// over; 0 means every writable volume is a candidate. It only affects
// collections created afterwards, so call it before serving.
func (t *Topology) SetMaxWritableVolumes(n int) {
	t.maxWritableVolumes = n
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
	var vids []needle.VolumeId
	for _, vl := range collection.(*Collection).GetAllVolumeLayouts() {
		vids = append(vids, vl.ListVids()...)
		vl.releaseLookupOwnership()
	}
	// a vid colliding with a copy in this collection is no longer a duplicate
	for _, vid := range vids {
		t.releaseResolvedQuarantine(vid)
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
		// RegisterVolume reads the quarantine record and marks under
		// accessLock; holding quarantineLock keeps that pair serialized with
		// releaseResolvedQuarantine's clear-and-delete.
		t.quarantineLock.Lock()
		registered := vl.RegisterVolume(&v, dn, func(n *DataNode) bool {
			return t.isQuarantined(n, v.Id)
		})
		t.quarantineLock.Unlock()
		if registered {
			// RegisterVolume rebuilds only the read-only flags, so a
			// location list recreated flagless would pass the writable
			// check before the next heartbeat re-measures the volume.
			// This report carries the size; mark from it first. A report
			// that carries none leaves the remembered verdict in place.
			if v.Size > 0 {
				vl.UpdateOversizedState(&v, dn)
			}
			vl.EnsureCorrectWritables(&v)
			t.flagDuplicateVolumeIdAsync(v, dn)
			return
		}
		// Dropped with its collection; the next lookup creates a fresh one.
	}
}

func (t *Topology) flagDuplicateVolumeIdAsync(v storage.VolumeInfo, dn *DataNode) {
	t.duplicateFlagChecks.Add(1)
	go func() {
		defer t.duplicateFlagChecks.Done()
		t.flagDuplicateVolumeId(v, dn)
	}()
}

// flagDuplicateVolumeId reports and quarantines v.Id when another data node
// already serves it under a different layout key. It runs after registration
// so a legitimate same-node move (disk type or collection change) settles
// first and does not trip it. Every copy across the conflicting layouts is
// quarantined: either could be the impostor, and arrival order must not
// decide which copy stays writable.
func (t *Topology) flagDuplicateVolumeId(v storage.VolumeInfo, dn *DataNode) {
	// Every pass ends with a release: a conflict could resolve between the
	// scan below and the marks, and a check finding no hits is also the retry
	// a kept record waits for once its replica reports a readable layout.
	defer t.releaseResolvedQuarantine(v.Id)
	hits := t.findDuplicateVolumeLayouts(v, dn)
	if len(hits) == 0 {
		return
	}
	// This runs on a goroutine after registration, so the reporter's copy may
	// already be unregistered (a finished move drops the losing copy). The
	// surviving copies are then legitimate, and flagging them quarantines a
	// healthy volume.
	own := t.FindVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, types.ToDiskType(v.DiskType))
	if own == nil || !own.HasDataNode(v.Id, dn) {
		return
	}
	stats.MasterDuplicateVolumeIds.WithLabelValues(v.Collection, v.Id.String()).Inc()
	var otherKeys []string
	for _, h := range hits {
		otherKeys = append(otherKeys, h.layoutKey())
	}
	glog.Errorf("duplicate volume id %d: %s reports it under %s but other data nodes also serve it under %s; marking all copies read-only", v.Id, dn.Id(), volumeLayoutKey(v.Collection, v.ReplicaPlacement, v.Ttl, types.ToDiskType(v.DiskType)), strings.Join(otherKeys, ", "))
	quarantine := func(n *DataNode, vl *VolumeLayout) {
		q := quarantinedVolume{n.Id(), v.Id}
		reported := false
		if vi, err := n.GetVolumesById(v.Id); err == nil {
			reported = vi.ReadOnly
		}
		t.quarantineLock.Lock()
		t.quarantinedVolumeIds.LoadOrStore(q, quarantinedVolumeState{reported: reported})
		if vl != nil {
			vl.SetVolumeQuarantined(n, v.Id)
		}
		t.quarantineLock.Unlock()
		if t.QuarantineVolumeFn == nil {
			return
		}
		pushed, err := t.pushQuarantine(n, v.Id, true)
		if err != nil {
			return
		}
		t.quarantineLock.Lock()
		val, ok := t.quarantinedVolumeIds.Load(q)
		if ok {
			state := val.(quarantinedVolumeState)
			state.enforced = state.enforced || pushed
			t.quarantinedVolumeIds.Store(q, state)
			t.quarantineLock.Unlock()
			return
		}
		t.quarantineLock.Unlock()
		if pushed && !reported {
			// The conflict resolved while the push was in flight; leave the
			// server consistent with the released layout.
			t.releaseQuarantineWritable(n, v.Id)
		}
	}
	quarantine(dn, own)
	for _, h := range hits {
		for _, n := range h.nodes {
			quarantine(n, h.vl)
		}
	}
}

// releaseResolvedQuarantine lifts read-only marks once the conflicting copy
// is gone, so a finished move does not leave the surviving copy quarantined.
// The writable push is a remote call, so the locked pass only applies the
// layout marks and records which replicas still need it; the pushes run
// after the lock is dropped, a skipped push means a newer record re-armed
// the pair so its quarantine stands, and a failed push re-adds the record
// so a later release pass retries it.
func (t *Topology) releaseResolvedQuarantine(vid needle.VolumeId) {
	var writablePushes []*DataNode
	t.quarantineLock.Lock()
	t.quarantinedVolumeIds.Range(func(key, val any) bool {
		q := key.(quarantinedVolume)
		if q.vid != vid {
			return true
		}
		dn, vl, vi := t.findQuarantinedVolume(q.dnId, vid)
		if dn == nil {
			t.quarantinedVolumeIds.Delete(key)
			return true
		}
		if vi.ReplicaPlacement == nil {
			// The replica is in the lookup index but unreadable on its node,
			// so its layout key is unknown and the conflict cannot be ruled
			// out; keep the mark for a later pass.
			return true
		}
		if hits := t.findDuplicateVolumeLayouts(vi, dn); len(hits) > 0 {
			return true
		}
		state, _ := val.(quarantinedVolumeState)
		if state.reported {
			dn.SetVolumeReadOnly(vid, true)
			vl.SetReplicaReadOnlyFlag(dn, vid, true)
		} else {
			vl.SetVolumeWritable(dn, vid)
			if state.enforced && t.QuarantineVolumeFn != nil {
				writablePushes = append(writablePushes, dn)
			}
		}
		t.quarantinedVolumeIds.Delete(key)
		return true
	})
	t.quarantineLock.Unlock()
	for _, dn := range writablePushes {
		t.releaseQuarantineWritable(dn, vid)
	}
}

// restoreQuarantineRecord re-adds a released record after a failed writable
// push so the next volume report retries the release. An existing record keeps its
// reported flag; the server is known read-only, so enforced and retry are set.
// A record restored from scratch also gets its layout read-only mark back:
// the release pass cleared it with the record, and leaving it cleared would
// advertise a writable replica the server still holds read-only.
func (t *Topology) restoreQuarantineRecord(dn *DataNode, vid needle.VolumeId) {
	t.quarantineLock.Lock()
	defer t.quarantineLock.Unlock()
	key := quarantinedVolume{dn.Id(), vid}
	if cur, ok := t.quarantinedVolumeIds.LoadOrStore(key, quarantinedVolumeState{enforced: true, retry: true}); ok {
		state := cur.(quarantinedVolumeState)
		state.enforced = true
		state.retry = true
		t.quarantinedVolumeIds.Store(key, state)
		return
	}
	if _, vl, _ := t.findQuarantinedVolume(dn.Id(), vid); vl != nil {
		vl.SetVolumeReadOnly(dn, vid)
	}
}

func (t *Topology) findQuarantinedVolume(dnId NodeId, vid needle.VolumeId) (*DataNode, *VolumeLayout, storage.VolumeInfo) {
	for _, item := range t.collectionMap.Items() {
		for _, vl := range item.(*Collection).GetAllVolumeLayouts() {
			for _, n := range vl.Lookup(vid) {
				if n.Id() == dnId {
					vi, _ := n.GetVolumesById(vid)
					return n, vl, vi
				}
			}
		}
	}
	return nil, nil, storage.VolumeInfo{}
}

// duplicateHit is one layout that holds v.Id on data nodes other than the
// reporter's.
type duplicateHit struct {
	collection string
	vl         *VolumeLayout
	nodes      []*DataNode
}

func (h duplicateHit) layoutKey() string {
	return volumeLayoutKey(h.collection, h.vl.rp, h.vl.ttl, h.vl.diskType)
}

func volumeLayoutKey(collection string, rp *super_block.ReplicaPlacement, ttl *needle.TTL, diskType types.DiskType) string {
	return fmt.Sprintf("collection=%q rp=%s ttl=%s disk=%s", collection, rp, ttl, diskType)
}

func ttlEqual(a, b *needle.TTL) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.ToUint32() == b.ToUint32()
}

// findDuplicateVolumeLayouts reports every layout that already holds v.Id on
// data nodes other than dn. Volume ids are global, so the same id held by two
// nodes under two layouts means the allocator reused an id.
func (t *Topology) findDuplicateVolumeLayouts(v storage.VolumeInfo, dn *DataNode) []duplicateHit {
	if v.ReplicaPlacement == nil {
		return nil
	}
	diskType := types.ToDiskType(v.DiskType)
	var hits []duplicateHit
	for _, item := range t.collectionMap.Items() {
		c := item.(*Collection)
		for _, vl := range c.GetAllVolumeLayouts() {
			if c.Name == v.Collection && vl.diskType == diskType && ttlEqual(vl.ttl, v.Ttl) {
				// The reporter's own layout, or an rp-only difference:
				// a replication change rolls through the nodes one at a
				// time, so the same volume legitimately reports the old
				// and new rp while it settles.
				continue
			}
			var others []*DataNode
			for _, n := range vl.Lookup(v.Id) {
				if n != dn {
					others = append(others, n)
				}
			}
			if len(others) > 0 {
				hits = append(hits, duplicateHit{collection: c.Name, vl: vl, nodes: others})
			}
		}
	}
	return hits
}

func (t *Topology) UnRegisterVolumeLayout(v storage.VolumeInfo, dn *DataNode) {
	glog.Infof("removing volume info: %+v from %v", v, dn.id)
	defer t.releaseResolvedQuarantine(v.Id)
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
	for i := range volumeInfos {
		volumeInfos[i] = t.enforceQuarantine(dn, volumeInfos[i])
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
		t.quarantineLock.Lock()
		t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, types.ToDiskType(v.DiskType)).SetReplicaReadOnlyFlag(dn, v.Id, v.ReadOnly || t.isQuarantined(dn, v.Id))
		t.quarantineLock.Unlock()
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
			for {
				t.quarantineLock.Lock()
				registered := vl.RegisterVolume(&v, dn, func(n *DataNode) bool {
					return t.isQuarantined(n, v.Id)
				})
				t.quarantineLock.Unlock()
				if registered {
					break
				}
				// Dropped with its collection; the next lookup creates a fresh
				// one, which the calls below must use too.
				vl = t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl, diskType)
			}
			t.flagDuplicateVolumeIdAsync(v, dn)
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
		newVis = append(newVis, t.enforceQuarantine(dn, vi))
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
		volumeInfos = append(volumeInfos, t.enforceQuarantine(dn, vi))
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
		for becameServable {
			t.quarantineLock.Lock()
			registered := vl.RegisterVolume(&vi, dn, func(n *DataNode) bool {
				return t.isQuarantined(n, vi.Id)
			})
			t.quarantineLock.Unlock()
			if registered {
				break
			}
			// Dropped with its collection; the next lookup creates a fresh one.
			vl = t.GetVolumeLayout(vi.Collection, vi.ReplicaPlacement, vi.Ttl, types.ToDiskType(vi.DiskType))
		}
		if becameServable {
			t.flagDuplicateVolumeIdAsync(vi, dn)
		}
		if isNew || becameServable || tierTransition || isChanged {
			newVolumes = append(newVolumes, vi)
		}
		if isChanged {
			t.quarantineLock.Lock()
			vl.SetReplicaReadOnlyFlag(dn, vi.Id, vi.ReadOnly || t.isQuarantined(dn, vi.Id))
			t.quarantineLock.Unlock()
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
	t.reportedTopologyIdsLock.Lock()
	defer t.reportedTopologyIdsLock.Unlock()
	t.checkReportedTopologyIdLocked(reported)
}

// checkReportedTopologyIdLocked runs the mismatch check inside
// reportedTopologyIdsLock, the same critical section the init gate drains
// under: a report received before the gate opens is either stashed or frozen
// before the gate can release.
func (t *Topology) checkReportedTopologyIdLocked(reported string) {
	own := t.GetTopologyId()
	if own == "" {
		// The local topology id is still being minted. Keep the report so a
		// later heartbeat echo cannot erase the mismatch on the server side
		// before the freeze check ever ran.
		if t.pendingReportedTopologyIds == nil {
			t.pendingReportedTopologyIds = make(map[string]struct{})
		}
		t.pendingReportedTopologyIds[reported] = struct{}{}
		return
	}
	if reported == own {
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
				go t.replicateGrowthUnfreeze()
			}
			return false
		}
	}
	return true
}

// replicateGrowthUnfreeze pushes the recovery-wait lift through raft like
// ResumeVolumeGrowth, so a restart does not replay the freeze. A freeze that
// landed after the lift owns the flag; skipping keeps its later command last
// in the log. If the write never lands, the flag is re-armed so a later
// IsGrowthFrozen retries the lift instead of diverging from the log.
func (t *Topology) replicateGrowthUnfreeze() {
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
		t.reportedTopologyIdsLock.Lock()
		if t.growthFrozen.Load() {
			t.reportedTopologyIdsLock.Unlock()
			return
		}
		err := t.applyGrowthFreeze(false)
		t.reportedTopologyIdsLock.Unlock()
		if err == nil {
			return
		}
	}
	if t.growthFrozen.CompareAndSwap(false, true) {
		stats.MasterVolumeGrowthFrozen.Set(1)
		glog.Warningf("volume growth unfreeze not replicated; freeze re-armed for retry")
	}
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
func (t *Topology) applyGrowthFreeze(frozen bool) error {
	cmd := NewMaxVolumeIdCommand(t.GetMaxVolumeId(), t.GetTopologyId())
	cmd.GrowthFrozen = &frozen
	t.RaftServerAccessLock.RLock()
	defer t.RaftServerAccessLock.RUnlock()
	if t.RaftServer != nil {
		if _, err := t.RaftServer.Do(cmd); err != nil {
			glog.Warningf("apply growth freeze %v: %v", frozen, err)
			return err
		}
		return nil
	}
	if t.HashicorpRaft != nil {
		b, err := json.Marshal(cmd)
		if err != nil {
			glog.Warningf("marshal growth freeze %v: %v", frozen, err)
			return err
		}
		if future := t.HashicorpRaft.Apply(b, time.Second); future.Error() != nil {
			glog.Warningf("apply growth freeze %v: %v", frozen, future.Error())
			return future.Error()
		}
		return nil
	}
	t.SetGrowthFrozen(frozen)
	return nil
}

// SetTopologyInitPending gates allocation while a topology id is being
// minted, so growth cannot slip in before the warmup window is installed.
// Stashed reports are applied while the gate is still closed: clearing it
// first would let an allocation through before a queued mismatch freezes
// growth.
func (t *Topology) SetTopologyInitPending(pending bool) {
	if pending {
		t.topologyInitPending.Store(true)
		return
	}
	// The stash is drained and the gate released inside one critical section:
	// a mismatching report checked here, or still in flight under the same
	// lock, lands its freeze before any allocation can observe the open gate.
	t.reportedTopologyIdsLock.Lock()
	defer t.reportedTopologyIdsLock.Unlock()
	reported := t.pendingReportedTopologyIds
	t.pendingReportedTopologyIds = nil
	for id := range reported {
		t.checkReportedTopologyIdLocked(id)
	}
	t.topologyInitPending.Store(false)
}

// ReleaseAllocationOnFirstReport opts the topology into lifting the post-mint
// allocation hold once one full volume-server inventory has been applied, and
// pins the admitted volume-server reporter: the early release is only sound
// when a single embedded server (weed mini) can carry prior cluster state, so
// every other reporter must be refused rather than silently trusted.
func (t *Topology) ReleaseAllocationOnFirstReport(onlyReporter ...string) {
	if len(onlyReporter) > 0 {
		t.singleReporter.Store(onlyReporter[0])
	}
	t.releaseAllocOnReport.Store(true)
}

// AdmitVolumeReporter enforces the single-reporter restriction the early
// release relies on: a late-arriving foreign server would carry cluster state
// the dropped hold was meant to wait for.
func (t *Topology) AdmitVolumeReporter(addr string) bool {
	if !t.releaseAllocOnReport.Load() {
		return true
	}
	expected, _ := t.singleReporter.Load().(string)
	return expected == "" || expected == addr
}

// NoteInventoryApplied marks a heartbeat's full volume list as applied to this
// topology. Under ReleaseAllocationOnFirstReport it also lifts the post-mint
// allocation hold: the bound raise and topology-id freeze checks run while the
// heartbeat is processed, so nothing protected remains pending afterwards.
func (t *Topology) NoteInventoryApplied() {
	t.inventoryApplied.Store(true)
	if t.releaseAllocOnReport.Load() {
		t.allocationNotBefore.Store(0)
	}
}

// DeferAllocation holds off new id allocation for d. Called when the leader
// mints a fresh topology id: on a rebuilt master that window gives veteran
// servers a pulse to raise the bound or trip the freeze.
func (t *Topology) DeferAllocation(d time.Duration) {
	// Under release-on-report a hold is pointless once inventory has landed:
	// the veteran heartbeat that mattered has already been applied.
	if t.releaseAllocOnReport.Load() && t.inventoryApplied.Load() {
		return
	}
	now := time.Now()
	t.allocationNotBefore.Store(now.Add(d).UnixNano())
	t.allocationEvidenceSince.Store(now.UnixNano())
	// Close the race with a report applied between the check above and the
	// store, which would otherwise leave the full window armed.
	if t.releaseAllocOnReport.Load() && t.inventoryApplied.Load() {
		t.allocationNotBefore.Store(0)
	}
}

// NoteAllocationEvidence records a heartbeat signal that could still carry
// prior cluster state — a server joining or reporting held volumes, a raised
// id bound, or a foreign topology id. The deferral lifts two pulses after the
// last such signal, so a cluster with nothing left to report stops waiting.
func (t *Topology) NoteAllocationEvidence() {
	t.allocationEvidenceSince.Store(time.Now().UnixNano())
}

// SetAllocationDeferral overrides the post-mint allocation hold: zero disables
// it, a positive duration sets the cap, negative restores the warmup window.
func (t *Topology) SetAllocationDeferral(d time.Duration) {
	t.allocationDeferral.Store(int64(d))
}

// AllocationDeferralDuration returns the configured post-mint hold, or one
// warmup window when unset.
func (t *Topology) AllocationDeferralDuration() time.Duration {
	if d := t.allocationDeferral.Load(); d >= 0 {
		return time.Duration(d)
	}
	return t.WarmupDuration()
}

func (t *Topology) allocationDelay() time.Duration {
	deadline := t.allocationNotBefore.Load()
	if deadline == 0 {
		return 0
	}
	if quietUntil := t.allocationEvidenceSince.Load() + 2*int64(t.pulse)*int64(time.Second); quietUntil < deadline {
		deadline = quietUntil
	}
	return time.Until(time.Unix(0, deadline))
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
		t.lastPersistedGrowthFrozen.Store(*cmd.GrowthFrozen)
		t.SetGrowthFrozen(*cmd.GrowthFrozen)
	}
}

// PersistMaxVolumeId writes the bound to the mirror file. Called by the
// leader before a raft apply and by both raft implementations on apply and
// recovery, so follower mirrors stay current too.
func (t *Topology) PersistMaxVolumeId(vid needle.VolumeId) error {
	topoId := t.GetTopologyId()
	frozen := t.IsGrowthFrozen()
	mirrored := func() bool {
		return t.lastPersistedMaxVolumeId.Load() >= uint32(vid) && t.lastPersistedTopologyId.Load() == topoId && t.lastPersistedGrowthFrozen.Load() == frozen
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
		t.lastPersistedGrowthFrozen.Store(frozen)
		return nil
	}
	cmd := NewMaxVolumeIdCommand(vid, topoId)
	cmd.GrowthFrozen = &frozen
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
	t.lastPersistedGrowthFrozen.Store(frozen)
	return nil
}

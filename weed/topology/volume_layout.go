package topology

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
)

type copyState int

const (
	noCopies copyState = 0 + iota
	insufficientCopies
	enoughCopies
)

type volumeState string

const (
	readOnlyState     volumeState = "ReadOnly"
	oversizedState                = "Oversized"
	crowdedState                  = "Crowded"
	NoWritableVolumes             = "No writable volumes"
)

type stateIndicator func(copyState) bool

func ExistCopies() stateIndicator {
	return func(state copyState) bool { return state != noCopies }
}

type volumesBinaryState struct {
	rp        *super_block.ReplicaPlacement
	name      volumeState    // the name for volume state (eg. "Readonly", "Oversized")
	indicator stateIndicator // indicate whether the volumes should be marked as `name`
	copyMap   map[needle.VolumeId]*VolumeLocationList
}

func NewVolumesBinaryState(name volumeState, rp *super_block.ReplicaPlacement, indicator stateIndicator) *volumesBinaryState {
	return &volumesBinaryState{
		rp:        rp,
		name:      name,
		indicator: indicator,
		copyMap:   make(map[needle.VolumeId]*VolumeLocationList),
	}
}

func (v *volumesBinaryState) Dump() (res []uint32) {
	for vid, list := range v.copyMap {
		if v.indicator(v.copyState(list)) {
			res = append(res, uint32(vid))
		}
	}
	return
}

func (v *volumesBinaryState) IsTrue(vid needle.VolumeId) bool {
	list, _ := v.copyMap[vid]
	return v.indicator(v.copyState(list))
}

func (v *volumesBinaryState) Add(vid needle.VolumeId, dn *DataNode) {
	list, _ := v.copyMap[vid]
	if list != nil {
		list.Set(dn)
		return
	}
	list = NewVolumeLocationList()
	list.Set(dn)
	v.copyMap[vid] = list
}

func (v *volumesBinaryState) Remove(vid needle.VolumeId, dn *DataNode) {
	list, _ := v.copyMap[vid]
	if list != nil {
		list.Remove(dn)
		if list.Length() == 0 {
			delete(v.copyMap, vid)
		}
	}
}

func (v *volumesBinaryState) copyState(list *VolumeLocationList) copyState {
	if list == nil {
		return noCopies
	}
	if list.Length() < v.rp.GetCopyCount() {
		return insufficientCopies
	}
	return enoughCopies
}

// volumeSizeTracking holds per-volume size accounting for weighted assignment.
type volumeSizeTracking struct {
	effectiveSize   uint64    // reported + pending assigned bytes
	reportedSize    uint64    // last heartbeat-reported size (dedup replicas)
	compactRevision uint32    // detect compaction to reset instead of decay
	lastUpdateTime  time.Time // dedup replicas within the same heartbeat cycle
}

// mapping from volume to its locations, inverted from server to volume
type VolumeLayout struct {
	growRequest      atomic.Bool
	lastGrowCount    atomic.Uint32
	rp               *super_block.ReplicaPlacement
	ttl              *needle.TTL
	diskType         types.DiskType
	vid2location     map[needle.VolumeId]*VolumeLocationList
	writables        []needle.VolumeId // transient array of writable volume id
	crowded          map[needle.VolumeId]struct{}
	readonlyVolumes  *volumesBinaryState // readonly volumes
	oversizedVolumes *volumesBinaryState // oversized volumes
	vacuumedVolumes  map[needle.VolumeId]time.Time
	volumeSizeLimit  uint64
	replicationAsMin bool
	accessLock       sync.RWMutex
	sizeTracking     map[needle.VolumeId]*volumeSizeTracking
}

type VolumeLayoutStats struct {
	TotalSize uint64
	UsedSize  uint64
	FileCount uint64
}

func NewVolumeLayout(rp *super_block.ReplicaPlacement, ttl *needle.TTL, diskType types.DiskType, volumeSizeLimit uint64, replicationAsMin bool) *VolumeLayout {
	return &VolumeLayout{
		rp:               rp,
		ttl:              ttl,
		diskType:         diskType,
		vid2location:     make(map[needle.VolumeId]*VolumeLocationList),
		writables:        *new([]needle.VolumeId),
		crowded:          make(map[needle.VolumeId]struct{}),
		readonlyVolumes:  NewVolumesBinaryState(readOnlyState, rp, ExistCopies()),
		oversizedVolumes: NewVolumesBinaryState(oversizedState, rp, ExistCopies()),
		vacuumedVolumes:  make(map[needle.VolumeId]time.Time),
		volumeSizeLimit:  volumeSizeLimit,
		replicationAsMin: replicationAsMin,
		sizeTracking:    make(map[needle.VolumeId]*volumeSizeTracking),
	}
}

func (vl *VolumeLayout) String() string {
	return fmt.Sprintf("rp:%v, ttl:%v, writables:%v, volumeSizeLimit:%v", vl.rp, vl.ttl, vl.writables, vl.volumeSizeLimit)
}

func (vl *VolumeLayout) RegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	defer vl.rememberOversizedVolume(v, dn)

	if _, ok := vl.vid2location[v.Id]; !ok {
		vl.vid2location[v.Id] = NewVolumeLocationList()
	}
	vl.vid2location[v.Id].Set(dn)
	// For new volumes, initialize size tracking from reported size.
	if _, exists := vl.sizeTracking[v.Id]; !exists {
		vl.sizeTracking[v.Id] = &volumeSizeTracking{
			effectiveSize:   v.Size,
			reportedSize:    v.Size,
			compactRevision: v.CompactRevision,
		}
	}
	// glog.V(4).Infof("volume %d added to %s len %d copy %d", v.Id, dn.Id(), vl.vid2location[v.Id].Length(), v.ReplicaPlacement.GetCopyCount())
	for _, dn := range vl.vid2location[v.Id].list {
		if vInfo, err := dn.GetVolumesById(v.Id); err == nil {
			if vInfo.ReadOnly {
				glog.V(1).Infof("vid %d removed from writable", v.Id)
				vl.removeFromWritable(v.Id)
				vl.readonlyVolumes.Add(v.Id, dn)
				return
			} else {
				vl.readonlyVolumes.Remove(v.Id, dn)
			}
		} else {
			glog.V(1).Infof("vid %d removed from writable", v.Id)
			vl.removeFromWritable(v.Id)
			vl.readonlyVolumes.Remove(v.Id, dn)
			return
		}
	}

}

func (vl *VolumeLayout) rememberOversizedVolume(v *storage.VolumeInfo, dn *DataNode) {
	if vl.isOversized(v) {
		vl.oversizedVolumes.Add(v.Id, dn)
	} else {
		vl.oversizedVolumes.Remove(v.Id, dn)
	}
}

// UpdateVolumeSize is called on every heartbeat for every reported volume.
// It decays the pending size estimate toward the reported size and updates
// crowded state. Replicated volumes report from multiple DataNodes; decay
// runs only once per new reported size to avoid double-halving.
// If the compact revision changed, the size drop is from compaction (not
// pending writes), so we reset effectiveSize to the reported size instead of
// decaying.
func (vl *VolumeLayout) UpdateVolumeSize(vid needle.VolumeId, reportedSize uint64, compactRevision uint32) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	now := time.Now()
	st := vl.sizeTracking[vid]
	if st == nil {
		st = &volumeSizeTracking{
			effectiveSize:   reportedSize,
			reportedSize:    reportedSize,
			compactRevision: compactRevision,
			lastUpdateTime:  now,
		}
		vl.sizeTracking[vid] = st
	} else if now.Sub(st.lastUpdateTime) < 2*time.Second {
		return // duplicate replica in the same heartbeat cycle
	} else {
		st.lastUpdateTime = now
		st.reportedSize = reportedSize
		if compactRevision != st.compactRevision {
			// Compaction happened — size drop is real, not pending. Reset.
			st.compactRevision = compactRevision
			st.effectiveSize = reportedSize
		} else if st.effectiveSize > reportedSize {
			st.effectiveSize = reportedSize + (st.effectiveSize-reportedSize)/2
		} else {
			st.effectiveSize = reportedSize
		}
	}

	if float64(st.effectiveSize) > float64(vl.volumeSizeLimit)*VolumeGrowStrategy.Threshold {
		vl.setVolumeCrowded(vid)
	} else {
		vl.removeFromCrowded(vid)
	}
}

func (vl *VolumeLayout) UnRegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	// remove from vid2location map
	location, ok := vl.vid2location[v.Id]
	if !ok {
		return
	}

	if location.Remove(dn) {

		vl.readonlyVolumes.Remove(v.Id, dn)
		vl.oversizedVolumes.Remove(v.Id, dn)
		vl.ensureCorrectWritables(v.Id)

		if location.Length() == 0 {
			delete(vl.vid2location, v.Id)
			delete(vl.sizeTracking, v.Id)
			vl.removeFromCrowded(v.Id)
		}

	}
}

func (vl *VolumeLayout) EnsureCorrectWritables(v *storage.VolumeInfo) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	vl.ensureCorrectWritables(v.Id)
}

func (vl *VolumeLayout) ensureCorrectWritables(vid needle.VolumeId) {
	isEnoughCopies := vl.enoughCopies(vid)
	isAllWritable := vl.isAllWritable(vid)
	isOversizedVolume := vl.oversizedVolumes.IsTrue(vid)
	if isEnoughCopies && isAllWritable && !isOversizedVolume {
		vl.setVolumeWritable(vid)
	} else {
		if !isEnoughCopies {
			glog.V(0).Infof("volume %d does not have enough copies", vid)
		}
		if !isAllWritable {
			glog.V(0).Infof("volume %d are not all writable", vid)
		}
		if isOversizedVolume {
			glog.V(1).Infof("volume %d are oversized", vid)
		}
		glog.V(0).Infof("volume %d remove from writable", vid)
		vl.removeFromWritable(vid)
	}
}

func (vl *VolumeLayout) isAllWritable(vid needle.VolumeId) bool {
	if location, ok := vl.vid2location[vid]; ok {
		for _, dn := range location.list {
			if v, getError := dn.GetVolumesById(vid); getError == nil {
				if v.ReadOnly {
					return false
				}
			}
		}
	} else {
		return false
	}

	return true
}

func (vl *VolumeLayout) isOversized(v *storage.VolumeInfo) bool {
	return uint64(v.Size) >= vl.volumeSizeLimit
}

func (vl *VolumeLayout) isCrowdedVolume(v *storage.VolumeInfo) bool {
	return float64(v.Size) > float64(vl.volumeSizeLimit)*VolumeGrowStrategy.Threshold
}

// RecordAssign adds the estimated byte size to the volume's tracked effective
// size and marks it crowded if it crosses the threshold.
func (vl *VolumeLayout) RecordAssign(vid needle.VolumeId, pendingDelta int64) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	st := vl.sizeTracking[vid]
	if st == nil {
		return
	}
	if pendingDelta > 0 {
		st.effectiveSize += uint64(pendingDelta)
	}
	if float64(st.effectiveSize) > float64(vl.volumeSizeLimit)*VolumeGrowStrategy.Threshold {
		vl.setVolumeCrowded(vid)
	}
}

const maxDrainWait = 30 * time.Second
const pendingSizeThreshold uint64 = 2 * 1024 * 1024 // 2 MB

// GetPendingSize returns the estimated in-flight bytes for a volume:
// the gap between the effective tracked size and the last heartbeat-reported size.
func (vl *VolumeLayout) GetPendingSize(vid needle.VolumeId) uint64 {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()
	if st := vl.sizeTracking[vid]; st != nil && st.effectiveSize > st.reportedSize {
		return st.effectiveSize - st.reportedSize
	}
	return 0
}

// waitForPendingDrain polls until pending bytes for the volume decay below
// the threshold, the timeout expires, or the context is cancelled. Since the
// volume is already removed from the writable list, no new assigns accumulate
// — pending only decreases via heartbeat decay.
func (vl *VolumeLayout) waitForPendingDrain(ctx context.Context, vid needle.VolumeId) {
	deadline := time.Now().Add(maxDrainWait)
	for time.Now().Before(deadline) {
		if vl.GetPendingSize(vid) <= pendingSizeThreshold {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
	}
	glog.Warningf("volume %d: %d pending bytes remain after drain timeout", vid, vl.GetPendingSize(vid))
}

// DrainAndRemoveFromWritable removes the volume from the writable list
// immediately, then waits for pending assigned bytes to decay.
// Used by vacuum before compaction.
func (vl *VolumeLayout) DrainAndRemoveFromWritable(vid needle.VolumeId) {
	vl.accessLock.Lock()
	vl.removeFromWritable(vid)
	vl.accessLock.Unlock()
	vl.waitForPendingDrain(context.Background(), vid)
}


func (vl *VolumeLayout) isEmpty() bool {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	return len(vl.vid2location) == 0
}

func (vl *VolumeLayout) Lookup(vid needle.VolumeId) []*DataNode {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	if location := vl.vid2location[vid]; location != nil {
		return location.list
	}
	return nil
}

func (vl *VolumeLayout) ListVolumeServers() (nodes []*DataNode) {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	for _, location := range vl.vid2location {
		nodes = append(nodes, location.list...)
	}
	return
}

func (vl *VolumeLayout) PickForWrite(count uint64, option *VolumeGrowOption) (vid needle.VolumeId, counter uint64, locationList *VolumeLocationList, shouldGrow bool, err error) {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	lenWriters := len(vl.writables)
	if lenWriters <= 0 {
		return 0, 0, nil, true, fmt.Errorf("%s", NoWritableVolumes)
	}
	if option.DataCenter == "" && option.Rack == "" && option.DataNode == "" {
		vid, locationList = vl.pickWeightedByRemaining(vl.writables)
		if locationList == nil || len(locationList.list) == 0 {
			return 0, 0, nil, false, fmt.Errorf("Strangely vid %s is on no machine!", vid.String())
		}
		return vid, count, locationList.Copy(), false, nil
	}

	// Scan from a random offset to collect up to pickSampleSize matching
	// candidates, avoiding a full scan + allocation in the common case.
	var sample [pickSampleSize]needle.VolumeId
	found := 0
	start := rand.IntN(lenWriters)
	for i := 0; i < lenWriters && found < pickSampleSize; i++ {
		writableVolumeId := vl.writables[(start+i)%lenWriters]
		volumeLocationList := vl.vid2location[writableVolumeId]
		for _, dn := range volumeLocationList.list {
			if option.DataCenter != "" && dn.GetDataCenter().Id() != NodeId(option.DataCenter) {
				continue
			}
			if option.Rack != "" && dn.GetRack().Id() != NodeId(option.Rack) {
				continue
			}
			if option.DataNode != "" && dn.Id() != NodeId(option.DataNode) {
				continue
			}
			sample[found] = writableVolumeId
			found++
			break
		}
	}
	if found == 0 {
		return vid, count, locationList, true, fmt.Errorf("%s in DataCenter:%v Rack:%v DataNode:%v", NoWritableVolumes, option.DataCenter, option.Rack, option.DataNode)
	}
	vid, locationList = vl.weightedPick(sample[:found])
	return vid, count, locationList.Copy(), false, nil
}

// pickSampleSize is how many random candidates to sample before doing a
// weighted pick. Keeps cost O(1) regardless of total writable volume count
// while still biasing toward emptier volumes.
const pickSampleSize = 3

// pickWeightedByRemaining randomly samples a few candidates from the list,
// then does a weighted pick among them by remaining capacity.
// Sampled candidates may repeat when len(candidates) is small relative to
// pickSampleSize; this is harmless — a repeated volume just gets proportionally
// more weight, which is a negligible statistical effect.
func (vl *VolumeLayout) pickWeightedByRemaining(candidates []needle.VolumeId) (needle.VolumeId, *VolumeLocationList) {
	n := len(candidates)
	if n <= pickSampleSize {
		return vl.weightedPick(candidates)
	}

	var sample [pickSampleSize]needle.VolumeId
	for i := range sample {
		sample[i] = candidates[rand.IntN(n)]
	}
	return vl.weightedPick(sample[:])
}

func (vl *VolumeLayout) weightedPick(candidates []needle.VolumeId) (needle.VolumeId, *VolumeLocationList) {
	if len(candidates) == 1 {
		vid := candidates[0]
		return vid, vl.vid2location[vid]
	}

	// first pass: sum weights
	var totalRemaining uint64
	for _, vid := range candidates {
		totalRemaining += vl.remainingSize(vid)
	}

	// second pass: weighted random pick
	pick := rand.Uint64N(totalRemaining)
	var cumulative uint64
	for _, vid := range candidates {
		cumulative += vl.remainingSize(vid)
		if pick < cumulative {
			return vid, vl.vid2location[vid]
		}
	}

	vid := candidates[0]
	return vid, vl.vid2location[vid]
}

func (vl *VolumeLayout) remainingSize(vid needle.VolumeId) uint64 {
	var size uint64
	if st := vl.sizeTracking[vid]; st != nil {
		size = st.effectiveSize
	}
	if size < vl.volumeSizeLimit {
		if r := vl.volumeSizeLimit - size; r > 1 {
			return r
		}
	}
	return 1
}

func (vl *VolumeLayout) HasGrowRequest() bool {
	return vl.growRequest.Load()
}
func (vl *VolumeLayout) AddGrowRequest() {
	vl.growRequest.Store(true)
}
func (vl *VolumeLayout) DoneGrowRequest() {
	vl.growRequest.Store(false)
}

func (vl *VolumeLayout) SetLastGrowCount(count uint32) {
	if vl.lastGrowCount.Load() != count && count != 0 {
		vl.lastGrowCount.Store(count)
	}
}

func (vl *VolumeLayout) GetLastGrowCount() uint32 {
	return vl.lastGrowCount.Load()
}

func (vl *VolumeLayout) ShouldGrowVolumes() bool {
	writable, crowded := vl.GetWritableVolumeCount()
	return writable <= crowded
}

func (vl *VolumeLayout) ShouldGrowVolumesByDcAndRack(writables *[]needle.VolumeId, dcId NodeId, rackId NodeId) bool {
	// When replication spans multiple racks (DiffRackCount > 0), a writable
	// volume's replicas only cover some racks in a DC. It is wrong to
	// require every rack to host a replica — that would create volumes
	// endlessly in any DC with more racks than the copy count.
	// Instead, check at the DC level: if the DC already has a non-crowded
	// writable volume, no growth is needed for uncovered racks.
	checkDcOnly := vl.rp.DiffRackCount > 0
	for _, v := range *writables {
		for _, dn := range vl.Lookup(v) {
			if dn.GetDataCenter().Id() != dcId {
				continue
			}
			if !checkDcOnly && dn.GetRack().Id() != rackId {
				continue
			}
			if _, err := dn.GetVolumesById(v); err == nil {
				vl.accessLock.RLock()
				var size uint64
				if st := vl.sizeTracking[v]; st != nil {
					size = st.effectiveSize
				}
				vl.accessLock.RUnlock()
				if float64(size) <= float64(vl.volumeSizeLimit)*VolumeGrowStrategy.Threshold {
					return false
				}
			}
		}
	}
	return true
}

func (vl *VolumeLayout) GetWritableVolumeCount() (active, crowded int) {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()
	return len(vl.writables), len(vl.crowded)
}

func (vl *VolumeLayout) CloneWritableVolumes() (writables []needle.VolumeId) {
	vl.accessLock.RLock()
	writables = make([]needle.VolumeId, len(vl.writables))
	copy(writables, vl.writables)
	vl.accessLock.RUnlock()
	return writables
}

func (vl *VolumeLayout) removeFromWritable(vid needle.VolumeId) bool {
	toDeleteIndex := -1
	for k, id := range vl.writables {
		if id == vid {
			toDeleteIndex = k
			break
		}
	}
	if toDeleteIndex >= 0 {
		glog.V(0).Infoln("Volume", vid, "becomes unwritable")
		vl.writables = append(vl.writables[0:toDeleteIndex], vl.writables[toDeleteIndex+1:]...)
		return true
	}
	return false
}
func (vl *VolumeLayout) setVolumeWritable(vid needle.VolumeId) bool {
	for _, v := range vl.writables {
		if v == vid {
			return false
		}
	}
	glog.V(1).Infoln("Volume", vid, "becomes writable")
	vl.writables = append(vl.writables, vid)
	return true
}

func (vl *VolumeLayout) SetVolumeReadOnly(dn *DataNode, vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if _, ok := vl.vid2location[vid]; ok {
		vl.readonlyVolumes.Add(vid, dn)
		return vl.removeFromWritable(vid)
	}
	return true
}

func (vl *VolumeLayout) SetVolumeWritable(dn *DataNode, vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if _, ok := vl.vid2location[vid]; ok {
		vl.readonlyVolumes.Remove(vid, dn)
	}

	if vl.enoughCopies(vid) {
		return vl.setVolumeWritable(vid)
	}
	return false
}

func (vl *VolumeLayout) SetVolumeUnavailable(dn *DataNode, vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if location, ok := vl.vid2location[vid]; ok {
		if location.Remove(dn) {
			vl.readonlyVolumes.Remove(vid, dn)
			vl.oversizedVolumes.Remove(vid, dn)
			if location.Length() < vl.rp.GetCopyCount() {
				glog.V(0).Infoln("Volume", vid, "has", location.Length(), "replica, less than required", vl.rp.GetCopyCount())
				return vl.removeFromWritable(vid)
			}
		}
	}
	return false
}
func (vl *VolumeLayout) SetVolumeAvailable(dn *DataNode, vid needle.VolumeId, isReadOnly, isFullCapacity bool) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	vInfo, err := dn.GetVolumesById(vid)
	if err != nil {
		return false
	}

	vl.vid2location[vid].Set(dn)

	if vInfo.ReadOnly || isReadOnly || isFullCapacity {
		return false
	}

	if vl.enoughCopies(vid) {
		return vl.setVolumeWritable(vid)
	}
	return false
}

func (vl *VolumeLayout) enoughCopies(vid needle.VolumeId) bool {
	locations := vl.vid2location[vid].Length()
	desired := vl.rp.GetCopyCount()
	return locations == desired || (vl.replicationAsMin && locations > desired)
}

func (vl *VolumeLayout) SetVolumeCapacityFull(vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	wasWritable := vl.removeFromWritable(vid)
	if wasWritable {
		glog.V(0).Infof("Volume %d reaches full capacity.", vid)
	}
	return wasWritable
}

func (vl *VolumeLayout) removeFromCrowded(vid needle.VolumeId) {
	if _, ok := vl.crowded[vid]; ok {
		glog.V(0).Infoln("Volume", vid, "becomes uncrowded")
		delete(vl.crowded, vid)
	}
}

func (vl *VolumeLayout) setVolumeCrowded(vid needle.VolumeId) {
	if _, ok := vl.crowded[vid]; !ok {
		vl.crowded[vid] = struct{}{}
		glog.V(0).Infoln("Volume", vid, "becomes crowded")
	}
}

func (vl *VolumeLayout) SetVolumeCrowded(vid needle.VolumeId) {
	// since delete is guarded by accessLock.Lock(),
	// and is always called in sequential order,
	// RLock() should be safe enough
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	vl.setVolumeCrowded(vid)
}

type VolumeLayoutInfo struct {
	Replication string            `json:"replication"`
	TTL         string            `json:"ttl"`
	Writables   []needle.VolumeId `json:"writables"`
	Collection  string            `json:"collection"`
	DiskType    string            `json:"diskType"`
}

func (vl *VolumeLayout) ToInfo() (info VolumeLayoutInfo) {
	info.Replication = vl.rp.String()
	info.TTL = vl.ttl.String()
	info.Writables = vl.writables
	info.DiskType = vl.diskType.ReadableString()
	//m["locations"] = vl.vid2location
	return
}

func (vlc *VolumeLayoutCollection) ToVolumeGrowRequest() *master_pb.VolumeGrowRequest {
	return &master_pb.VolumeGrowRequest{
		Collection:  vlc.Collection,
		Replication: vlc.VolumeLayout.rp.String(),
		Ttl:         vlc.VolumeLayout.ttl.String(),
		DiskType:    vlc.VolumeLayout.diskType.String(),
	}
}

func (vl *VolumeLayout) Stats() *VolumeLayoutStats {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	ret := &VolumeLayoutStats{}

	freshThreshold := time.Now().Unix() - 60

	for vid, vll := range vl.vid2location {
		size, fileCount := vll.Stats(vid, freshThreshold)
		ret.FileCount += uint64(fileCount)
		ret.UsedSize += size * uint64(vll.Length())
		if vl.readonlyVolumes.IsTrue(vid) {
			ret.TotalSize += size * uint64(vll.Length())
		} else {
			ret.TotalSize += vl.volumeSizeLimit * uint64(vll.Length())
		}
	}

	return ret
}

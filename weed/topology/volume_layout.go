package topology

import (
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

func NoCopies() stateIndicator {
	return func(state copyState) bool { return state == noCopies }
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

func (vl *VolumeLayout) isWritable(v *storage.VolumeInfo) bool {
	return !vl.isOversized(v) &&
		v.Version == needle.CurrentVersion &&
		!v.ReadOnly
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
		vid := vl.writables[rand.IntN(lenWriters)]
		locationList = vl.vid2location[vid]
		if locationList == nil || len(locationList.list) == 0 {
			return 0, 0, nil, false, fmt.Errorf("Strangely vid %s is on no machine!", vid.String())
		}
		return vid, count, locationList.Copy(), false, nil
	}

	// clone vl.writables
	writables := make([]needle.VolumeId, len(vl.writables))
	copy(writables, vl.writables)
	// randomize the writables
	rand.Shuffle(len(writables), func(i, j int) {
		writables[i], writables[j] = writables[j], writables[i]
	})

	for _, writableVolumeId := range writables {
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
			vid, locationList, counter = writableVolumeId, volumeLocationList.Copy(), count
			return
		}
	}
	return vid, count, locationList, true, fmt.Errorf("%s in DataCenter:%v Rack:%v DataNode:%v", NoWritableVolumes, option.DataCenter, option.Rack, option.DataNode)
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
	for _, v := range *writables {
		for _, dn := range vl.Lookup(v) {
			if dn.GetDataCenter().Id() == dcId && dn.GetRack().Id() == rackId {
				if info, err := dn.GetVolumesById(v); err == nil && !vl.isCrowdedVolume(&info) {
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
	vl.removeFromCrowded(vid)
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
	glog.V(0).Infoln("Volume", vid, "becomes writable")
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

package topology

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
)

// mapping from volume to its locations, inverted from server to volume
type VolumeLayout struct {
	rp               *super_block.ReplicaPlacement
	ttl              *needle.TTL
	vid2location     map[needle.VolumeId]*VolumeLocationList
	writables        []needle.VolumeId        // transient array of writable volume id
	readonlyVolumes  map[needle.VolumeId]bool // transient set of readonly volumes
	oversizedVolumes map[needle.VolumeId]bool // set of oversized volumes
	volumeSizeLimit  uint64
	replicationAsMin bool
	accessLock       sync.RWMutex
}

type VolumeLayoutStats struct {
	TotalSize uint64
	UsedSize  uint64
	FileCount uint64
}

func NewVolumeLayout(rp *super_block.ReplicaPlacement, ttl *needle.TTL, volumeSizeLimit uint64, replicationAsMin bool) *VolumeLayout {
	return &VolumeLayout{
		rp:               rp,
		ttl:              ttl,
		vid2location:     make(map[needle.VolumeId]*VolumeLocationList),
		writables:        *new([]needle.VolumeId),
		readonlyVolumes:  make(map[needle.VolumeId]bool),
		oversizedVolumes: make(map[needle.VolumeId]bool),
		volumeSizeLimit:  volumeSizeLimit,
		replicationAsMin: replicationAsMin,
	}
}

func (vl *VolumeLayout) String() string {
	return fmt.Sprintf("rp:%v, ttl:%v, vid2location:%v, writables:%v, volumeSizeLimit:%v", vl.rp, vl.ttl, vl.vid2location, vl.writables, vl.volumeSizeLimit)
}

func (vl *VolumeLayout) RegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	defer vl.ensureCorrectWritables(v)
	defer vl.rememberOversizedVolume(v)

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
				vl.readonlyVolumes[v.Id] = true
				return
			} else {
				delete(vl.readonlyVolumes, v.Id)
			}
		} else {
			glog.V(1).Infof("vid %d removed from writable", v.Id)
			vl.removeFromWritable(v.Id)
			delete(vl.readonlyVolumes, v.Id)
			return
		}
	}

}

func (vl *VolumeLayout) rememberOversizedVolume(v *storage.VolumeInfo) {
	if vl.isOversized(v) {
		vl.oversizedVolumes[v.Id] = true
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

		vl.ensureCorrectWritables(v)

		if location.Length() == 0 {
			delete(vl.vid2location, v.Id)
		}

	}
}

func (vl *VolumeLayout) ensureCorrectWritables(v *storage.VolumeInfo) {
	if vl.enoughCopies(v.Id) && vl.isWritable(v) {
		if _, ok := vl.oversizedVolumes[v.Id]; !ok {
			vl.setVolumeWritable(v.Id)
		}
	} else {
		vl.removeFromWritable(v.Id)
	}
}

func (vl *VolumeLayout) isOversized(v *storage.VolumeInfo) bool {
	return uint64(v.Size) >= vl.volumeSizeLimit
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

func (vl *VolumeLayout) PickForWrite(count uint64, option *VolumeGrowOption) (*needle.VolumeId, uint64, *VolumeLocationList, error) {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	lenWriters := len(vl.writables)
	if lenWriters <= 0 {
		glog.V(0).Infoln("No more writable volumes!")
		return nil, 0, nil, errors.New("No more writable volumes!")
	}
	if option.DataCenter == "" {
		vid := vl.writables[rand.Intn(lenWriters)]
		locationList := vl.vid2location[vid]
		if locationList != nil {
			return &vid, count, locationList, nil
		}
		return nil, 0, nil, errors.New("Strangely vid " + vid.String() + " is on no machine!")
	}
	var vid needle.VolumeId
	var locationList *VolumeLocationList
	counter := 0
	for _, v := range vl.writables {
		volumeLocationList := vl.vid2location[v]
		for _, dn := range volumeLocationList.list {
			if dn.GetDataCenter().Id() == NodeId(option.DataCenter) {
				if option.Rack != "" && dn.GetRack().Id() != NodeId(option.Rack) {
					continue
				}
				if option.DataNode != "" && dn.Id() != NodeId(option.DataNode) {
					continue
				}
				counter++
				if rand.Intn(counter) < 1 {
					vid, locationList = v, volumeLocationList
				}
			}
		}
	}
	return &vid, count, locationList, nil
}

func (vl *VolumeLayout) GetActiveVolumeCount(option *VolumeGrowOption) int {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	if option.DataCenter == "" {
		return len(vl.writables)
	}
	counter := 0
	for _, v := range vl.writables {
		for _, dn := range vl.vid2location[v].list {
			if dn.GetDataCenter().Id() == NodeId(option.DataCenter) {
				if option.Rack != "" && dn.GetRack().Id() != NodeId(option.Rack) {
					continue
				}
				if option.DataNode != "" && dn.Id() != NodeId(option.DataNode) {
					continue
				}
				counter++
			}
		}
	}
	return counter
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
	glog.V(0).Infoln("Volume", vid, "becomes writable")
	vl.writables = append(vl.writables, vid)
	return true
}

func (vl *VolumeLayout) SetVolumeUnavailable(dn *DataNode, vid needle.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if location, ok := vl.vid2location[vid]; ok {
		if location.Remove(dn) {
			if location.Length() < vl.rp.GetCopyCount() {
				glog.V(0).Infoln("Volume", vid, "has", location.Length(), "replica, less than required", vl.rp.GetCopyCount())
				return vl.removeFromWritable(vid)
			}
		}
	}
	return false
}
func (vl *VolumeLayout) SetVolumeAvailable(dn *DataNode, vid needle.VolumeId, isReadOnly bool) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	vInfo, err := dn.GetVolumesById(vid)
	if err != nil {
		return false
	}

	vl.vid2location[vid].Set(dn)

	if vInfo.ReadOnly || isReadOnly {
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

	// glog.V(0).Infoln("Volume", vid, "reaches full capacity.")
	return vl.removeFromWritable(vid)
}

func (vl *VolumeLayout) ToMap() map[string]interface{} {
	m := make(map[string]interface{})
	m["replication"] = vl.rp.String()
	m["ttl"] = vl.ttl.String()
	m["writables"] = vl.writables
	//m["locations"] = vl.vid2location
	return m
}

func (vl *VolumeLayout) Stats() *VolumeLayoutStats {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	ret := &VolumeLayoutStats{}

	freshThreshold := time.Now().Unix() - 60

	for vid, vll := range vl.vid2location {
		size, fileCount := vll.Stats(vid, freshThreshold)
		ret.FileCount += uint64(fileCount)
		ret.UsedSize += size
		if vl.readonlyVolumes[vid] {
			ret.TotalSize += size
		} else {
			ret.TotalSize += vl.volumeSizeLimit
		}
	}

	return ret
}

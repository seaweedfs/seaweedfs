package topology

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

// mapping from volume to its locations, inverted from server to volume
type VolumeLayout struct {
	rp           *storage.ReplicaPlacement
	ttl          *storage.TTL
	vid2location map[storage.VolumeId]*VolumeLocationList

	volumeSizeLimit uint64
	accessLock      sync.RWMutex
}

type VolumeLayoutStats struct {
	TotalSize uint64
	UsedSize  uint64
	FileCount uint64
}

func NewVolumeLayout(rp *storage.ReplicaPlacement, ttl *storage.TTL, volumeSizeLimit uint64) *VolumeLayout {
	return &VolumeLayout{
		rp:              rp,
		ttl:             ttl,
		vid2location:    make(map[storage.VolumeId]*VolumeLocationList),
		volumeSizeLimit: volumeSizeLimit,
	}
}

func (vl *VolumeLayout) String() string {
	return fmt.Sprintf("rp:%v, ttl:%v, vid2location:%v, volumeSizeLimit:%v", vl.rp, vl.ttl, vl.vid2location, vl.volumeSizeLimit)
}

func (vl *VolumeLayout) RegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if _, ok := vl.vid2location[v.Id]; !ok {
		vl.vid2location[v.Id] = NewVolumeLocationList()
	}
	vl.vid2location[v.Id].Set(dn)
	// glog.V(4).Infof("volume %d added to %s len %d copy %d", v.Id, dn.Id(), vl.vid2location[v.Id].Length(), v.ReplicaPlacement.GetCopyCount())
	for _, dn := range vl.vid2location[v.Id].list {
		if vInfo, err := dn.GetVolumesById(v.Id); err == nil {
			if vInfo.IsReadOnly() {
				glog.V(3).Infof("vid %d removed from writable", v.Id)
				return
			}
		} else {
			glog.V(3).Infof("vid %d removed from writable", v.Id)
			return
		}
	}
}

func (vl *VolumeLayout) UnRegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	delete(vl.vid2location, v.Id)
}

func (vl *VolumeLayout) isOverSized(v *storage.VolumeInfo) bool {
	return uint64(v.Size) >= vl.volumeSizeLimit
}

func (vl *VolumeLayout) isWritable(v *storage.VolumeInfo) bool {
	return !vl.isOverSized(v) && v.Version == storage.CurrentVersion && !v.IsReadOnly()
}

func (vl *VolumeLayout) isEmpty() bool {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	return len(vl.vid2location) == 0
}

func (vl *VolumeLayout) Lookup(vid storage.VolumeId) []*DataNode {
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

func (vl *VolumeLayout) PickForWrite(count uint64, option *VolumeGrowOption) (*storage.VolumeId, uint64, *VolumeLocationList, error) {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	var vid storage.VolumeId
	var locationList *VolumeLocationList
	counter := 0

NextVolume:
	for v, vll := range vl.vid2location {
		if option.ReplicaPlacement.GetCopyCount() != vll.Length() {
			continue
		}

		for _, dn := range vll.list {
			if !option.MatchesDataCenter(dn) || !option.MatchesRackDataNode(dn) {
				continue NextVolume
			}

			vi, _ := dn.GetVolumesById(v)
			if !vl.isWritable(&vi) {
				continue NextVolume
			}
		}

		counter++
		if rand.Intn(counter) < 1 {
			vid, locationList = v, vll
		}
	}
	return &vid, count, locationList, nil
}

func (vl *VolumeLayout) GetWritableVolumeCount(option *VolumeGrowOption) int {
	vl.accessLock.RLock()
	defer vl.accessLock.RUnlock()

	counter := 0

NextVolume:
	for vid, vll := range vl.vid2location {
		if option.ReplicaPlacement.GetCopyCount() != vll.Length() {
			continue
		}

		for _, dn := range vll.list {
			if !option.MatchesDataCenter(dn) || !option.MatchesRackDataNode(dn) {
				continue NextVolume
			}

			vi, _ := dn.GetVolumesById(vid)
			if !vl.isWritable(&vi) {
				continue NextVolume
			}
		}

		counter++
	}

	return counter
}

func (vl *VolumeLayout) SetVolumeAvailable(dn *DataNode, vid storage.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	vl.vid2location[vid].Set(dn)
	return false
}

func (vl *VolumeLayout) ToMap() map[string]interface{} {
	m := make(map[string]interface{})
	m["replication"] = vl.rp.String()
	m["ttl"] = vl.ttl.String()
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

		writable := true
		for _, dn := range vll.list {
			vi, _ := dn.GetVolumesById(vid)
			if !vl.isWritable(&vi) {
				writable = false
				break
			}
		}

		if writable {
			ret.TotalSize += size
		} else {
			ret.TotalSize += vl.volumeSizeLimit
		}
	}

	return ret
}

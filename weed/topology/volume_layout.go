package topology

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

// mapping from volume to its locations, inverted from server to volume
type VolumeLayout struct {
	rp              *storage.ReplicaPlacement
	ttl             *storage.TTL
	vid2location    map[storage.VolumeId]*VolumeLocationList
	writables       []storage.VolumeId // transient array of writable volume id
	volumeSizeLimit uint64
	mutex           sync.RWMutex
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
	vl.mutex.RLock()
	defer vl.mutex.RUnlock()
	return fmt.Sprintf("rp:%v, ttl:%v, vid2location:%v, writables:%v, volumeSizeLimit:%v", vl.rp, vl.ttl, vl.vid2location, vl.writables, vl.volumeSizeLimit)
}

func (vl *VolumeLayout) RegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.mutex.Lock()
	defer vl.mutex.Unlock()

	if _, ok := vl.vid2location[v.Id]; !ok {
		vl.vid2location[v.Id] = NewVolumeLocationList()
	}
	vl.vid2location[v.Id].Set(dn)
	glog.V(4).Infoln("volume", v.Id, "added to dn", dn.Id(), "len", vl.vid2location[v.Id].Length())
	//TODO balancing data when have more replications
	if vl.vid2location[v.Id].Length() == vl.rp.GetCopyCount() && vl.IsWritable(v) {
		vl.addToWritable(v.Id)
	} else {
		vl.removeFromWritable(v.Id)
	}
}

func (vl *VolumeLayout) UnRegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.mutex.Lock()
	defer vl.mutex.Unlock()
	//TODO only delete data node from locations?
	vl.removeFromWritable(v.Id)
	delete(vl.vid2location, v.Id)
}

func (vl *VolumeLayout) addToWritable(vid storage.VolumeId) {
	for _, id := range vl.writables {
		if vid == id {
			return
		}
	}
	vl.writables = append(vl.writables, vid)
}

func (vl *VolumeLayout) IsWritable(v *storage.VolumeInfo) bool {
	return uint64(v.Size) < vl.volumeSizeLimit &&
		v.Version == storage.CurrentVersion &&
		!v.ReadOnly
}

func (vl *VolumeLayout) Lookup(vid storage.VolumeId) *VolumeLocationList {
	vl.mutex.RLock()
	defer vl.mutex.RUnlock()
	if location := vl.vid2location[vid]; location != nil {
		return location.Duplicate()
	}
	return nil
}

func (vl *VolumeLayout) ListVolumeServers() (nodes []*DataNode) {
	vl.mutex.RLock()
	defer vl.mutex.RUnlock()
	for _, location := range vl.vid2location {
		nodes = append(nodes, location.AllDataNode()...)
	}
	return
}

func (vl *VolumeLayout) ListVolumeId() (vids []storage.VolumeId) {
	vl.mutex.RLock()
	defer vl.mutex.RUnlock()
	vids = make([]storage.VolumeId, 0, len(vl.vid2location))
	for vid := range vl.vid2location {
		vids = append(vids, vid)
	}
	return
}

func (vl *VolumeLayout) PickForWrite(count uint64, option *VolumeGrowOption) (*storage.VolumeId, uint64, *VolumeLocationList, error) {
	vl.mutex.RLock()
	defer vl.mutex.RUnlock()
	len_writers := len(vl.writables)
	if len_writers <= 0 {
		glog.V(0).Infoln("No more writable volumes!")
		return nil, 0, nil, errors.New("No more writable volumes!")
	}
	if option.DataCenter == "" {
		vid := vl.writables[rand.Intn(len_writers)]
		locationList := vl.vid2location[vid]
		if locationList != nil {
			return &vid, count, locationList.Duplicate(), nil
		}
		return nil, 0, nil, errors.New("Strangely vid " + vid.String() + " is on no machine!")
	}
	var vid storage.VolumeId
	var locationList *VolumeLocationList
	counter := 0
	for _, v := range vl.writables {
		volumeLocationList := vl.vid2location[v]
		for _, dn := range volumeLocationList.AllDataNode() {
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
	return &vid, count, locationList.Duplicate(), nil
}

func (vl *VolumeLayout) GetActiveVolumeCount(option *VolumeGrowOption) int {
	vl.mutex.RLock()
	defer vl.mutex.RUnlock()
	if option.DataCenter == "" {
		return len(vl.writables)
	}
	counter := 0
	for _, v := range vl.writables {
		for _, dn := range vl.vid2location[v].AllDataNode() {
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

func (vl *VolumeLayout) removeFromWritable(vid storage.VolumeId) bool {
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

func (vl *VolumeLayout) RemoveFromWritable(vid storage.VolumeId) bool {
	vl.mutex.Lock()
	defer vl.mutex.Unlock()
	return vl.removeFromWritable(vid)
}

func (vl *VolumeLayout) setVolumeWritable(vid storage.VolumeId) bool {
	for _, v := range vl.writables {
		if v == vid {
			return false
		}
	}
	glog.V(0).Infoln("Volume", vid, "becomes writable")
	vl.writables = append(vl.writables, vid)
	return true
}

func (vl *VolumeLayout) SetVolumeUnavailable(dn *DataNode, vid storage.VolumeId) bool {
	vl.mutex.Lock()
	defer vl.mutex.Unlock()

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
func (vl *VolumeLayout) SetVolumeAvailable(dn *DataNode, vid storage.VolumeId) bool {
	vl.mutex.Lock()
	defer vl.mutex.Unlock()

	vl.vid2location[vid].Set(dn)
	if vl.vid2location[vid].Length() >= vl.rp.GetCopyCount() {
		return vl.setVolumeWritable(vid)
	}
	return false
}

func (vl *VolumeLayout) SetVolumeCapacityFull(vid storage.VolumeId) bool {
	vl.mutex.Lock()
	defer vl.mutex.Unlock()

	// glog.V(0).Infoln("Volume", vid, "reaches full capacity.")
	return vl.removeFromWritable(vid)
}

func (vl *VolumeLayout) ToMap() map[string]interface{} {
	vl.mutex.RLock()
	defer vl.mutex.RUnlock()
	m := make(map[string]interface{})
	m["replication"] = vl.rp.String()
	m["ttl"] = vl.ttl.String()
	m["writables"] = vl.writables
	//m["locations"] = vl.vid2location
	return m
}

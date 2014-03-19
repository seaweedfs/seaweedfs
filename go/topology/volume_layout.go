package topology

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/storage"
	"errors"
	"math/rand"
	"sync"
)

// mapping from volume to its locations, inverted from server to volume
type VolumeLayout struct {
	rp              *storage.ReplicaPlacement
	vid2location    map[storage.VolumeId]*VolumeLocationList
	writables       []storage.VolumeId // transient array of writable volume id
	volumeSizeLimit uint64
	accessLock      sync.Mutex
}

func NewVolumeLayout(rp *storage.ReplicaPlacement, volumeSizeLimit uint64) *VolumeLayout {
	return &VolumeLayout{
		rp:              rp,
		vid2location:    make(map[storage.VolumeId]*VolumeLocationList),
		writables:       *new([]storage.VolumeId),
		volumeSizeLimit: volumeSizeLimit,
	}
}

func (vl *VolumeLayout) RegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	if _, ok := vl.vid2location[v.Id]; !ok {
		vl.vid2location[v.Id] = NewVolumeLocationList()
	}
	vl.vid2location[v.Id].Set(dn)
	glog.V(3).Infoln("volume", v.Id, "added to dn", dn, "len", vl.vid2location[v.Id].Length(), "copy", v.ReplicaPlacement.GetCopyCount())
	if vl.vid2location[v.Id].Length() == vl.rp.GetCopyCount() && vl.isWritable(v) {
		vl.AddToWritable(v.Id)
	} else {
		vl.removeFromWritable(v.Id)
	}
}

func (vl *VolumeLayout) AddToWritable(vid storage.VolumeId) {
	for _, id := range vl.writables {
		if vid == id {
			return
		}
	}
	vl.writables = append(vl.writables, vid)
}

func (vl *VolumeLayout) isWritable(v *storage.VolumeInfo) bool {
	return uint64(v.Size) < vl.volumeSizeLimit &&
		v.Version == storage.CurrentVersion &&
		!v.ReadOnly
}

func (vl *VolumeLayout) Lookup(vid storage.VolumeId) []*DataNode {
	if location := vl.vid2location[vid]; location != nil {
		return location.list
	}
	return nil
}

func (vl *VolumeLayout) ListVolumeServers() (nodes []*DataNode) {
	for _, location := range vl.vid2location {
		nodes = append(nodes, location.list...)
	}
	return
}

func (vl *VolumeLayout) PickForWrite(count int, dataCenter string) (*storage.VolumeId, int, *VolumeLocationList, error) {
	len_writers := len(vl.writables)
	if len_writers <= 0 {
		glog.V(0).Infoln("No more writable volumes!")
		return nil, 0, nil, errors.New("No more writable volumes!")
	}
	if dataCenter == "" {
		vid := vl.writables[rand.Intn(len_writers)]
		locationList := vl.vid2location[vid]
		if locationList != nil {
			return &vid, count, locationList, nil
		}
		return nil, 0, nil, errors.New("Strangely vid " + vid.String() + " is on no machine!")
	} else {
		var vid storage.VolumeId
		var locationList *VolumeLocationList
		counter := 0
		for _, v := range vl.writables {
			volumeLocationList := vl.vid2location[v]
			for _, dn := range volumeLocationList.list {
				if dn.GetDataCenter().Id() == NodeId(dataCenter) {
					counter++
					if rand.Intn(counter) < 1 {
						vid, locationList = v, volumeLocationList
					}
				}
			}
		}
		return &vid, count, locationList, nil
	}
	return nil, 0, nil, errors.New("Strangely This Should Never Have Happened!")
}

func (vl *VolumeLayout) GetActiveVolumeCount(dataCenter string) int {
	if dataCenter == "" {
		return len(vl.writables)
	}
	counter := 0
	for _, v := range vl.writables {
		for _, dn := range vl.vid2location[v].list {
			if dn.GetDataCenter().Id() == NodeId(dataCenter) {
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
func (vl *VolumeLayout) SetVolumeAvailable(dn *DataNode, vid storage.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	vl.vid2location[vid].Set(dn)
	if vl.vid2location[vid].Length() >= vl.rp.GetCopyCount() {
		return vl.setVolumeWritable(vid)
	}
	return false
}

func (vl *VolumeLayout) SetVolumeCapacityFull(vid storage.VolumeId) bool {
	vl.accessLock.Lock()
	defer vl.accessLock.Unlock()

	// glog.V(0).Infoln("Volume", vid, "reaches full capacity.")
	return vl.removeFromWritable(vid)
}

func (vl *VolumeLayout) ToMap() map[string]interface{} {
	m := make(map[string]interface{})
	m["replication"] = vl.rp.String()
	m["writables"] = vl.writables
	//m["locations"] = vl.vid2location
	return m
}

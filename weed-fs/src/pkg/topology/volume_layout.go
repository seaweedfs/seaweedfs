package topology

import (
	"errors"
	"fmt"
	"math/rand"
	"pkg/storage"
)

type VolumeLayout struct {
	repType         storage.ReplicationType
	vid2location    map[storage.VolumeId]*DataNodeLocationList
	writables       []storage.VolumeId // transient array of writable volume id
	pulse           int64
	volumeSizeLimit uint64
}

func NewVolumeLayout(repType storage.ReplicationType, volumeSizeLimit uint64, pulse int64) *VolumeLayout {
	return &VolumeLayout{
		repType:         repType,
		vid2location:    make(map[storage.VolumeId]*DataNodeLocationList),
		writables:       *new([]storage.VolumeId),
		pulse:           pulse,
		volumeSizeLimit: volumeSizeLimit,
	}
}

func (vl *VolumeLayout) RegisterVolume(v *storage.VolumeInfo, dn *DataNode) {
	if _, ok := vl.vid2location[v.Id]; !ok {
		vl.vid2location[v.Id] = NewDataNodeLocationList()
	}
	if vl.vid2location[v.Id].Add(dn) {
		if len(vl.vid2location[v.Id].list) == v.RepType.GetCopyCount() {
			if uint64(v.Size) < vl.volumeSizeLimit {
				vl.writables = append(vl.writables, v.Id)
			}
		}
	}
}

func (vl *VolumeLayout) PickForWrite(count int) (*storage.VolumeId, int, *DataNodeLocationList, error) {
	len_writers := len(vl.writables)
	if len_writers <= 0 {
		fmt.Println("No more writable volumes!")
		return nil, 0, nil, errors.New("No more writable volumes!")
	}
	vid := vl.writables[rand.Intn(len_writers)]
	locationList := vl.vid2location[vid]
	if locationList != nil {
		return &vid, count, locationList, nil
	}
	return nil, 0, nil, errors.New("Strangely vid " + vid.String() + " is on no machine!")
}

func (vl *VolumeLayout) GetActiveVolumeCount() int {
	return len(vl.writables)
}

func (vl *VolumeLayout) SetVolumeReadOnly(vid storage.VolumeId) bool {
  for i, v := range vl.writables{
    if v == vid {
      vl.writables = append(vl.writables[:i],vl.writables[i+1:]...)
      return true
    }
  }
  return false
}

func (vl *VolumeLayout) SetVolumeWritable(vid storage.VolumeId) bool {
  for _, v := range vl.writables{
    if v == vid {
      return false
    }
  }
  vl.writables = append(vl.writables, vid)
  return true
}

func (vl *VolumeLayout) ToMap() interface{} {
	m := make(map[string]interface{})
	m["replication"] = vl.repType.String()
	m["writables"] = vl.writables
	//m["locations"] = vl.vid2location
	return m
}

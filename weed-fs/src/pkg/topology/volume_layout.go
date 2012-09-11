package topology

import (
	"errors"
	"fmt"
	"math/rand"
	"pkg/storage"
)

type VolumeLayout struct {
	vid2location    map[storage.VolumeId]*DataNodeLocationList
	writables       []storage.VolumeId // transient array of writable volume id
	pulse           int64
	volumeSizeLimit uint64
}

func NewVolumeLayout(volumeSizeLimit uint64, pulse int64) *VolumeLayout {
	return &VolumeLayout{
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
	vl.vid2location[v.Id].Add(dn)
	if len(vl.vid2location[v.Id].list) >= storage.GetCopyCount(v) {
		if uint64(v.Size) < vl.volumeSizeLimit {
			vl.writables = append(vl.writables, v.Id)
		}
	}
}

func (vl *VolumeLayout) PickForWrite(count int) (int, *DataNodeLocationList, error) {
	len_writers := len(vl.writables)
	if len_writers <= 0 {
		fmt.Println("No more writable volumes!")
		return 0, nil, errors.New("No more writable volumes!")
	}
	vid := vl.writables[rand.Intn(len_writers)]
	locationList := vl.vid2location[vid]
	if locationList != nil {
		return count, locationList, nil
	}
	return 0, nil, errors.New("Strangely vid " + vid.String() + " is on no machine!")
}

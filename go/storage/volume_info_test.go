package storage

import "testing"

func TestSortVolumeInfos(t *testing.T) {
	vis := []*VolumeInfo{
		&VolumeInfo{
			Id: 2,
		},
		&VolumeInfo{
			Id: 1,
		},
		&VolumeInfo{
			Id: 3,
		},
	}
	sortVolumeInfos(vis)
	for i := 0; i < len(vis); i++ {
		if vis[i].Id != VolumeId(i+1) {
			t.Fatal()
		}
	}
}

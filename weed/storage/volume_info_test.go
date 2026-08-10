package storage

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

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
		if vis[i].Id != needle.VolumeId(i+1) {
			t.Fatal()
		}
	}
}

func TestNewVolumeInfoFromShortKeepsDiskId(t *testing.T) {
	vi, err := NewVolumeInfoFromShort(&master_pb.VolumeShortInformationMessage{
		Id:      7,
		Version: 3,
		DiskId:  3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if vi.DiskId != 3 {
		t.Fatalf("DiskId = %d, want 3", vi.DiskId)
	}
}

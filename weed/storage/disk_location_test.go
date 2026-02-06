package storage

import (
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type (
	mockBackendStorageFile struct {
		backend.DiskFile

		datSize int64
	}
)

func (df *mockBackendStorageFile) GetStat() (datSize int64, modTime time.Time, err error) {
	return df.datSize, time.Now(), nil
}

type (
	mockNeedleMapper struct {
		NeedleMap

		idxSize uint64
	}
)

func (nm *mockNeedleMapper) IndexFileSize() (idxSize uint64) {
	return nm.idxSize
}

func TestUnUsedSpace(t *testing.T) {
	minFreeSpace := util.MinFreeSpace{Type: util.AsPercent, Percent: 1, Raw: "1"}

	diskLocation := DiskLocation{
		Directory:              "/test/",
		DirectoryUuid:          "1234",
		IdxDirectory:           "/test/",
		DiskType:               types.HddType,
		MaxVolumeCount:         0,
		OriginalMaxVolumeCount: 0,
		MinFreeSpace:           minFreeSpace,
	}
	diskLocation.volumes = make(map[needle.VolumeId]*Volume)

	volumes := [3]*Volume{
		{dir: diskLocation.Directory, dirIdx: diskLocation.IdxDirectory, Collection: "", Id: 0, DataBackend: &mockBackendStorageFile{datSize: 990}, nm: &mockNeedleMapper{idxSize: 10}},
		{dir: diskLocation.Directory, dirIdx: diskLocation.IdxDirectory, Collection: "", Id: 1, DataBackend: &mockBackendStorageFile{datSize: 990}, nm: &mockNeedleMapper{idxSize: 10}},
		{dir: diskLocation.Directory, dirIdx: diskLocation.IdxDirectory, Collection: "", Id: 2, DataBackend: &mockBackendStorageFile{datSize: 990}, nm: &mockNeedleMapper{idxSize: 10}},
	}

	for i, vol := range volumes {
		diskLocation.SetVolume(needle.VolumeId(i), vol)
	}

	// Testing when there's still space
	unUsedSpace := diskLocation.UnUsedSpace(1200)
	if unUsedSpace != 600 {
		t.Errorf("unUsedSpace incorrect: %d != %d", unUsedSpace, 1500)
	}

	// Testing when there's exactly 0 space
	unUsedSpace = diskLocation.UnUsedSpace(1000)
	if unUsedSpace != 0 {
		t.Errorf("unUsedSpace incorrect: %d != %d", unUsedSpace, 0)
	}

	// Testing when there's negative free space
	unUsedSpace = diskLocation.UnUsedSpace(900)
	if unUsedSpace != 0 {
		t.Errorf("unUsedSpace incorrect: %d != %d", unUsedSpace, 0)
	}
}

func TestResolveVolumeIDs(t *testing.T) {
	l := DiskLocation{
		volumes: map[needle.VolumeId]*Volume{
			0: &Volume{},
			1: &Volume{},
			2: &Volume{},
		},
		ecVolumes: map[needle.VolumeId]*erasure_coding.EcVolume{
			3: &erasure_coding.EcVolume{},
			4: &erasure_coding.EcVolume{},
			5: &erasure_coding.EcVolume{},
		},
	}

	if got, want := l.VolumeIds(), []needle.VolumeId{0, 1, 2}; !reflect.DeepEqual(got, want) {
		t.Errorf("wanted volume IDs %v, got %v", want, got)
	}
	if got, want := l.EcVolumeIds(), []needle.VolumeId{3, 4, 5}; !reflect.DeepEqual(got, want) {
		t.Errorf("wanted EC volume IDs %v, got %v", want, got)
	}
}

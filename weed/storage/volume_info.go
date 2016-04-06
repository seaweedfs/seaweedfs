package storage

import (
	"fmt"
	"sort"

	"github.com/chrislusf/seaweedfs/weed/weedpb"
)

type VolumeInfo struct {
	Id               VolumeId
	Size             uint64
	Ttl              *TTL
	Collection       string
	Version          Version
	FileCount        int
	DeleteCount      int
	DeletedByteCount uint64
	ReadOnly         bool
}

func NewVolumeInfo(m *weedpb.VolumeInformationMessage) (vi *VolumeInfo, err error) {
	vi = &VolumeInfo{
		Id:               VolumeId(m.Id),
		Size:             m.Size,
		Collection:       m.Collection,
		FileCount:        int(m.FileCount),
		DeleteCount:      int(m.DeleteCount),
		DeletedByteCount: m.DeletedByteCount,
		ReadOnly:         m.ReadOnly,
		Version:          Version(m.Version),
	}
	vi.Ttl = LoadTTLFromUint32(m.Ttl)
	return vi, nil
}

func (vi VolumeInfo) String() string {
	return fmt.Sprintf("Id:%d, Size:%d, Collection:%s, Version:%v, FileCount:%d, DeleteCount:%d, DeletedByteCount:%d, ReadOnly:%v",
		vi.Id, vi.Size, vi.Collection, vi.Version, vi.FileCount, vi.DeleteCount, vi.DeletedByteCount, vi.ReadOnly)
}

/*VolumesInfo sorting*/

type volumeInfos []*VolumeInfo

func (vis volumeInfos) Len() int {
	return len(vis)
}

func (vis volumeInfos) Less(i, j int) bool {
	return vis[i].Id < vis[j].Id
}

func (vis volumeInfos) Swap(i, j int) {
	vis[i], vis[j] = vis[j], vis[i]
}

func sortVolumeInfos(vis volumeInfos) {
	sort.Sort(vis)
}

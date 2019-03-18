package storage

import (
	"fmt"
	"sort"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

type VolumeInfo struct {
	Id               VolumeId
	Size             uint64
	ReplicaPlacement *ReplicaPlacement
	Ttl              *TTL
	Collection       string
	Version          Version
	FileCount        int
	DeleteCount      int
	DeletedByteCount uint64
	ReadOnly         bool
}

func NewVolumeInfo(m *master_pb.VolumeInformationMessage) (vi VolumeInfo, err error) {
	vi = VolumeInfo{
		Id:               VolumeId(m.Id),
		Size:             m.Size,
		Collection:       m.Collection,
		FileCount:        int(m.FileCount),
		DeleteCount:      int(m.DeleteCount),
		DeletedByteCount: m.DeletedByteCount,
		ReadOnly:         m.ReadOnly,
		Version:          Version(m.Version),
	}
	rp, e := NewReplicaPlacementFromByte(byte(m.ReplicaPlacement))
	if e != nil {
		return vi, e
	}
	vi.ReplicaPlacement = rp
	vi.Ttl = LoadTTLFromUint32(m.Ttl)
	return vi, nil
}

func (vi VolumeInfo) String() string {
	return fmt.Sprintf("Id:%d, Size:%d, ReplicaPlacement:%s, Collection:%s, Version:%v, FileCount:%d, DeleteCount:%d, DeletedByteCount:%d, ReadOnly:%v",
		vi.Id, vi.Size, vi.ReplicaPlacement, vi.Collection, vi.Version, vi.FileCount, vi.DeleteCount, vi.DeletedByteCount, vi.ReadOnly)
}

func (vi VolumeInfo) ToVolumeInformationMessage() *master_pb.VolumeInformationMessage {
	return &master_pb.VolumeInformationMessage{
		Id:               uint32(vi.Id),
		Size:             uint64(vi.Size),
		Collection:       vi.Collection,
		FileCount:        uint64(vi.FileCount),
		DeleteCount:      uint64(vi.DeleteCount),
		DeletedByteCount: vi.DeletedByteCount,
		ReadOnly:         vi.ReadOnly,
		ReplicaPlacement: uint32(vi.ReplicaPlacement.Byte()),
		Version:          uint32(vi.Version),
		Ttl:              vi.Ttl.ToUint32(),
	}
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

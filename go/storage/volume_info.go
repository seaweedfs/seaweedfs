package storage

import (
	"code.google.com/p/weed-fs/go/operation"
)

type VolumeInfo struct {
	Id               VolumeId
	Size             uint64
	ReplicaPlacement *ReplicaPlacement
	Collection       string
	Version          Version
	FileCount        int
	DeleteCount      int
	DeletedByteCount uint64
	ReadOnly         bool
}

func NewVolumeInfo(m *operation.VolumeInformationMessage) (vi VolumeInfo, err error) {
	vi = VolumeInfo{
		Id:               VolumeId(*m.Id),
		Size:             *m.Size,
		Collection:       *m.Collection,
		FileCount:        int(*m.FileCount),
		DeleteCount:      int(*m.DeleteCount),
		DeletedByteCount: *m.DeletedByteCount,
		ReadOnly:         *m.ReadOnly,
		Version:          Version(*m.Version),
	}
	rp, e := NewReplicaPlacementFromByte(byte(*m.ReplicaPlacement))
	if e != nil {
		return vi, e
	}
	vi.ReplicaPlacement = rp
	return vi, nil
}

package storage

import ()

type VolumeInfo struct {
	Id               VolumeId
	Size             uint64
	RepType          ReplicationType
	Collection       string
	Version          Version
	FileCount        int
	DeleteCount      int
	DeletedByteCount uint64
	ReadOnly         bool
}

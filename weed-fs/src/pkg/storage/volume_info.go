package storage

import ()

type VolumeInfo struct {
	Id               VolumeId
	Size             uint64
	RepType          ReplicationType
	Version          Version
	FileCount        int
	DeleteCount      int
	DeletedByteCount uint64
}

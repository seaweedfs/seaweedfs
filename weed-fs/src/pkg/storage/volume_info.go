package storage

import (
)

type VolumeInfo struct {
	Id      VolumeId
	Size    int64
	RepType ReplicationType
	FileCount int
	DeleteCount int
}

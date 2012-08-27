package storage

import (
)

type StorageLimit struct {
	sizeLimit     uint64
}

func NewStorageLimit(desiredLimit uint64) *StorageLimit {
	sl := &StorageLimit{sizeLimit: desiredLimit}
	return sl
}

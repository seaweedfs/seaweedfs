package storage

import (
	"syscall"
)

type StorageLimit struct {
	sizeLimit     uint64
	detectedLimit uint64
}

func NewStorageLimit(desiredLimit uint64) *StorageLimit {
	s := syscall.Statfs_t{}
	errNo := syscall.Statfs(".", &s)
	detected := uint64(0)
	if errNo==nil {
    detected = s.Bavail*uint64(s.Bsize)
	}
	sl := &StorageLimit{sizeLimit: desiredLimit, detectedLimit: detected}
	return sl
}

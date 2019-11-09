// +build linux

package storage

import (
	"os"
	"syscall"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
)

func createVolumeFile(fileName string, preallocate int64, memoryMapSizeMB uint32) (backend.DataStorageBackend, error) {
	file, e := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if preallocate != 0 {
		syscall.Fallocate(int(file.Fd()), 1, 0, preallocate)
		glog.V(0).Infof("Preallocated %d bytes disk space for %s", preallocate, fileName)
	}
	return backend.NewDiskFile(file), e
}

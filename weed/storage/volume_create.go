// +build !linux,!windows

package storage

import (
	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
)

func createVolumeFile(fileName string, preallocate int64, memoryMapSizeMB uint32) (backend.DataStorageBackend, error) {
	file, e := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if preallocate > 0 {
		glog.V(0).Infof("Preallocated disk space for %s is not supported", fileName)
	}
	return backend.NewDiskFile(file), e
}

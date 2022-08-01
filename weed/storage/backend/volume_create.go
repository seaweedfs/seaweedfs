//go:build !linux && !windows
// +build !linux,!windows

package backend

import (
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

func CreateVolumeFile(fileName string, preallocate int64, memoryMapSizeMB uint32) (BackendStorageFile, error) {
	file, e := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if e != nil {
		return nil, e
	}
	if preallocate > 0 {
		glog.V(2).Infof("Preallocated disk space for %s is not supported", fileName)
	}
	return NewDiskFile(file), nil
}

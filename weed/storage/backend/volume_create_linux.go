//go:build linux

package backend

import (
	"os"
	"syscall"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

func CreateVolumeFile(fileName string, preallocate int64, memoryMapSizeMB uint32) (BackendStorageFile, error) {
	file, e := OpenVolumeFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	if e != nil {
		return nil, e
	}
	if preallocate != 0 {
		syscall.Fallocate(int(file.Fd()), 1, 0, preallocate)
		glog.V(1).Infof("Preallocated %d bytes disk space for %s", preallocate, fileName)
	}
	return NewDiskFile(file), nil
}

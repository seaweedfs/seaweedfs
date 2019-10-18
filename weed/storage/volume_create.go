// +build !linux,!windows

package storage

import (
	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

func createVolumeFile(fileName string, preallocate int64, memoryMapSizeMB uint32) (*os.File, error) {
	file, e := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if preallocate > 0 {
		glog.V(0).Infof("Preallocated disk space for %s is not supported", fileName)
	}
	return file, e
}

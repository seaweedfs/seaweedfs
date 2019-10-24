// +build windows

package storage

import (
	"os"

	"github.com/chrislusf/seaweedfs/weed/storage/memory_map"
	"golang.org/x/sys/windows"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/memory_map/os_overloads"
)

func createVolumeFile(fileName string, preallocate int64, memoryMapSizeMB uint32) (*os.File, error) {

	mMap, exists := memory_map.FileMemoryMap[fileName]
	if !exists {

		if preallocate > 0 {
			glog.V(0).Infof("Preallocated disk space for %s is not supported", fileName)
		}

		if memoryMapSizeMB > 0 {
			file, e := os_overloads.OpenFile(fileName, windows.O_RDWR|windows.O_CREAT, 0644, true)
			memory_map.FileMemoryMap[fileName] = new(memory_map.MemoryMap)

			new_mMap := memory_map.FileMemoryMap[fileName]
			new_mMap.CreateMemoryMap(file, 1024*1024*uint64(memoryMapSizeMB))
			return file, e
		} else {
			file, e := os_overloads.OpenFile(fileName, windows.O_RDWR|windows.O_CREAT|windows.O_TRUNC, 0644, false)
			return file, e
		}
	} else {
		return mMap.File, nil
	}
}

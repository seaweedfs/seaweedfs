// +build windows

package storage

import (
	"os"

	"github.com/joeslay/seaweedfs/weed/storage/memory_map"
	"golang.org/x/sys/windows"

	"github.com/joeslay/seaweedfs/weed/glog"
	"github.com/joeslay/seaweedfs/weed/os_overloads"
)

func createVolumeFile(fileName string, preallocate int64) (*os.File, error) {

	mem_map, exists := memory_map.FileMemoryMap[fileName]
	if !exists {
		file, e := os_overloads.OpenFile(fileName, windows.O_RDWR|windows.O_CREAT, 0644, true)
		memory_map.FileMemoryMap[fileName] = new(memory_map.MemoryMap)

		new_mem_map := memory_map.FileMemoryMap[fileName]
		new_mem_map.CreateMemoryMap(file, 1024*1024*1024*4)

		if preallocate > 0 {
			glog.V(0).Infof("Preallocated disk space for %s is not supported", fileName)
		}
		return file, e
	} else {
		return mem_map.File, nil
	}
}

package memory_map

import (
	"os"
	"strconv"
)

type MemoryBuffer struct {
	Buffer []byte
}

type MemoryMap struct {
	File        *os.File
	End_of_file int64
}

func ReadMemoryMapMaxSizeMb(memoryMapMaxSizeMbString string) (uint32, error) {
	if memoryMapMaxSizeMbString == "" {
		return 0, nil
	}
	memoryMapMaxSize64, err := strconv.ParseUint(memoryMapMaxSizeMbString, 10, 32)
	return uint32(memoryMapMaxSize64), err
}

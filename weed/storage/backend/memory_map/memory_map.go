package memory_map

import (
	"os"
	"strconv"
)

type MemoryBuffer struct {
	aligned_length uint64
	length         uint64
	aligned_ptr    uintptr
	ptr            uintptr
	Buffer         []byte
}

type MemoryMap struct {
	File                   *os.File
	file_memory_map_handle uintptr
	write_map_views        []MemoryBuffer
	max_length             uint64
	End_of_file            int64
}

func ReadMemoryMapMaxSizeMb(memoryMapMaxSizeMbString string) (uint32, error) {
	if memoryMapMaxSizeMbString == "" {
		return 0, nil
	}
	memoryMapMaxSize64, err := strconv.ParseUint(memoryMapMaxSizeMbString, 10, 32)
	return uint32(memoryMapMaxSize64), err
}

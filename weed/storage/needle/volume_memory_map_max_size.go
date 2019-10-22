package needle

import "strconv"

func ReadMemoryMapMaxSizeMB(MemoryMapMaxSizeMBString string) (uint32, error) {
	if MemoryMapMaxSizeMBString == "" {
		return 0, nil
	}
	var MemoryMapMaxSize64 uint64
	var err error
	MemoryMapMaxSize64, err = strconv.ParseUint(MemoryMapMaxSizeMBString, 10, 32)
	MemoryMapMaxSize := uint32(MemoryMapMaxSize64)
	return MemoryMapMaxSize, err
}

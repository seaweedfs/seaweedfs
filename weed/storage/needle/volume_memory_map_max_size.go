package needle

import "strconv"

func ReadMemoryMapMaxSizeMb(MemoryMapMaxSizeMbString string) (uint32, error) {
	if MemoryMapMaxSizeMbString == "" {
		return 0, nil
	}
	var MemoryMapMaxSize64 uint64
	var err error
	MemoryMapMaxSize64, err = strconv.ParseUint(MemoryMapMaxSizeMbString, 10, 32)
	MemoryMapMaxSize := uint32(MemoryMapMaxSize64)
	return MemoryMapMaxSize, err
}

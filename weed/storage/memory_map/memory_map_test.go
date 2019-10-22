package memory_map

import "testing"

func TestMemoryMapMaxSizeReadWrite(t *testing.T) {
	memoryMapSize, _ := ReadMemoryMapMaxSizeMb("5000")
	if memoryMapSize != 5000 {
		t.Errorf("empty memoryMapSize:%v", memoryMapSize)
	}
}

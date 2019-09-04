package needle

import "testing"

func TestMemoryMapMaxSizeReadWrite(t *testing.T) {
	memoryMapSize, _ := ReadMemoryMapMaxSizeMB("5000")
	if memoryMapSize != 5000 {
		t.Errorf("empty memoryMapSize:%v", memoryMapSize)
	}
}

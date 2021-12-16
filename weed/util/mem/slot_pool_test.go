package mem

import (
	"testing"
)

func TestAllocateFree(t *testing.T) {
	buf := Allocate(12)
	Free(buf)
	if cap(buf) != min_size {
		t.Errorf("min size error allocated capacity=%d", cap(buf))
	}
	if len(buf) != 12 {
		t.Errorf("size error")
	}

	buf = Allocate(4883)
	Free(buf)
	if cap(buf) != 1024<<bitCount(4883) {
		t.Errorf("min size error allocated capacity=%d", cap(buf))
	}
	if len(buf) != 4883 {
		t.Errorf("size error")
	}

}

func TestBitCount(t *testing.T) {
	count := bitCount(12)
	if count != 0 {
		t.Errorf("bitCount error count=%d", count)
	}
	if count != bitCount(min_size) {
		t.Errorf("bitCount error count=%d", count)
	}

}

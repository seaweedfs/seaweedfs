package storage

import (
	"reflect"
	"testing"
	"unsafe"
)

// Nothing else in the package would notice if someone grouped the fields by
// meaning again, and the padding is multiplied by every volume a master holds.
func TestVolumeInfoHasNoInteriorPadding(t *testing.T) {
	typ := reflect.TypeOf(VolumeInfo{})

	var used, interior uintptr
	prevEnd := uintptr(0)
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if gap := f.Offset - prevEnd; gap > 0 {
			t.Errorf("%d bytes of padding before %s, at offset %d: order the fields by size so they pack",
				gap, f.Name, prevEnd)
			interior += gap
		}
		used += f.Type.Size()
		prevEnd = f.Offset + f.Type.Size()
	}

	size := unsafe.Sizeof(VolumeInfo{})
	if tail := size - prevEnd; tail > 7 {
		t.Errorf("%d bytes of padding at the end, more than alignment requires", tail)
	}
	if interior == 0 {
		t.Logf("%d bytes of fields in a %d byte struct", used, size)
	}
}

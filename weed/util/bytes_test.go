package util

import (
	"reflect"
	"testing"
)

// go test github.com/chrislusf/seaweedfs/weed/util -run '^(TestAll)$' -v

func TestAll(t *testing.T) {
	for i := 0; i <= 0xff; i += 7 {
		assertU16(t, uint16(i))
	}
	for i := 0; i <= 0xffff; i += 211 {
		assertU32(t, uint32(i))
	}
	for i := 0; i <= 0xffffffff; i += 1000000007 {
		assertU64(t, uint64(i))
	}
}

func assertU16(t *testing.T, i uint16) {
	var buf1, buf2 [2]byte
	Uint16toBytesOld(buf1[:], i)
	Uint16toBytes(buf2[:], i)
	if !reflect.DeepEqual(buf1, buf2) {
		t.Errorf("i: %d, buf1: %v, buf2: %v\n", i, buf1, buf2)
	}
	v1 := BytesToUint16Old(buf1[:])
	v2 := BytesToUint16(buf2[:])
	if v1 != v2 {
		t.Errorf("buf1: %v, v1: %d, buf2: %v, v2: %d\n", buf1, v1, buf2, v2)
	}
}

func assertU32(t *testing.T, i uint32) {
	var buf1, buf2 [4]byte
	Uint32toBytesOld(buf1[:], i)
	Uint32toBytes(buf2[:], i)
	if !reflect.DeepEqual(buf1, buf2) {
		t.Errorf("i: %d, buf1: %v, buf2: %v\n", i, buf1, buf2)
	}
	v1 := BytesToUint32Old(buf1[:])
	v2 := BytesToUint32(buf2[:])
	if v1 != v2 {
		t.Errorf("buf1: %v, v1: %d, buf2: %v, v2: %d\n", buf1, v1, buf2, v2)
	}
}

func assertU64(t *testing.T, i uint64) {
	var buf1, buf2 [8]byte
	Uint64toBytesOld(buf1[:], i)
	Uint64toBytes(buf2[:], i)
	if !reflect.DeepEqual(buf1, buf2) {
		t.Errorf("i: %d, buf1: %v, buf2: %v\n", i, buf1, buf2)
	}
	v1 := BytesToUint64Old(buf1[:])
	v2 := BytesToUint64(buf2[:])
	if v1 != v2 {
		t.Errorf("buf1: %v, v1: %d, buf2: %v, v2: %d\n", buf1, v1, buf2, v2)
	}
}

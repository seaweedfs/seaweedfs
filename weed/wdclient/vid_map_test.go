package wdclient

import (
	"fmt"
	"math"
	"testing"
)

func TestLocationIndex(t *testing.T) {
	vm := vidMap{}
	// test must be failed
	mustFailed := func(length int64) {
		_, err := vm.getLocationIndex(length)
		if err == nil {
			t.Errorf("length %d must be failed", length)
		}
		if err.Error() != fmt.Sprintf("invalid length: %d", length) {
			t.Errorf("length %d must be failed. error: %v", length, err)
		}
	}

	mustFailed(-1)
	mustFailed(0)

	mustOk := func(length, cursor, expect int64) {
		if length <= 0 {
			t.Fatal("please don't do this")
		}
		vm.cursor = cursor
		got, err := vm.getLocationIndex(length)
		if err != nil {
			t.Errorf("length: %d, why? %v\n", length, err)
			return
		}
		if got != expect {
			t.Errorf("cursor: %d, length: %d, expect: %d, got: %d\n", cursor, length, expect, got)
			return
		}
	}

	for i := int64(-1); i < 100; i++ {
		mustOk(7, i, (i+1)%7)
	}

	// when cursor reaches MaxInt64
	mustOk(7, math.MaxInt64, 0)

	// test with constructor
	vm = newVidMap()
	length := int64(7)
	for i := int64(0); i < 100; i++ {
		got, err := vm.getLocationIndex(length)
		if err != nil {
			t.Errorf("length: %d, why? %v\n", length, err)
			return
		}
		if got != i%length {
			t.Errorf("length: %d, i: %d, got: %d\n", length, i, got)
		}
	}
}

func BenchmarkLocationIndex(b *testing.B) {
	b.SetParallelism(8)
	vm := vidMap{
		cursor: math.MaxInt64 - 10000,
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := vm.getLocationIndex(3)
			if err != nil {
				b.Error(err)
			}
		}
	})
}

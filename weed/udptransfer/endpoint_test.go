package udptransfer

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func Test_insert_delete_rid(t *testing.T) {
	var a []uint32
	var b = make([]uint32, 0, 1e3)
	var uniq = make(map[uint32]int)
	// insert into a with random
	for i := 0; i < cap(b); i++ {
		n := uint32(rand.Int31())
		if _, y := uniq[n]; !y {
			b = append(b, n)
			uniq[n] = 1
		}
		dups := 1
		if i&0xf == 0xf {
			dups = 3
		}
		for j := 0; j < dups; j++ {
			if aa := insertRid(a, n); aa != nil {
				a = aa
			}
		}
	}
	sort.Sort(u32Slice(b))
	bStr := fmt.Sprintf("%d", b)
	aStr := fmt.Sprintf("%d", a)
	assert(aStr == bStr, t, "a!=b")

	for i := 0; i < len(b); i++ {
		if aa := deleteRid(a, b[i]); aa != nil {
			a = aa
		}
	}
	assert(len(a) == 0, t, "a!=0")
}

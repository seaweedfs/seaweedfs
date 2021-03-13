package udptransfer

import (
	"fmt"
	"math/rand"
	"testing"
)

var lmap *linkedMap

func init() {
	lmap = newLinkedMap(_QModeIn)
}

func node(seq int) *qNode {
	return &qNode{packet: &packet{seq: uint32(seq)}}
}

func Test_get(t *testing.T) {
	var i, n *qNode
	assert(!lmap.contains(0), t, "0 nil")
	n = node(0)
	lmap.head = n
	lmap.tail = n
	lmap.qmap[0] = n
	i = lmap.get(0)
	assert(i == n, t, "0=n")
}

func Test_insert(t *testing.T) {
	lmap.reset()
	n := node(1)
	// appendTail
	lmap.appendTail(n)
	assert(lmap.head == n, t, "head n")
	assert(lmap.tail == n, t, "head n")
	n = node(2)
	lmap.appendTail(n)
	assert(lmap.head != n, t, "head n")
	assert(lmap.tail == n, t, "head n")
	assert(lmap.size() == 2, t, "size")
}

func Test_insertAfter(t *testing.T) {
	n1 := lmap.get(1)
	n2 := n1.next
	n3 := node(3)
	lmap.insertAfter(n1, n3)
	assert(n1.next == n3, t, "n3")
	assert(n1 == n3.prev, t, "left n3")
	assert(n2 == n3.next, t, "n3 right")
}

func Test_insertBefore(t *testing.T) {
	n3 := lmap.get(3)
	n2 := n3.next
	n4 := node(4)
	lmap.insertAfter(n3, n4)
	assert(n3.next == n4, t, "n4")
	assert(n3 == n4.prev, t, "left n4")
	assert(n2 == n4.next, t, "n4 right")
}

func Test_deleteBefore(t *testing.T) {
	lmap.reset()
	for i := 1; i < 10; i++ {
		n := node(i)
		lmap.appendTail(n)
	}

	var assertRangeEquals = func(n *qNode, start, wantCount int) {
		var last *qNode
		var count int
		for ; n != nil; n = n.next {
			assert(int(n.seq) == start, t, "nseq=%d start=%d", n.seq, start)
			last = n
			start++
			count++
		}
		assert(last.next == nil, t, "tail nil")
		assert(count == wantCount, t, "count")
	}
	assertRangeEquals(lmap.head, 1, 9)
	var n *qNode
	n = lmap.get(3)
	n, _ = lmap.deleteBefore(n)
	assertRangeEquals(n, 1, 3)
	assert(lmap.head.seq == 4, t, "head")

	n = lmap.get(8)
	n, _ = lmap.deleteBefore(n)
	assertRangeEquals(n, 4, 5)
	assert(lmap.head.seq == 9, t, "head")

	n = lmap.get(9)
	n, _ = lmap.deleteBefore(n)
	assertRangeEquals(n, 9, 1)

	assert(lmap.size() == 0, t, "size 0")
	assert(lmap.head == nil, t, "head nil")
	assert(lmap.tail == nil, t, "tail nil")
}

func testBitmap(t *testing.T, bmap []uint64, prev uint32) {
	var j uint
	var k int
	bits := bmap[k]
	t.Logf("test-%d %016x", k, bits)
	var checkNextPage = func() {
		if j >= 64 {
			j = 0
			k++
			bits = bmap[k]
			t.Logf("test-%d %016x", k, bits)
		}
	}
	for i := lmap.head; i != nil && k < len(bmap); i = i.next {
		checkNextPage()
		dis := i.seq - prev
		prev = i.seq
		if dis == 1 {
			bit := (bits >> j) & 1
			assert(bit == 1, t, "1 bit=%d j=%d", bit, j)
			j++
		} else {
			for ; dis > 0; dis-- {
				checkNextPage()
				bit := (bits >> j) & 1
				want := uint64(0)
				if dis == 1 {
					want = 1
				}
				assert(bit == want, t, "?=%d bit=%d j=%d", want, bit, j)
				j++
			}
		}
	}
	// remains bits should be 0
	for i := j & 63; i > 0; i-- {
		bit := (bits >> j) & 1
		assert(bit == 0, t, "00 bit=%d j=%d", bit, j)
		j++
	}
}

func Test_bitmap(t *testing.T) {
	var prev uint32
	var head uint32 = prev + 1

	lmap.reset()
	// test 66-%3 and record holes
	var holes = make([]uint32, 0, 50)
	for i := head; i < 366; i++ {
		if i%3 == 0 {
			holes = append(holes, i)
			continue
		}
		n := node(int(i))
		lmap.appendTail(n)
	}
	bmap, tbl := lmap.makeHolesBitmap(prev)
	testBitmap(t, bmap, prev)

	lmap.reset()
	// full 66, do deleteByBitmap then compare
	for i := head; i < 366; i++ {
		n := node(int(i))
		lmap.appendTail(n)
	}

	lmap.deleteByBitmap(bmap, head, tbl)
	var holesResult = make([]uint32, 0, 50)
	for i := lmap.head; i != nil; i = i.next {
		if i.scnt != _SENT_OK {
			holesResult = append(holesResult, i.seq)
		}
	}
	a := fmt.Sprintf("%x", holes)
	b := fmt.Sprintf("%x", holesResult)
	assert(a == b, t, "deleteByBitmap \na=%s \nb=%s", a, b)

	lmap.reset()
	// test stride across page 1
	for i := head; i < 69; i++ {
		if i >= 63 && i <= 65 {
			continue
		}
		n := node(int(i))
		lmap.appendTail(n)
	}
	bmap, _ = lmap.makeHolesBitmap(prev)
	testBitmap(t, bmap, prev)

	lmap.reset()
	prev = 65
	head = prev + 1
	// test stride across page 0
	for i := head; i < 68; i++ {
		n := node(int(i))
		lmap.appendTail(n)
	}
	bmap, _ = lmap.makeHolesBitmap(prev)
	testBitmap(t, bmap, prev)
}

var ackbitmap []uint64

func init_benchmark_map() {
	if lmap.size() != 640 {
		lmap.reset()
		for i := 1; i <= 640; i++ {
			lmap.appendTail(node(i))
		}
		ackbitmap = make([]uint64, 10)
		for i := 0; i < len(ackbitmap); i++ {
			n := rand.Int63()
			ackbitmap[i] = uint64(n) << 1
		}
	}
}

func Benchmark_make_bitmap(b *testing.B) {
	init_benchmark_map()

	for i := 0; i < b.N; i++ {
		lmap.makeHolesBitmap(0)
	}
}

func Benchmark_apply_bitmap(b *testing.B) {
	init_benchmark_map()

	for i := 0; i < b.N; i++ {
		lmap.deleteByBitmap(ackbitmap, 1, 64)
	}
}

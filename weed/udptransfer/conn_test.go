package udptransfer

import (
	"math/rand"
	"sort"
	"testing"
)

var conn *Conn

func init() {
	conn = &Conn{
		outQ: newLinkedMap(_QModeOut),
		inQ:  newLinkedMap(_QModeIn),
	}
}

func assert(cond bool, t testing.TB, format string, args ...interface{}) {
	if !cond {
		t.Errorf(format, args...)
		panic("last error")
	}
}

func Test_ordered_insert(t *testing.T) {
	data := []byte{1}
	var pk *packet
	for i := int32(1); i < 33; i++ {
		pk = &packet{
			seq:     uint32(i),
			payload: data,
		}
		conn.insertData(pk)
		assert(conn.inQ.size() == i, t, "len inQ=%d", conn.inQ.size())
		assert(conn.inQ.maxCtnSeq == pk.seq, t, "lastCtnIn")
		assert(!conn.inQDirty, t, "dirty")
	}
}

func Test_unordered_insert(t *testing.T) {
	conn.inQ.reset()

	data := []byte{1}
	var pk *packet
	var seqs = make([]int, 0xfff)
	// unordered insert, and assert size
	for i := 1; i < len(seqs); i++ {
		var seq uint32
		for conn.inQ.contains(seq) || seq == 0 {
			seq = uint32(rand.Int31n(0xFFffff))
		}
		seqs[i] = int(seq)
		pk = &packet{
			seq:     seq,
			payload: data,
		}
		conn.insertData(pk)
		assert(conn.inQ.size() == int32(i), t, "i=%d inQ.len=%d", i, conn.inQ.size())
	}
	// assert lastCtnSeq
	sort.Ints(seqs)
	var zero = 0
	var last *int
	for i := 0; i < len(seqs); i++ {
		if i == 0 && seqs[0] != 0 {
			last = &zero
			break
		}
		if last != nil && seqs[i]-*last > 1 {
			if i == 1 {
				last = &zero
			}
			break
		}
		last = &seqs[i]
	}
	if *last != int(conn.inQ.maxCtnSeq) {
		for i, j := range seqs {
			if i < 10 {
				t.Logf("seq %d", j)
			}
		}
	}
	assert(*last == int(conn.inQ.maxCtnSeq), t, "lastCtnSeq=%d but expected=%d", conn.inQ.maxCtnSeq, *last)
	t.Logf("lastCtnSeq=%d dirty=%v", conn.inQ.maxCtnSeq, conn.inQDirty)
}

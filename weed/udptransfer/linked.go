package udptransfer

type qNode struct {
	*packet
	prev   *qNode
	next   *qNode
	sent   int64 // last sent time
	sent_1 int64 // prev sent time
	miss   int   // sack miss count
}

type linkedMap struct {
	head      *qNode
	tail      *qNode
	qmap      map[uint32]*qNode
	lastIns   *qNode
	maxCtnSeq uint32
	mode      int
}

const (
	_QModeIn  = 1
	_QModeOut = 2
)

func newLinkedMap(qmode int) *linkedMap {
	return &linkedMap{
		qmap: make(map[uint32]*qNode),
		mode: qmode,
	}
}

func (l *linkedMap) get(seq uint32) (i *qNode) {
	i = l.qmap[seq]
	return
}

func (l *linkedMap) contains(seq uint32) bool {
	_, y := l.qmap[seq]
	return y
}

func (l *linkedMap) size() int32 {
	return int32(len(l.qmap))
}

func (l *linkedMap) reset() {
	l.head = nil
	l.tail = nil
	l.lastIns = nil
	l.maxCtnSeq = 0
	l.qmap = make(map[uint32]*qNode)
}

func (l *linkedMap) isEqualsHead(seq uint32) bool {
	return l.head != nil && seq == l.head.seq
}

func (l *linkedMap) distanceOfHead(seq uint32) int32 {
	if l.head != nil {
		return int32(seq - l.head.seq)
	} else {
		return -1
	}
}

func (l *linkedMap) appendTail(one *qNode) {
	if l.tail != nil {
		l.tail.next = one
		one.prev = l.tail
		l.tail = one
	} else {
		l.head = one
		l.tail = one
	}
	l.qmap[one.seq] = one
}

// xxx - n - yyy
// xxx - yyy
func (l *linkedMap) deleteAt(n *qNode) {
	x, y := n.prev, n.next
	if x != nil {
		x.next = y
	} else {
		l.head = y
	}
	if y != nil {
		y.prev = x
	} else {
		l.tail = x
	}
	n.prev, n.next = nil, nil
	delete(l.qmap, n.seq)
}

// delete with n <- ...n |
func (l *linkedMap) deleteBefore(n *qNode) (left *qNode, deleted int32) {
	for i := n; i != nil; i = i.prev {
		delete(l.qmap, i.seq)
		if i.scnt != _SENT_OK {
			deleted++
			// only outQ could delete at here
			if l.mode == _QModeOut {
				bpool.Put(i.buffer)
				i.buffer = nil
			}
		}
	}
	left = l.head
	l.head = n.next
	n.next = nil
	if l.head != nil {
		l.head.prev = nil
	} else { // n.next is the tail and is nil
		l.tail = nil
	}
	return
}

// xxx - ref
// xxx - one - ref
func (l *linkedMap) insertBefore(ref, one *qNode) {
	x := ref.prev
	if x != nil {
		x.next = one
		one.prev = x
	} else {
		l.head = one
	}
	ref.prev = one
	one.next = ref
	l.qmap[one.seq] = one
}

// ref - zzz
// ref - one - zzz
func (l *linkedMap) insertAfter(ref, one *qNode) {
	z := ref.next
	if z == nil { // append
		ref.next = one
		l.tail = one
	} else { // insert mid
		z.prev = one
		ref.next = one
	}
	one.prev = ref
	one.next = z
	l.qmap[one.seq] = one
}

// baseHead: the left outside boundary
// if inserted, return the distance between newNode with baseHead
func (l *linkedMap) searchInsert(one *qNode, baseHead uint32) (dis int64) {
	for i := l.tail; i != nil; i = i.prev {
		dis = int64(one.seq) - int64(i.seq)
		if dis > 0 {
			l.insertAfter(i, one)
			return
		} else if dis == 0 {
			// duplicated
			return
		}
	}
	if one.seq <= baseHead {
		return 0
	}
	if l.head != nil {
		l.insertBefore(l.head, one)
	} else {
		l.head = one
		l.tail = one
		l.qmap[one.seq] = one
	}
	dis = int64(one.seq) - int64(baseHead)
	return
}

func (l *linkedMap) updateContinuous(i *qNode) bool {
	var lastCtnSeq = l.maxCtnSeq
	for ; i != nil; i = i.next {
		if i.seq-lastCtnSeq == 1 {
			lastCtnSeq = i.seq
		} else {
			break
		}
	}
	if lastCtnSeq != l.maxCtnSeq {
		l.maxCtnSeq = lastCtnSeq
		return true
	}
	return false
}

func (l *linkedMap) isWholeContinuous() bool {
	return l.tail != nil && l.maxCtnSeq == l.tail.seq
}

/*
func (l *linkedMap) searchMaxContinued(baseHead uint32) (*qNode, bool) {
	var last *qNode
	for i := l.head; i != nil; i = i.next {
		if last != nil {
			if i.seq-last.seq > 1 {
				return last, true
			}
		} else {
			if i.seq != baseHead {
				return nil, false
			}
		}
		last = i
	}
	if last != nil {
		return last, true
	} else {
		return nil, false
	}
}
*/

func (q *qNode) forward(n int) *qNode {
	for ; n > 0 && q != nil; n-- {
		q = q.next
	}
	return q
}

// prev of bitmap start point
func (l *linkedMap) makeHolesBitmap(prev uint32) ([]uint64, uint32) {
	var start *qNode
	var bits uint64
	var j uint32
	for i := l.head; i != nil; i = i.next {
		if i.seq >= prev+1 {
			start = i
			break
		}
	}
	var bitmap []uint64
	// search start which is the recent successor of [prev]
Iterator:
	for i := start; i != nil; i = i.next {
		dis := i.seq - prev
		prev = i.seq
		j += dis // j is next bit index
		for j >= 65 {
			if len(bitmap) >= 20 { // bitmap too long
				break Iterator
			}
			bitmap = append(bitmap, bits)
			bits = 0
			j -= 64
		}
		bits |= 1 << (j - 1)
	}
	if j > 0 {
		// j -> (0, 64]
		bitmap = append(bitmap, bits)
	}
	return bitmap, j
}

// from= the bitmap start point
func (l *linkedMap) deleteByBitmap(bmap []uint64, from uint32, tailBitsLen uint32) (deleted, missed int32, lastContinued bool) {
	var start = l.qmap[from]
	if start != nil {
		// delete predecessors
		if pred := start.prev; pred != nil {
			_, n := l.deleteBefore(pred)
			deleted += n
		}
	} else {
		// [from] is out of bounds
		return
	}
	var j, bitsLen uint32
	var bits uint64
	bits, bmap = bmap[0], bmap[1:]
	if len(bmap) > 0 {
		bitsLen = 64
	} else {
		// bmap.len==1, tail is here
		bitsLen = tailBitsLen
	}
	// maxContinued will save the max continued node (from [start]) which could be deleted safely.
	// keep the queue smallest
	var maxContinued *qNode
	lastContinued = true

	for i := start; i != nil; j++ {
		if j >= bitsLen {
			if len(bmap) > 0 {
				j = 0
				bits, bmap = bmap[0], bmap[1:]
				if len(bmap) > 0 {
					bitsLen = 64
				} else {
					bitsLen = tailBitsLen
				}
			} else {
				// no more pages
				goto finished
			}
		}
		if bits&1 == 1 {
			if lastContinued {
				maxContinued = i
			}
			if i.scnt != _SENT_OK {
				// no mark means first deleting
				deleted++
			}
			// don't delete, just mark it
			i.scnt = _SENT_OK
		} else {
			// known it may be lost
			if i.miss == 0 {
				missed++
			}
			i.miss++
			lastContinued = false
		}
		bits >>= 1
		i = i.next
	}

finished:
	if maxContinued != nil {
		l.deleteBefore(maxContinued)
	}
	return
}

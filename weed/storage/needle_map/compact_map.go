package needle_map

/* CompactMap is an in-memory map of needle indeces, optimized for memory usage.
 *
 * It's implemented as a map of sorted indeces segments, which are in turn accessed through binary
 * search. This guarantees a best-case scenario (ordered inserts/updates) of O(1) and a worst case
 * scenario of O(log n) runtime, with memory usage unaffected by insert ordering.
 *
 * Note that even at O(log n), the clock time for both reads and writes is very low, so CompactMap
 * will seldom bottleneck index operations.
 */

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

const (
	MaxCompactKey    = math.MaxUint16
	SegmentChunkSize = 50000 // should be <= MaxCompactKey
)

type CompactKey uint16
type CompactOffset [types.OffsetSize]byte
type CompactNeedleValue struct {
	key    CompactKey
	offset CompactOffset
	size   types.Size
}

type Chunk uint64
type CompactMapSegment struct {
	list     []CompactNeedleValue
	chunk    Chunk
	firstKey CompactKey
	lastKey  CompactKey
}

type CompactMap struct {
	sync.RWMutex

	segments map[Chunk]*CompactMapSegment
}

func (ck CompactKey) Key(chunk Chunk) types.NeedleId {
	return (types.NeedleId(SegmentChunkSize) * types.NeedleId(chunk)) + types.NeedleId(ck)
}

func OffsetToCompact(offset types.Offset) CompactOffset {
	var co CompactOffset
	types.OffsetToBytes(co[:], offset)
	return co
}

func (co CompactOffset) Offset() types.Offset {
	return types.BytesToOffset(co[:])
}

func (cnv CompactNeedleValue) NeedleValue(chunk Chunk) NeedleValue {
	return NeedleValue{
		Key:    cnv.key.Key(chunk),
		Offset: cnv.offset.Offset(),
		Size:   cnv.size,
	}
}

func newCompactMapSegment(chunk Chunk) *CompactMapSegment {
	return &CompactMapSegment{
		list:     []CompactNeedleValue{},
		chunk:    chunk,
		firstKey: MaxCompactKey,
		lastKey:  0,
	}
}

func (cs *CompactMapSegment) len() int {
	return len(cs.list)
}

func (cs *CompactMapSegment) cap() int {
	return cap(cs.list)
}

func (cs *CompactMapSegment) compactKey(key types.NeedleId) CompactKey {
	return CompactKey(key - (types.NeedleId(SegmentChunkSize) * types.NeedleId(cs.chunk)))
}

// bsearchKey returns the CompactNeedleValue index for a given ID key.
// If the key is not found, it returns the index where it should be inserted instead.
func (cs *CompactMapSegment) bsearchKey(key types.NeedleId) (int, bool) {
	ck := cs.compactKey(key)

	switch {
	case len(cs.list) == 0:
		return 0, false
	case ck == cs.firstKey:
		return 0, true
	case ck <= cs.firstKey:
		return 0, false
	case ck == cs.lastKey:
		return len(cs.list) - 1, true
	case ck > cs.lastKey:
		return len(cs.list), false
	}

	i := sort.Search(len(cs.list), func(i int) bool {
		return cs.list[i].key >= ck
	})
	return i, cs.list[i].key == ck
}

// set inserts/updates a CompactNeedleValue.
// If the operation is an update, returns the overwritten value's previous offset and size.
func (cs *CompactMapSegment) set(key types.NeedleId, offset types.Offset, size types.Size) (oldOffset types.Offset, oldSize types.Size) {
	i, found := cs.bsearchKey(key)
	if found {
		// update
		o := cs.list[i].offset.Offset()
		oldOffset.OffsetLower = o.OffsetLower
		oldOffset.OffsetHigher = o.OffsetHigher
		oldSize = cs.list[i].size

		o.OffsetLower = offset.OffsetLower
		o.OffsetHigher = offset.OffsetHigher
		cs.list[i].offset = OffsetToCompact(o)
		cs.list[i].size = size
		return
	}

	// insert
	if len(cs.list) >= SegmentChunkSize {
		panic(fmt.Sprintf("attempted to write more than %d entries on CompactMapSegment %p!!!", SegmentChunkSize, cs))
	}
	if len(cs.list) == SegmentChunkSize-1 {
		// if we max out our segment storage, pin its capacity to minimize memory usage
		nl := make([]CompactNeedleValue, SegmentChunkSize, SegmentChunkSize)
		copy(nl, cs.list[:i])
		copy(nl[i+1:], cs.list[i:])
		cs.list = nl
	} else {
		cs.list = append(cs.list, CompactNeedleValue{})
		copy(cs.list[i+1:], cs.list[i:])
	}

	ck := cs.compactKey(key)
	cs.list[i] = CompactNeedleValue{
		key:    ck,
		offset: OffsetToCompact(offset),
		size:   size,
	}
	if ck < cs.firstKey {
		cs.firstKey = ck
	}
	if ck > cs.lastKey {
		cs.lastKey = ck
	}

	return
}

// get seeks a map entry by key. Returns an entry pointer, with a boolean specifiying if the entry was found.
func (cs *CompactMapSegment) get(key types.NeedleId) (*CompactNeedleValue, bool) {
	if i, found := cs.bsearchKey(key); found {
		return &cs.list[i], true
	}

	return nil, false
}

// delete deletes a map entry by key. Returns the entries' previous Size, if available.
func (cs *CompactMapSegment) delete(key types.NeedleId) types.Size {
	if i, found := cs.bsearchKey(key); found {
		if !cs.list[i].size.IsDeleted() {
			ret := cs.list[i].size
			if cs.list[i].size == 0 {
				// size=0 needles can't be marked deleted by negating, use tombstone
				cs.list[i].size = types.TombstoneFileSize
			} else {
				cs.list[i].size = -cs.list[i].size
			}
			return ret
		}
	}

	return types.Size(0)
}

func NewCompactMap() *CompactMap {
	return &CompactMap{
		segments: map[Chunk]*CompactMapSegment{},
	}
}

func (cm *CompactMap) Len() int {
	l := 0
	for _, s := range cm.segments {
		l += s.len()
	}
	return l
}

func (cm *CompactMap) Cap() int {
	c := 0
	for _, s := range cm.segments {
		c += s.cap()
	}
	return c
}

func (cm *CompactMap) String() string {
	if cm.Len() == 0 {
		return "empty"
	}
	return fmt.Sprintf(
		"%d/%d elements on %d segments, %.02f%% efficiency",
		cm.Len(), cm.Cap(), len(cm.segments),
		float64(100)*float64(cm.Len())/float64(cm.Cap()))
}

func (cm *CompactMap) segmentForKey(key types.NeedleId) *CompactMapSegment {
	chunk := Chunk(key / SegmentChunkSize)
	if cs, ok := cm.segments[chunk]; ok {
		return cs
	}

	cs := newCompactMapSegment(chunk)
	cm.segments[chunk] = cs
	return cs
}

// Set inserts/updates a NeedleValue.
// If the operation is an update, returns the overwritten value's previous offset and size.
func (cm *CompactMap) Set(key types.NeedleId, offset types.Offset, size types.Size) (oldOffset types.Offset, oldSize types.Size) {
	cm.RLock()
	defer cm.RUnlock()

	cs := cm.segmentForKey(key)
	return cs.set(key, offset, size)
}

// Get seeks a map entry by key. Returns an entry pointer, with a boolean specifiying if the entry was found.
func (cm *CompactMap) Get(key types.NeedleId) (*NeedleValue, bool) {
	cm.RLock()
	defer cm.RUnlock()

	cs := cm.segmentForKey(key)
	if cnv, found := cs.get(key); found {
		nv := cnv.NeedleValue(cs.chunk)
		return &nv, true
	}
	return nil, false
}

// Delete deletes a map entry by key. Returns the entries' previous Size, if available.
func (cm *CompactMap) Delete(key types.NeedleId) types.Size {
	cm.RLock()
	defer cm.RUnlock()

	cs := cm.segmentForKey(key)
	return cs.delete(key)
}

// AscendingVisit runs a function on all entries, in ascending key order. Returns any errors hit while visiting.
func (cm *CompactMap) AscendingVisit(visit func(NeedleValue) error) error {
	cm.RLock()
	defer cm.RUnlock()

	chunks := []Chunk{}
	for c := range cm.segments {
		chunks = append(chunks, c)
	}
	slices.Sort(chunks)

	for _, c := range chunks {
		cs := cm.segments[c]
		for _, cnv := range cs.list {
			nv := cnv.NeedleValue(cs.chunk)
			if err := visit(nv); err != nil {
				return err
			}
		}
	}
	return nil
}

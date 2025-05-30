package needle_map

import (
	"fmt"
	"sort"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

const (
	SegmentChunkSize = 25000
)

type CompactMapSegment struct {
	// TODO: maybe a compact-er structure for needle values?
	list     []NeedleValue
	firstKey types.NeedleId
	lastKey  types.NeedleId
}

type CompactMap struct {
	sync.RWMutex

	segments map[int]*CompactMapSegment
}

func newCompactMapSegment(chunk int) *CompactMapSegment {
	startKey := types.NeedleId(chunk * SegmentChunkSize)
	return &CompactMapSegment{
		list:     []NeedleValue{},
		firstKey: startKey + SegmentChunkSize - 1,
		lastKey:  startKey,
	}
}

func (cs *CompactMapSegment) len() int {
	return len(cs.list)
}

func (cs *CompactMapSegment) cap() int {
	return cap(cs.list)
}

// bsearchKey returns the NeedleValue index for a given ID key.
// If the key is not found, it returns the index where it should be inserted instead.
func (cs *CompactMapSegment) bsearchKey(key types.NeedleId) (int, bool) {
	switch {
	case len(cs.list) == 0:
		return 0, false
	case key == cs.firstKey:
		return 0, true
	case key <= cs.firstKey:
		return 0, false
	case key == cs.lastKey:
		return len(cs.list) - 1, true
	case key > cs.lastKey:
		return len(cs.list), false
	}

	i := sort.Search(len(cs.list), func(i int) bool {
		return cs.list[i].Key >= key
	})
	return i, cs.list[i].Key == key
}

// set inserts/updates a NeedleValue.
// If the operation is an update, returns the overwritten value's previous offset and size.
func (cs *CompactMapSegment) set(key types.NeedleId, offset types.Offset, size types.Size) (oldOffset types.Offset, oldSize types.Size) {
	i, found := cs.bsearchKey(key)
	if found {
		// update
		oldOffset.OffsetLower = cs.list[i].Offset.OffsetLower
		oldOffset.OffsetHigher = cs.list[i].Offset.OffsetHigher
		oldSize = cs.list[i].Size

		cs.list[i].Size = size
		cs.list[i].Offset.OffsetLower = offset.OffsetLower
		cs.list[i].Offset.OffsetHigher = offset.OffsetHigher
		return
	}

	// insert
	if len(cs.list) >= SegmentChunkSize {
		panic(fmt.Sprintf("attempted to write more than %d entries on CompactMapSegment %p!!!", SegmentChunkSize, cs))
	}
	if len(cs.list) == SegmentChunkSize-1 {
		// if we max out our segment storage, pin its capacity to minimize memory usage
		nl := make([]NeedleValue, SegmentChunkSize, SegmentChunkSize)
		copy(nl, cs.list[:i])
		copy(nl[i+1:], cs.list[i:])
		cs.list = nl
	} else {
		cs.list = append(cs.list, NeedleValue{})
		copy(cs.list[i+1:], cs.list[i:])
	}

	cs.list[i] = NeedleValue{
		Key:    key,
		Offset: offset,
		Size:   size,
	}
	if key < cs.firstKey {
		cs.firstKey = key
	}
	if key > cs.lastKey {
		cs.lastKey = key
	}

	return
}

// get seeks a map entry by key. Returns an entry pointer, with a boolean specifiying if the entry was found.
func (cs *CompactMapSegment) get(key types.NeedleId) (*NeedleValue, bool) {
	if i, found := cs.bsearchKey(key); found {
		return &cs.list[i], true
	}

	return nil, false
}

// delete deletes a map entry by key. Returns the entries' previous Size, if available.
func (cs *CompactMapSegment) delete(key types.NeedleId) types.Size {
	if i, found := cs.bsearchKey(key); found {
		if cs.list[i].Size > 0 && cs.list[i].Size.IsValid() {
			ret := cs.list[i].Size
			cs.list[i].Size = -cs.list[i].Size
			return ret
		}
	}

	return types.Size(0)
}

func NewCompactMap() *CompactMap {
	return &CompactMap{
		segments: map[int]*CompactMapSegment{},
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
	return fmt.Sprintf(
		"%d/%d elements on %d segments, %.02f%% efficiency",
		cm.Len(), cm.Cap(), len(cm.segments),
		float64(100)*float64(cm.Len())/float64(cm.Cap()))
}

func (cm *CompactMap) segmentForKey(key types.NeedleId) *CompactMapSegment {
	chunk := int(key / SegmentChunkSize)
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
	return cs.get(key)
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

	chunks := []int{}
	for c := range cm.segments {
		chunks = append(chunks, c)
	}
	sort.Ints(chunks)

	for _, c := range chunks {
		for _, nv := range cm.segments[c].list {
			if err := visit(nv); err != nil {
				return err
			}
		}
	}
	return nil
}

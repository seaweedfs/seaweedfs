package needle_map

import (
	"sort"
	"sync"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

const (
	MaxSectionBucketSize = 1024 * 8
	LookBackWindowSize   = 1024 // how many entries to look back when inserting into a section
)

type SectionalNeedleId uint32

const SectionalNeedleIdLimit = 1<<32 - 1

type SectionalNeedleValue struct {
	Key          SectionalNeedleId
	OffsetLower  OffsetLower `comment:"Volume offset"` //since aligned to 8 bytes, range is 4G*8=32G
	Size         Size        `comment:"Size of the data portion"`
	OffsetHigher OffsetHigher
}

type CompactSection struct {
	sync.RWMutex
	values   []SectionalNeedleValue
	overflow Overflow
	start    NeedleId
	end      NeedleId
}

type Overflow []SectionalNeedleValue

func NewCompactSection(start NeedleId) *CompactSection {
	return &CompactSection{
		values:   make([]SectionalNeedleValue, 0),
		overflow: Overflow(make([]SectionalNeedleValue, 0)),
		start:    start,
	}
}

// return old entry size
func (cs *CompactSection) Set(key NeedleId, offset Offset, size Size) (oldOffset Offset, oldSize Size) {
	cs.Lock()
	defer cs.Unlock()

	if key > cs.end {
		cs.end = key
	}
	skey := SectionalNeedleId(key - cs.start)
	if i := cs.binarySearchValues(skey); i >= 0 {
		// update
		oldOffset.OffsetHigher, oldOffset.OffsetLower, oldSize = cs.values[i].OffsetHigher, cs.values[i].OffsetLower, cs.values[i].Size
		cs.values[i].OffsetHigher, cs.values[i].OffsetLower, cs.values[i].Size = offset.OffsetHigher, offset.OffsetLower, size
		return
	}

	var lkey SectionalNeedleId
	if len(cs.values) > 0 {
		lkey = cs.values[len(cs.values)-1].Key
	}

	hasAdded := false
	switch {
	case len(cs.values) < MaxSectionBucketSize && lkey <= skey:
		// non-overflow insert
		cs.values = append(cs.values, SectionalNeedleValue{
			Key:          skey,
			OffsetLower:  offset.OffsetLower,
			Size:         size,
			OffsetHigher: offset.OffsetHigher,
		})
		hasAdded = true
	case len(cs.values) < MaxSectionBucketSize:
		// still has capacity and only partially out of order
		lookBackIndex := len(cs.values) - LookBackWindowSize
		if lookBackIndex < 0 {
			lookBackIndex = 0
		}
		if cs.values[lookBackIndex].Key <= skey {
			for ; lookBackIndex < len(cs.values); lookBackIndex++ {
				if cs.values[lookBackIndex].Key >= skey {
					break
				}
			}
			cs.values = append(cs.values, SectionalNeedleValue{})
			copy(cs.values[lookBackIndex+1:], cs.values[lookBackIndex:])
			cs.values[lookBackIndex].Key, cs.values[lookBackIndex].Size = skey, size
			cs.values[lookBackIndex].OffsetLower, cs.values[lookBackIndex].OffsetHigher = offset.OffsetLower, offset.OffsetHigher
			hasAdded = true
		}
	}

	// overflow insert
	if !hasAdded {
		if oldValue, found := cs.findOverflowEntry(skey); found {
			oldOffset.OffsetHigher, oldOffset.OffsetLower, oldSize = oldValue.OffsetHigher, oldValue.OffsetLower, oldValue.Size
		}
		cs.setOverflowEntry(skey, offset, size)
	} else {
		// if we maxed out our values bucket, pin its capacity to minimize memory usage
		if len(cs.values) == MaxSectionBucketSize {
			bucket := make([]SectionalNeedleValue, len(cs.values))
			copy(bucket, cs.values)
			cs.values = bucket
		}
	}

	return
}

func (cs *CompactSection) setOverflowEntry(skey SectionalNeedleId, offset Offset, size Size) {
	needleValue := SectionalNeedleValue{Key: skey, OffsetLower: offset.OffsetLower, Size: size, OffsetHigher: offset.OffsetHigher}
	insertCandidate := sort.Search(len(cs.overflow), func(i int) bool {
		return cs.overflow[i].Key >= needleValue.Key
	})

	if insertCandidate != len(cs.overflow) && cs.overflow[insertCandidate].Key == needleValue.Key {
		cs.overflow[insertCandidate] = needleValue
		return
	}

	cs.overflow = append(cs.overflow, SectionalNeedleValue{})
	copy(cs.overflow[insertCandidate+1:], cs.overflow[insertCandidate:])
	cs.overflow[insertCandidate] = needleValue
}

func (cs *CompactSection) findOverflowEntry(key SectionalNeedleId) (nv SectionalNeedleValue, found bool) {
	foundCandidate := sort.Search(len(cs.overflow), func(i int) bool {
		return cs.overflow[i].Key >= key
	})
	if foundCandidate != len(cs.overflow) && cs.overflow[foundCandidate].Key == key {
		return cs.overflow[foundCandidate], true
	}
	return nv, false
}

func (cs *CompactSection) deleteOverflowEntry(key SectionalNeedleId) {
	length := len(cs.overflow)
	deleteCandidate := sort.Search(length, func(i int) bool {
		return cs.overflow[i].Key >= key
	})
	if deleteCandidate != length && cs.overflow[deleteCandidate].Key == key {
		if cs.overflow[deleteCandidate].Size.IsValid() {
			cs.overflow[deleteCandidate].Size = -cs.overflow[deleteCandidate].Size
		}
	}
}

// return old entry size
func (cs *CompactSection) Delete(key NeedleId) Size {
	cs.Lock()
	defer cs.Unlock()
	ret := Size(0)
	if key > cs.end {
		return ret
	}
	skey := SectionalNeedleId(key - cs.start)
	if i := cs.binarySearchValues(skey); i >= 0 {
		if cs.values[i].Size > 0 && cs.values[i].Size.IsValid() {
			ret = cs.values[i].Size
			cs.values[i].Size = -cs.values[i].Size
		}
	}
	if v, found := cs.findOverflowEntry(skey); found {
		cs.deleteOverflowEntry(skey)
		ret = v.Size
	}
	return ret
}
func (cs *CompactSection) Get(key NeedleId) (*NeedleValue, bool) {
	cs.RLock()
	defer cs.RUnlock()
	if key > cs.end {
		return nil, false
	}
	skey := SectionalNeedleId(key - cs.start)
	if v, ok := cs.findOverflowEntry(skey); ok {
		nv := toNeedleValue(v, cs)
		return &nv, true
	}
	if i := cs.binarySearchValues(skey); i >= 0 {
		nv := toNeedleValue(cs.values[i], cs)
		return &nv, true
	}
	return nil, false
}
func (cs *CompactSection) binarySearchValues(key SectionalNeedleId) int {
	x := sort.Search(len(cs.values), func(i int) bool {
		return cs.values[i].Key >= key
	})
	if x >= len(cs.values) {
		return -1
	}
	if cs.values[x].Key > key {
		return -2
	}
	return x
}

// This map assumes mostly inserting increasing keys
// This map assumes mostly inserting increasing keys
type CompactMap struct {
	list []*CompactSection
}

func NewCompactMap() *CompactMap {
	return &CompactMap{}
}

func (cm *CompactMap) Set(key NeedleId, offset Offset, size Size) (oldOffset Offset, oldSize Size) {
	x := cm.binarySearchCompactSection(key)
	if x < 0 || (key-cm.list[x].start) > SectionalNeedleIdLimit {
		// println(x, "adding to existing", len(cm.list), "sections, starting", key)
		cs := NewCompactSection(key)
		cm.list = append(cm.list, cs)
		x = len(cm.list) - 1
		//keep compact section sorted by start
		for x >= 0 {
			if x > 0 && cm.list[x-1].start > key {
				cm.list[x] = cm.list[x-1]
				// println("shift", x, "start", cs.start, "to", x-1)
				x = x - 1
			} else {
				cm.list[x] = cs
				// println("cs", x, "start", cs.start)
				break
			}
		}
	}
	// println(key, "set to section[", x, "].start", cm.list[x].start)
	return cm.list[x].Set(key, offset, size)
}
func (cm *CompactMap) Delete(key NeedleId) Size {
	x := cm.binarySearchCompactSection(key)
	if x < 0 {
		return Size(0)
	}
	return cm.list[x].Delete(key)
}
func (cm *CompactMap) Get(key NeedleId) (*NeedleValue, bool) {
	x := cm.binarySearchCompactSection(key)
	if x < 0 {
		return nil, false
	}
	return cm.list[x].Get(key)
}
func (cm *CompactMap) binarySearchCompactSection(key NeedleId) int {
	l, h := 0, len(cm.list)-1
	if h < 0 {
		return -5
	}
	if cm.list[h].start <= key {
		if len(cm.list[h].values) < MaxSectionBucketSize || key <= cm.list[h].end {
			return h
		}
		return -4
	}
	for l <= h {
		m := (l + h) / 2
		if key < cm.list[m].start {
			h = m - 1
		} else { // cm.list[m].start <= key
			if cm.list[m+1].start <= key {
				l = m + 1
			} else {
				return m
			}
		}
	}
	return -3
}

// Visit visits all entries or stop if any error when visiting
func (cm *CompactMap) AscendingVisit(visit func(NeedleValue) error) error {
	for _, cs := range cm.list {
		cs.RLock()
		var i, j int
		for i, j = 0, 0; i < len(cs.overflow) && j < len(cs.values); {
			if cs.overflow[i].Key < cs.values[j].Key {
				if err := visit(toNeedleValue(cs.overflow[i], cs)); err != nil {
					cs.RUnlock()
					return err
				}
				i++
			} else if cs.overflow[i].Key == cs.values[j].Key {
				j++
			} else {
				if err := visit(toNeedleValue(cs.values[j], cs)); err != nil {
					cs.RUnlock()
					return err
				}
				j++
			}
		}
		for ; i < len(cs.overflow); i++ {
			if err := visit(toNeedleValue(cs.overflow[i], cs)); err != nil {
				cs.RUnlock()
				return err
			}
		}
		for ; j < len(cs.values); j++ {
			if err := visit(toNeedleValue(cs.values[j], cs)); err != nil {
				cs.RUnlock()
				return err
			}
		}
		cs.RUnlock()
	}
	return nil
}

func toNeedleValue(snv SectionalNeedleValue, cs *CompactSection) NeedleValue {
	offset := Offset{
		OffsetHigher: snv.OffsetHigher,
		OffsetLower:  snv.OffsetLower,
	}
	return NeedleValue{Key: NeedleId(snv.Key) + cs.start, Offset: offset, Size: snv.Size}
}

func (nv NeedleValue) toSectionalNeedleValue(cs *CompactSection) SectionalNeedleValue {
	return SectionalNeedleValue{
		Key:          SectionalNeedleId(nv.Key - cs.start),
		OffsetLower:  nv.Offset.OffsetLower,
		Size:         nv.Size,
		OffsetHigher: nv.Offset.OffsetHigher,
	}
}

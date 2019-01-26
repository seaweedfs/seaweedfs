package needle

import (
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
	"sort"
	"sync"
)

const (
	batch = 100000
)

type SectionalNeedleId uint32

const SectionalNeedleIdLimit = 1<<32 - 1

type SectionalNeedleValue struct {
	Key    SectionalNeedleId
	Offset Offset `comment:"Volume offset"` //since aligned to 8 bytes, range is 4G*8=32G
	Size   uint32 `comment:"Size of the data portion"`
}

type CompactSection struct {
	sync.RWMutex
	values   []SectionalNeedleValue
	overflow Overflow
	start    NeedleId
	end      NeedleId
	counter  int
}

type Overflow []SectionalNeedleValue

func NewCompactSection(start NeedleId) *CompactSection {
	return &CompactSection{
		values:   make([]SectionalNeedleValue, batch),
		overflow: Overflow(make([]SectionalNeedleValue, 0)),
		start:    start,
	}
}

//return old entry size
func (cs *CompactSection) Set(key NeedleId, offset Offset, size uint32) (oldOffset Offset, oldSize uint32) {
	cs.Lock()
	if key > cs.end {
		cs.end = key
	}
	skey := SectionalNeedleId(key - cs.start)
	if i := cs.binarySearchValues(skey); i >= 0 {
		oldOffset, oldSize = cs.values[i].Offset, cs.values[i].Size
		//println("key", key, "old size", ret)
		cs.values[i].Offset, cs.values[i].Size = offset, size
	} else {
		needOverflow := cs.counter >= batch
		needOverflow = needOverflow || cs.counter > 0 && cs.values[cs.counter-1].Key > skey
		if needOverflow {
			//println("start", cs.start, "counter", cs.counter, "key", key)
			if oldValue, found := cs.overflow.findOverflowEntry(skey); found {
				oldOffset, oldSize = oldValue.Offset, oldValue.Size
			}
			cs.overflow = cs.overflow.setOverflowEntry(SectionalNeedleValue{Key: skey, Offset: offset, Size: size})
		} else {
			p := &cs.values[cs.counter]
			p.Key, p.Offset, p.Size = skey, offset, size
			//println("added index", cs.counter, "key", key, cs.values[cs.counter].Key)
			cs.counter++
		}
	}
	cs.Unlock()
	return
}

//return old entry size
func (cs *CompactSection) Delete(key NeedleId) uint32 {
	skey := SectionalNeedleId(key - cs.start)
	cs.Lock()
	ret := uint32(0)
	if i := cs.binarySearchValues(skey); i >= 0 {
		if cs.values[i].Size > 0 && cs.values[i].Size != TombstoneFileSize {
			ret = cs.values[i].Size
			cs.values[i].Size = TombstoneFileSize
		}
	}
	if v, found := cs.overflow.findOverflowEntry(skey); found {
		cs.overflow = cs.overflow.deleteOverflowEntry(skey)
		ret = v.Size
	}
	cs.Unlock()
	return ret
}
func (cs *CompactSection) Get(key NeedleId) (*NeedleValue, bool) {
	cs.RLock()
	skey := SectionalNeedleId(key - cs.start)
	if v, ok := cs.overflow.findOverflowEntry(skey); ok {
		cs.RUnlock()
		nv := v.toNeedleValue(cs)
		return &nv, true
	}
	if i := cs.binarySearchValues(skey); i >= 0 {
		cs.RUnlock()
		nv := cs.values[i].toNeedleValue(cs)
		return &nv, true
	}
	cs.RUnlock()
	return nil, false
}
func (cs *CompactSection) binarySearchValues(key SectionalNeedleId) int {
	x := sort.Search(cs.counter, func(i int) bool {
		return cs.values[i].Key >= key
	})
	if x == cs.counter {
		return -1
	}
	if cs.values[x].Key > key {
		return -2
	}
	return x
}

//This map assumes mostly inserting increasing keys
//This map assumes mostly inserting increasing keys
type CompactMap struct {
	list []*CompactSection
}

func NewCompactMap() *CompactMap {
	return &CompactMap{}
}

func (cm *CompactMap) Set(key NeedleId, offset Offset, size uint32) (oldOffset Offset, oldSize uint32) {
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
func (cm *CompactMap) Delete(key NeedleId) uint32 {
	x := cm.binarySearchCompactSection(key)
	if x < 0 {
		return uint32(0)
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
		if cm.list[h].counter < batch || key <= cm.list[h].end {
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
func (cm *CompactMap) Visit(visit func(NeedleValue) error) error {
	for _, cs := range cm.list {
		cs.RLock()
		for _, v := range cs.overflow {
			if err := visit(v.toNeedleValue(cs)); err != nil {
				cs.RUnlock()
				return err
			}
		}
		for i, v := range cs.values {
			if i >= cs.counter {
				break
			}
			if _, found := cs.overflow.findOverflowEntry(v.Key); !found {
				if err := visit(v.toNeedleValue(cs)); err != nil {
					cs.RUnlock()
					return err
				}
			}
		}
		cs.RUnlock()
	}
	return nil
}

func (o Overflow) deleteOverflowEntry(key SectionalNeedleId) Overflow {
	length := len(o)
	deleteCandidate := sort.Search(length, func(i int) bool {
		return o[i].Key >= key
	})
	if deleteCandidate != length && o[deleteCandidate].Key == key {
		for i := deleteCandidate; i < length-1; i++ {
			o[i] = o[i+1]
		}
		o = o[0 : length-1]
	}
	return o
}

func (o Overflow) setOverflowEntry(needleValue SectionalNeedleValue) Overflow {
	insertCandidate := sort.Search(len(o), func(i int) bool {
		return o[i].Key >= needleValue.Key
	})
	if insertCandidate != len(o) && o[insertCandidate].Key == needleValue.Key {
		o[insertCandidate] = needleValue
	} else {
		o = append(o, needleValue)
		for i := len(o) - 1; i > insertCandidate; i-- {
			o[i] = o[i-1]
		}
		o[insertCandidate] = needleValue
	}
	return o
}

func (o Overflow) findOverflowEntry(key SectionalNeedleId) (nv SectionalNeedleValue, found bool) {
	foundCandidate := sort.Search(len(o), func(i int) bool {
		return o[i].Key >= key
	})
	if foundCandidate != len(o) && o[foundCandidate].Key == key {
		return o[foundCandidate], true
	}
	return nv, false
}

func (snv SectionalNeedleValue) toNeedleValue(cs *CompactSection) NeedleValue {
	return NeedleValue{NeedleId(snv.Key) + cs.start, snv.Offset, snv.Size}
}

func (nv NeedleValue) toSectionalNeedleValue(cs *CompactSection) SectionalNeedleValue {
	return SectionalNeedleValue{SectionalNeedleId(nv.Key - cs.start), nv.Offset, nv.Size}
}

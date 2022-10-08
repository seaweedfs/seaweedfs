package needle_map

import (
	"sort"
	"sync"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

const (
	batch = 10000
)

type SectionalNeedleId uint32

const SectionalNeedleIdLimit = 1<<32 - 1

type SectionalNeedleValue struct {
	Key         SectionalNeedleId
	OffsetLower OffsetLower `comment:"Volume offset"` //since aligned to 8 bytes, range is 4G*8=32G
	Size        Size        `comment:"Size of the data portion"`
}

type SectionalNeedleValueExtra struct {
	OffsetHigher OffsetHigher
}

type CompactSection struct {
	sync.RWMutex
	values        []SectionalNeedleValue
	valuesExtra   []SectionalNeedleValueExtra
	overflow      Overflow
	overflowExtra OverflowExtra
	start         NeedleId
	end           NeedleId
	counter       int
}

type Overflow []SectionalNeedleValue
type OverflowExtra []SectionalNeedleValueExtra

func NewCompactSection(start NeedleId) *CompactSection {
	return &CompactSection{
		values:        make([]SectionalNeedleValue, batch),
		valuesExtra:   make([]SectionalNeedleValueExtra, batch),
		overflow:      Overflow(make([]SectionalNeedleValue, 0)),
		overflowExtra: OverflowExtra(make([]SectionalNeedleValueExtra, 0)),
		start:         start,
	}
}

// return old entry size
func (cs *CompactSection) Set(key NeedleId, offset Offset, size Size) (oldOffset Offset, oldSize Size) {
	cs.Lock()
	if key > cs.end {
		cs.end = key
	}
	skey := SectionalNeedleId(key - cs.start)
	if i := cs.binarySearchValues(skey); i >= 0 {
		oldOffset.OffsetHigher, oldOffset.OffsetLower, oldSize = cs.valuesExtra[i].OffsetHigher, cs.values[i].OffsetLower, cs.values[i].Size
		//println("key", key, "old size", ret)
		cs.valuesExtra[i].OffsetHigher, cs.values[i].OffsetLower, cs.values[i].Size = offset.OffsetHigher, offset.OffsetLower, size
	} else {
		needOverflow := cs.counter >= batch
		needOverflow = needOverflow || cs.counter > 0 && cs.values[cs.counter-1].Key > skey
		if needOverflow {
			lookBackIndex := cs.counter - 128
			if lookBackIndex < 0 {
				lookBackIndex = 0
			}
			if cs.counter < batch && cs.values[lookBackIndex].Key < skey {
				// still has capacity and only partially out of order
				p := &cs.values[cs.counter]
				p.Key, cs.valuesExtra[cs.counter].OffsetHigher, p.OffsetLower, p.Size = skey, offset.OffsetHigher, offset.OffsetLower, size
				//println("added index", cs.counter, "key", key, cs.values[cs.counter].Key)
				for x := cs.counter - 1; x >= lookBackIndex; x-- {
					if cs.values[x].Key > cs.values[x+1].Key {
						cs.values[x], cs.values[x+1] = cs.values[x+1], cs.values[x]
						cs.valuesExtra[x], cs.valuesExtra[x+1] = cs.valuesExtra[x+1], cs.valuesExtra[x]
					} else {
						break
					}
				}
				cs.counter++
			} else {
				//println("start", cs.start, "counter", cs.counter, "key", key)
				if oldValueExtra, oldValue, found := cs.findOverflowEntry(skey); found {
					oldOffset.OffsetHigher, oldOffset.OffsetLower, oldSize = oldValueExtra.OffsetHigher, oldValue.OffsetLower, oldValue.Size
				}
				cs.setOverflowEntry(skey, offset, size)
			}
		} else {
			p := &cs.values[cs.counter]
			p.Key, cs.valuesExtra[cs.counter].OffsetHigher, p.OffsetLower, p.Size = skey, offset.OffsetHigher, offset.OffsetLower, size
			//println("added index", cs.counter, "key", key, cs.values[cs.counter].Key)
			cs.counter++
		}
	}
	cs.Unlock()
	return
}

func (cs *CompactSection) setOverflowEntry(skey SectionalNeedleId, offset Offset, size Size) {
	needleValue := SectionalNeedleValue{Key: skey, OffsetLower: offset.OffsetLower, Size: size}
	needleValueExtra := SectionalNeedleValueExtra{OffsetHigher: offset.OffsetHigher}
	insertCandidate := sort.Search(len(cs.overflow), func(i int) bool {
		return cs.overflow[i].Key >= needleValue.Key
	})
	if insertCandidate != len(cs.overflow) && cs.overflow[insertCandidate].Key == needleValue.Key {
		cs.overflow[insertCandidate] = needleValue
	} else {
		cs.overflow = append(cs.overflow, needleValue)
		cs.overflowExtra = append(cs.overflowExtra, needleValueExtra)
		for i := len(cs.overflow) - 1; i > insertCandidate; i-- {
			cs.overflow[i] = cs.overflow[i-1]
			cs.overflowExtra[i] = cs.overflowExtra[i-1]
		}
		cs.overflow[insertCandidate] = needleValue
		cs.overflowExtra[insertCandidate] = needleValueExtra
	}
}

func (cs *CompactSection) findOverflowEntry(key SectionalNeedleId) (nve SectionalNeedleValueExtra, nv SectionalNeedleValue, found bool) {
	foundCandidate := sort.Search(len(cs.overflow), func(i int) bool {
		return cs.overflow[i].Key >= key
	})
	if foundCandidate != len(cs.overflow) && cs.overflow[foundCandidate].Key == key {
		return cs.overflowExtra[foundCandidate], cs.overflow[foundCandidate], true
	}
	return nve, nv, false
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
	if _, v, found := cs.findOverflowEntry(skey); found {
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
	if ve, v, ok := cs.findOverflowEntry(skey); ok {
		nv := toNeedleValue(ve, v, cs)
		return &nv, true
	}
	if i := cs.binarySearchValues(skey); i >= 0 {
		nv := toNeedleValue(cs.valuesExtra[i], cs.values[i], cs)
		return &nv, true
	}
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
func (cm *CompactMap) AscendingVisit(visit func(NeedleValue) error) error {
	for _, cs := range cm.list {
		cs.RLock()
		var i, j int
		for i, j = 0, 0; i < len(cs.overflow) && j < len(cs.values) && j < cs.counter; {
			if cs.overflow[i].Key < cs.values[j].Key {
				if err := visit(toNeedleValue(cs.overflowExtra[i], cs.overflow[i], cs)); err != nil {
					cs.RUnlock()
					return err
				}
				i++
			} else if cs.overflow[i].Key == cs.values[j].Key {
				j++
			} else {
				if err := visit(toNeedleValue(cs.valuesExtra[j], cs.values[j], cs)); err != nil {
					cs.RUnlock()
					return err
				}
				j++
			}
		}
		for ; i < len(cs.overflow); i++ {
			if err := visit(toNeedleValue(cs.overflowExtra[i], cs.overflow[i], cs)); err != nil {
				cs.RUnlock()
				return err
			}
		}
		for ; j < len(cs.values) && j < cs.counter; j++ {
			if err := visit(toNeedleValue(cs.valuesExtra[j], cs.values[j], cs)); err != nil {
				cs.RUnlock()
				return err
			}
		}
		cs.RUnlock()
	}
	return nil
}

func toNeedleValue(snve SectionalNeedleValueExtra, snv SectionalNeedleValue, cs *CompactSection) NeedleValue {
	offset := Offset{
		OffsetHigher: snve.OffsetHigher,
		OffsetLower:  snv.OffsetLower,
	}
	return NeedleValue{Key: NeedleId(snv.Key) + cs.start, Offset: offset, Size: snv.Size}
}

func (nv NeedleValue) toSectionalNeedleValue(cs *CompactSection) (SectionalNeedleValue, SectionalNeedleValueExtra) {
	return SectionalNeedleValue{
			SectionalNeedleId(nv.Key - cs.start),
			nv.Offset.OffsetLower,
			nv.Size,
		}, SectionalNeedleValueExtra{
			nv.Offset.OffsetHigher,
		}
}

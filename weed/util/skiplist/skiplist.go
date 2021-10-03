package skiplist

import (
	"bytes"
	"fmt"
	"math/bits"
	"math/rand"
	"time"
)

const (
	// maxLevel denotes the maximum height of the skiplist. This height will keep the skiplist
	// efficient for up to 34m entries. If there is a need for much more, please adjust this constant accordingly.
	maxLevel = 25
)

type SkipList struct {
	startLevels [maxLevel]*SkipListElementReference
	endLevels   [maxLevel]*SkipListElementReference
	maxNewLevel int
	maxLevel    int
	// elementCount int
}

// NewSeedEps returns a new empty, initialized Skiplist.
// Given a seed, a deterministic height/list behaviour can be achieved.
// Eps is used to compare keys given by the ExtractKey() function on equality.
func NewSeed(seed int64) *SkipList {

	// Initialize random number generator.
	rand.Seed(seed)
	//fmt.Printf("SkipList seed: %v\n", seed)

	list := &SkipList{
		maxNewLevel: maxLevel,
		maxLevel:    0,
		// elementCount: 0,
	}

	return list
}

// New returns a new empty, initialized Skiplist.
func New() *SkipList {
	return NewSeed(time.Now().UTC().UnixNano())
}

// IsEmpty checks, if the skiplist is empty.
func (t *SkipList) IsEmpty() bool {
	return t.startLevels[0] == nil
}

func (t *SkipList) generateLevel(maxLevel int) int {
	level := maxLevel - 1
	// First we apply some mask which makes sure that we don't get a level
	// above our desired level. Then we find the first set bit.
	var x = rand.Uint64() & ((1 << uint(maxLevel-1)) - 1)
	zeroes := bits.TrailingZeros64(x)
	if zeroes <= maxLevel {
		level = zeroes
	}

	return level
}

func (t *SkipList) findEntryIndex(key []byte, level int) int {
	// Find good entry point so we don't accidentally skip half the list.
	for i := t.maxLevel; i >= 0; i-- {
		if t.startLevels[i] != nil && bytes.Compare(t.startLevels[i].Key, key) < 0 || i <= level {
			return i
		}
	}
	return 0
}

func (t *SkipList) findExtended(key []byte, findGreaterOrEqual bool) (foundElem *SkipListElement, ok bool) {

	foundElem = nil
	ok = false

	if t.IsEmpty() {
		return
	}

	index := t.findEntryIndex(key, 0)
	var currentNode *SkipListElement

	currentNode = t.startLevels[index].Load()

	// In case, that our first element is already greater-or-equal!
	if findGreaterOrEqual && compareElement(currentNode, key) > 0 {
		foundElem = currentNode
		ok = true
		return
	}

	for {
		if compareElement(currentNode, key) == 0 {
			foundElem = currentNode
			ok = true
			return
		}

		// Which direction are we continuing next time?
		if currentNode.Next[index] != nil && bytes.Compare(currentNode.Next[index].Key, key) <= 0 {
			// Go right
			currentNode = currentNode.Next[index].Load()
		} else {
			if index > 0 {

				// Early exit
				if currentNode.Next[0] != nil && bytes.Compare(currentNode.Next[0].Key, key) == 0 {
					currentNodeNext := currentNode.Next[0].Load()
					foundElem = currentNodeNext
					ok = true
					return
				}
				// Go down
				index--
			} else {
				// Element is not found and we reached the bottom.
				if findGreaterOrEqual {
					foundElem = currentNode.Next[index].Load()
					ok = foundElem != nil
				}

				return
			}
		}
	}
}

// Find tries to find an element in the skiplist based on the key from the given ListElement.
// elem can be used, if ok is true.
// Find runs in approx. O(log(n))
func (t *SkipList) Find(key []byte) (elem *SkipListElement, ok bool) {

	if t == nil || key == nil {
		return
	}

	elem, ok = t.findExtended(key, false)
	return
}

// FindGreaterOrEqual finds the first element, that is greater or equal to the given ListElement e.
// The comparison is done on the keys (So on ExtractKey()).
// FindGreaterOrEqual runs in approx. O(log(n))
func (t *SkipList) FindGreaterOrEqual(key []byte) (elem *SkipListElement, ok bool) {

	if t == nil || key == nil {
		return
	}

	elem, ok = t.findExtended(key, true)
	return
}

// Delete removes an element equal to e from the skiplist, if there is one.
// If there are multiple entries with the same value, Delete will remove one of them
// (Which one will change based on the actual skiplist layout)
// Delete runs in approx. O(log(n))
func (t *SkipList) Delete(key []byte) {

	if t == nil || t.IsEmpty() || key == nil {
		return
	}

	index := t.findEntryIndex(key, t.maxLevel)

	var currentNode *SkipListElement
	var nextNode *SkipListElement

	for {

		if currentNode == nil {
			nextNode = t.startLevels[index].Load()
		} else {
			nextNode = currentNode.Next[index].Load()
		}

		// Found and remove!
		if nextNode != nil && compareElement(nextNode, key) == 0 {

			if currentNode != nil {
				currentNode.Next[index] = nextNode.Next[index]
				currentNode.Save()
			}

			if index == 0 {
				if nextNode.Next[index] != nil {
					nextNextNode := nextNode.Next[index].Load()
					nextNextNode.Prev = currentNode.Reference()
					nextNextNode.Save()
				}
				// t.elementCount--
				nextNode.DeleteSelf()
			}

			// Link from start needs readjustments.
			startNextKey := t.startLevels[index].Key
			if compareElement(nextNode, startNextKey) == 0 {
				t.startLevels[index] = nextNode.Next[index]
				// This was our currently highest node!
				if t.startLevels[index] == nil {
					t.maxLevel = index - 1
				}
			}

			// Link from end needs readjustments.
			if nextNode.Next[index] == nil {
				t.endLevels[index] = currentNode.Reference()
			}
			nextNode.Next[index] = nil
		}

		if nextNode != nil && compareElement(nextNode, key) < 0 {
			// Go right
			currentNode = nextNode
		} else {
			// Go down
			index--
			if index < 0 {
				break
			}
		}
	}

}

// Insert inserts the given ListElement into the skiplist.
// Insert runs in approx. O(log(n))
func (t *SkipList) Insert(key, value []byte) {

	if t == nil || key == nil {
		return
	}

	level := t.generateLevel(t.maxNewLevel)

	// Only grow the height of the skiplist by one at a time!
	if level > t.maxLevel {
		level = t.maxLevel + 1
		t.maxLevel = level
	}

	elem := &SkipListElement{
		Id:    rand.Int63(),
		Next:  make([]*SkipListElementReference, t.maxNewLevel, t.maxNewLevel),
		Level: int32(level),
		Key:   key,
		Value: value,
	}

	// t.elementCount++

	newFirst := true
	newLast := true
	if !t.IsEmpty() {
		newFirst = compareElement(elem, t.startLevels[0].Key) < 0
		newLast = compareElement(elem, t.endLevels[0].Key) > 0
	}

	normallyInserted := false
	if !newFirst && !newLast {

		normallyInserted = true

		index := t.findEntryIndex(key, level)

		var currentNode *SkipListElement
		var nextNodeRef *SkipListElementReference

		for {

			if currentNode == nil {
				nextNodeRef = t.startLevels[index]
			} else {
				nextNodeRef = currentNode.Next[index]
			}

			var nextNode *SkipListElement

			// Connect node to next
			if index <= level && (nextNodeRef == nil || bytes.Compare(nextNodeRef.Key, key) > 0) {
				elem.Next[index] = nextNodeRef
				if currentNode != nil {
					currentNode.Next[index] = elem.Reference()
					currentNode.Save()
				}
				if index == 0 {
					elem.Prev = currentNode.Reference()
					if nextNodeRef != nil {
						nextNode = nextNodeRef.Load()
						nextNode.Prev = elem.Reference()
						nextNode.Save()
					}
				}
			}

			if nextNodeRef != nil && bytes.Compare(nextNodeRef.Key, key) <= 0 {
				// Go right
				if nextNode == nil {
					// reuse nextNode when index == 0
					nextNode = nextNodeRef.Load()
				}
				currentNode = nextNode
			} else {
				// Go down
				index--
				if index < 0 {
					break
				}
			}
		}
	}

	// Where we have a left-most position that needs to be referenced!
	for i := level; i >= 0; i-- {

		didSomething := false

		if newFirst || normallyInserted {

			if t.startLevels[i] == nil || bytes.Compare(t.startLevels[i].Key, key) > 0 {
				if i == 0 && t.startLevels[i] != nil {
					startLevelElement := t.startLevels[i].Load()
					startLevelElement.Prev = elem.Reference()
					startLevelElement.Save()
				}
				elem.Next[i] = t.startLevels[i]
				t.startLevels[i] = elem.Reference()
			}

			// link the endLevels to this element!
			if elem.Next[i] == nil {
				t.endLevels[i] = elem.Reference()
			}

			didSomething = true
		}

		if newLast {
			// Places the element after the very last element on this level!
			// This is very important, so we are not linking the very first element (newFirst AND newLast) to itself!
			if !newFirst {
				if t.endLevels[i] != nil {
					endLevelElement := t.endLevels[i].Load()
					endLevelElement.Next[i] = elem.Reference()
					endLevelElement.Save()
				}
				if i == 0 {
					elem.Prev = t.endLevels[i]
				}
				t.endLevels[i] = elem.Reference()
			}

			// Link the startLevels to this element!
			if t.startLevels[i] == nil || bytes.Compare(t.startLevels[i].Key, key) > 0 {
				t.startLevels[i] = elem.Reference()
			}

			didSomething = true
		}

		if !didSomething {
			break
		}
	}

	elem.Save()

}

// GetSmallestNode returns the very first/smallest node in the skiplist.
// GetSmallestNode runs in O(1)
func (t *SkipList) GetSmallestNode() *SkipListElement {
	return t.startLevels[0].Load()
}

// GetLargestNode returns the very last/largest node in the skiplist.
// GetLargestNode runs in O(1)
func (t *SkipList) GetLargestNode() *SkipListElement {
	return t.endLevels[0].Load()
}

// Next returns the next element based on the given node.
// Next will loop around to the first node, if you call it on the last!
func (t *SkipList) Next(e *SkipListElement) *SkipListElement {
	if e.Next[0] == nil {
		return t.startLevels[0].Load()
	}
	return e.Next[0].Load()
}

// Prev returns the previous element based on the given node.
// Prev will loop around to the last node, if you call it on the first!
func (t *SkipList) Prev(e *SkipListElement) *SkipListElement {
	if e.Prev == nil {
		return t.endLevels[0].Load()
	}
	return e.Prev.Load()
}

// String returns a string format of the skiplist. Useful to get a graphical overview and/or debugging.
func (t *SkipList) println() {

	print("start --> ")
	for i, l := range t.startLevels {
		if l == nil {
			break
		}
		if i > 0 {
			print(" -> ")
		}
		next := "---"
		if l != nil {
			next = string(l.Key)
		}
		print(fmt.Sprintf("[%v]", next))
	}
	println()

	nodeRef := t.startLevels[0]
	for nodeRef != nil {
		print(fmt.Sprintf("%v: ", string(nodeRef.Key)))
		node := nodeRef.Load()
		for i := 0; i <= int(node.Level); i++ {

			l := node.Next[i]

			next := "---"
			if l != nil {
				next = string(l.Key)
			}

			if i == 0 {
				prev := "---"

				if node.Prev != nil {
					prev = string(node.Prev.Key)
				}
				print(fmt.Sprintf("[%v|%v]", prev, next))
			} else {
				print(fmt.Sprintf("[%v]", next))
			}
			if i < int(node.Level) {
				print(" -> ")
			}

		}
		println()
		nodeRef = node.Next[0]
	}

	print("end --> ")
	for i, l := range t.endLevels {
		if l == nil {
			break
		}
		if i > 0 {
			print(" -> ")
		}
		next := "---"
		if l != nil {
			next = string(l.Key)
		}
		print(fmt.Sprintf("[%v]", next))
	}
	println()
}

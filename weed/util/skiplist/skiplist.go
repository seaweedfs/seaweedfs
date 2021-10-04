package skiplist

// adapted from https://github.com/MauriceGit/skiplist/blob/master/skiplist.go

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
	listStore   ListStore
	hasChanges  bool
	// elementCount int
}

// NewSeedEps returns a new empty, initialized Skiplist.
// Given a seed, a deterministic height/list behaviour can be achieved.
// Eps is used to compare keys given by the ExtractKey() function on equality.
func NewSeed(seed int64, listStore ListStore) *SkipList {

	// Initialize random number generator.
	rand.Seed(seed)
	//fmt.Printf("SkipList seed: %v\n", seed)

	list := &SkipList{
		maxNewLevel: maxLevel,
		maxLevel:    0,
		listStore:   listStore,
		// elementCount: 0,
	}

	return list
}

// New returns a new empty, initialized Skiplist.
func New(listStore ListStore) *SkipList {
	return NewSeed(time.Now().UTC().UnixNano(), listStore)
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

func (t *SkipList) findEntryIndex(key []byte, minLevel int) int {
	// Find good entry point so we don't accidentally skip half the list.
	for i := t.maxLevel; i >= 0; i-- {
		if t.startLevels[i] != nil && bytes.Compare(t.startLevels[i].Key, key) < 0 || i <= minLevel {
			return i
		}
	}
	return 0
}

func (t *SkipList) findExtended(key []byte, findGreaterOrEqual bool) (prevElementIfVisited *SkipListElement, foundElem *SkipListElement, ok bool, err error) {

	foundElem = nil
	ok = false

	if t.IsEmpty() {
		return
	}

	index := t.findEntryIndex(key, 0)
	var currentNode *SkipListElement

	currentNode, err = t.loadElement(t.startLevels[index])
	if err != nil {
		return
	}
	if currentNode == nil {
		return
	}

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
			currentNode, err = t.loadElement(currentNode.Next[index])
			if err != nil {
				return
			}
			if currentNode == nil {
				return
			}
		} else {
			if index > 0 {

				// Early exit
				if currentNode.Next[0] != nil && bytes.Compare(currentNode.Next[0].Key, key) == 0 {
					prevElementIfVisited = currentNode
					var currentNodeNext *SkipListElement
					currentNodeNext, err = t.loadElement(currentNode.Next[0])
					if err != nil {
						return
					}
					if currentNodeNext == nil {
						return
					}
					foundElem = currentNodeNext
					ok = true
					return
				}
				// Go down
				index--
			} else {
				// Element is not found and we reached the bottom.
				if findGreaterOrEqual {
					foundElem, err = t.loadElement(currentNode.Next[index])
					if err != nil {
						return
					}
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
func (t *SkipList) Find(key []byte) (prevIfVisited *SkipListElement, elem *SkipListElement, ok bool, err error) {

	if t == nil || key == nil {
		return
	}

	prevIfVisited, elem, ok, err = t.findExtended(key, false)
	return
}

// FindGreaterOrEqual finds the first element, that is greater or equal to the given ListElement e.
// The comparison is done on the keys (So on ExtractKey()).
// FindGreaterOrEqual runs in approx. O(log(n))
func (t *SkipList) FindGreaterOrEqual(key []byte) (prevIfVisited *SkipListElement, elem *SkipListElement, ok bool, err error) {

	if t == nil || key == nil {
		return
	}

	prevIfVisited, elem, ok, err = t.findExtended(key, true)
	return
}

// Delete removes an element equal to e from the skiplist, if there is one.
// If there are multiple entries with the same value, Delete will remove one of them
// (Which one will change based on the actual skiplist layout)
// Delete runs in approx. O(log(n))
func (t *SkipList) Delete(key []byte) (err error) {

	if t == nil || t.IsEmpty() || key == nil {
		return
	}

	index := t.findEntryIndex(key, t.maxLevel)

	var currentNode *SkipListElement
	var nextNode *SkipListElement

	for {

		if currentNode == nil {
			nextNode, err = t.loadElement(t.startLevels[index])
		} else {
			nextNode, err = t.loadElement(currentNode.Next[index])
		}
		if err != nil {
			return err
		}

		// Found and remove!
		if nextNode != nil && compareElement(nextNode, key) == 0 {

			if currentNode != nil {
				currentNode.Next[index] = nextNode.Next[index]
				if err = t.saveElement(currentNode); err != nil {
					return err
				}
			}

			if index == 0 {
				if nextNode.Next[index] != nil {
					nextNextNode, err := t.loadElement(nextNode.Next[index])
					if err != nil {
						return err
					}
					if nextNextNode != nil {
						nextNextNode.Prev = currentNode.Reference()
						if err = t.saveElement(nextNextNode); err != nil {
							return err
						}
					}
				}
				// t.elementCount--
				if err = t.deleteElement(nextNode); err != nil {
					return err
				}
			}

			// Link from start needs readjustments.
			startNextKey := t.startLevels[index].Key
			if compareElement(nextNode, startNextKey) == 0 {
				t.hasChanges = true
				t.startLevels[index] = nextNode.Next[index]
				// This was our currently highest node!
				if t.startLevels[index] == nil {
					t.maxLevel = index - 1
				}
			}

			// Link from end needs readjustments.
			if nextNode.Next[index] == nil {
				t.endLevels[index] = currentNode.Reference()
				t.hasChanges = true
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
	return
}

// Insert inserts the given ListElement into the skiplist.
// Insert runs in approx. O(log(n))
func (t *SkipList) Insert(key, value []byte) (err error) {

	if t == nil || key == nil {
		return
	}

	level := t.generateLevel(t.maxNewLevel)

	// Only grow the height of the skiplist by one at a time!
	if level > t.maxLevel {
		level = t.maxLevel + 1
		t.maxLevel = level
		t.hasChanges = true
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
					if err = t.saveElement(currentNode); err != nil {
						return
					}
				}
				if index == 0 {
					elem.Prev = currentNode.Reference()
					if nextNodeRef != nil {
						if nextNode, err = t.loadElement(nextNodeRef); err != nil {
							return
						}
						if nextNode != nil {
							nextNode.Prev = elem.Reference()
							if err = t.saveElement(nextNode); err != nil {
								return
							}
						}
					}
				}
			}

			if nextNodeRef != nil && bytes.Compare(nextNodeRef.Key, key) <= 0 {
				// Go right
				if nextNode == nil {
					// reuse nextNode when index == 0
					if nextNode, err = t.loadElement(nextNodeRef); err != nil {
						return
					}
				}
				currentNode = nextNode
				if currentNode == nil {
					return
				}
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
					startLevelElement, err := t.loadElement(t.startLevels[i])
					if err != nil {
						return err
					}
					if startLevelElement != nil {
						startLevelElement.Prev = elem.Reference()
						if err = t.saveElement(startLevelElement); err != nil {
							return err
						}
					}
				}
				elem.Next[i] = t.startLevels[i]
				t.startLevels[i] = elem.Reference()
				t.hasChanges = true
			}

			// link the endLevels to this element!
			if elem.Next[i] == nil {
				t.endLevels[i] = elem.Reference()
				t.hasChanges = true
			}

			didSomething = true
		}

		if newLast {
			// Places the element after the very last element on this level!
			// This is very important, so we are not linking the very first element (newFirst AND newLast) to itself!
			if !newFirst {
				if t.endLevels[i] != nil {
					endLevelElement, err := t.loadElement(t.endLevels[i])
					if err != nil {
						return err
					}
					if endLevelElement != nil {
						endLevelElement.Next[i] = elem.Reference()
						if err = t.saveElement(endLevelElement); err != nil {
							return err
						}
					}
				}
				if i == 0 {
					elem.Prev = t.endLevels[i]
				}
				t.endLevels[i] = elem.Reference()
				t.hasChanges = true
			}

			// Link the startLevels to this element!
			if t.startLevels[i] == nil || bytes.Compare(t.startLevels[i].Key, key) > 0 {
				t.startLevels[i] = elem.Reference()
				t.hasChanges = true
			}

			didSomething = true
		}

		if !didSomething {
			break
		}
	}

	if err = t.saveElement(elem); err != nil {
		return err
	}
	return nil

}

// GetSmallestNode returns the very first/smallest node in the skiplist.
// GetSmallestNode runs in O(1)
func (t *SkipList) GetSmallestNode() (*SkipListElement, error) {
	return t.loadElement(t.startLevels[0])
}

// GetLargestNode returns the very last/largest node in the skiplist.
// GetLargestNode runs in O(1)
func (t *SkipList) GetLargestNode() (*SkipListElement, error) {
	return t.loadElement(t.endLevels[0])
}

// Next returns the next element based on the given node.
// Next will loop around to the first node, if you call it on the last!
func (t *SkipList) Next(e *SkipListElement) (*SkipListElement, error) {
	if e.Next[0] == nil {
		return t.loadElement(t.startLevels[0])
	}
	return t.loadElement(e.Next[0])
}

// Prev returns the previous element based on the given node.
// Prev will loop around to the last node, if you call it on the first!
func (t *SkipList) Prev(e *SkipListElement) (*SkipListElement, error) {
	if e.Prev == nil {
		return t.loadElement(t.endLevels[0])
	}
	return t.loadElement(e.Prev)
}

// ChangeValue can be used to change the actual value of a node in the skiplist
// without the need of Deleting and reinserting the node again.
// Be advised, that ChangeValue only works, if the actual key from ExtractKey() will stay the same!
// ok is an indicator, wether the value is actually changed.
func (t *SkipList) ChangeValue(e *SkipListElement, newValue []byte) (err error) {
	// The key needs to stay correct, so this is very important!
	e.Value = newValue
	return t.saveElement(e)
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
		node, _ := t.loadElement(nodeRef)
		if node == nil {
			break
		}
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
		nodeRef = node.Next[0]
		println()
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

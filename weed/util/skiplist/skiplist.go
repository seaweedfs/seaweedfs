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
	StartLevels [maxLevel]*SkipListElementReference
	EndLevels   [maxLevel]*SkipListElementReference
	MaxNewLevel int
	MaxLevel    int
	ListStore   ListStore
	HasChanges  bool
	// elementCount int
}

// NewSeed returns a new empty, initialized Skiplist.
// Given a seed, a deterministic height/list behaviour can be achieved.
// Eps is used to compare keys given by the ExtractKey() function on equality.
func NewSeed(seed int64, listStore ListStore) *SkipList {

	// Initialize random number generator.
	rand.Seed(seed)
	//fmt.Printf("SkipList seed: %v\n", seed)

	list := &SkipList{
		MaxNewLevel: maxLevel,
		MaxLevel:    0,
		ListStore:   listStore,
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
	return t.StartLevels[0] == nil
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
	for i := t.MaxLevel; i >= 0; i-- {
		if t.StartLevels[i] != nil && bytes.Compare(t.StartLevels[i].Key, key) < 0 || i <= minLevel {
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

	currentNode, err = t.LoadElement(t.StartLevels[index])
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
			currentNode, err = t.LoadElement(currentNode.Next[index])
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
					currentNodeNext, err = t.LoadElement(currentNode.Next[0])
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
					foundElem, err = t.LoadElement(currentNode.Next[index])
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
func (t *SkipList) DeleteByKey(key []byte) (id int64, err error) {

	if t == nil || t.IsEmpty() || key == nil {
		return
	}

	index := t.findEntryIndex(key, t.MaxLevel)

	var currentNode *SkipListElement
	var nextNode *SkipListElement

	for {

		if currentNode == nil {
			nextNode, err = t.LoadElement(t.StartLevels[index])
		} else {
			nextNode, err = t.LoadElement(currentNode.Next[index])
		}
		if err != nil {
			return id, err
		}

		// Found and remove!
		if nextNode != nil && compareElement(nextNode, key) == 0 {

			if currentNode != nil {
				currentNode.Next[index] = nextNode.Next[index]
				if err = t.SaveElement(currentNode); err != nil {
					return id, err
				}
			}

			if index == 0 {
				if nextNode.Next[index] != nil {
					nextNextNode, err := t.LoadElement(nextNode.Next[index])
					if err != nil {
						return id, err
					}
					if nextNextNode != nil {
						nextNextNode.Prev = currentNode.Reference()
						if err = t.SaveElement(nextNextNode); err != nil {
							return id, err
						}
					}
				}
				// t.elementCount--
				id = nextNode.Id
				if err = t.DeleteElement(nextNode); err != nil {
					return id, err
				}
			}

			// Link from start needs readjustments.
			startNextKey := t.StartLevels[index].Key
			if compareElement(nextNode, startNextKey) == 0 {
				t.HasChanges = true
				t.StartLevels[index] = nextNode.Next[index]
				// This was our currently highest node!
				if t.StartLevels[index] == nil {
					t.MaxLevel = index - 1
				}
			}

			// Link from end needs readjustments.
			if nextNode.Next[index] == nil {
				t.EndLevels[index] = currentNode.Reference()
				t.HasChanges = true
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
func (t *SkipList) InsertByKey(key []byte, idIfKnown int64, value []byte) (id int64, err error) {

	if t == nil || key == nil {
		return
	}

	level := t.generateLevel(t.MaxNewLevel)

	// Only grow the height of the skiplist by one at a time!
	if level > t.MaxLevel {
		level = t.MaxLevel + 1
		t.MaxLevel = level
		t.HasChanges = true
	}

	id = idIfKnown
	if id == 0 {
		id = rand.Int63()
	}
	elem := &SkipListElement{
		Id:    id,
		Next:  make([]*SkipListElementReference, t.MaxNewLevel, t.MaxNewLevel),
		Level: int32(level),
		Key:   key,
		Value: value,
	}

	// t.elementCount++

	newFirst := true
	newLast := true
	if !t.IsEmpty() {
		newFirst = compareElement(elem, t.StartLevels[0].Key) < 0
		newLast = compareElement(elem, t.EndLevels[0].Key) > 0
	}

	normallyInserted := false
	if !newFirst && !newLast {

		normallyInserted = true

		index := t.findEntryIndex(key, level)

		var currentNode *SkipListElement
		var nextNodeRef *SkipListElementReference

		for {

			if currentNode == nil {
				nextNodeRef = t.StartLevels[index]
			} else {
				nextNodeRef = currentNode.Next[index]
			}

			var nextNode *SkipListElement

			// Connect node to next
			if index <= level && (nextNodeRef == nil || bytes.Compare(nextNodeRef.Key, key) > 0) {
				elem.Next[index] = nextNodeRef
				if currentNode != nil {
					currentNode.Next[index] = elem.Reference()
					if err = t.SaveElement(currentNode); err != nil {
						return
					}
				}
				if index == 0 {
					elem.Prev = currentNode.Reference()
					if nextNodeRef != nil {
						if nextNode, err = t.LoadElement(nextNodeRef); err != nil {
							return
						}
						if nextNode != nil {
							nextNode.Prev = elem.Reference()
							if err = t.SaveElement(nextNode); err != nil {
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
					if nextNode, err = t.LoadElement(nextNodeRef); err != nil {
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

			if t.StartLevels[i] == nil || bytes.Compare(t.StartLevels[i].Key, key) > 0 {
				if i == 0 && t.StartLevels[i] != nil {
					startLevelElement, err := t.LoadElement(t.StartLevels[i])
					if err != nil {
						return id, err
					}
					if startLevelElement != nil {
						startLevelElement.Prev = elem.Reference()
						if err = t.SaveElement(startLevelElement); err != nil {
							return id, err
						}
					}
				}
				elem.Next[i] = t.StartLevels[i]
				t.StartLevels[i] = elem.Reference()
				t.HasChanges = true
			}

			// link the EndLevels to this element!
			if elem.Next[i] == nil {
				t.EndLevels[i] = elem.Reference()
				t.HasChanges = true
			}

			didSomething = true
		}

		if newLast {
			// Places the element after the very last element on this level!
			// This is very important, so we are not linking the very first element (newFirst AND newLast) to itself!
			if !newFirst {
				if t.EndLevels[i] != nil {
					endLevelElement, err := t.LoadElement(t.EndLevels[i])
					if err != nil {
						return id, err
					}
					if endLevelElement != nil {
						endLevelElement.Next[i] = elem.Reference()
						if err = t.SaveElement(endLevelElement); err != nil {
							return id, err
						}
					}
				}
				if i == 0 {
					elem.Prev = t.EndLevels[i]
				}
				t.EndLevels[i] = elem.Reference()
				t.HasChanges = true
			}

			// Link the startLevels to this element!
			if t.StartLevels[i] == nil || bytes.Compare(t.StartLevels[i].Key, key) > 0 {
				t.StartLevels[i] = elem.Reference()
				t.HasChanges = true
			}

			didSomething = true
		}

		if !didSomething {
			break
		}
	}

	if err = t.SaveElement(elem); err != nil {
		return id, err
	}
	return id, nil

}

// GetSmallestNode returns the very first/smallest node in the skiplist.
// GetSmallestNode runs in O(1)
func (t *SkipList) GetSmallestNode() (*SkipListElement, error) {
	return t.LoadElement(t.StartLevels[0])
}

// GetLargestNode returns the very last/largest node in the skiplist.
// GetLargestNode runs in O(1)
func (t *SkipList) GetLargestNode() (*SkipListElement, error) {
	return t.LoadElement(t.EndLevels[0])
}
func (t *SkipList) GetLargestNodeReference() *SkipListElementReference {
	return t.EndLevels[0]
}

// Next returns the next element based on the given node.
// Next will loop around to the first node, if you call it on the last!
func (t *SkipList) Next(e *SkipListElement) (*SkipListElement, error) {
	if e.Next[0] == nil {
		return t.LoadElement(t.StartLevels[0])
	}
	return t.LoadElement(e.Next[0])
}

// Prev returns the previous element based on the given node.
// Prev will loop around to the last node, if you call it on the first!
func (t *SkipList) Prev(e *SkipListElement) (*SkipListElement, error) {
	if e.Prev == nil {
		return t.LoadElement(t.EndLevels[0])
	}
	return t.LoadElement(e.Prev)
}

// ChangeValue can be used to change the actual value of a node in the skiplist
// without the need of Deleting and reinserting the node again.
// Be advised, that ChangeValue only works, if the actual key from ExtractKey() will stay the same!
// ok is an indicator, wether the value is actually changed.
func (t *SkipList) ChangeValue(e *SkipListElement, newValue []byte) (err error) {
	// The key needs to stay correct, so this is very important!
	e.Value = newValue
	return t.SaveElement(e)
}

// String returns a string format of the skiplist. Useful to get a graphical overview and/or debugging.
func (t *SkipList) println() {

	print("start --> ")
	for i, l := range t.StartLevels {
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

	nodeRef := t.StartLevels[0]
	for nodeRef != nil {
		print(fmt.Sprintf("%v: ", string(nodeRef.Key)))
		node, _ := t.LoadElement(nodeRef)
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
	for i, l := range t.EndLevels {
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

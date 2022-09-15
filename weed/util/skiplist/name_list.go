package skiplist

import (
	"bytes"
)

type NameList struct {
	skipList  *SkipList
	batchSize int
}

func newNameList(store ListStore, batchSize int) *NameList {
	return &NameList{
		skipList:  New(store),
		batchSize: batchSize,
	}
}

/*
Be reluctant to create new nodes. Try to fit into either previous node or next node.
Prefer to add to previous node.

There are multiple cases after finding the name for greater or equal node

 1. found and node.Key == name
    The node contains a batch with leading key the same as the name
    nothing to do

 2. no such node found or node.Key > name

    if no such node found
    prevNode = list.LargestNode

    // case 2.1
    if previousNode contains name
    nothing to do

    // prefer to add to previous node
    if prevNode != nil {
    // case 2.2
    if prevNode has capacity
    prevNode.add name, and save
    return
    // case 2.3
    split prevNode by name
    }

    // case 2.4
    // merge into next node. Avoid too many nodes if adding data in reverse order.
    if nextNode is not nil and nextNode has capacity
    delete nextNode.Key
    nextNode.Key = name
    nextNode.batch.add name
    insert nodeNode.Key
    return

    // case 2.5
    if prevNode is nil
    insert new node with key = name, value = batch{name}
    return
*/
func (nl *NameList) WriteName(name string) error {

	lookupKey := []byte(name)
	prevNode, nextNode, found, err := nl.skipList.FindGreaterOrEqual(lookupKey)
	if err != nil {
		return err
	}
	// case 1: the name already exists as one leading key in the batch
	if found && bytes.Compare(nextNode.Key, lookupKey) == 0 {
		return nil
	}

	if !found {
		prevNode, err = nl.skipList.GetLargestNode()
		if err != nil {
			return err
		}
	}

	if nextNode != nil && prevNode == nil {
		prevNode, err = nl.skipList.LoadElement(nextNode.Prev)
		if err != nil {
			return err
		}
	}

	if prevNode != nil {
		prevNameBatch := LoadNameBatch(prevNode.Value)
		// case 2.1
		if prevNameBatch.ContainsName(name) {
			return nil
		}

		// case 2.2
		if len(prevNameBatch.names) < nl.batchSize {
			prevNameBatch.WriteName(name)
			return nl.skipList.ChangeValue(prevNode, prevNameBatch.ToBytes())
		}

		// case 2.3
		x, y := prevNameBatch.SplitBy(name)
		addToX := len(x.names) <= len(y.names)
		if len(x.names) != len(prevNameBatch.names) {
			if addToX {
				x.WriteName(name)
			}
			if x.key == prevNameBatch.key {
				if err := nl.skipList.ChangeValue(prevNode, x.ToBytes()); err != nil {
					return err
				}
			} else {
				if _, err := nl.skipList.InsertByKey([]byte(x.key), 0, x.ToBytes()); err != nil {
					return err
				}
			}
		}
		if len(y.names) != len(prevNameBatch.names) {
			if !addToX {
				y.WriteName(name)
			}
			if y.key == prevNameBatch.key {
				if err := nl.skipList.ChangeValue(prevNode, y.ToBytes()); err != nil {
					return err
				}
			} else {
				if _, err := nl.skipList.InsertByKey([]byte(y.key), 0, y.ToBytes()); err != nil {
					return err
				}
			}
		}
		return nil

	}

	// case 2.4
	if nextNode != nil {
		nextNameBatch := LoadNameBatch(nextNode.Value)
		if len(nextNameBatch.names) < nl.batchSize {
			if _, err := nl.skipList.DeleteByKey(nextNode.Key); err != nil {
				return err
			}
			nextNameBatch.WriteName(name)
			if _, err := nl.skipList.InsertByKey([]byte(nextNameBatch.key), 0, nextNameBatch.ToBytes()); err != nil {
				return err
			}
			return nil
		}
	}

	// case 2.5
	// now prevNode is nil
	newNameBatch := NewNameBatch()
	newNameBatch.WriteName(name)
	if _, err := nl.skipList.InsertByKey([]byte(newNameBatch.key), 0, newNameBatch.ToBytes()); err != nil {
		return err
	}

	return nil
}

/*
// case 1: exists in nextNode

	if nextNode != nil && nextNode.Key == name {
		remove from nextNode, update nextNode
		// TODO: merge with prevNode if possible?
		return
	}

if nextNode is nil

	prevNode = list.Largestnode

if prevNode == nil and nextNode.Prev != nil

	prevNode = load(nextNode.Prev)

// case 2: does not exist
// case 2.1

	if prevNode == nil {
		return
	}

// case 2.2

	if prevNameBatch does not contain name {
		return
	}

// case 3
delete from prevNameBatch
if prevNameBatch + nextNode < capacityList

	// case 3.1
	merge

else

	// case 3.2
	update prevNode
*/
func (nl *NameList) DeleteName(name string) error {
	lookupKey := []byte(name)
	prevNode, nextNode, found, err := nl.skipList.FindGreaterOrEqual(lookupKey)
	if err != nil {
		return err
	}

	// case 1
	var nextNameBatch *NameBatch
	if nextNode != nil {
		nextNameBatch = LoadNameBatch(nextNode.Value)
	}
	if found && bytes.Compare(nextNode.Key, lookupKey) == 0 {
		if _, err := nl.skipList.DeleteByKey(nextNode.Key); err != nil {
			return err
		}
		nextNameBatch.DeleteName(name)
		if len(nextNameBatch.names) > 0 {
			if _, err := nl.skipList.InsertByKey([]byte(nextNameBatch.key), 0, nextNameBatch.ToBytes()); err != nil {
				return err
			}
		}
		return nil
	}

	if !found {
		prevNode, err = nl.skipList.GetLargestNode()
		if err != nil {
			return err
		}
	}

	if nextNode != nil && prevNode == nil {
		prevNode, err = nl.skipList.LoadElement(nextNode.Prev)
		if err != nil {
			return err
		}
	}

	// case 2
	if prevNode == nil {
		// case 2.1
		return nil
	}
	prevNameBatch := LoadNameBatch(prevNode.Value)
	if !prevNameBatch.ContainsName(name) {
		// case 2.2
		return nil
	}

	// case 3
	prevNameBatch.DeleteName(name)
	if len(prevNameBatch.names) == 0 {
		if _, err := nl.skipList.DeleteByKey(prevNode.Key); err != nil {
			return err
		}
		return nil
	}
	if nextNameBatch != nil && len(nextNameBatch.names)+len(prevNameBatch.names) < nl.batchSize {
		// case 3.1 merge nextNode and prevNode
		if _, err := nl.skipList.DeleteByKey(nextNode.Key); err != nil {
			return err
		}
		for nextName := range nextNameBatch.names {
			prevNameBatch.WriteName(nextName)
		}
		return nl.skipList.ChangeValue(prevNode, prevNameBatch.ToBytes())
	} else {
		// case 3.2 update prevNode
		return nl.skipList.ChangeValue(prevNode, prevNameBatch.ToBytes())
	}

	return nil
}

func (nl *NameList) ListNames(startFrom string, visitNamesFn func(name string) bool) error {
	lookupKey := []byte(startFrom)
	prevNode, nextNode, found, err := nl.skipList.FindGreaterOrEqual(lookupKey)
	if err != nil {
		return err
	}
	if found && bytes.Compare(nextNode.Key, lookupKey) == 0 {
		prevNode = nil
	}
	if !found {
		prevNode, err = nl.skipList.GetLargestNode()
		if err != nil {
			return err
		}
	}

	if prevNode != nil {
		prevNameBatch := LoadNameBatch(prevNode.Value)
		if !prevNameBatch.ListNames(startFrom, visitNamesFn) {
			return nil
		}
	}

	for nextNode != nil {
		nextNameBatch := LoadNameBatch(nextNode.Value)
		if !nextNameBatch.ListNames(startFrom, visitNamesFn) {
			return nil
		}
		nextNode, err = nl.skipList.LoadElement(nextNode.Next[0])
		if err != nil {
			return err
		}
	}

	return nil
}

func (nl *NameList) RemoteAllListElement() error {

	t := nl.skipList

	nodeRef := t.StartLevels[0]
	for nodeRef != nil {
		node, err := t.LoadElement(nodeRef)
		if err != nil {
			return err
		}
		if node == nil {
			return nil
		}
		if err := t.DeleteElement(node); err != nil {
			return err
		}
		nodeRef = node.Next[0]
	}
	return nil

}

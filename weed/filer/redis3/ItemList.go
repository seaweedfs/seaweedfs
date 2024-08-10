package redis3

import (
	"bytes"
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/util/skiplist"
)

type ItemList struct {
	skipList  *skiplist.SkipList
	batchSize int
	client    redis.UniversalClient
	prefix    string
}

func newItemList(client redis.UniversalClient, prefix string, store skiplist.ListStore, batchSize int) *ItemList {
	return &ItemList{
		skipList:  skiplist.New(store),
		batchSize: batchSize,
		client:    client,
		prefix:    prefix,
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

func (nl *ItemList) canAddMember(node *skiplist.SkipListElementReference, name string) (alreadyContains bool, nodeSize int, err error) {
	ctx := context.Background()
	pipe := nl.client.TxPipeline()
	key := fmt.Sprintf("%s%dm", nl.prefix, node.ElementPointer)
	countOperation := pipe.ZLexCount(ctx, key, "-", "+")
	scoreOperationt := pipe.ZScore(ctx, key, name)
	if _, err = pipe.Exec(ctx); err != nil && err != redis.Nil {
		return false, 0, err
	}
	if err == redis.Nil {
		err = nil
	}
	alreadyContains = scoreOperationt.Err() == nil
	nodeSize = int(countOperation.Val())
	return
}

func (nl *ItemList) WriteName(name string) error {

	lookupKey := []byte(name)
	prevNode, nextNode, found, err := nl.skipList.FindGreaterOrEqual(lookupKey)
	if err != nil {
		return err
	}
	// case 1: the name already exists as one leading key in the batch
	if found && bytes.Compare(nextNode.Key, lookupKey) == 0 {
		return nil
	}

	var prevNodeReference *skiplist.SkipListElementReference
	if !found {
		prevNodeReference = nl.skipList.GetLargestNodeReference()
	}

	if nextNode != nil && prevNode == nil {
		prevNodeReference = nextNode.Prev
	}

	if prevNodeReference != nil {
		alreadyContains, nodeSize, err := nl.canAddMember(prevNodeReference, name)
		if err != nil {
			return err
		}
		if alreadyContains {
			// case 2.1
			return nil
		}

		// case 2.2
		if nodeSize < nl.batchSize {
			return nl.NodeAddMember(prevNodeReference, name)
		}

		// case 2.3
		x := nl.NodeInnerPosition(prevNodeReference, name)
		y := nodeSize - x
		addToX := x <= y
		// add to a new node
		if x == 0 || y == 0 {
			if err := nl.ItemAdd(lookupKey, 0, name); err != nil {
				return err
			}
			return nil
		}
		if addToX {
			// collect names before name, add them to X
			namesToX, err := nl.NodeRangeBeforeExclusive(prevNodeReference, name)
			if err != nil {
				return nil
			}
			// delete skiplist reference to old node
			if _, err := nl.skipList.DeleteByKey(prevNodeReference.Key); err != nil {
				return err
			}
			// add namesToY and name to a new X
			namesToX = append(namesToX, name)
			if err := nl.ItemAdd([]byte(namesToX[0]), 0, namesToX...); err != nil {
				return nil
			}
			// remove names less than name from current Y
			if err := nl.NodeDeleteBeforeExclusive(prevNodeReference, name); err != nil {
				return nil
			}

			// point skip list to current Y
			if err := nl.ItemAdd(lookupKey, prevNodeReference.ElementPointer); err != nil {
				return nil
			}
			return nil
		} else {
			// collect names after name, add them to Y
			namesToY, err := nl.NodeRangeAfterExclusive(prevNodeReference, name)
			if err != nil {
				return nil
			}
			// add namesToY and name to a new Y
			namesToY = append(namesToY, name)
			if err := nl.ItemAdd(lookupKey, 0, namesToY...); err != nil {
				return nil
			}
			// remove names after name from current X
			if err := nl.NodeDeleteAfterExclusive(prevNodeReference, name); err != nil {
				return nil
			}
			return nil
		}

	}

	// case 2.4
	if nextNode != nil {
		nodeSize := nl.NodeSize(nextNode.Reference())
		if nodeSize < nl.batchSize {
			if id, err := nl.skipList.DeleteByKey(nextNode.Key); err != nil {
				return err
			} else {
				if err := nl.ItemAdd(lookupKey, id, name); err != nil {
					return err
				}
			}
			return nil
		}
	}

	// case 2.5
	// now prevNode is nil
	return nl.ItemAdd(lookupKey, 0, name)
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
func (nl *ItemList) DeleteName(name string) error {
	lookupKey := []byte(name)
	prevNode, nextNode, found, err := nl.skipList.FindGreaterOrEqual(lookupKey)
	if err != nil {
		return err
	}

	// case 1
	if found && bytes.Compare(nextNode.Key, lookupKey) == 0 {
		if _, err := nl.skipList.DeleteByKey(nextNode.Key); err != nil {
			return err
		}
		if err := nl.NodeDeleteMember(nextNode.Reference(), name); err != nil {
			return err
		}
		minName := nl.NodeMin(nextNode.Reference())
		if minName == "" {
			return nl.NodeDelete(nextNode.Reference())
		}
		return nl.ItemAdd([]byte(minName), nextNode.Id)
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
	if !nl.NodeContainsItem(prevNode.Reference(), name) {
		return nil
	}

	// case 3
	if err := nl.NodeDeleteMember(prevNode.Reference(), name); err != nil {
		return err
	}
	prevSize := nl.NodeSize(prevNode.Reference())
	if prevSize == 0 {
		if _, err := nl.skipList.DeleteByKey(prevNode.Key); err != nil {
			return err
		}
		return nil
	}
	nextSize := nl.NodeSize(nextNode.Reference())
	if nextSize > 0 && prevSize+nextSize < nl.batchSize {
		// case 3.1 merge nextNode and prevNode
		if _, err := nl.skipList.DeleteByKey(nextNode.Key); err != nil {
			return err
		}
		nextNames, err := nl.NodeRangeBeforeExclusive(nextNode.Reference(), "")
		if err != nil {
			return err
		}
		if err := nl.NodeAddMember(prevNode.Reference(), nextNames...); err != nil {
			return err
		}
		return nl.NodeDelete(nextNode.Reference())
	} else {
		// case 3.2 update prevNode
		// no action to take
		return nil
	}

	return nil
}

func (nl *ItemList) ListNames(startFrom string, visitNamesFn func(name string) bool) error {
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
		if !nl.NodeScanInclusiveAfter(prevNode.Reference(), startFrom, visitNamesFn) {
			return nil
		}
	}

	for nextNode != nil {
		if !nl.NodeScanInclusiveAfter(nextNode.Reference(), startFrom, visitNamesFn) {
			return nil
		}
		nextNode, err = nl.skipList.LoadElement(nextNode.Next[0])
		if err != nil {
			return err
		}
	}

	return nil
}

func (nl *ItemList) RemoteAllListElement() error {

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
		if err := nl.NodeDelete(node.Reference()); err != nil {
			return err
		}
		nodeRef = node.Next[0]
	}
	return nil

}

func (nl *ItemList) NodeContainsItem(node *skiplist.SkipListElementReference, item string) bool {
	key := fmt.Sprintf("%s%dm", nl.prefix, node.ElementPointer)
	_, err := nl.client.ZScore(context.Background(), key, item).Result()
	if err == redis.Nil {
		return false
	}
	if err == nil {
		return true
	}
	return false
}

func (nl *ItemList) NodeSize(node *skiplist.SkipListElementReference) int {
	if node == nil {
		return 0
	}
	key := fmt.Sprintf("%s%dm", nl.prefix, node.ElementPointer)
	return int(nl.client.ZLexCount(context.Background(), key, "-", "+").Val())
}

func (nl *ItemList) NodeAddMember(node *skiplist.SkipListElementReference, names ...string) error {
	key := fmt.Sprintf("%s%dm", nl.prefix, node.ElementPointer)
	var members []redis.Z
	for _, name := range names {
		members = append(members, redis.Z{
			Score:  0,
			Member: name,
		})
	}
	return nl.client.ZAddNX(context.Background(), key, members...).Err()
}
func (nl *ItemList) NodeDeleteMember(node *skiplist.SkipListElementReference, name string) error {
	key := fmt.Sprintf("%s%dm", nl.prefix, node.ElementPointer)
	return nl.client.ZRem(context.Background(), key, name).Err()
}

func (nl *ItemList) NodeDelete(node *skiplist.SkipListElementReference) error {
	key := fmt.Sprintf("%s%dm", nl.prefix, node.ElementPointer)
	return nl.client.Del(context.Background(), key).Err()
}

func (nl *ItemList) NodeInnerPosition(node *skiplist.SkipListElementReference, name string) int {
	key := fmt.Sprintf("%s%dm", nl.prefix, node.ElementPointer)
	return int(nl.client.ZLexCount(context.Background(), key, "-", "("+name).Val())
}

func (nl *ItemList) NodeMin(node *skiplist.SkipListElementReference) string {
	key := fmt.Sprintf("%s%dm", nl.prefix, node.ElementPointer)
	slice := nl.client.ZRangeByLex(context.Background(), key, &redis.ZRangeBy{
		Min:    "-",
		Max:    "+",
		Offset: 0,
		Count:  1,
	}).Val()
	if len(slice) > 0 {
		s := slice[0]
		return s
	}
	return ""
}

func (nl *ItemList) NodeScanInclusiveAfter(node *skiplist.SkipListElementReference, startFrom string, visitNamesFn func(name string) bool) bool {
	key := fmt.Sprintf("%s%dm", nl.prefix, node.ElementPointer)
	if startFrom == "" {
		startFrom = "-"
	} else {
		startFrom = "[" + startFrom
	}
	names := nl.client.ZRangeByLex(context.Background(), key, &redis.ZRangeBy{
		Min: startFrom,
		Max: "+",
	}).Val()
	for _, n := range names {
		if !visitNamesFn(n) {
			return false
		}
	}
	return true
}

func (nl *ItemList) NodeRangeBeforeExclusive(node *skiplist.SkipListElementReference, stopAt string) ([]string, error) {
	key := fmt.Sprintf("%s%dm", nl.prefix, node.ElementPointer)
	if stopAt == "" {
		stopAt = "+"
	} else {
		stopAt = "(" + stopAt
	}
	return nl.client.ZRangeByLex(context.Background(), key, &redis.ZRangeBy{
		Min: "-",
		Max: stopAt,
	}).Result()
}
func (nl *ItemList) NodeRangeAfterExclusive(node *skiplist.SkipListElementReference, startFrom string) ([]string, error) {
	key := fmt.Sprintf("%s%dm", nl.prefix, node.ElementPointer)
	if startFrom == "" {
		startFrom = "-"
	} else {
		startFrom = "(" + startFrom
	}
	return nl.client.ZRangeByLex(context.Background(), key, &redis.ZRangeBy{
		Min: startFrom,
		Max: "+",
	}).Result()
}

func (nl *ItemList) NodeDeleteBeforeExclusive(node *skiplist.SkipListElementReference, stopAt string) error {
	key := fmt.Sprintf("%s%dm", nl.prefix, node.ElementPointer)
	if stopAt == "" {
		stopAt = "+"
	} else {
		stopAt = "(" + stopAt
	}
	return nl.client.ZRemRangeByLex(context.Background(), key, "-", stopAt).Err()
}
func (nl *ItemList) NodeDeleteAfterExclusive(node *skiplist.SkipListElementReference, startFrom string) error {
	key := fmt.Sprintf("%s%dm", nl.prefix, node.ElementPointer)
	if startFrom == "" {
		startFrom = "-"
	} else {
		startFrom = "(" + startFrom
	}
	return nl.client.ZRemRangeByLex(context.Background(), key, startFrom, "+").Err()
}

func (nl *ItemList) ItemAdd(lookupKey []byte, idIfKnown int64, names ...string) error {
	if id, err := nl.skipList.InsertByKey(lookupKey, idIfKnown, nil); err != nil {
		return err
	} else {
		if len(names) > 0 {
			return nl.NodeAddMember(&skiplist.SkipListElementReference{
				ElementPointer: id,
				Key:            lookupKey,
			}, names...)
		}
	}
	return nil
}

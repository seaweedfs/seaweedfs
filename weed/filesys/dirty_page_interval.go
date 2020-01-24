package filesys

import (
	"bytes"
	"io"
	"math"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

type IntervalNode struct {
	Data   []byte
	Offset int64
	Size   int64
	Next   *IntervalNode
}

type IntervalLinkedList struct {
	Head *IntervalNode
	Tail *IntervalNode
}

type ContinuousIntervals struct {
	lists []*IntervalLinkedList
}

func (list *IntervalLinkedList) Offset() int64 {
	return list.Head.Offset
}
func (list *IntervalLinkedList) Size() int64 {
	return list.Tail.Offset + list.Tail.Size - list.Head.Offset
}
func (list *IntervalLinkedList) addNodeToTail(node *IntervalNode) {
	// glog.V(0).Infof("add to tail [%d,%d) + [%d,%d) => [%d,%d)", list.Head.Offset, list.Tail.Offset+list.Tail.Size, node.Offset, node.Offset+node.Size, list.Head.Offset, node.Offset+node.Size)
	list.Tail.Next = node
	list.Tail = node
}
func (list *IntervalLinkedList) addNodeToHead(node *IntervalNode) {
	// glog.V(0).Infof("add to head [%d,%d) + [%d,%d) => [%d,%d)", node.Offset, node.Offset+node.Size, list.Head.Offset, list.Tail.Offset+list.Tail.Size, node.Offset, list.Tail.Offset+list.Tail.Size)
	node.Next = list.Head
	list.Head = node
}

func (list *IntervalLinkedList) ReadData(buf []byte, start, stop int64) {
	t := list.Head
	for {

		nodeStart, nodeStop := max(start, t.Offset), min(stop, t.Offset+t.Size)
		if nodeStart < nodeStop {
			// glog.V(0).Infof("copying start=%d stop=%d t=[%d,%d) t.data=%d => bufSize=%d nodeStart=%d, nodeStop=%d", start, stop, t.Offset, t.Offset+t.Size, len(t.Data), len(buf), nodeStart, nodeStop)
			copy(buf[nodeStart-start:], t.Data[nodeStart-t.Offset:nodeStop-t.Offset])
		}

		if t.Next == nil {
			break
		}
		t = t.Next
	}
}

func (c *ContinuousIntervals) TotalSize() (total int64) {
	for _, list := range c.lists {
		total += list.Size()
	}
	return
}

func (c *ContinuousIntervals) AddInterval(data []byte, offset int64) (hasOverlap bool) {
	interval := &IntervalNode{Data: data, Offset: offset, Size: int64(len(data))}

	var prevList, nextList *IntervalLinkedList

	for _, list := range c.lists {
		if list.Head.Offset == interval.Offset+interval.Size {
			nextList = list
			break
		}
	}

	for _, list := range c.lists {
		if list.Head.Offset+list.Size() == offset {
			list.addNodeToTail(interval)
			prevList = list
			break
		}
		if list.Head.Offset <= offset && offset < list.Head.Offset+list.Size() {
			if list.Tail.Offset <= offset {
				dataStartIndex := list.Tail.Offset + list.Tail.Size - offset
				// glog.V(4).Infof("overlap data new [0,%d) same=%v", dataStartIndex, bytes.Compare(interval.Data[0:dataStartIndex], list.Tail.Data[len(list.Tail.Data)-int(dataStartIndex):]))
				interval.Data = interval.Data[dataStartIndex:]
				interval.Size -= dataStartIndex
				interval.Offset = offset + dataStartIndex
				// glog.V(4).Infof("overlapping append as [%d,%d) dataSize=%d", interval.Offset, interval.Offset+interval.Size, len(interval.Data))
				list.addNodeToTail(interval)
				prevList = list
				break
			}
			glog.V(4).Infof("overlapped! interval is [%d,%d) dataSize=%d", interval.Offset, interval.Offset+interval.Size, len(interval.Data))
			hasOverlap = true
			return
		}
	}

	if prevList != nil && nextList != nil {
		// glog.V(4).Infof("connecting [%d,%d) + [%d,%d) => [%d,%d)", prevList.Head.Offset, prevList.Tail.Offset+prevList.Tail.Size, nextList.Head.Offset, nextList.Tail.Offset+nextList.Tail.Size, prevList.Head.Offset, nextList.Tail.Offset+nextList.Tail.Size)
		prevList.Tail.Next = nextList.Head
		prevList.Tail = nextList.Tail
		c.removeList(nextList)
	} else if nextList != nil {
		// add to head was not done when checking
		nextList.addNodeToHead(interval)
	}
	if prevList == nil && nextList == nil {
		c.lists = append(c.lists, &IntervalLinkedList{
			Head: interval,
			Tail: interval,
		})
	}

	return
}

func (c *ContinuousIntervals) RemoveLargestIntervalLinkedList() *IntervalLinkedList {
	var maxSize int64
	maxIndex := -1
	for k, list := range c.lists {
		if maxSize <= list.Size() {
			maxSize = list.Size()
			maxIndex = k
		}
	}
	if maxSize <= 0 {
		return nil
	}

	t := c.lists[maxIndex]
	c.lists = append(c.lists[0:maxIndex], c.lists[maxIndex+1:]...)
	return t

}

func (c *ContinuousIntervals) removeList(target *IntervalLinkedList) {
	index := -1
	for k, list := range c.lists {
		if list.Offset() == target.Offset() {
			index = k
		}
	}
	if index < 0 {
		return
	}

	c.lists = append(c.lists[0:index], c.lists[index+1:]...)

}

func (c *ContinuousIntervals) ReadData(data []byte, startOffset int64) (offset int64, size int) {
	var minOffset int64 = math.MaxInt64
	var maxStop int64
	for _, list := range c.lists {
		start := max(startOffset, list.Offset())
		stop := min(startOffset+int64(len(data)), list.Offset()+list.Size())
		if start <= stop {
			list.ReadData(data[start-startOffset:], start, stop)
			minOffset = min(minOffset, start)
			maxStop = max(maxStop, stop)
		}
	}

	if minOffset == math.MaxInt64 {
		return 0, 0
	}

	offset = minOffset
	size = int(maxStop - offset)
	return
}

func (l *IntervalLinkedList) ToReader() io.Reader {
	var readers []io.Reader
	t := l.Head
	readers = append(readers, bytes.NewReader(t.Data))
	for t.Next != nil {
		t = t.Next
		readers = append(readers, bytes.NewReader(t.Data))
	}
	return io.MultiReader(readers...)
}

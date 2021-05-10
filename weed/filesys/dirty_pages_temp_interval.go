package filesys

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"io"
	"os"
)

type WrittenIntervalNode struct {
	DataOffset int64
	TempOffset int64
	Size       int64
	Next       *WrittenIntervalNode
}

type WrittenIntervalLinkedList struct {
	tempFile *os.File
	Head     *WrittenIntervalNode
	Tail     *WrittenIntervalNode
}

type WrittenContinuousIntervals struct {
	tempFile *os.File
	lists    []*WrittenIntervalLinkedList
}

func (list *WrittenIntervalLinkedList) Offset() int64 {
	return list.Head.DataOffset
}
func (list *WrittenIntervalLinkedList) Size() int64 {
	return list.Tail.DataOffset + list.Tail.Size - list.Head.DataOffset
}
func (list *WrittenIntervalLinkedList) addNodeToTail(node *WrittenIntervalNode) {
	// glog.V(4).Infof("add to tail [%d,%d) + [%d,%d) => [%d,%d)", list.Head.Offset, list.Tail.Offset+list.Tail.Size, node.Offset, node.Offset+node.Size, list.Head.Offset, node.Offset+node.Size)
	if list.Tail.TempOffset+list.Tail.Size == node.TempOffset {
		// already connected
		list.Tail.Size += node.Size
	} else {
		list.Tail.Next = node
		list.Tail = node
	}
}
func (list *WrittenIntervalLinkedList) addNodeToHead(node *WrittenIntervalNode) {
	// glog.V(4).Infof("add to head [%d,%d) + [%d,%d) => [%d,%d)", node.Offset, node.Offset+node.Size, list.Head.Offset, list.Tail.Offset+list.Tail.Size, node.Offset, list.Tail.Offset+list.Tail.Size)
	node.Next = list.Head
	list.Head = node
}

func (list *WrittenIntervalLinkedList) ReadData(buf []byte, start, stop int64) {
	t := list.Head
	for {

		nodeStart, nodeStop := max(start, t.DataOffset), min(stop, t.DataOffset+t.Size)
		if nodeStart < nodeStop {
			// glog.V(0).Infof("copying start=%d stop=%d t=[%d,%d) t.data=%d => bufSize=%d nodeStart=%d, nodeStop=%d", start, stop, t.Offset, t.Offset+t.Size, len(t.Data), len(buf), nodeStart, nodeStop)
			list.tempFile.ReadAt(buf[nodeStart-start:nodeStop-start], nodeStart-t.TempOffset)
		}

		if t.Next == nil {
			break
		}
		t = t.Next
	}
}

func (c *WrittenContinuousIntervals) TotalSize() (total int64) {
	for _, list := range c.lists {
		total += list.Size()
	}
	return
}

func (list *WrittenIntervalLinkedList) subList(start, stop int64) *WrittenIntervalLinkedList {
	var nodes []*WrittenIntervalNode
	for t := list.Head; t != nil; t = t.Next {
		nodeStart, nodeStop := max(start, t.DataOffset), min(stop, t.DataOffset+t.Size)
		if nodeStart >= nodeStop {
			// skip non overlapping WrittenIntervalNode
			continue
		}
		nodes = append(nodes, &WrittenIntervalNode{
			TempOffset: t.TempOffset + nodeStart - t.DataOffset,
			DataOffset: nodeStart,
			Size:       nodeStop - nodeStart,
			Next:       nil,
		})
	}
	for i := 1; i < len(nodes); i++ {
		nodes[i-1].Next = nodes[i]
	}
	return &WrittenIntervalLinkedList{
		tempFile: list.tempFile,
		Head:     nodes[0],
		Tail:     nodes[len(nodes)-1],
	}
}

func (c *WrittenContinuousIntervals) AddInterval(tempOffset int64, dataSize int, dataOffset int64) {

	interval := &WrittenIntervalNode{DataOffset: dataOffset, TempOffset: tempOffset, Size: int64(dataSize)}

	// append to the tail and return
	if len(c.lists) == 1 {
		lastSpan := c.lists[0]
		if lastSpan.Tail.DataOffset+lastSpan.Tail.Size == dataOffset {
			lastSpan.addNodeToTail(interval)
			return
		}
	}

	var newLists []*WrittenIntervalLinkedList
	for _, list := range c.lists {
		// if list is to the left of new interval, add to the new list
		if list.Tail.DataOffset+list.Tail.Size <= interval.DataOffset {
			newLists = append(newLists, list)
		}
		// if list is to the right of new interval, add to the new list
		if interval.DataOffset+interval.Size <= list.Head.DataOffset {
			newLists = append(newLists, list)
		}
		// if new interval overwrite the right part of the list
		if list.Head.DataOffset < interval.DataOffset && interval.DataOffset < list.Tail.DataOffset+list.Tail.Size {
			// create a new list of the left part of existing list
			newLists = append(newLists, list.subList(list.Offset(), interval.DataOffset))
		}
		// if new interval overwrite the left part of the list
		if list.Head.DataOffset < interval.DataOffset+interval.Size && interval.DataOffset+interval.Size < list.Tail.DataOffset+list.Tail.Size {
			// create a new list of the right part of existing list
			newLists = append(newLists, list.subList(interval.DataOffset+interval.Size, list.Tail.DataOffset+list.Tail.Size))
		}
		// skip anything that is fully overwritten by the new interval
	}

	c.lists = newLists
	// add the new interval to the lists, connecting neighbor lists
	var prevList, nextList *WrittenIntervalLinkedList

	for _, list := range c.lists {
		if list.Head.DataOffset == interval.DataOffset+interval.Size {
			nextList = list
			break
		}
	}

	for _, list := range c.lists {
		if list.Head.DataOffset+list.Size() == dataOffset {
			list.addNodeToTail(interval)
			prevList = list
			break
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
		c.lists = append(c.lists, &WrittenIntervalLinkedList{
			tempFile: c.tempFile,
			Head:     interval,
			Tail:     interval,
		})
	}

	return
}

func (c *WrittenContinuousIntervals) RemoveLargestIntervalLinkedList() *WrittenIntervalLinkedList {
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
	t.tempFile = c.tempFile
	c.lists = append(c.lists[0:maxIndex], c.lists[maxIndex+1:]...)
	return t

}

func (c *WrittenContinuousIntervals) removeList(target *WrittenIntervalLinkedList) {
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

func (c *WrittenContinuousIntervals) ReadDataAt(data []byte, startOffset int64) (maxStop int64) {
	for _, list := range c.lists {
		start := max(startOffset, list.Offset())
		stop := min(startOffset+int64(len(data)), list.Offset()+list.Size())
		if start < stop {
			list.ReadData(data[start-startOffset:], start, stop)
			maxStop = max(maxStop, stop)
		}
	}
	return
}

func (l *WrittenIntervalLinkedList) ToReader(start int64, stop int64) io.Reader {
	// TODO: optimize this to avoid another loop
	var readers []io.Reader
	for t := l.Head; ; t = t.Next {
		startOffset, stopOffset := max(t.DataOffset, start), min(t.DataOffset+t.Size, stop)
		if startOffset < stopOffset {
			readers = append(readers, newFileSectionReader(l.tempFile, startOffset-t.DataOffset+t.TempOffset, startOffset, stopOffset-startOffset))
		}
		if t.Next == nil {
			break
		}
	}
	if len(readers) == 1 {
		return readers[0]
	}
	return io.MultiReader(readers...)
}

type FileSectionReader struct {
	file      *os.File
	Offset    int64
	dataStart int64
	dataStop  int64
}

var _ = io.Reader(&FileSectionReader{})

func newFileSectionReader(tempfile *os.File, offset int64, dataOffset int64, size int64) *FileSectionReader {
	return &FileSectionReader{
		file:      tempfile,
		Offset:    offset,
		dataStart: dataOffset,
		dataStop:  dataOffset + size,
	}
}

func (f *FileSectionReader) Read(p []byte) (n int, err error) {
	dataLen := min(f.dataStop-f.Offset, int64(len(p)))
	if dataLen < 0 {
		return 0, io.EOF
	}
	glog.V(4).Infof("reading %v [%d,%d)", f.file.Name(), f.Offset, f.Offset+dataLen)
	n, err = f.file.ReadAt(p[:dataLen], f.Offset)
	if n > 0 {
		f.Offset += int64(n)
	} else {
		err = io.EOF
	}
	return
}

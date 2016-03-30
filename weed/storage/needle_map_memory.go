package storage

import (
	"io"
	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

type NeedleMap struct {
	m *CompactMap

	baseNeedleMapper
}

func NewNeedleMap(file *os.File) *NeedleMap {
	nm := &NeedleMap{
		m: NewCompactMap(),
	}
	nm.indexFile = file
	return nm
}

const (
	RowsToRead = 1024
)

func LoadNeedleMap(file *os.File) (*NeedleMap, error) {
	nm := NewNeedleMap(file)
	e := WalkIndexFile(file, func(key uint64, offset, size uint32) error {
		if key > nm.maximumFileKey {
			nm.maximumFileKey = key
		}
		nm.fileCounter++
		nm.fileByteCounter = nm.fileByteCounter + uint64(size)
		if offset > 0 {
			oldSize := nm.m.Set(Key(key), offset, size)
			glog.V(3).Infoln("reading key", key, "offset", offset*NeedlePaddingSize, "size", size, "oldSize", oldSize)
			if oldSize > 0 {
				nm.deletionCounter++
				nm.deletionByteCounter = nm.deletionByteCounter + uint64(oldSize)
			}
		} else {
			oldSize := nm.m.Delete(Key(key))
			glog.V(3).Infoln("removing key", key, "offset", offset*NeedlePaddingSize, "size", size, "oldSize", oldSize)
			nm.deletionCounter++
			nm.deletionByteCounter = nm.deletionByteCounter + uint64(oldSize)
		}
		return nil
	})
	glog.V(1).Infoln("max file key:", nm.maximumFileKey)
	return nm, e
}

// walks through the index file, calls fn function with each key, offset, size
// stops with the error returned by the fn function
func WalkIndexFile(r *os.File, fn func(key uint64, offset, size uint32) error) error {
	var readerOffset int64
	bytes := make([]byte, 16*RowsToRead)
	count, e := r.ReadAt(bytes, readerOffset)
	glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
	readerOffset += int64(count)
	var (
		key          uint64
		offset, size uint32
		i            int
	)

	for count > 0 && e == nil || e == io.EOF {
		for i = 0; i+16 <= count; i += 16 {
			key, offset, size = idxFileEntry(bytes[i : i+16])
			if e = fn(key, offset, size); e != nil {
				return e
			}
		}
		if e == io.EOF {
			return nil
		}
		count, e = r.ReadAt(bytes, readerOffset)
		glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
		readerOffset += int64(count)
	}
	return e
}

func (nm *NeedleMap) Put(key uint64, offset uint32, size uint32) error {
	oldSize := nm.m.Set(Key(key), offset, size)
	nm.logPut(key, oldSize, size)
	return nm.appendToIndexFile(key, offset, size)
}
func (nm *NeedleMap) Get(key uint64) (element *NeedleValue, ok bool) {
	element, ok = nm.m.Get(Key(key))
	return
}
func (nm *NeedleMap) Delete(key uint64) error {
	deletedBytes := nm.m.Delete(Key(key))
	nm.logDelete(deletedBytes)
	return nm.appendToIndexFile(key, 0, 0)
}
func (nm *NeedleMap) Close() {
	nm.mutex.Lock()
	nm.indexFile.Close()
	nm.mutex.Unlock()
}
func (nm *NeedleMap) Destroy() error {
	nm.Close()
	return os.Remove(nm.indexFile.Name())
}

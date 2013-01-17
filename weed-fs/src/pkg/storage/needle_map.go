package storage

import (
	"io"
	"log"
	"os"
	"pkg/util"
)

type NeedleMap struct {
	indexFile *os.File
	m         MapGetSetter // modifiable map
	fm        MapGetter    // frozen map

	//transient
	bytes []byte

	deletionCounter     int
	fileCounter         int
	deletionByteCounter uint64
	fileByteCounter     uint64
}

// Map interface for frozen maps
type MapGetter interface {
	Get(key Key) (element *NeedleValue, ok bool)
	Walk(pedestrian func(*NeedleValue) error) error
}

// Modifiable map interface
type MapSetter interface {
	Set(key Key, offset, size uint32) (oldsize uint32)
	Delete(key Key) uint32
}

// Settable and gettable map
type MapGetSetter interface {
	MapGetter
	MapSetter
}

// New in-memory needle map, backed by "file" index file
func NewNeedleMap(file *os.File) *NeedleMap {
	return &NeedleMap{
		m:         NewCompactMap(),
		bytes:     make([]byte, 16),
		indexFile: file,
	}
}

// Nes frozen (on-disk, not modifiable(!)) needle map
func NewFrozenNeedleMap(file *os.File) (*NeedleMap, error) {
	fm, err := NewCdbMapFromIndex(file)
	if err != nil {
		return nil, err
	}
	return &NeedleMap{
		fm:    fm,
		bytes: make([]byte, 16),
	}, nil
}

const (
	RowsToRead = 1024
)

func LoadNeedleMap(file *os.File) (*NeedleMap, error) {
	nm := NewNeedleMap(file)

	var (
		key                   uint64
		offset, size, oldSize uint32
	)
	iterFun := func(buf []byte) error {
		key = util.BytesToUint64(buf[:8])
		offset = util.BytesToUint32(buf[8:12])
		size = util.BytesToUint32(buf[12:16])
		nm.fileCounter++
		nm.fileByteCounter = nm.fileByteCounter + uint64(size)
		if offset > 0 {
			oldSize = nm.m.Set(Key(key), offset, size)
			//log.Println("reading key", key, "offset", offset, "size", size, "oldSize", oldSize)
			if oldSize > 0 {
				nm.deletionCounter++
				nm.deletionByteCounter = nm.deletionByteCounter + uint64(oldSize)
			}
		} else {
			nm.m.Delete(Key(key))
			//log.Println("removing key", key)
			nm.deletionCounter++
			nm.deletionByteCounter = nm.deletionByteCounter + uint64(size)
		}

		return nil
	}
	if err := readIndexFile(file, iterFun); err != nil {
		return nil, err
	}
	return nm, nil
}

// calls iterFun with each row (raw 16 bytes)
func readIndexFile(indexFile *os.File, iterFun func([]byte) error) error {
	buf := make([]byte, 16*RowsToRead)
	count, e := io.ReadAtLeast(indexFile, buf, 16)
	if e != nil && count > 0 {
		fstat, err := indexFile.Stat()
		if err != nil {
			log.Println("ERROR stating %s: %s", indexFile, err)
		} else {
			log.Println("Loading index file", fstat.Name(), "size", fstat.Size())
		}
	}
	for count > 0 && e == nil {
		for i := 0; i < count; i += 16 {
			if e = iterFun(buf[i : i+16]); e != nil {
				return e
			}
		}

		count, e = io.ReadAtLeast(indexFile, buf, 16)
	}
	return nil
}

func (nm *NeedleMap) Put(key uint64, offset uint32, size uint32) (int, error) {
	oldSize := nm.m.Set(Key(key), offset, size)
	util.Uint64toBytes(nm.bytes[0:8], key)
	util.Uint32toBytes(nm.bytes[8:12], offset)
	util.Uint32toBytes(nm.bytes[12:16], size)
	nm.fileCounter++
	nm.fileByteCounter = nm.fileByteCounter + uint64(size)
	if oldSize > 0 {
		nm.deletionCounter++
		nm.deletionByteCounter = nm.deletionByteCounter + uint64(oldSize)
	}
	return nm.indexFile.Write(nm.bytes)
}
func (nm *NeedleMap) Get(key uint64) (element *NeedleValue, ok bool) {
	element, ok = nm.m.Get(Key(key))
	return
}
func (nm *NeedleMap) Delete(key uint64) {
	nm.deletionByteCounter = nm.deletionByteCounter + uint64(nm.m.Delete(Key(key)))
	util.Uint64toBytes(nm.bytes[0:8], key)
	util.Uint32toBytes(nm.bytes[8:12], 0)
	util.Uint32toBytes(nm.bytes[12:16], 0)
	nm.indexFile.Write(nm.bytes)
	nm.deletionCounter++
}
func (nm *NeedleMap) Close() {
	nm.indexFile.Close()
}
func (nm *NeedleMap) ContentSize() uint64 {
	return nm.fileByteCounter
}

// iterate through all needles using the iterator function
func (nm *NeedleMap) Walk(pedestrian func(*NeedleValue) error) (err error) {
	return nm.m.Walk(pedestrian)
}

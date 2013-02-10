package storage

import (
	//"log"
	"os"
	"weed/util"
)

type NeedleMap struct {
	indexFile *os.File
	m         CompactMap

	//transient
	bytes []byte

	deletionCounter     int
	fileCounter         int
	deletionByteCounter uint64
	fileByteCounter     uint64
}

func NewNeedleMap(file *os.File) *NeedleMap {
	nm := &NeedleMap{
		m:         NewCompactMap(),
		bytes:     make([]byte, 16),
		indexFile: file,
	}
	return nm
}

const (
	RowsToRead = 1024
)

func LoadNeedleMap(file *os.File) *NeedleMap {
	nm := NewNeedleMap(file)
	bytes := make([]byte, 16*RowsToRead)
	count, e := nm.indexFile.Read(bytes)
	for count > 0 && e == nil {
		for i := 0; i < count; i += 16 {
			key := util.BytesToUint64(bytes[i : i+8])
			offset := util.BytesToUint32(bytes[i+8 : i+12])
			size := util.BytesToUint32(bytes[i+12 : i+16])
			nm.fileCounter++
			nm.fileByteCounter = nm.fileByteCounter + uint64(size)
			if offset > 0 {
				oldSize := nm.m.Set(Key(key), offset, size)
				//log.Println("reading key", key, "offset", offset, "size", size, "oldSize", oldSize)
				if oldSize > 0 {
					nm.deletionCounter++
					nm.deletionByteCounter = nm.deletionByteCounter + uint64(oldSize)
				}
			} else {
				oldSize := nm.m.Delete(Key(key))
				//log.Println("removing key", key, "offset", offset, "size", size, "oldSize", oldSize)
				nm.deletionCounter++
				nm.deletionByteCounter = nm.deletionByteCounter + uint64(oldSize)
			}
		}

		count, e = nm.indexFile.Read(bytes)
	}
	return nm
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
func (nm *NeedleMap) Visit(visit func(NeedleValue) error) (err error) {
	return nm.m.Visit(visit)
}

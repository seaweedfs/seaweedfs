package storage

import (
	"log"
	"os"
	"pkg/util"
)

type NeedleMap struct {
	indexFile *os.File
	m         CompactMap

	//transient
	bytes           []byte
	deletionCounter int
	fileCounter     int
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
	if count > 0 {
		fstat, _ := file.Stat()
		log.Println("Loading index file", fstat.Name(), "size", fstat.Size())
	}
	for count > 0 && e == nil {
		for i := 0; i < count; i += 16 {
			key := util.BytesToUint64(bytes[i : i+8])
			offset := util.BytesToUint32(bytes[i+8 : i+12])
			size := util.BytesToUint32(bytes[i+12 : i+16])
			if offset > 0 {
				nm.m.Set(Key(key), offset, size)
				nm.fileCounter++
			} else {
				nm.m.Delete(Key(key))
				nm.deletionCounter++
			}
		}

		count, e = nm.indexFile.Read(bytes)
	}
	return nm
}

func (nm *NeedleMap) Put(key uint64, offset uint32, size uint32) (int, error) {
	nm.m.Set(Key(key), offset, size)
	util.Uint64toBytes(nm.bytes[0:8], key)
	util.Uint32toBytes(nm.bytes[8:12], offset)
	util.Uint32toBytes(nm.bytes[12:16], size)
	nm.fileCounter++
	return nm.indexFile.Write(nm.bytes)
}
func (nm *NeedleMap) Get(key uint64) (element *NeedleValue, ok bool) {
	element, ok = nm.m.Get(Key(key))
	return
}
func (nm *NeedleMap) Delete(key uint64) {
	nm.m.Delete(Key(key))
	util.Uint64toBytes(nm.bytes[0:8], key)
	util.Uint32toBytes(nm.bytes[8:12], 0)
	util.Uint32toBytes(nm.bytes[12:16], 0)
	nm.indexFile.Write(nm.bytes)
	nm.deletionCounter++
}
func (nm *NeedleMap) Close() {
	nm.indexFile.Close()
}

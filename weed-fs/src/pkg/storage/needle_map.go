package storage

import (
	"log"
	"os"
	. "util"
)

type NeedleValue struct {
	Offset uint32 "Volume offset" //since aligned to 8 bytes, range is 4G*8=32G
	Size   uint32 "Size of the data portion"
}

type NeedleMap struct {
	indexFile *os.File
	m         map[uint64]*NeedleValue //mapping needle key(uint64) to NeedleValue
	bytes     []byte
}

func NewNeedleMap(file *os.File) *NeedleMap {
	nm := &NeedleMap{
		m:         make(map[uint64]*NeedleValue),
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
		log.Println("Loading index file", fstat.Name, "size", fstat.Size)
	}
	for count > 0 && e == nil {
		for i := 0; i < count; i += 16 {
			key := BytesToUint64(bytes[i : i+8])
			offset := BytesToUint32(bytes[i+8 : i+12])
			size := BytesToUint32(bytes[i+12 : i+16])
			if offset > 0 {
				nm.m[key] = &NeedleValue{Offset: offset, Size: size}
			}
		}
		count, e = nm.indexFile.Read(bytes)
	}
	return nm
}
func (nm *NeedleMap) Put(key uint64, offset uint32, size uint32) (int, os.Error) {
    nm.m[key] = &NeedleValue{Offset: offset, Size: size}
	Uint64toBytes(nm.bytes[0:8], key)
	Uint32toBytes(nm.bytes[8:12], offset)
	Uint32toBytes(nm.bytes[12:16], size)
	return nm.indexFile.Write(nm.bytes)
}
func (nm *NeedleMap) Get(key uint64) (element *NeedleValue, ok bool) {
	element, ok = nm.m[key]
	return
}
func (nm *NeedleMap) Delete(key uint64) {
	nm.m[key] = nil, false
	Uint64toBytes(nm.bytes[0:8], key)
	Uint32toBytes(nm.bytes[8:12], 0)
	Uint32toBytes(nm.bytes[12:16], 0)
	nm.indexFile.Write(nm.bytes)
}
func (nm *NeedleMap) Close() {
	nm.indexFile.Close()
}

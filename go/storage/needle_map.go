package storage

import (
	"bufio"
	"code.google.com/p/weed-fs/go/util"
	"fmt"
	"io"
	"os"
)

type NeedleMapper interface {
	Put(key uint64, offset uint32, size uint32) (int, error)
	Get(key uint64) (element *NeedleValue, ok bool)
	Delete(key uint64) error
	Close()
	ContentSize() uint64
	DeletedSize() uint64
	FileCount() int
	DeletedCount() int
	Visit(visit func(NeedleValue) error) (err error)
}

type mapMetric struct {
	DeletionCounter     int    `json:"DeletionCounter"`
	FileCounter         int    `json:"FileCounter"`
	DeletionByteCounter uint64 `json:"DeletionByteCounter"`
	FileByteCounter     uint64 `json:"FileByteCounter"`
}

type NeedleMap struct {
	indexFile *os.File
	m         CompactMap

	//transient
	bytes []byte

	mapMetric
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

func LoadNeedleMap(file *os.File) (*NeedleMap, error) {
	nm := NewNeedleMap(file)
  bufferReader := bufio.NewReaderSize(nm.indexFile, 1024*1024)
	bytes := make([]byte, 16*RowsToRead)
	count, e := bufferReader.Read(bytes)
	for count > 0 && e == nil {
		for i := 0; i < count; i += 16 {
			key := util.BytesToUint64(bytes[i : i+8])
			offset := util.BytesToUint32(bytes[i+8 : i+12])
			size := util.BytesToUint32(bytes[i+12 : i+16])
			nm.FileCounter++
			nm.FileByteCounter = nm.FileByteCounter + uint64(size)
			if offset > 0 {
				oldSize := nm.m.Set(Key(key), offset, size)
				//log.Println("reading key", key, "offset", offset, "size", size, "oldSize", oldSize)
				if oldSize > 0 {
					nm.DeletionCounter++
					nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
				}
			} else {
				oldSize := nm.m.Delete(Key(key))
				//log.Println("removing key", key, "offset", offset, "size", size, "oldSize", oldSize)
				nm.DeletionCounter++
				nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
			}
		}

		count, e = bufferReader.Read(bytes)
	}
	if e == io.EOF {
		e = nil
	}
	return nm, e
}

func (nm *NeedleMap) Put(key uint64, offset uint32, size uint32) (int, error) {
	oldSize := nm.m.Set(Key(key), offset, size)
	util.Uint64toBytes(nm.bytes[0:8], key)
	util.Uint32toBytes(nm.bytes[8:12], offset)
	util.Uint32toBytes(nm.bytes[12:16], size)
	nm.FileCounter++
	nm.FileByteCounter = nm.FileByteCounter + uint64(size)
	if oldSize > 0 {
		nm.DeletionCounter++
		nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
	}
	return nm.indexFile.Write(nm.bytes)
}
func (nm *NeedleMap) Get(key uint64) (element *NeedleValue, ok bool) {
	element, ok = nm.m.Get(Key(key))
	return
}
func (nm *NeedleMap) Delete(key uint64) error {
	nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(nm.m.Delete(Key(key)))
	offset, err := nm.indexFile.Seek(0, 1)
	if err != nil {
		return fmt.Errorf("cannot get position of indexfile: %s", err)
	}
	util.Uint64toBytes(nm.bytes[0:8], key)
	util.Uint32toBytes(nm.bytes[8:12], 0)
	util.Uint32toBytes(nm.bytes[12:16], 0)
	if _, err = nm.indexFile.Write(nm.bytes); err != nil {
		plus := ""
		if e := nm.indexFile.Truncate(offset); e != nil {
			plus = "\ncouldn't truncate index file: " + e.Error()
		}
		return fmt.Errorf("error writing to indexfile %s: %s%s", nm.indexFile, err, plus)
	}
	nm.DeletionCounter++
	return nil
}
func (nm *NeedleMap) Close() {
	_ = nm.indexFile.Close()
}
func (nm NeedleMap) ContentSize() uint64 {
	return nm.FileByteCounter
}
func (nm NeedleMap) DeletedSize() uint64 {
	return nm.DeletionByteCounter
}
func (nm NeedleMap) FileCount() int {
	return nm.FileCounter
}
func (nm NeedleMap) DeletedCount() int {
	return nm.DeletionCounter
}
func (nm *NeedleMap) Visit(visit func(NeedleValue) error) (err error) {
	return nm.m.Visit(visit)
}
